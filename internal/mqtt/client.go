// Package mqtt provides MQTT client and connection pooling for publishing and ACK handling.
package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/ibs-source/syslog-consumer/internal/compress"
	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

// Client wraps a single paho MQTT connection.
type Client struct {
	client     mqtt.Client
	ackHandler atomic.Pointer[func(message.AckMessage)]
	log        *log.Logger

	publishTopic string
	ackTopic     string

	connectTimeout    time.Duration
	writeTimeout      time.Duration
	subscribeTimeout  time.Duration
	disconnectTimeout time.Duration
	connectRetryDelay time.Duration

	connected atomic.Bool
	qos       byte
}

// errNotConnected signals callers to back off and retry.
var errNotConnected = errors.New("mqtt: broker connection not open")

// NewClient prepares the paho options but does not establish the connection;
// call Connect afterwards.
func NewClient(ctx context.Context, cfg *config.MQTTConfig, logger *log.Logger) (*Client, error) {
	c := &Client{
		publishTopic:      cfg.PublishTopic,
		ackTopic:          cfg.AckTopic,
		qos:               cfg.QoS,
		connectTimeout:    cfg.ConnectTimeout,
		writeTimeout:      cfg.WriteTimeout,
		subscribeTimeout:  cfg.SubscribeTimeout,
		disconnectTimeout: cfg.DisconnectTimeout,
		connectRetryDelay: cfg.ConnectRetryDelay,
		log:               logger,
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.Broker)
	opts.SetClientID(cfg.ClientID)
	opts.SetConnectTimeout(cfg.ConnectTimeout)
	opts.SetWriteTimeout(cfg.WriteTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(cfg.MaxReconnectInterval)

	opts.SetKeepAlive(cfg.KeepAlive)
	opts.SetPingTimeout(cfg.PingTimeout)
	opts.SetMessageChannelDepth(cfg.MessageChannelDepth)
	opts.SetResumeSubs(true)
	opts.SetOrderMatters(false)
	opts.SetMaxResumePubInFlight(cfg.MaxResumePubInFlight)

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.connected.Store(false)
		if err != nil {
			logger.Errorf(ctx, "MQTT connection lost: %v", err)
		}
	})

	opts.SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
		logger.Infof(ctx, "MQTT reconnecting...")
	})

	opts.SetOnConnectHandler(func(mc mqtt.Client) {
		c.connected.Store(true)
		logger.Infof(ctx, "MQTT connected successfully")
		c.resubscribeAck(ctx, mc)
	})

	if cfg.TLSEnabled {
		tlsConfig, err := newTLSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
	}

	c.client = mqtt.NewClient(opts)
	return c, nil
}

// Connect retries with back-off until the broker responds or ctx is canceled.
func (c *Client) Connect(ctx context.Context) error {
	for attempt := 0; ; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tok := c.client.Connect()
		if ok := tok.WaitTimeout(c.connectTimeout); !ok {
			c.log.Errorf(ctx, "mqtt connect timeout, retrying (attempt %d)", attempt)
			if retrySleep(ctx, c.connectRetryDelay) {
				return ctx.Err()
			}
			continue
		}
		if err := tok.Error(); err != nil {
			c.log.Errorf(ctx, "mqtt connect failed, retrying (attempt %d): %v", attempt, err)
			if retrySleep(ctx, c.connectRetryDelay) {
				return ctx.Err()
			}
			continue
		}
		return nil
	}
}

func retrySleep(ctx context.Context, d time.Duration) (canceled bool) {
	timer := time.NewTimer(d)
	select {
	case <-timer.C:
		return false
	case <-ctx.Done():
		timer.Stop()
		return true
	}
}

func newTLSConfig(cfg *config.MQTTConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if cfg.InsecureSkip {
		tlsConfig.InsecureSkipVerify = true
	}

	if cfg.CACert != "" {
		caCert, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to parse CA cert")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if cfg.ClientCert != "" && cfg.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// Publish is fire-and-forget at QoS 0; for QoS >= 1 it waits for broker ack
// up to writeTimeout.
func (c *Client) Publish(ctx context.Context, payload []byte) error {
	if !c.connected.Load() {
		return errNotConnected
	}

	token := c.client.Publish(c.publishTopic, c.qos, false, payload)

	if c.qos == 0 {
		return nil
	}

	if !token.WaitTimeout(c.writeTimeout) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("mqtt publish timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt publish failed: %w", err)
	}
	return nil
}

// SubscribeAck registers handler; resubscribeAck restores it after reconnect.
func (c *Client) SubscribeAck(ctx context.Context, handler func(message.AckMessage)) error {
	c.ackHandler.Store(&handler)

	token := c.client.Subscribe(c.ackTopic, c.qos, func(_ mqtt.Client, msg mqtt.Message) {
		c.handleAckMessage(ctx, msg.Payload())
	})

	if !token.WaitTimeout(c.subscribeTimeout) {
		return errors.New("mqtt ack subscription timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to subscribe to ack topic: %w", err)
	}

	return nil
}

// ackDecompBufPool reuses decompression buffers; parseAck copies all strings
// out so recycling is safe.
var ackDecompBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

func (c *Client) handleAckMessage(ctx context.Context, payload []byte) {
	hp := c.ackHandler.Load()
	if hp == nil {
		return
	}
	handler := *hp

	if compress.IsCompressed(payload) {
		bufp, ok := ackDecompBufPool.Get().(*[]byte)
		if !ok || bufp == nil {
			b := make([]byte, 0, 4096)
			bufp = &b
		}
		decompressed, err := compress.Decompress(*bufp, payload)
		if err != nil {
			*bufp = decompressed[:0]
			ackDecompBufPool.Put(bufp)
			c.log.Debugf(ctx, "Ignoring ACK: zstd decompress failed: %v", err)
			return
		}
		payload = decompressed
		defer func() {
			*bufp = decompressed[:0]
			ackDecompBufPool.Put(bufp)
		}()
	}

	ack, err := parseAck(payload)
	if err != nil {
		c.log.Debugf(ctx, "Ignoring malformed ACK message: %v (payload length: %d)", err, len(payload))
		return
	}

	handler(ack)
}

// Close issues a paho Disconnect using disconnectTimeout as the grace period.
func (c *Client) Close() error {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(uint(max(c.disconnectTimeout.Milliseconds(), 0)))
	}
	return nil
}

// IsConnected mirrors the OnConnect/ConnectionLost handlers; cheaper than the
// paho client's own probe.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// resubscribeAck is a no-op on the very first connect when ackHandler is nil.
func (c *Client) resubscribeAck(ctx context.Context, mc mqtt.Client) {
	if c.ackHandler.Load() == nil {
		return
	}

	c.log.Infof(ctx, "Re-subscribing to ACK topic after reconnect")
	token := mc.Subscribe(c.ackTopic, c.qos, func(_ mqtt.Client, msg mqtt.Message) {
		c.handleAckMessage(ctx, msg.Payload())
	})
	if !token.WaitTimeout(c.subscribeTimeout) {
		c.log.Errorf(ctx, "Failed to re-subscribe to ACK topic: timeout")
		return
	}
	if err := token.Error(); err != nil {
		c.log.Errorf(ctx, "Failed to re-subscribe to ACK topic: %v", err)
	}
}
