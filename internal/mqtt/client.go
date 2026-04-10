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

// Client manages MQTT publishing and ACK subscription
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

	connected atomic.Bool // updated by OnConnect/ConnectionLost handlers
	qos       byte
}

// errNotConnected is returned by Publish when the TCP connection to the
// broker is not established. Callers should back off and retry.
var errNotConnected = errors.New("mqtt: broker connection not open")

// NewClient creates a new MQTT client
func NewClient(cfg *config.MQTTConfig, logger *log.Logger) (*Client, error) {
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

	// Performance optimizations.
	opts.SetKeepAlive(cfg.KeepAlive)
	opts.SetPingTimeout(cfg.PingTimeout)
	opts.SetMessageChannelDepth(cfg.MessageChannelDepth)
	opts.SetResumeSubs(true)
	opts.SetOrderMatters(false)
	opts.SetMaxResumePubInFlight(cfg.MaxResumePubInFlight)

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.connected.Store(false)
		if err != nil {
			logger.Error("MQTT connection lost: %v", err)
		}
	})

	opts.SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
		logger.Info("MQTT reconnecting...")
	})

	opts.SetOnConnectHandler(func(mc mqtt.Client) {
		c.connected.Store(true)
		logger.Info("MQTT connected successfully")
		c.resubscribeAck(mc)
	})

	// Configure TLS if enabled
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

// Connect establishes the MQTT connection, retrying with back-off until
// the broker responds or ctx is canceled.
func (c *Client) Connect(ctx context.Context) error {
	for attempt := 0; ; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tok := c.client.Connect()
		if ok := tok.WaitTimeout(c.connectTimeout); !ok {
			c.log.Error("mqtt connect timeout, retrying (attempt %d)", attempt)
			if retrySleep(ctx, c.connectRetryDelay) {
				return ctx.Err()
			}
			continue
		}
		if err := tok.Error(); err != nil {
			c.log.Error("mqtt connect failed, retrying (attempt %d): %v", attempt, err)
			if retrySleep(ctx, c.connectRetryDelay) {
				return ctx.Err()
			}
			continue
		}
		return nil
	}
}

// retrySleep waits for d or returns true if ctx is canceled.
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

// newTLSConfig creates a TLS configuration from MQTT config
func newTLSConfig(cfg *config.MQTTConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// InsecureSkipVerify is configurable for testing environments.
	// When enabled it weakens TLS security and should not be used in production.
	if cfg.InsecureSkip {
		tlsConfig.InsecureSkipVerify = true
	}

	// Load CA certificate if provided
	if cfg.CACert != "" {
		caCert, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate and key if provided
	if cfg.ClientCert != "" && cfg.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// Publish sends a message to the MQTT broker.
// For QoS 0, publish is fire-and-forget (no broker ACK expected).
func (c *Client) Publish(ctx context.Context, payload []byte) error {
	// Check cached connection state (updated by OnConnect/ConnectionLost handlers)
	// to avoid the overhead of checking the paho client on every publish.
	if !c.connected.Load() {
		return errNotConnected
	}

	token := c.client.Publish(c.publishTopic, c.qos, false, payload)

	// For QoS 0, the paho library enqueues the message and there is no
	// broker acknowledgement to wait for. Fire-and-forget.
	if c.qos == 0 {
		return nil
	}

	// For QoS >= 1, wait for broker acknowledgement with bounded timeout.
	// No background goroutine needed — WaitTimeout already provides the bound.
	if !token.WaitTimeout(c.writeTimeout) {
		// Check if context was canceled during the wait.
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("mqtt publish timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt publish failed: %w", err)
	}
	return nil
}

// SubscribeAck registers a callback for ACK messages.
// After a broker reconnect the OnConnect handler calls resubscribeAck
// to restore the subscription.
func (c *Client) SubscribeAck(handler func(message.AckMessage)) error {
	c.ackHandler.Store(&handler)

	// Subscribe to ACK topic
	token := c.client.Subscribe(c.ackTopic, c.qos, func(_ mqtt.Client, msg mqtt.Message) {
		c.handleAckMessage(msg.Payload())
	})

	if !token.WaitTimeout(c.subscribeTimeout) {
		return fmt.Errorf("mqtt ack subscription timeout")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("failed to subscribe to ack topic: %w", err)
	}

	return nil
}

// ackDecompBufPool reuses decompression buffers for incoming ACK payloads.
// parseAck copies all strings out of the buffer, so recycling is safe.
var ackDecompBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// handleAckMessage processes incoming ACK messages
func (c *Client) handleAckMessage(payload []byte) {
	hp := c.ackHandler.Load()
	if hp == nil {
		return
	}
	handler := *hp

	// Decompress if zstd-compressed.
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
			c.log.Debug("Ignoring ACK: zstd decompress failed: %v", err)
			return
		}
		payload = decompressed
		// Defer return of buffer: parseAck copies strings, so payload
		// memory is safe to recycle after handler() returns.
		defer func() {
			*bufp = decompressed[:0]
			ackDecompBufPool.Put(bufp)
		}()
	}

	// Parse ACK message
	ack, err := parseAck(payload)
	if err != nil {
		c.log.Debug("Ignoring malformed ACK message: %v (payload length: %d)", err, len(payload))
		return
	}

	handler(ack)
}

// Close disconnects from the MQTT broker
func (c *Client) Close() error {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(uint(max(c.disconnectTimeout.Milliseconds(), 0)))
	}
	return nil
}

// IsConnected reports whether the TCP connection to the broker is open.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// resubscribeAck re-subscribes to the ACK topic after a broker reconnect.
// Called from the paho OnConnect handler. On the very first connect the
// ackHandler is still nil, so the function returns immediately.
func (c *Client) resubscribeAck(mc mqtt.Client) {
	if c.ackHandler.Load() == nil {
		return
	}

	c.log.Info("Re-subscribing to ACK topic after reconnect")
	token := mc.Subscribe(c.ackTopic, c.qos, func(_ mqtt.Client, msg mqtt.Message) {
		c.handleAckMessage(msg.Payload())
	})
	if !token.WaitTimeout(c.subscribeTimeout) {
		c.log.Error("Failed to re-subscribe to ACK topic: timeout")
		return
	}
	if err := token.Error(); err != nil {
		c.log.Error("Failed to re-subscribe to ACK topic: %v", err)
	}
}
