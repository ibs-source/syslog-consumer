// Package mqtt provides MQTT client and connection pooling for publishing and ACK handling.
package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

// Client manages MQTT publishing and ACK subscription
type Client struct {
	client            mqtt.Client
	publishTopic      string
	ackTopic          string
	qos               byte
	writeTimeout      time.Duration
	subscribeTimeout  time.Duration
	disconnectTimeout uint
	ackHandler        func(message.AckMessage)
	mu                sync.RWMutex
	log               *log.Logger
}

// NewClient creates a new MQTT client
func NewClient(cfg *config.MQTTConfig, logger *log.Logger) (*Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.Broker)
	opts.SetClientID(cfg.ClientID)
	opts.SetConnectTimeout(cfg.ConnectTimeout)
	opts.SetWriteTimeout(cfg.WriteTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(cfg.MaxReconnectInterval)

	// Performance optimizations for high throughput
	opts.SetKeepAlive(60 * time.Second)   // Keep connections alive
	opts.SetPingTimeout(10 * time.Second) // Faster ping timeout
	opts.SetMessageChannelDepth(10000)    // Large internal queue for outgoing messages
	opts.SetResumeSubs(true)              // Resume subscriptions on reconnect
	opts.SetOrderMatters(false)           // Allow out-of-order delivery for better throughput
	opts.SetMaxResumePubInFlight(1000)    // Resume more in-flight messages

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		if err != nil {
			logger.Error("MQTT connection lost: %v", err)
		}
	})

	opts.SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
		logger.Info("MQTT reconnecting...")
	})

	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		logger.Info("MQTT connected successfully")
	})

	// Configure TLS if enabled
	if cfg.TLSEnabled {
		tlsConfig, err := newTLSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(cfg.ConnectTimeout) {
		return nil, fmt.Errorf("mqtt connection timeout")
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("failed to connect to MQTT: %w", err)
	}

	return &Client{
		client:            client,
		publishTopic:      cfg.PublishTopic,
		ackTopic:          cfg.AckTopic,
		qos:               cfg.QoS,
		writeTimeout:      cfg.WriteTimeout,
		subscribeTimeout:  cfg.SubscribeTimeout,
		disconnectTimeout: cfg.DisconnectTimeout,
		log:               logger,
	}, nil
}

// newTLSConfig creates a TLS configuration from MQTT config
func newTLSConfig(cfg *config.MQTTConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		// Note: Enabling InsecureSkipVerify weakens TLS security and should only be used for testing.
		InsecureSkipVerify: cfg.InsecureSkip, // #nosec G402 - configurable for testing environments
		MinVersion:         tls.VersionTLS12,
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

// Publish sends a message to the MQTT broker
func (c *Client) Publish(ctx context.Context, payload []byte) error {
	token := c.client.Publish(c.publishTopic, c.qos, false, payload)

	// Wait for publish with timeout
	done := make(chan struct{})
	go func() {
		token.Wait()
		close(done)
	}()

	select {
	case <-done:
		if err := token.Error(); err != nil {
			return fmt.Errorf("mqtt publish failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.writeTimeout):
		return fmt.Errorf("mqtt publish timeout")
	}
}

// SubscribeAck registers a callback for ACK messages
func (c *Client) SubscribeAck(handler func(message.AckMessage)) error {
	c.mu.Lock()
	c.ackHandler = handler
	c.mu.Unlock()

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

// handleAckMessage processes incoming ACK messages
func (c *Client) handleAckMessage(payload []byte) {
	c.mu.RLock()
	handler := c.ackHandler
	c.mu.RUnlock()

	if handler == nil {
		return
	}

	// Parse ACK message
	ack, err := parseAck(payload)
	if err != nil {
		// Silently ignore malformed ACKs
		return
	}

	handler(ack)
}

// Close disconnects from the MQTT broker
func (c *Client) Close() error {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(c.disconnectTimeout)
	}
	return nil
}
