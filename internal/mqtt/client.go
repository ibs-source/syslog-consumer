// Package mqtt implements an MQTT client with a lock-free handler registry and secure TLS configuration.
package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	mqttlib "github.com/eclipse/paho.mqtt.golang"
	"github.com/ibs-source/syslog/consumer/golang/internal/config"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

// client implements ports.MQTTClient using Paho (single client) with lock-free handler registry.
type client struct {
	client     mqttlib.Client
	cfg        *config.MQTTConfig
	userPrefix string
	logger     ports.Logger

	isConnected atomic.Bool

	// Handlers registry (lock-free via atomic pointer to immutable map)
	handlers atomic.Pointer[map[string]ports.MessageHandler]
}

// NewClient creates a new MQTT client (convenience) using standard Paho client
func NewClient(cfg *config.Config, logger ports.Logger) (ports.MQTTClient, error) {
	c := &client{
		cfg:    &cfg.MQTT,
		logger: logger.WithFields(ports.Field{Key: "component", Value: "mqtt-client"}),
	}

	// initialize empty handlers map
	initial := make(map[string]ports.MessageHandler)
	c.handlers.Store(&initial)

	// Extract user prefix from certificate if TLS enabled
	if cfg.MQTT.TLS.Enabled {
		prefix, err := c.extractUserPrefix(&cfg.MQTT.TLS)
		if err != nil {
			c.logger.Warn("Failed to extract user prefix from certificate", ports.Field{Key: "error", Value: err})
		} else {
			c.userPrefix = prefix
		}
	}

	opts := mqttlib.NewClientOptions()
	for _, broker := range cfg.MQTT.Brokers {
		opts.AddBroker(broker)
	}
	opts.SetClientID(cfg.MQTT.ClientID)
	if cfg.MQTT.TLS.Enabled && c.userPrefix != "" {
		opts.SetUsername(c.userPrefix)
		opts.SetPassword("")
	}
	opts.SetCleanSession(cfg.MQTT.CleanSession)
	opts.SetOrderMatters(cfg.MQTT.OrderMatters)
	opts.SetKeepAlive(cfg.MQTT.KeepAlive)
	opts.SetConnectTimeout(cfg.MQTT.ConnectTimeout)
	opts.SetMaxReconnectInterval(cfg.MQTT.MaxReconnectDelay)
	opts.SetAutoReconnect(true)
	opts.SetProtocolVersion(4) // MQTT 3.1.1
	// Apply channel depth tuning from config (nesting reduced)
	if d := cfg.MQTT.MessageChannelDepth; d > 0 {
		if d > int(math.MaxUint32) {
			opts.SetMessageChannelDepth(uint(math.MaxUint32))
		} else {
			opts.SetMessageChannelDepth(uint(d))
		}
	}

	if cfg.MQTT.TLS.Enabled {
		tlsConf, err := c.createTLSConfig(&cfg.MQTT.TLS, cfg.MQTT.Brokers)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConf)
	}

	opts.SetOnConnectHandler(c.onConnect)
	opts.SetConnectionLostHandler(c.onConnectionLost)

	// Log MQTT topic prefix configuration
	c.logger.Info("MQTT topic prefix configuration",
		ports.Field{Key: "useUserPrefix", Value: c.cfg.Topics.UseUserPrefix},
		ports.Field{Key: "userPrefix", Value: c.userPrefix},
	)

	c.client = mqttlib.NewClient(opts)
	return c, nil
}

func (c *client) onConnect(cli mqttlib.Client) {
	c.isConnected.Store(true)
	c.logger.Info("MQTT connected")

	current := c.handlers.Load()
	if current == nil {
		return
	}
	for topic := range *current {
		c.logger.Info("Re-subscribing to MQTT topic", ports.Field{Key: "topic", Value: topic})
		token := cli.Subscribe(topic, c.cfg.QoS, c.onMessage)
		if ok := token.WaitTimeout(c.cfg.WriteTimeout); !ok || token.Error() != nil {
			c.logger.Error("Failed to re-subscribe topic",
				ports.Field{Key: "topic", Value: topic},
				ports.Field{Key: "error", Value: token.Error()},
			)
		}
	}
}

func (c *client) onConnectionLost(_ mqttlib.Client, err error) {
	c.isConnected.Store(false)
	c.logger.Warn("MQTT connection lost", ports.Field{Key: "error", Value: err})
}

// Connect establishes connection to MQTT brokers
func (c *client) Connect(ctx context.Context) error {
	token := c.client.Connect()

	waitUntil := time.Now().Add(c.cfg.ConnectTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(waitUntil) {
		waitUntil = dl
	}

	// Derive a polling tick from ConnectTimeout to avoid hardcoded waits; clamp to [50ms, 500ms].
	tick := c.cfg.ConnectTimeout / 20
	if tick <= 0 {
		tick = 50 * time.Millisecond
	}
	if tick > 500*time.Millisecond {
		tick = 500 * time.Millisecond
	}
	for !token.WaitTimeout(tick) && time.Now().Before(waitUntil) && ctx.Err() == nil {
		// yield while waiting
		runtime.Gosched()
	}

	if err := token.Error(); err != nil {
		return err
	}
	c.isConnected.Store(true)
	return nil
}

// Disconnect gracefully disconnects
func (c *client) Disconnect(timeout time.Duration) {
	if c.client == nil {
		return
	}
	ms := timeout.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	if ms > int64(math.MaxUint32) {
		ms = int64(math.MaxUint32)
	}
	var msU uint
	if ms >= 0 {
		if ms > int64(math.MaxUint32) {
			msU = uint(math.MaxUint32)
		} else {
			msU = uint(ms)
		}
	}
	c.client.Disconnect(msU)
	c.isConnected.Store(false)
}

// IsConnected returns current connection status
func (c *client) IsConnected() bool {
	if c.client == nil {
		return false
	}
	return c.client.IsConnected() && c.isConnected.Load()
}

// waitForToken waits for a Paho token to complete, honoring both ctx and a max wait duration.
// It polls with a bounded tick to avoid long blocking calls and returns a contextualized error.
func (c *client) waitForToken(ctx context.Context, token mqttlib.Token, wait time.Duration, op string) error {
	deadline := time.Now().Add(wait)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}

	// Polling loop that exits promptly on ctx.Done()
	tick := wait / 20
	if tick <= 0 {
		tick = 50 * time.Millisecond
	}
	if tick > 500*time.Millisecond {
		tick = 500 * time.Millisecond
	}

	for {
		if token.WaitTimeout(tick) {
			if err := token.Error(); err != nil {
				return fmt.Errorf("%s failed: %w", op, err)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s timeout after %s", op, wait)
		}
	}
}

// Publish publishes a message
func (c *client) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error {
	if !c.IsConnected() {
		return fmt.Errorf("mqtt not connected")
	}
	fullTopic := c.buildTopic(topic)
	c.logger.Trace("MQTT publish",
		ports.Field{Key: "requestedTopic", Value: topic},
		ports.Field{Key: "effectiveTopic", Value: fullTopic},
		ports.Field{Key: "qos", Value: qos},
		ports.Field{Key: "retained", Value: retained},
		ports.Field{Key: "payload_bytes", Value: len(payload)},
	)
	token := c.client.Publish(fullTopic, qos, retained, payload)
	return c.waitForToken(ctx, token, c.cfg.WriteTimeout, "publish")
}

// Subscribe subscribes to a topic
func (c *client) Subscribe(ctx context.Context, topic string, qos byte, handler ports.MessageHandler) error {
	if !c.IsConnected() {
		return fmt.Errorf("mqtt not connected")
	}
	fullTopic := c.buildTopic(topic)
	c.logger.Info("MQTT subscribe",
		ports.Field{Key: "requestedTopic", Value: topic},
		ports.Field{Key: "effectiveTopic", Value: fullTopic},
		ports.Field{Key: "qos", Value: qos},
	)

	// Lock-free add handler via copy-on-write map
	c.addHandler(fullTopic, handler)

	token := c.client.Subscribe(fullTopic, qos, c.onMessage)
	return c.waitForToken(ctx, token, c.cfg.WriteTimeout, "subscribe")
}

// Unsubscribe removes subscription(s)
func (c *client) Unsubscribe(ctx context.Context, topics ...string) error {
	if !c.IsConnected() {
		return fmt.Errorf("mqtt not connected")
	}

	fullTopics := make([]string, len(topics))
	for i, t := range topics {
		fullTopics[i] = c.buildTopic(t)
	}

	// Lock-free remove handlers via copy-on-write map
	c.removeHandlers(fullTopics)

	token := c.client.Unsubscribe(fullTopics...)
	return c.waitForToken(ctx, token, c.cfg.WriteTimeout, "unsubscribe")
}

// GetUserPrefix returns extracted user prefix (from cert CN if TLS enabled)
func (c *client) GetUserPrefix() string {
	return c.userPrefix
}

// onMessage resolves the handler lock-free using the current immutable map and early-returns if missing.
func (c *client) onMessage(_ mqttlib.Client, msg mqttlib.Message) {
	c.logger.Trace("MQTT onMessage received",
		ports.Field{Key: "topic", Value: msg.Topic()},
		ports.Field{Key: "payload_bytes", Value: len(msg.Payload())},
	)
	current := c.handlers.Load()
	if current == nil {
		return
	}
	handler, ok := (*current)[msg.Topic()]
	if !ok || handler == nil {
		return
	}
	handler(msg.Topic(), msg.Payload())
}

func (c *client) addHandler(fullTopic string, h ports.MessageHandler) {
	for {
		old := c.handlers.Load()
		var snapshot map[string]ports.MessageHandler
		if old != nil {
			snapshot = *old
		}
		newMap := make(map[string]ports.MessageHandler, len(snapshot)+1)
		for k, v := range snapshot {
			newMap[k] = v
		}
		newMap[fullTopic] = h
		if c.handlers.CompareAndSwap(old, &newMap) {
			return
		}
	}
}

func (c *client) removeHandlers(fullTopics []string) {
	if len(fullTopics) == 0 {
		return
	}
	toRemove := make(map[string]struct{}, len(fullTopics))
	for _, t := range fullTopics {
		toRemove[t] = struct{}{}
	}
	for {
		old := c.handlers.Load()
		if old == nil {
			return
		}
		snapshot := *old

		newMap := make(map[string]ports.MessageHandler, len(snapshot))
		for k, v := range snapshot {
			if _, drop := toRemove[k]; !drop {
				newMap[k] = v
			}
		}
		if c.handlers.CompareAndSwap(old, &newMap) {
			return
		}
	}
}

func (c *client) buildTopic(base string) string {
	base = strings.TrimPrefix(base, "/")
	if c.cfg.Topics.UseUserPrefix && c.userPrefix != "" {
		return fmt.Sprintf("%s/%s", c.userPrefix, base)
	}
	return base
}

func (c *client) extractUserPrefix(tlsCfg *config.TLSConfig) (string, error) {
	cert, err := tls.LoadX509KeyPair(tlsCfg.ClientCertFile, tlsCfg.ClientKeyFile)
	if err != nil {
		return "", err
	}
	if len(cert.Certificate) == 0 {
		return "", fmt.Errorf("no certificate in key pair")
	}
	x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return "", err
	}
	if x509Cert.Subject.CommonName == "" {
		return "", fmt.Errorf("certificate has no common name")
	}
	return x509Cert.Subject.CommonName, nil
}

func (c *client) createTLSConfig(tlsCfg *config.TLSConfig, brokers []string) (*tls.Config, error) {
	caCert, err := os.ReadFile(tlsCfg.CACertFile)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("append CA cert")
	}

	clientCert, err := tls.LoadX509KeyPair(tlsCfg.ClientCertFile, tlsCfg.ClientKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load client cert: %w", err)
	}

	serverName := tlsCfg.ServerName
	if serverName == "" && len(brokers) > 0 {
		b := brokers[0]
		if idx := strings.Index(b, "://"); idx != -1 {
			b = b[idx+3:]
		}
		if idx := strings.LastIndex(b, ":"); idx != -1 {
			serverName = b[:idx]
		} else {
			serverName = b
		}
	}

	return &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
		// Always verify TLS (no InsecureSkipVerify)
		InsecureSkipVerify: false,
		ServerName:         serverName,
		MinVersion:         tls.VersionTLS12,
	}, nil
}
