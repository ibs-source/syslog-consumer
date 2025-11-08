package mqtt

import (
	"context"
	"testing"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

// setupIntegrationConfig configures environment and loads MQTT config for integration tests
func setupIntegrationConfig(t *testing.T) *config.MQTTConfig {
	t.Helper()

	// Set integration test configuration
	t.Setenv("MQTT_BROKER", "ssl://broker.supia.it:8883")
	t.Setenv("MQTT_TLS_ENABLED", "true")
	t.Setenv("MQTT_CA_CERT", "../../testdata/authority.pem")
	t.Setenv("MQTT_CLIENT_CERT", "../../testdata/certificate.pem")
	t.Setenv("MQTT_CLIENT_KEY", "../../testdata/key.pem")

	// Load config using proper config loader
	fullCfg, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	return &fullCfg.MQTT
}

// TestIntegration_MQTTConnection tests real MQTT connection with TLS
func TestIntegration_MQTTConnection(t *testing.T) {
	cfg := setupIntegrationConfig(t)
	cfg.ClientID = "integration-test-client"
	logger := log.New()

	t.Run("Connect", func(t *testing.T) { testMQTTConnect(t, cfg, logger) })
	t.Run("Publish", func(t *testing.T) { testMQTTPublish(t, cfg, logger) })
	t.Run("PublishMultiple", func(t *testing.T) { testMQTTPublishMultiple(t, cfg, logger) })
	t.Run("Subscribe", func(t *testing.T) { testMQTTSubscribe(t, cfg, logger) })
	t.Run("PublishAndSubscribe", func(t *testing.T) { testMQTTPublishAndSubscribe(t, cfg, logger) })
}

func testMQTTConnect(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) {
	t.Helper()
	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT client: %v", err)
	}
	defer func() { _ = client.Close() }()
	t.Log("Successfully connected to MQTT broker with TLS")
}

func testMQTTPublish(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) {
	t.Helper()
	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	testPayload := []byte(`{"test":"integration","timestamp":"2025-01-08T12:00:00Z"}`)
	err = client.Publish(ctx, testPayload)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
	t.Log("Successfully published message to MQTT broker")
}

func testMQTTPublishMultiple(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) {
	t.Helper()
	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		payload := []byte(`{"test":"multiple","index":` + string(rune(i+'0')) + `}`)
		err = client.Publish(ctx, payload)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}
	t.Log("Successfully published 10 messages")
}

func testMQTTSubscribe(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) {
	t.Helper()
	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ackReceived := make(chan message.AckMessage, 1)
	err = client.SubscribeAck(func(ack message.AckMessage) {
		ackReceived <- ack
	})
	if err != nil {
		t.Fatalf("Failed to subscribe to ACK topic: %v", err)
	}
	t.Log("Successfully subscribed to ACK topic")
	time.Sleep(500 * time.Millisecond)
}

func testMQTTPublishAndSubscribe(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) {
	t.Helper()
	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ackReceived := make(chan message.AckMessage, 10)
	err = client.SubscribeAck(func(ack message.AckMessage) {
		select {
		case ackReceived <- ack:
		default:
			t.Log("ACK channel full, dropping message")
		}
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	testPayload := []byte(`{"test":"pubsub","timestamp":"2025-01-08T12:00:00Z"}`)
	err = client.Publish(ctx, testPayload)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
	t.Log("Successfully tested publish and subscribe")
}

// TestIntegration_MQTTPool tests MQTT connection pool
func TestIntegration_MQTTPool(t *testing.T) {
	cfg := setupIntegrationConfig(t)
	cfg.ClientID = "integration-test-pool"
	logger := log.New()
	poolSize := 3

	t.Run("CreatePool", func(t *testing.T) { testCreatePool(t, cfg, poolSize, logger) })
	t.Run("PublishWithPool", func(t *testing.T) { testPublishWithPool(t, cfg, poolSize, logger) })
	t.Run("PoolSubscribeAck", func(t *testing.T) { testPoolSubscribeAck(t, cfg, poolSize, logger) })
	t.Run("PoolConcurrentPublish", func(t *testing.T) { testPoolConcurrentPublish(t, cfg, poolSize, logger) })
}

func testCreatePool(t *testing.T, cfg *config.MQTTConfig, poolSize int, logger *log.Logger) {
	t.Helper()
	pool, err := NewPool(cfg, poolSize, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT pool: %v", err)
	}
	defer func() { _ = pool.Close() }()

	t.Logf("Successfully created MQTT pool with %d connections", poolSize)
}

func testPublishWithPool(t *testing.T, cfg *config.MQTTConfig, poolSize int, logger *log.Logger) {
	t.Helper()
	pool, err := NewPool(cfg, poolSize, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT pool: %v", err)
	}
	defer func() { _ = pool.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		testPayload := []byte(`{"test":"pool","index":` + string(rune(i+'0')) + `}`)
		err = pool.Publish(ctx, testPayload)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}
	t.Log("Successfully published 10 messages using pool")
}

func testPoolSubscribeAck(t *testing.T, cfg *config.MQTTConfig, poolSize int, logger *log.Logger) {
	t.Helper()
	pool, err := NewPool(cfg, poolSize, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT pool: %v", err)
	}
	defer func() { _ = pool.Close() }()

	ackReceived := make(chan message.AckMessage, 10)
	err = pool.SubscribeAck(func(ack message.AckMessage) {
		select {
		case ackReceived <- ack:
		default:
			t.Log("ACK channel full")
		}
	})
	if err != nil {
		t.Fatalf("Failed to subscribe with pool: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	t.Log("Successfully subscribed to ACK topic with pool")
}

func testPoolConcurrentPublish(t *testing.T, cfg *config.MQTTConfig, poolSize int, logger *log.Logger) {
	t.Helper()
	pool, err := NewPool(cfg, poolSize, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT pool: %v", err)
	}
	defer func() { _ = pool.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan error, 5)
	for g := 0; g < 5; g++ {
		go func(goroutineID int) {
			for i := 0; i < 5; i++ {
				payload := []byte(
					`{"test":"concurrent","goroutine":` +
						string(rune(goroutineID+'0')) +
						`,"msg":` + string(rune(i+'0')) + `}`)
				if err := pool.Publish(ctx, payload); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(g)
	}

	for g := 0; g < 5; g++ {
		if err := <-done; err != nil {
			t.Fatalf("Concurrent publish failed: %v", err)
		}
	}
	t.Log("Successfully published 25 messages concurrently using pool")
}

// TestIntegration_TLSConfig tests TLS configuration
func TestIntegration_TLSConfig(t *testing.T) {
	cfg := setupIntegrationConfig(t)

	t.Run("ValidTLSConfig", func(t *testing.T) {
		tlsConfig, err := newTLSConfig(cfg)
		if err != nil {
			t.Fatalf("Failed to create TLS config: %v", err)
		}

		if tlsConfig == nil {
			t.Fatal("TLS config is nil")
		}

		if tlsConfig.RootCAs == nil {
			t.Error("RootCAs not set")
		}

		if cfg.ClientCert != "" && len(tlsConfig.Certificates) == 0 {
			t.Error("Client certificates not loaded")
		}

		t.Log("TLS config created successfully")
	})

	t.Run("InvalidCACert", func(t *testing.T) {
		badCfg := &config.MQTTConfig{
			TLSEnabled: true,
			CACert:     "/nonexistent/ca.crt",
		}

		_, err := newTLSConfig(badCfg)
		if err == nil {
			t.Error("Expected error for invalid CA cert, got nil")
		}
	})

	t.Run("InvalidClientCert", func(t *testing.T) {
		badCfg := &config.MQTTConfig{
			TLSEnabled: true,
			CACert:     cfg.CACert,
			ClientCert: "/nonexistent/client.crt",
			ClientKey:  "/nonexistent/client.key",
		}

		_, err := newTLSConfig(badCfg)
		if err == nil {
			t.Error("Expected error for invalid client cert, got nil")
		}
	})
}

// TestIntegration_ReconnectScenario tests connection resilience
func TestIntegration_ReconnectScenario(t *testing.T) {
	cfg := setupIntegrationConfig(t)
	cfg.ClientID = "integration-test-reconnect"
	logger := log.New()

	t.Run("MultipleConnectionCycles", func(t *testing.T) {
		// Connect and disconnect multiple times
		for i := 0; i < 3; i++ {
			client, err := NewClient(cfg, logger)
			if err != nil {
				t.Fatalf("Failed to create client on iteration %d: %v", i, err)
			}

			// Publish a message
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			payload := []byte(`{"test":"reconnect","iteration":` + string(rune(i+'0')) + `}`)
			err = client.Publish(ctx, payload)
			cancel()
			if err != nil {
				t.Fatalf("Failed to publish on iteration %d: %v", i, err)
			}

			// Close connection
			_ = client.Close()

			// Small delay between cycles
			time.Sleep(100 * time.Millisecond)
		}

		t.Log("Successfully completed 3 connection cycles")
	})
}

// TestIntegration_ErrorHandling tests error scenarios
func TestIntegration_ErrorHandling(t *testing.T) {
	cfg := setupIntegrationConfig(t)
	cfg.ClientID = "integration-test-errors"
	logger := log.New()

	t.Run("PublishTimeout", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Fatalf("Failed to create client: %v", err)
		}
		defer func() { _ = client.Close() }()

		// Create a context that times out immediately
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		time.Sleep(10 * time.Millisecond) // Ensure context is expired

		payload := []byte(`{"test":"timeout"}`)
		err = client.Publish(ctx, payload)
		if err == nil {
			t.Error("Expected timeout error, got nil")
		}

		t.Logf("Correctly handled timeout: %v", err)
	})

	t.Run("LargePayload", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Fatalf("Failed to create client: %v", err)
		}
		defer func() { _ = client.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Create a large payload (1MB)
		largePayload := make([]byte, 1024*1024)
		for i := range largePayload {
			largePayload[i] = byte(i % 256)
		}

		err = client.Publish(ctx, largePayload)
		if err != nil {
			t.Logf("Large payload rejected (expected for some brokers): %v", err)
		} else {
			t.Log("Successfully published large payload")
		}
	})
}
