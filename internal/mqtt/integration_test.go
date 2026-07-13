//go:build integration

package mqtt

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

const (
	// The broker is shared; isolation comes from the tenant client
	// certificate. Tests authenticate as the dedicated "github" tenant,
	// whose certificates are issued by the deployer at
	// https://github.syslog.ibs.cloud/generate (see the manager script).
	defaultTestBroker   = "ssl://syslog.ibs.cloud:8883"
	defaultTestTenant   = "github"
	defaultTestDeployer = "https://TENANT.syslog.ibs.cloud/generate"
)

func closeQuietly(t *testing.T, c interface{ Close() error }) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// newConnectedClient is a test helper that creates and connects a client.
func newConnectedClient(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) *Client {
	t.Helper()
	client, err := NewClient(t.Context(), cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT client: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect MQTT client: %v", err)
	}
	return client
}

// setupIntegrationConfig resolves broker and certificates for integration
// tests. Certificates come from explicit env paths or, like the container's
// manager script, as a fresh bundle from the certificate deployer; the test
// skips when neither source is configured or the deployer is unreachable.
func setupIntegrationConfig(t *testing.T) *config.MQTTConfig {
	t.Helper()

	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = defaultTestBroker
	}

	ca, cert, key := os.Getenv("MQTT_CA_CERT"), os.Getenv("MQTT_CLIENT_CERT"), os.Getenv("MQTT_CLIENT_KEY")
	if ca == "" || cert == "" || key == "" {
		deployer, apiKey := os.Getenv("CERTIFICATE_DEPLOYER"), os.Getenv("CERTIFICATE_DEPLOYER_KEY")
		if deployer == "" {
			deployer = defaultTestDeployer
		}
		if apiKey == "" {
			t.Skip("MQTT integration: set MQTT_CA_CERT/MQTT_CLIENT_CERT/MQTT_CLIENT_KEY " +
				"or CERTIFICATE_DEPLOYER_KEY")
		}
		tenant := os.Getenv("TENANT")
		if tenant == "" {
			tenant = defaultTestTenant
		}
		var err error
		ca, cert, key, err = runManagerUpdate(t, deployer, apiKey, tenant)
		if err != nil {
			t.Skipf("MQTT integration: certificate deployer unavailable: %v", err)
		}
	}

	t.Setenv("MQTT_BROKER", broker)
	t.Setenv("MQTT_TLS_ENABLED", "true")
	t.Setenv("MQTT_CA_CERT", ca)
	t.Setenv("MQTT_CLIENT_CERT", cert)
	t.Setenv("MQTT_CLIENT_KEY", key)

	fullCfg, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	return &fullCfg.MQTT
}

// runManagerUpdate obtains a fresh certificate bundle by running the
// repository's manager script — the exact code path the container uses
// (expiration/CRL checks, deployer download, extraction).
func runManagerUpdate(t *testing.T, deployer, apiKey, tenant string) (ca, cert, key string, err error) {
	t.Helper()
	dir := t.TempDir()

	cmd := exec.CommandContext(t.Context(), "bash", "../../manager", "update")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"CERTIFICATE_DEPLOYER=" + deployer,
		"CERTIFICATE_DEPLOYER_KEY=" + apiKey,
		"CERTIFICATE_RENEWAL=30",
		"CERTIFICATE_PATH=" + dir,
		"TENANT=" + tenant,
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", "", fmt.Errorf("manager update: %w: %s", err, out)
	}

	ca = filepath.Join(dir, "authority.pem")
	cert = filepath.Join(dir, "certificate.pem")
	key = filepath.Join(dir, "key.pem")
	for _, p := range []string{ca, cert, key} {
		if _, statErr := os.Stat(p); statErr != nil {
			return "", "", "", fmt.Errorf("manager did not produce %s: %s", filepath.Base(p), out)
		}
	}
	return ca, cert, key, nil
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
	client := newConnectedClient(t, cfg, logger)
	defer closeQuietly(t, client)
	t.Log("Successfully connected to MQTT broker with TLS")
}

func testMQTTPublish(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) {
	t.Helper()
	client := newConnectedClient(t, cfg, logger)
	defer closeQuietly(t, client)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	testPayload := []byte(`{"test":"integration","timestamp":"2025-01-08T12:00:00Z"}`)
	err := client.Publish(ctx, testPayload)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
	t.Log("Successfully published message to MQTT broker")
}

func testMQTTPublishMultiple(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) {
	t.Helper()
	client := newConnectedClient(t, cfg, logger)
	defer closeQuietly(t, client)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for i := range 10 {
		payload := []byte(`{"test":"multiple","index":` + string(rune(i+'0')) + `}`)
		if err := client.Publish(ctx, payload); err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}
	t.Log("Successfully published 10 messages")
}

func testMQTTSubscribe(t *testing.T, cfg *config.MQTTConfig, logger *log.Logger) {
	t.Helper()
	client := newConnectedClient(t, cfg, logger)
	defer closeQuietly(t, client)

	ackReceived := make(chan message.AckMessage, 1)
	err := client.SubscribeAck(t.Context(), func(ack message.AckMessage) {
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
	client := newConnectedClient(t, cfg, logger)
	defer closeQuietly(t, client)

	ackReceived := make(chan message.AckMessage, 10)
	err := client.SubscribeAck(t.Context(), func(ack message.AckMessage) {
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

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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
	pool, err := NewPool(t.Context(), cfg, poolSize, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT pool: %v", err)
	}
	defer closeQuietly(t, pool)

	t.Logf("Successfully created MQTT pool with %d connections", poolSize)
}

func testPublishWithPool(t *testing.T, cfg *config.MQTTConfig, poolSize int, logger *log.Logger) {
	t.Helper()
	pool, err := NewPool(t.Context(), cfg, poolSize, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT pool: %v", err)
	}
	defer closeQuietly(t, pool)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for i := range 10 {
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
	pool, err := NewPool(t.Context(), cfg, poolSize, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT pool: %v", err)
	}
	defer closeQuietly(t, pool)

	ackReceived := make(chan message.AckMessage, 10)
	err = pool.SubscribeAck(t.Context(), func(ack message.AckMessage) {
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
	pool, err := NewPool(t.Context(), cfg, poolSize, logger)
	if err != nil {
		t.Fatalf("Failed to create MQTT pool: %v", err)
	}
	defer closeQuietly(t, pool)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	done := make(chan error, 5)
	for g := range 5 {
		go func(goroutineID int) {
			for i := range 5 {
				payload := []byte(
					`{"test":"concurrent","goroutine":` + strconv.Itoa(goroutineID) +
						`,"msg":` + strconv.Itoa(i) + `}`)
				if err := pool.Publish(ctx, payload); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(g)
	}

	for range 5 {
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
		for i := range 3 {
			client := newConnectedClient(t, cfg, logger)

			// Publish a message
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			payload := []byte(`{"test":"reconnect","iteration":` + string(rune(i+'0')) + `}`)
			err := client.Publish(ctx, payload)
			cancel()
			if err != nil {
				t.Fatalf("Failed to publish on iteration %d: %v", i, err)
			}

			// Close connection
			closeQuietly(t, client)

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
		client := newConnectedClient(t, cfg, logger)
		defer closeQuietly(t, client)

		// Create a context that times out immediately
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Nanosecond)
		defer cancel()

		time.Sleep(10 * time.Millisecond) // Ensure context is expired

		payload := []byte(`{"test":"timeout"}`)
		err := client.Publish(ctx, payload)
		if err == nil {
			t.Error("Expected timeout error, got nil")
		}

		t.Logf("Correctly handled timeout: %v", err)
	})

	t.Run("LargePayload", func(t *testing.T) {
		client := newConnectedClient(t, cfg, logger)
		defer closeQuietly(t, client)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		// Create a large payload (1MB)
		largePayload := make([]byte, 1024*1024)
		for i := range largePayload {
			largePayload[i] = byte(i % 256)
		}

		err := client.Publish(ctx, largePayload)
		if err != nil {
			t.Logf("Large payload rejected (expected for some brokers): %v", err)
		} else {
			t.Log("Successfully published large payload")
		}
	})
}

// TestManagerUpdate_MockDeployer proves the whole certificate flow by running
// the real manager script against a local mock serving the same tar.gz bundle
// the deployer does.
func TestManagerUpdate_MockDeployer(t *testing.T) {
	if exec.CommandContext(t.Context(), "ping", "-c", "1", "-W", "2", "8.8.8.8").Run() != nil {
		t.Skip("manager requires ICMP connectivity")
	}

	caPath, certPath, keyPath := genTestCerts(t)
	bundle := buildCertBundle(t, caPath, certPath, keyPath)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(bundle) //nolint:errcheck // test server
	}))
	defer srv.Close()

	ca, cert, key, err := runManagerUpdate(t, srv.URL, "test-key", defaultTestTenant)
	if err != nil {
		t.Fatalf("runManagerUpdate() error = %v", err)
	}
	if _, pairErr := tls.LoadX509KeyPair(cert, key); pairErr != nil {
		t.Errorf("fetched cert/key pair does not load: %v", pairErr)
	}
	caPEM, caErr := os.ReadFile(ca) //nolint:gosec // G304: temp path from t.TempDir
	if caErr != nil || !strings.Contains(string(caPEM), "BEGIN CERTIFICATE") {
		t.Errorf("fetched CA is not a certificate (err=%v)", caErr)
	}

	if _, _, _, err := runManagerUpdate(t, srv.URL, "wrong-key", defaultTestTenant); err == nil {
		t.Error("expected error with wrong API key")
	}
}

func buildCertBundle(t *testing.T, paths ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, p := range paths {
		data, err := os.ReadFile(p) //nolint:gosec // G304: temp path from t.TempDir
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: filepath.Base(p), Mode: 0o600, Size: int64(len(data)),
		}); err != nil {
			t.Fatalf("tar header: %v", err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatalf("tar write: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}
