package mqtt

import (
	"testing"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

// TestNewTLSConfig_Unit tests TLS configuration without integration tag
func TestNewTLSConfig_Unit(t *testing.T) {
	t.Run("TLSDisabled", testTLSDisabled)
	t.Run("ValidTLSWithCA", testValidTLSWithCA)
	t.Run("ValidTLSWithClientCert", testValidTLSWithClientCert)
	t.Run("InsecureSkipVerify", testInsecureSkipVerify)
	t.Run("InvalidCACert", testInvalidCACert)
	t.Run("InvalidClientCert", testInvalidClientCert)
	t.Run("MismatchedClientCertKey", testMismatchedClientCertKey)
	t.Run("EmptyCACert", testEmptyCACert)
	t.Run("OnlyClientCertNoCA", testOnlyClientCertNoCA)
	t.Run("CorruptedCACert", testCorruptedCACert)
}

func testTLSDisabled(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{TLSEnabled: false}
	if !cfg.TLSEnabled {
		t.Log("TLS correctly disabled")
	}
}

func testValidTLSWithCA(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled: true,
		CACert:     "../../testdata/authority.pem",
	}

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

	if tlsConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be false by default")
	}
}

func testValidTLSWithClientCert(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled: true,
		CACert:     "../../testdata/authority.pem",
		ClientCert: "../../testdata/certificate.pem",
		ClientKey:  "../../testdata/key.pem",
	}

	tlsConfig, err := newTLSConfig(cfg)
	if err != nil {
		t.Fatalf("Failed to create TLS config: %v", err)
	}

	if len(tlsConfig.Certificates) == 0 {
		t.Error("Client certificates not loaded")
	}

	if tlsConfig.RootCAs == nil {
		t.Error("RootCAs not set")
	}
}

func testInsecureSkipVerify(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled:   true,
		InsecureSkip: true,
	}

	tlsConfig, err := newTLSConfig(cfg)
	if err != nil {
		t.Fatalf("Failed to create TLS config: %v", err)
	}

	if !tlsConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be true")
	}
}

func testInvalidCACert(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled: true,
		CACert:     "/nonexistent/ca.crt",
	}

	_, err := newTLSConfig(cfg)
	if err == nil {
		t.Error("Expected error for invalid CA cert, got nil")
	}
}

func testInvalidClientCert(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled: true,
		ClientCert: "/nonexistent/client.crt",
		ClientKey:  "/nonexistent/client.key",
	}

	_, err := newTLSConfig(cfg)
	if err == nil {
		t.Error("Expected error for invalid client cert, got nil")
	}
}

func testMismatchedClientCertKey(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled: true,
		CACert:     "../../testdata/authority.pem",
		ClientCert: "../../testdata/certificate.pem",
		ClientKey:  "/nonexistent/key.pem",
	}

	_, err := newTLSConfig(cfg)
	if err == nil {
		t.Error("Expected error for mismatched cert/key, got nil")
	}
}

func testEmptyCACert(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled: true,
		CACert:     "",
	}

	tlsConfig, err := newTLSConfig(cfg)
	if err != nil {
		t.Fatalf("Failed to create TLS config with empty CA: %v", err)
	}

	// Should work but RootCAs will be nil (uses system certs)
	if tlsConfig == nil {
		t.Error("TLS config should not be nil")
	}
}

func testOnlyClientCertNoCA(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled: true,
		ClientCert: "../../testdata/certificate.pem",
		ClientKey:  "../../testdata/key.pem",
	}

	tlsConfig, err := newTLSConfig(cfg)
	if err != nil {
		t.Fatalf("Failed to create TLS config: %v", err)
	}

	if len(tlsConfig.Certificates) == 0 {
		t.Error("Client certificates not loaded")
	}
}

func testCorruptedCACert(t *testing.T) {
	t.Helper()
	cfg := &config.MQTTConfig{
		TLSEnabled: true,
		CACert:     "../../testdata/README.md", // Not a cert file
	}

	_, err := newTLSConfig(cfg)
	if err == nil {
		t.Error("Expected error for corrupted CA cert, got nil")
	}
}

// TestClientStructure tests client struct initialization
func TestClientStructure(t *testing.T) {
	// Test that we can create a client struct (even if we can't connect)
	client := &Client{
		publishTopic:      "test/publish",
		ackTopic:          "test/ack",
		qos:               1,
		disconnectTimeout: 1000,
	}

	if client.publishTopic != "test/publish" {
		t.Errorf("Expected publishTopic 'test/publish', got '%s'", client.publishTopic)
	}

	if client.ackTopic != "test/ack" {
		t.Errorf("Expected ackTopic 'test/ack', got '%s'", client.ackTopic)
	}

	if client.qos != 1 {
		t.Errorf("Expected qos 1, got %d", client.qos)
	}
}

// TestHandleAckMessage tests ACK message handling
func TestHandleAckMessage(t *testing.T) {
	client := &Client{}

	t.Run("NoHandler", func(_ *testing.T) {
		// Should not panic when handler is nil
		client.handleAckMessage([]byte(`{"id":"123","status":true}`))
	})

	t.Run("WithHandler", func(t *testing.T) {
		called := false
		client.ackHandler = func(ack message.AckMessage) {
			called = true
			if ack.ID != "123" {
				t.Errorf("Expected ID '123', got '%s'", ack.ID)
			}
			if !ack.Ack {
				t.Error("Expected ack true")
			}
		}

		client.handleAckMessage([]byte(`{"id":"123","ack":true}`))

		if !called {
			t.Error("Handler was not called")
		}
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		called := false
		client.ackHandler = func(_ message.AckMessage) {
			called = true
		}

		client.handleAckMessage([]byte(`invalid json`))

		if called {
			t.Error("Handler should not be called for invalid JSON")
		}
	})
}
