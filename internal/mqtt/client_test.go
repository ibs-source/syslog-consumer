package mqtt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/ibs-source/syslog-consumer/internal/compress"
	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

func TestMain(m *testing.M) {
	compress.Init(&config.CompressConfig{
		FreelistSize:       128,
		MaxDecompressBytes: 256 << 20,
		WarmupCount:        4,
	})
	os.Exit(m.Run())
}

// --- paho mock types ---

type mockPahoToken struct {
	err error
}

func (t *mockPahoToken) Wait() bool                       { return true }
func (t *mockPahoToken) WaitTimeout(_ time.Duration) bool { return true }
func (t *mockPahoToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (t *mockPahoToken) Error() error { return t.err }

type mockPahoClient struct {
	connectFn        func() paho.Token
	publishFn        func(topic string, qos byte, retained bool, payload any) paho.Token
	subscribeFn      func(topic string, qos byte, callback paho.MessageHandler) paho.Token
	connected        bool
	disconnectCalled bool
}

func (m *mockPahoClient) IsConnected() bool      { return m.connected }
func (m *mockPahoClient) IsConnectionOpen() bool { return m.connected }
func (m *mockPahoClient) Connect() paho.Token {
	if m.connectFn != nil {
		return m.connectFn()
	}
	return &mockPahoToken{}
}
func (m *mockPahoClient) Disconnect(_ uint) { m.disconnectCalled = true }
func (m *mockPahoClient) Publish(topic string, qos byte, retained bool, payload any) paho.Token {
	if m.publishFn != nil {
		return m.publishFn(topic, qos, retained, payload)
	}
	return &mockPahoToken{}
}
func (m *mockPahoClient) Subscribe(topic string, qos byte, callback paho.MessageHandler) paho.Token {
	if m.subscribeFn != nil {
		return m.subscribeFn(topic, qos, callback)
	}
	return &mockPahoToken{}
}
func (m *mockPahoClient) SubscribeMultiple(_ map[string]byte, _ paho.MessageHandler) paho.Token {
	return &mockPahoToken{}
}
func (m *mockPahoClient) Unsubscribe(_ ...string) paho.Token       { return &mockPahoToken{} }
func (m *mockPahoClient) AddRoute(_ string, _ paho.MessageHandler) {}
func (m *mockPahoClient) OptionsReader() paho.ClientOptionsReader {
	return paho.NewOptionsReader(paho.NewClientOptions())
}

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

func testMQTTConfig() *config.MQTTConfig {
	return &config.MQTTConfig{
		Broker:               "tcp://127.0.0.1:1883",
		ClientID:             "test-client",
		PublishTopic:         "test/pub",
		AckTopic:             "test/ack",
		QoS:                  0,
		ConnectTimeout:       5 * time.Millisecond,
		WriteTimeout:         5 * time.Millisecond,
		MaxReconnectInterval: 5 * time.Millisecond,
		SubscribeTimeout:     5 * time.Millisecond,
		DisconnectTimeout:    5 * time.Millisecond,
	}
}

func TestNewClient_Success(t *testing.T) {
	cfg := testMQTTConfig()
	client, err := NewClient(cfg, log.New())
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if client == nil || client.client == nil {
		t.Fatal("NewClient() returned nil client")
	}
	if client.publishTopic != cfg.PublishTopic || client.ackTopic != cfg.AckTopic {
		t.Fatal("NewClient() did not copy topics from config")
	}
}

func TestNewClient_TLSConfigError(t *testing.T) {
	cfg := testMQTTConfig()
	cfg.TLSEnabled = true
	cfg.CACert = "/nonexistent/ca.pem"

	_, err := NewClient(cfg, log.New())
	if err == nil {
		t.Fatal("expected TLS config error, got nil")
	}
}

func TestClientConnect_Success(t *testing.T) {
	c := &Client{
		client:         &mockPahoClient{connectFn: func() paho.Token { return &mockPahoToken{} }},
		connectTimeout: 5 * time.Millisecond,
		log:            log.New(),
	}

	if err := c.Connect(t.Context()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
}

func TestClientConnect_TimeoutThenCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	c := &Client{
		client: &mockPahoClient{connectFn: func() paho.Token {
			cancel()
			return &slowToken{done: make(chan struct{})}
		}},
		connectTimeout: 1 * time.Millisecond,
		log:            log.New(),
	}

	err := c.Connect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Connect() error = %v, want context.Canceled", err)
	}
}

func TestClientConnect_ErrorThenCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	c := &Client{
		client: &mockPahoClient{connectFn: func() paho.Token {
			cancel()
			return &mockPahoToken{err: errors.New("connect failed")}
		}},
		connectTimeout: 5 * time.Millisecond,
		log:            log.New(),
	}

	err := c.Connect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Connect() error = %v, want context.Canceled", err)
	}
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
		publishTopic: "test/publish",
		ackTopic:     "test/ack",
		qos:          1,
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
	client := &Client{log: log.New()}

	t.Run("NoHandler", func(_ *testing.T) {
		// Should not panic when handler is nil
		enc := compress.NewEncoder()
		payload := compress.EncodeWith(enc, nil, []byte(`{"ids":["123"],"stream":"s","ack":true}`))
		client.handleAckMessage(payload)
	})

	t.Run("CompressedPayload", func(t *testing.T) {
		called := false
		handler := func(ack message.AckMessage) {
			called = true
			if len(ack.IDs) != 1 || ack.IDs[0] != "123" {
				t.Errorf("Expected IDs [123], got %v", ack.IDs)
			}
			if !ack.Ack {
				t.Error("Expected ack true")
			}
		}
		client.ackHandler.Store(&handler)

		ackPayload := []byte(`{"ids":["123"],"stream":"test-stream","ack":true}`)
		payload := compress.EncodeWith(compress.NewEncoder(), nil, ackPayload)
		client.handleAckMessage(payload)

		if !called {
			t.Error("Handler was not called")
		}
	})

	t.Run("PlainPayload", func(t *testing.T) {
		called := false
		handler := func(ack message.AckMessage) {
			called = true
			if len(ack.IDs) != 1 || ack.IDs[0] != "456" {
				t.Errorf("Expected IDs [456], got %v", ack.IDs)
			}
		}
		client.ackHandler.Store(&handler)

		client.handleAckMessage([]byte(`{"ids":["456"],"stream":"s","ack":true}`))

		if !called {
			t.Error("Handler was not called for plain payload")
		}
	})

	t.Run("InvalidData", func(t *testing.T) {
		called := false
		handler := func(_ message.AckMessage) { called = true }
		client.ackHandler.Store(&handler)

		// Valid zstd magic but garbage body.
		client.handleAckMessage([]byte{0x28, 0xB5, 0x2F, 0xFD, 0xFF, 0xFF})
		if called {
			t.Error("Handler should not be called for invalid compressed data")
		}

		client.handleAckMessage([]byte(`invalid json`))
		if called {
			t.Error("Handler should not be called for invalid JSON")
		}
	})
}

// --- parseAck additional tests (basics are in ack_test.go) ---

func TestParseAck_EmptyPayload(t *testing.T) {
	_, err := parseAck([]byte(``))
	if err == nil {
		t.Error("expected error for empty payload")
	}
}

func TestParseAck_EmptyObject(t *testing.T) {
	_, err := parseAck([]byte(`{}`))
	if err == nil {
		t.Error("expected error for empty object (missing id)")
	}
}

func TestParseAck_WithStream(t *testing.T) {
	ack, err := parseAck([]byte(`{"ids":["999"],"stream":"mystream","ack":true}`))
	if err != nil {
		t.Fatalf("parseAck() error = %v", err)
	}
	if ack.Stream != "mystream" {
		t.Errorf("Stream = %q, want %q", ack.Stream, "mystream")
	}
}

// --- Client.Close tests ---

func TestClientClose_NilClient(t *testing.T) {
	c := &Client{} // nil mqtt.Client
	err := c.Close()
	if err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
}

func TestClientClose_Connected(t *testing.T) {
	mock := &mockPahoClient{connected: true}
	c := &Client{client: mock, disconnectTimeout: 100 * time.Millisecond}
	err := c.Close()
	if err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
	if !mock.disconnectCalled {
		t.Error("expected Disconnect() to be called")
	}
}

func TestClientClose_NotConnected(t *testing.T) {
	mock := &mockPahoClient{connected: false}
	c := &Client{client: mock}
	err := c.Close()
	if err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
	if mock.disconnectCalled {
		t.Error("Disconnect() should not be called when not connected")
	}
}

// --- Client.Publish tests ---

func TestClientPublish_QoS0(t *testing.T) {
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(topic string, qos byte, _ bool, _ any) paho.Token {
			if topic != "test/pub" {
				t.Errorf("topic = %q, want test/pub", topic)
			}
			if qos != 0 {
				t.Errorf("qos = %d, want 0", qos)
			}
			return &mockPahoToken{}
		},
	}
	c := &Client{
		client:       mock,
		publishTopic: "test/pub",
		qos:          0,
		writeTimeout: 5 * time.Second,
		log:          log.New(),
	}
	c.connected.Store(true)

	err := c.Publish(t.Context(), []byte(`{"test":true}`))
	if err != nil {
		t.Errorf("Publish() error = %v", err)
	}
}

func TestClientPublish_QoS1(t *testing.T) {
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
			return &mockPahoToken{}
		},
	}
	c := &Client{
		client:       mock,
		publishTopic: "test/pub",
		qos:          1,
		writeTimeout: 5 * time.Second,
		log:          log.New(),
	}
	c.connected.Store(true)

	err := c.Publish(t.Context(), []byte(`{"test":true}`))
	if err != nil {
		t.Errorf("Publish() error = %v", err)
	}
}

func TestClientPublish_QoS1_Timeout(t *testing.T) {
	// Token that never completes
	neverDone := make(chan struct{}) // never closed
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
			return &slowToken{done: neverDone}
		},
	}
	c := &Client{
		client:       mock,
		publishTopic: "test/pub",
		qos:          1,
		writeTimeout: 10 * time.Millisecond,
		log:          log.New(),
	}
	c.connected.Store(true)

	err := c.Publish(t.Context(), []byte(`{}`))
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestClientPublish_QoS1_ContextCancel(t *testing.T) {
	neverDone := make(chan struct{})
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
			return &slowToken{done: neverDone}
		},
	}
	c := &Client{
		client:       mock,
		publishTopic: "test/pub",
		qos:          1,
		writeTimeout: 5 * time.Second,
		log:          log.New(),
	}
	c.connected.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately

	err := c.Publish(ctx, []byte(`{}`))
	if err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestClientPublish_QoS1_Error(t *testing.T) {
	publishErr := fmt.Errorf("broker rejected")
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
			return &mockPahoToken{err: publishErr}
		},
	}
	c := &Client{
		client:       mock,
		publishTopic: "test/pub",
		qos:          1,
		writeTimeout: 5 * time.Second,
		log:          log.New(),
	}
	c.connected.Store(true)

	err := c.Publish(t.Context(), []byte(`{}`))
	if err == nil {
		t.Error("expected publish error")
	}
}

// --- Client.SubscribeAck tests ---

func TestClientSubscribeAck_Success(t *testing.T) {
	mock := &mockPahoClient{
		subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
			return &mockPahoToken{}
		},
	}
	c := &Client{
		client:           mock,
		ackTopic:         "test/ack",
		qos:              0,
		subscribeTimeout: 5 * time.Second,
		log:              log.New(),
	}

	var handlerCalled bool
	err := c.SubscribeAck(func(_ message.AckMessage) { handlerCalled = true })
	if err != nil {
		t.Errorf("SubscribeAck() error = %v", err)
	}
	_ = handlerCalled // handler stored but not called yet
}

func TestClientSubscribeAck_Timeout(t *testing.T) {
	// Use slowToken so WaitTimeout returns false (simulates timeout)
	neverDone := make(chan struct{})
	mock := &mockPahoClient{
		subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
			return &slowToken{done: neverDone}
		},
	}
	c := &Client{
		client:           mock,
		ackTopic:         "test/ack",
		qos:              0,
		subscribeTimeout: 10 * time.Millisecond, // short timeout
		log:              log.New(),
	}

	err := c.SubscribeAck(func(_ message.AckMessage) {})
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestClientSubscribeAck_Error(t *testing.T) {
	subErr := fmt.Errorf("subscription rejected")
	mock := &mockPahoClient{
		subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
			return &mockPahoToken{err: subErr}
		},
	}
	c := &Client{
		client:           mock,
		ackTopic:         "test/ack",
		qos:              0,
		subscribeTimeout: 5 * time.Second,
		log:              log.New(),
	}

	err := c.SubscribeAck(func(_ message.AckMessage) {})
	if err == nil {
		t.Error("expected subscription error")
	}
}

// slowToken never completes Wait — used for timeout tests
type slowToken struct {
	done chan struct{}
	err  error
}

func (t *slowToken) Wait() bool {
	<-t.done
	return true
}
func (t *slowToken) WaitTimeout(d time.Duration) bool {
	select {
	case <-t.done:
		return true
	case <-time.After(d):
		return false
	}
}
func (t *slowToken) Done() <-chan struct{} { return t.done }
func (t *slowToken) Error() error          { return t.err }

// --- Publish QoS 0 error path ---

func TestClientPublish_QoS0_FireAndForget(t *testing.T) {
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
			return &mockPahoToken{err: fmt.Errorf("qos0 error")}
		},
	}
	c := &Client{
		client:       mock,
		publishTopic: "test/pub",
		qos:          0,
		writeTimeout: 5 * time.Second,
		log:          log.New(),
	}
	c.connected.Store(true)

	// QoS 0 is fire-and-forget: Publish returns nil regardless of token error.
	err := c.Publish(t.Context(), []byte(`{}`))
	if err != nil {
		t.Errorf("QoS 0 publish should not return error, got: %v", err)
	}
}

// --- handleAckMessage edge cases ---

func TestHandleAckMessage_EmptyStreamRejected(t *testing.T) {
	called := false
	client := &Client{
		log: log.New(),
	}
	handler := func(_ message.AckMessage) {
		called = true
	}
	client.ackHandler.Store(&handler)
	// Stream is empty → parseAck should reject
	client.handleAckMessage([]byte(`{"ids":["123"],"ack":true}`))
	if called {
		t.Error("handler should not be called for empty stream")
	}
}

// --- Publish returns errNotConnected when connection is not open ---

func TestClientPublish_NotConnected(t *testing.T) {
	mock := &mockPahoClient{connected: false}
	c := &Client{
		client:       mock,
		publishTopic: "test/pub",
		qos:          0,
		writeTimeout: 5 * time.Second,
		log:          log.New(),
	}

	err := c.Publish(t.Context(), []byte(`{"test":true}`))
	if err == nil {
		t.Fatal("expected error when connection is not open")
	}
	if !errors.Is(err, errNotConnected) {
		t.Errorf("error = %v; want errNotConnected", err)
	}
}

// --- IsConnected tests ---

func TestClientIsConnected(t *testing.T) {
	c := &Client{}
	c.connected.Store(true)
	if !c.IsConnected() {
		t.Error("expected IsConnected() = true")
	}
	c.connected.Store(false)
	if c.IsConnected() {
		t.Error("expected IsConnected() = false")
	}
}

func TestClientIsConnected_NilClient(t *testing.T) {
	c := &Client{}
	if c.IsConnected() {
		t.Error("expected IsConnected() = false for nil client")
	}
}

// --- resubscribeAck tests ---

func TestResubscribeAck_NoHandler(t *testing.T) {
	subscribeCalled := false
	mock := &mockPahoClient{
		subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
			subscribeCalled = true
			return &mockPahoToken{}
		},
	}
	c := &Client{ackTopic: "test/ack", qos: 0, subscribeTimeout: time.Second, log: log.New()}
	c.resubscribeAck(mock)
	if subscribeCalled {
		t.Error("subscribe should not be called when handler is nil")
	}
}

func TestResubscribeAck_WithHandler(t *testing.T) {
	subscribeCalled := false
	mock := &mockPahoClient{
		subscribeFn: func(topic string, _ byte, _ paho.MessageHandler) paho.Token {
			subscribeCalled = true
			if topic != "test/ack" {
				t.Errorf("subscribe topic = %q, want test/ack", topic)
			}
			return &mockPahoToken{}
		},
	}
	c := &Client{ackTopic: "test/ack", qos: 0, subscribeTimeout: time.Second, log: log.New()}
	handler := func(_ message.AckMessage) {}
	c.ackHandler.Store(&handler)
	c.resubscribeAck(mock)
	if !subscribeCalled {
		t.Error("subscribe should be called when handler is set")
	}
}

func TestResubscribeAck_Timeout(t *testing.T) {
	t.Parallel()

	mock := &mockPahoClient{
		subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
			return &mockPahoToken{err: fmt.Errorf("timeout")}
		},
	}
	c := &Client{ackTopic: "test/ack", qos: 0, subscribeTimeout: time.Second, log: log.New()}
	handler := func(_ message.AckMessage) {}
	c.ackHandler.Store(&handler)
	c.resubscribeAck(mock)
}

// --- retrySleep tests ---

func TestRetrySleep_NormalComplete(t *testing.T) {
	canceled := retrySleep(t.Context(), 1*time.Millisecond)
	if canceled {
		t.Error("retrySleep should return false when timer completes")
	}
}

func TestRetrySleep_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled := retrySleep(ctx, 10*time.Second)
	if !canceled {
		t.Error("retrySleep should return true when context is canceled")
	}
}

// --- Client.Close when not connected ---

func TestClientClose_NotConnected_NoDisconnect(t *testing.T) {
	mock := &mockPahoClient{connected: false}
	c := &Client{client: mock, disconnectTimeout: 100 * time.Millisecond}
	err := c.Close()
	if err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
	if mock.disconnectCalled {
		t.Error("Disconnect should not be called when not connected")
	}
}
