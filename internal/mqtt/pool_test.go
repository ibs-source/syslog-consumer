package mqtt

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
)

// --- NewPool tests ---

func TestNewPool_InvalidPoolSize(t *testing.T) {
	_, err := NewPool(t.Context(), testMQTTConfig(), 0, log.New())
	if err == nil {
		t.Fatal("expected error for pool size 0")
	}
	if err.Error() != "mqtt: pool size must be positive" {
		t.Errorf("error = %q; want 'mqtt: pool size must be positive'", err)
	}
}

func TestNewPool_NegativePoolSize(t *testing.T) {
	_, err := NewPool(t.Context(), testMQTTConfig(), -1, log.New())
	if err == nil {
		t.Fatal("expected error for negative pool size")
	}
}

func TestNewPool_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := NewPool(ctx, testMQTTConfig(), 1, log.New())
	if err == nil {
		t.Fatal("expected NewPool to fail with canceled context")
	}
}

// --- Pool.Close tests ---

func TestPoolClose_NilClients(t *testing.T) {
	p := &Pool{} // no clients
	err := p.Close()
	if err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
}

func TestPoolClose_MultipleClients(t *testing.T) {
	m1 := &mockPahoClient{connected: true}
	m2 := &mockPahoClient{connected: true}
	p := &Pool{
		clients: []*Client{
			{client: m1, disconnectTimeout: 100 * time.Millisecond},
			{client: m2, disconnectTimeout: 100 * time.Millisecond},
		},
		size: 2,
	}
	err := p.Close()
	if err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
	if !m1.disconnectCalled || !m2.disconnectCalled {
		t.Error("expected Disconnect() to be called on all clients")
	}
}

func TestPoolClose_MixedState(t *testing.T) {
	mock := &mockPahoClient{connected: false}
	p := &Pool{
		clients: []*Client{
			{client: mock, disconnectTimeout: 100 * time.Millisecond},
			{client: nil}, // nil paho client → Close returns nil
		},
		size: 2,
	}
	err := p.Close()
	if err != nil {
		t.Errorf("Close() error = %v; want nil (both produce no error)", err)
	}
}

// --- Pool.Publish tests ---

func TestPoolPublish_RoundRobin(t *testing.T) {
	var calledCount int
	makeMock := func() *mockPahoClient {
		return &mockPahoClient{
			connected: true,
			publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
				calledCount++
				return &mockPahoToken{}
			},
		}
	}

	c1 := &Client{client: makeMock(), publishTopic: "t", qos: 0, writeTimeout: time.Second, log: log.New()}
	c1.connected.Store(true)
	c2 := &Client{client: makeMock(), publishTopic: "t", qos: 0, writeTimeout: time.Second, log: log.New()}
	c2.connected.Store(true)

	p := &Pool{clients: []*Client{c1, c2}, size: 2}
	ctx := t.Context()

	for range 4 {
		if err := p.Publish(ctx, []byte(`{}`)); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	if calledCount != 4 {
		t.Errorf("expected 4 publishes, got %d", calledCount)
	}
}

func TestPoolPublish_SkipsDisconnected(t *testing.T) {
	var publishedOn int
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
			publishedOn++
			return &mockPahoToken{}
		},
	}

	c1 := &Client{client: &mockPahoClient{}, publishTopic: "t", qos: 0, writeTimeout: time.Second, log: log.New()}
	// c1 NOT connected
	c2 := &Client{client: mock, publishTopic: "t", qos: 0, writeTimeout: time.Second, log: log.New()}
	c2.connected.Store(true)

	p := &Pool{clients: []*Client{c1, c2}, size: 2}

	err := p.Publish(t.Context(), []byte(`{}`))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if publishedOn != 1 {
		t.Errorf("expected 1 publish on connected client, got %d", publishedOn)
	}
}

func TestPoolPublish_AllDisconnected(t *testing.T) {
	c1 := &Client{log: log.New()}
	c2 := &Client{log: log.New()}
	p := &Pool{clients: []*Client{c1, c2}, size: 2}

	err := p.Publish(t.Context(), []byte(`{}`))
	if !errors.Is(err, errNotConnected) {
		t.Errorf("Publish() error = %v; want errNotConnected", err)
	}
}

// --- Pool.PublishFrom tests ---

func TestPoolPublishFrom_Success(t *testing.T) {
	var calledCount int
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
			calledCount++
			return &mockPahoToken{}
		},
	}

	c := &Client{client: mock, publishTopic: "t", qos: 0, writeTimeout: time.Second, log: log.New()}
	c.connected.Store(true)

	p := &Pool{clients: []*Client{c}, size: 1}

	err := p.PublishFrom(t.Context(), []byte(`{}`), 0)
	if err != nil {
		t.Fatalf("PublishFrom() error = %v", err)
	}
	if calledCount != 1 {
		t.Errorf("expected 1 publish, got %d", calledCount)
	}
}

func TestPoolPublishFrom_WrapsAround(t *testing.T) {
	var calledOn []int
	makeClient := func(idx int) *Client {
		mock := &mockPahoClient{
			connected: true,
			publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
				calledOn = append(calledOn, idx)
				return &mockPahoToken{}
			},
		}
		c := &Client{client: mock, publishTopic: "t", qos: 0, writeTimeout: time.Second, log: log.New()}
		c.connected.Store(true)
		return c
	}

	p := &Pool{
		clients: []*Client{makeClient(0), makeClient(1), makeClient(2)},
		size:    3,
	}

	// hint=5 with 3 clients → 5%3=2, should start at index 2
	err := p.PublishFrom(t.Context(), []byte(`{}`), 5)
	if err != nil {
		t.Fatalf("PublishFrom() error = %v", err)
	}
	if len(calledOn) != 1 || calledOn[0] != 2 {
		t.Errorf("expected publish on client 2, got %v", calledOn)
	}
}

func TestPoolPublishFrom_AllDisconnected(t *testing.T) {
	c := &Client{log: log.New()}
	p := &Pool{clients: []*Client{c}, size: 1}

	err := p.PublishFrom(t.Context(), []byte(`{}`), 0)
	if !errors.Is(err, errNotConnected) {
		t.Errorf("PublishFrom() error = %v; want errNotConnected", err)
	}
}

func TestPoolPublishFrom_SkipsDisconnected(t *testing.T) {
	var published bool
	mock := &mockPahoClient{
		connected: true,
		publishFn: func(_ string, _ byte, _ bool, _ any) paho.Token {
			published = true
			return &mockPahoToken{}
		},
	}

	c1 := &Client{log: log.New()} // disconnected
	c2 := &Client{client: mock, publishTopic: "t", qos: 0, writeTimeout: time.Second, log: log.New()}
	c2.connected.Store(true)

	p := &Pool{clients: []*Client{c1, c2}, size: 2}

	err := p.PublishFrom(t.Context(), []byte(`{}`), 0)
	if err != nil {
		t.Fatalf("PublishFrom() error = %v", err)
	}
	if !published {
		t.Error("expected publish on connected client")
	}
}

// --- Pool.SubscribeAck tests ---

func TestPoolSubscribeAck_AllClients(t *testing.T) {
	subscribeCount := 0
	makeMock := func() *mockPahoClient {
		return &mockPahoClient{
			subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
				subscribeCount++
				return &mockPahoToken{}
			},
		}
	}
	p := &Pool{
		clients: []*Client{
			{client: makeMock(), ackTopic: "ack", qos: 0, subscribeTimeout: time.Second, log: log.New()},
			{client: makeMock(), ackTopic: "ack", qos: 0, subscribeTimeout: time.Second, log: log.New()},
			{client: makeMock(), ackTopic: "ack", qos: 0, subscribeTimeout: time.Second, log: log.New()},
		},
		size: 3,
	}
	err := p.SubscribeAck(func(_ message.AckMessage) {})
	if err != nil {
		t.Errorf("SubscribeAck() error = %v", err)
	}
	if subscribeCount != 3 {
		t.Errorf("expected subscribe called on 3 clients, got %d", subscribeCount)
	}
}

func TestPoolSubscribeAck_ErrorPropagation(t *testing.T) {
	mock := &mockPahoClient{
		subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
			return &mockPahoToken{err: fmt.Errorf("subscribe failed")}
		},
	}
	p := &Pool{
		clients: []*Client{
			{client: mock, ackTopic: "ack", qos: 0, subscribeTimeout: time.Second, log: log.New()},
		},
		size: 1,
	}
	err := p.SubscribeAck(func(_ message.AckMessage) {})
	if err == nil {
		t.Error("expected error when subscribe fails")
	}
}

func TestPoolSubscribeAck_StopsOnFirstError(t *testing.T) {
	callCount := 0
	p := &Pool{
		clients: []*Client{
			{
				client: &mockPahoClient{
					subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
						callCount++
						return &mockPahoToken{err: fmt.Errorf("fail")}
					},
				},
				ackTopic: "ack", qos: 0, subscribeTimeout: time.Second, log: log.New(),
			},
			{
				client: &mockPahoClient{
					subscribeFn: func(_ string, _ byte, _ paho.MessageHandler) paho.Token {
						callCount++
						return &mockPahoToken{}
					},
				},
				ackTopic: "ack", qos: 0, subscribeTimeout: time.Second, log: log.New(),
			},
		},
		size: 2,
	}
	err := p.SubscribeAck(func(_ message.AckMessage) {})
	if err == nil {
		t.Fatal("expected error from SubscribeAck")
	}
	if callCount != 1 {
		t.Errorf("expected 1 subscribe attempt (stop on error), got %d", callCount)
	}
}

// --- Pool.IsConnected tests ---

func TestPoolIsConnected_OneConnected(t *testing.T) {
	c1 := &Client{}
	c2 := &Client{}
	c2.connected.Store(true)
	p := &Pool{clients: []*Client{c1, c2}, size: 2}
	if !p.IsConnected() {
		t.Error("expected Pool.IsConnected() = true when at least one client is connected")
	}
}

func TestPoolIsConnected_NoneConnected(t *testing.T) {
	c1 := &Client{}
	c2 := &Client{}
	p := &Pool{clients: []*Client{c1, c2}, size: 2}
	if p.IsConnected() {
		t.Error("expected Pool.IsConnected() = false when all disconnected")
	}
}

func TestPoolIsConnected_Empty(t *testing.T) {
	p := &Pool{}
	if p.IsConnected() {
		t.Error("expected Pool.IsConnected() = false for empty pool")
	}
}

// --- closeClients tests ---

func TestCloseClients_SkipsNil(t *testing.T) {
	logger := log.New()
	clients := make([]*Client, 3)
	// Leave all nil — closeClients must handle gracefully.
	closeClients(logger, clients, 3)
	for i, c := range clients {
		if c != nil {
			t.Errorf("client[%d] should still be nil after closeClients", i)
		}
	}
}

func TestCloseClients_ClosesConnected(t *testing.T) {
	logger := log.New()
	m1 := &mockPahoClient{connected: true}
	m2 := &mockPahoClient{connected: true}
	clients := []*Client{
		{client: m1, disconnectTimeout: 50 * time.Millisecond},
		nil,
		{client: m2, disconnectTimeout: 50 * time.Millisecond},
	}
	closeClients(logger, clients, 3)
	if !m1.disconnectCalled {
		t.Error("expected client 0 disconnected")
	}
	if !m2.disconnectCalled {
		t.Error("expected client 2 disconnected")
	}
}

func TestCloseClients_PartialCount(t *testing.T) {
	logger := log.New()
	m := &mockPahoClient{connected: true}
	clients := []*Client{
		{client: m, disconnectTimeout: 50 * time.Millisecond},
		{client: &mockPahoClient{connected: true}, disconnectTimeout: 50 * time.Millisecond},
	}
	// Only close first
	closeClients(logger, clients, 1)
	if !m.disconnectCalled {
		t.Error("expected client 0 disconnected")
	}
}
