package processor

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/config"
	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

// ---------- Fakes ----------

type fakeRedis struct {
	messagesOnce     atomic.Bool
	messages         []*domain.Message
	createdGroup     atomic.Bool
	ackedIDs         []string
	deletedIDs       []string
	consumerName     string
	pingCount        atomic.Int32
	getConsumersResp []ports.ConsumerInfo
}

func (f *fakeRedis) CreateConsumerGroup(_ context.Context, _ string, _ string, _ string) error {
	f.createdGroup.Store(true)
	return nil
}

func (f *fakeRedis) ReadMessages(
	ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	// Return messages only once
	if f.messagesOnce.CompareAndSwap(false, true) {
		return f.messages, nil
	}
	// Simulate a small block
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(10 * time.Millisecond):
	}
	return []*domain.Message{}, nil
}

func (f *fakeRedis) AckMessages(_ context.Context, _ string, _ string, ids ...string) error {
	f.ackedIDs = append(f.ackedIDs, ids...)
	return nil
}

func (f *fakeRedis) DeleteMessages(_ context.Context, _ string, ids ...string) error {
	f.deletedIDs = append(f.deletedIDs, ids...)
	return nil
}

func (f *fakeRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}

func (f *fakeRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}

func (f *fakeRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return f.getConsumersResp, nil
}

func (f *fakeRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}

func (f *fakeRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}

func (f *fakeRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}

func (f *fakeRedis) GetConsumerGroupInfo(_ context.Context, _ string, group string) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}

func (f *fakeRedis) GetConsumerName() string {
	if f.consumerName != "" {
		return f.consumerName
	}
	return "test-consumer"
}

func (f *fakeRedis) Ping(_ context.Context) error {
	f.pingCount.Add(1)
	return nil
}

func (f *fakeRedis) Close() error {
	return nil
}

type fakeMQTT struct {
	connected atomic.Bool
	publishes []struct {
		Topic   string
		QoS     byte
		Retain  bool
		Payload []byte
	}
	subscribed []string

	failPublish bool
}

func (m *fakeMQTT) Connect(_ context.Context) error { m.connected.Store(true); return nil }
func (m *fakeMQTT) Disconnect(_ time.Duration)      { m.connected.Store(false) }
func (m *fakeMQTT) IsConnected() bool               { return true }
func (m *fakeMQTT) GetUserPrefix() string           { return "" }
func (m *fakeMQTT) Unsubscribe(_ context.Context, _ ...string) error {
	return nil
}
func (m *fakeMQTT) Publish(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
	if m.failPublish {
		return errors.New("publish failed")
	}
	m.publishes = append(m.publishes, struct {
		Topic   string
		QoS     byte
		Retain  bool
		Payload []byte
	}{Topic: topic, QoS: qos, Retain: retained, Payload: payload})
	return nil
}
func (m *fakeMQTT) Subscribe(_ context.Context, topic string, _ byte, _ ports.MessageHandler) error {
	m.subscribed = append(m.subscribed, topic)
	return nil
}

type fakeCB struct {
	fail bool
}

func (cb *fakeCB) Execute(fn func() error) error {
	if cb.fail {
		return errors.New("cb open")
	}
	return fn()
}
func (cb *fakeCB) GetState() string {
	if cb.fail {
		return "open"
	}
	return "closed"
}
func (cb *fakeCB) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{Requests: 1, TotalSuccess: 1, State: cb.GetState()}
}

// ---------- Test helpers ----------

func buildApp() config.AppConfig {
	return config.AppConfig{
		Name:            "test",
		Environment:     "test",
		LogLevel:        "debug",
		LogFormat:       "json",
		ShutdownTimeout: 2 * time.Second,
	}
}

func buildRedis() config.RedisConfig {
	return config.RedisConfig{
		StreamName:              "stream",
		ConsumerGroup:           "group",
		ClaimMinIdleTime:        10 * time.Millisecond,
		ClaimBatchSize:          10,
		PendingCheckInterval:    50 * time.Millisecond,
		ClaimInterval:           50 * time.Millisecond,
		BatchSize:               10,
		BlockTime:               5 * time.Millisecond,
		AggressiveClaim:         false,
		DrainEnabled:            false,
		DrainInterval:           50 * time.Millisecond,
		DrainBatchSize:          100,
		ConsumerCleanupEnabled:  false,
		ConsumerIdleTimeout:     1 * time.Minute,
		ConsumerCleanupInterval: 1 * time.Minute,
	}
}

func buildMQTT() config.MQTTConfig {
	return config.MQTTConfig{
		QoS:            1,
		KeepAlive:      10 * time.Second,
		ConnectTimeout: 100 * time.Millisecond,
		Topics: config.TopicConfig{
			PublishTopic:   "out/topic",
			SubscribeTopic: "ack/topic",
			UseUserPrefix:  false,
		},
		WriteTimeout: 100 * time.Millisecond,
	}
}

func buildResource() config.ResourceConfig {
	return config.ResourceConfig{
		MinWorkers: 1,
		MaxWorkers: 2,
	}
}

func buildPipeline() config.PipelineConfig {
	return config.PipelineConfig{
		BufferSize:            64,
		BatchSize:             1,
		FlushInterval:         10 * time.Millisecond,
		BackpressureThreshold: 0.9,
		DropPolicy:            "newest",
		Retry: config.RetryConfig{
			Enabled:        true,
			MaxAttempts:    2,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			Multiplier:     2,
		},
		DLQ: config.DLQConfig{
			Enabled: false,
		},
	}
}

func buildCB() config.CircuitBreakerConfig {
	return config.CircuitBreakerConfig{
		Enabled:                true,
		ErrorThreshold:         50,
		SuccessThreshold:       1,
		Timeout:                100 * time.Millisecond,
		MaxConcurrentCalls:     10,
		RequestVolumeThreshold: 1,
	}
}

// ---------- Test helpers ----------

func minimalConfig() *config.Config {
	return &config.Config{
		App:            buildApp(),
		Redis:          buildRedis(),
		MQTT:           buildMQTT(),
		Resource:       buildResource(),
		Pipeline:       buildPipeline(),
		CircuitBreaker: buildCB(),
	}
}

// ---------- Tests ----------

func TestStreamProcessor_PublishesAndStartsStops(t *testing.T) {
	// Prepare message to process
	payload := map[string]interface{}{"foo": "bar"}
	data, _ := json.Marshal(payload)
	msg := &domain.Message{ID: "1-1", Timestamp: time.Now(), Data: data}

	fRedis := &fakeRedis{
		messages: []*domain.Message{msg},
	}
	fMQTT := &fakeMQTT{}
	cb := &fakeCB{fail: false}

	// Real logger (stdout), should not panic
	log, err := logger.NewLogrusLogger("debug", "json")
	if err != nil {
		t.Fatalf("logger init: %v", err)
	}

	cfg := minimalConfig()

	doneCh := make(chan struct{}, 1)
	p := NewStreamProcessor(cfg, fRedis, fMQTT, log, domain.NewMetrics(), cb, doneCh)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start error: %v", err)
	}

	// Wait until the message is processed (signal channel or timeout)
	select {
	case <-doneCh:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for message processed")
	}

	// Ensure publish happened
	if len(fMQTT.publishes) == 0 {
		t.Fatalf("expected at least one publish, got %d", len(fMQTT.publishes))
	}

	// Stop the processor gracefully
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := p.Stop(stopCtx); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	// Validate state string
	if state := p.GetState(); state != StateStoppedStr {
		t.Fatalf("expected state 'stopped', got %q", state)
	}
}
