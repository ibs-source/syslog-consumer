package processor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

// fakes specialized for DLQ path

type dlqFakeRedis struct{}

func (f *dlqFakeRedis) CreateConsumerGroup(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (f *dlqFakeRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	// No consumption in this test, we call processMessage directly
	return []*domain.Message{}, nil
}
func (f *dlqFakeRedis) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (f *dlqFakeRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (f *dlqFakeRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (f *dlqFakeRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (f *dlqFakeRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (f *dlqFakeRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (f *dlqFakeRedis) ReadStreamMessages(_ context.Context, _ string, _ string, _ int64) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (f *dlqFakeRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{}, nil
}
func (f *dlqFakeRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (f *dlqFakeRedis) GetConsumerName() string      { return "dlq-consumer" }
func (f *dlqFakeRedis) Ping(_ context.Context) error { return nil }
func (f *dlqFakeRedis) Close() error                 { return nil }

// mqtt fake that records publishes (including DLQ publish)
type recordMQTT struct {
	publishes []struct {
		Topic string
		Data  []byte
	}
}

func (m *recordMQTT) Connect(_ context.Context) error                  { return nil }
func (m *recordMQTT) Disconnect(_ time.Duration)                       {}
func (m *recordMQTT) IsConnected() bool                                { return true }
func (m *recordMQTT) GetUserPrefix() string                            { return "" }
func (m *recordMQTT) Unsubscribe(_ context.Context, _ ...string) error { return nil }
func (m *recordMQTT) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *recordMQTT) Publish(_ context.Context, topic string, _ byte, _ bool, payload []byte) error {
	m.publishes = append(m.publishes, struct {
		Topic string
		Data  []byte
	}{Topic: topic, Data: payload})
	return nil
}

type failCB struct{}

func (cb *failCB) Execute(fn func() error) error {
	_ = fn
	return errors.New("cb/open or publish failed")
}
func (cb *failCB) GetState() string { return "open" }
func (cb *failCB) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{Requests: 1, TotalFailure: 1, State: "open"}
}

func TestStreamProcessor_DLQ_OnPublishFailure(t *testing.T) {
	// Prepare a message that will fail main publish
	payload := map[string]any{"a": 1}
	data, _ := json.Marshal(payload)
	msg := &domain.Message{ID: "r-1", Timestamp: time.Now(), Data: data}

	r := &dlqFakeRedis{}
	m := &recordMQTT{}
	cb := &failCB{}
	log, _ := logger.NewLogrusLogger("error", "json")

	// Config with DLQ enabled and retry disabled to force DLQ path immediately
	cfg := minimalConfig()
	cfg.Pipeline.Retry.Enabled = false
	cfg.Pipeline.DLQ.Enabled = true
	cfg.Pipeline.DLQ.Topic = "dlq/topic"

	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), cb, nil)

	// Call processMessage directly to hit error path and DLQ publish
	p.processMessage(msg)

	// Expect one publish to DLQ topic
	if len(m.publishes) != 1 {
		t.Fatalf("expected 1 DLQ publish, got %d", len(m.publishes))
	}
	if m.publishes[0].Topic != cfg.Pipeline.DLQ.Topic {
		t.Fatalf("expected DLQ topic %q, got %q", cfg.Pipeline.DLQ.Topic, m.publishes[0].Topic)
	}
}

func TestStreamProcessor_StopWhenNotRunning(t *testing.T) {
	r := &dlqFakeRedis{}
	m := &recordMQTT{}
	cb := &failCB{}
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()

	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), cb, nil)

	// Stop without Start should error
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Stop(ctx); err == nil {
		t.Fatal("expected error when stopping processor that is not running")
	}
}
