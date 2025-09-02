package processor

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

// Local minimal fakes (kept small and deterministic)

type ackFakeRedis struct {
	ackedIDs   []string
	deletedIDs []string

	consumerName string
}

func (f *ackFakeRedis) CreateConsumerGroup(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (f *ackFakeRedis) ReadMessages(
	ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	// No new messages in this test
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
	}
	return []*domain.Message{}, nil
}
func (f *ackFakeRedis) AckMessages(_ context.Context, _ string, _ string, ids ...string) error {
	f.ackedIDs = append(f.ackedIDs, ids...)
	return nil
}
func (f *ackFakeRedis) DeleteMessages(_ context.Context, _ string, ids ...string) error {
	f.deletedIDs = append(f.deletedIDs, ids...)
	return nil
}
func (f *ackFakeRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (f *ackFakeRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (f *ackFakeRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (f *ackFakeRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (f *ackFakeRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (f *ackFakeRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (f *ackFakeRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{
		Name:            group,
		Consumers:       1,
		Pending:         0,
		LastDeliveredID: "0-0",
	}, nil
}
func (f *ackFakeRedis) GetConsumerName() string {
	if f.consumerName != "" {
		return f.consumerName
	}
	return "ack-consumer"
}
func (f *ackFakeRedis) Ping(_ context.Context) error { return nil }
func (f *ackFakeRedis) Close() error                 { return nil }

type noopMQTT struct{ connected atomic.Bool }

func (m *noopMQTT) Connect(_ context.Context) error { m.connected.Store(true); return nil }
func (m *noopMQTT) Disconnect(_ time.Duration)      { m.connected.Store(false) }
func (m *noopMQTT) IsConnected() bool               { return true }
func (m *noopMQTT) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return nil
}
func (m *noopMQTT) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *noopMQTT) Unsubscribe(_ context.Context, _ ...string) error { return nil }
func (m *noopMQTT) GetUserPrefix() string                            { return "" }

type passCB struct{}

func (cb *passCB) Execute(fn func() error) error { return fn() }
func (cb *passCB) GetState() string              { return cbClosedState }
func (cb *passCB) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: "closed"}
}

func TestHandleAckMessage_AcksAndDeletes(t *testing.T) {
	r := &ackFakeRedis{}
	m := &noopMQTT{}
	cb := &passCB{}

	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()

	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), cb, nil)

	ack := domain.AckMessage{ID: "1-1", Ack: true}
	b, _ := json.Marshal(ack)

	// Directly invoke handler (no need to start all goroutines)
	p.handleAckMessage(cfg.MQTT.Topics.SubscribeTopic, b)

	if len(r.ackedIDs) != 1 || r.ackedIDs[0] != "1-1" {
		t.Fatalf("expected acked id 1-1, got %#v", r.ackedIDs)
	}
	if len(r.deletedIDs) != 1 || r.deletedIDs[0] != "1-1" {
		t.Fatalf("expected deleted id 1-1, got %#v", r.deletedIDs)
	}
}

func TestPauseResumeFlow(t *testing.T) {
	r := &ackFakeRedis{}
	m := &noopMQTT{}
	cb := &passCB{}
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()

	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), cb, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start error: %v", err)
	}

	if err := p.Pause(); err != nil {
		t.Fatalf("pause error: %v", err)
	}
	if err := p.Resume(); err != nil {
		t.Fatalf("resume error: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := p.Stop(stopCtx); err != nil {
		t.Fatalf("stop error: %v", err)
	}
}
