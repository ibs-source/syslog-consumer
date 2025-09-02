package processor

import (
	"context"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

// Redis stub that returns one claimed message then none
type redisClaimOnce struct {
	calls int
}

func (r *redisClaimOnce) CreateConsumerGroup(_ context.Context, _, _, _ string) error {
	return nil
}
func (r *redisClaimOnce) ReadMessages(
	_ context.Context, _, _, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisClaimOnce) AckMessages(_ context.Context, _, _ string, _ ...string) error {
	return nil
}
func (r *redisClaimOnce) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *redisClaimOnce) ClaimPendingMessages(
	_ context.Context, _, _, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	if r.calls == 0 {
		r.calls++
		return []*domain.Message{{ID: "c-1", Data: []byte(`{}`), Timestamp: time.Now()}}, nil
	}
	return []*domain.Message{}, nil
}
func (r *redisClaimOnce) GetPendingMessages(
	_ context.Context, _, _ string, _, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *redisClaimOnce) GetConsumers(_ context.Context, _, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *redisClaimOnce) RemoveConsumer(_ context.Context, _, _, _ string) error {
	return nil
}
func (r *redisClaimOnce) ReadStreamMessages(_ context.Context, _ string, _ string, _ int64) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *redisClaimOnce) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *redisClaimOnce) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *redisClaimOnce) GetConsumerName() string      { return "claim-once" }
func (r *redisClaimOnce) Ping(_ context.Context) error { return nil }
func (r *redisClaimOnce) Close() error                 { return nil }

type mqttNoop3 struct{}

func (m *mqttNoop3) Connect(_ context.Context) error { return nil }
func (m *mqttNoop3) Disconnect(_ time.Duration)      {}
func (m *mqttNoop3) IsConnected() bool               { return true }
func (m *mqttNoop3) GetUserPrefix() string           { return "" }
func (m *mqttNoop3) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return nil
}
func (m *mqttNoop3) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *mqttNoop3) Unsubscribe(_ context.Context, _ ...string) error { return nil }

type cbPass4 struct{}

func (cb *cbPass4) Execute(fn func() error) error { return fn() }
func (cb *cbPass4) GetState() string              { return cbClosedState }
func (cb *cbPass4) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: cbClosedState}
}

func TestPerformSingleClaim_AddsToBuffer(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.ClaimBatchSize = 1
	cfg.Redis.ClaimMinIdleTime = 1 * time.Millisecond
	r := &redisClaimOnce{}
	p := NewStreamProcessor(cfg, r, &mqttNoop3{}, log, domain.NewMetrics(), &cbPass4{}, nil)

	// Single claim should buffer one message
	p.performSingleClaim()
	if p.buffer.Size() == 0 {
		t.Fatalf("expected buffer to have a claimed message")
	}
}

func TestPerformAggressiveClaim_WithCycleDelay(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.AggressiveClaim = true
	cfg.Redis.ClaimBatchSize = 1
	cfg.Redis.ClaimMinIdleTime = 1 * time.Millisecond
	cfg.Redis.ClaimCycleDelay = 1 * time.Millisecond

	r := &redisClaimOnce{}
	p := NewStreamProcessor(cfg, r, &mqttNoop3{}, log, domain.NewMetrics(), &cbPass4{}, nil)

	p.performAggressiveClaim()
	if p.buffer.Size() == 0 {
		t.Fatalf("expected buffer to contain at least one claimed message")
	}
}
