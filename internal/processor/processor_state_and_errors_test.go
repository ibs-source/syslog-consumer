package processor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

func TestGetState_AllEnums(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	p := NewStreamProcessor(cfg, &ackFakeRedis{}, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)

	p.state.Store(int32(StateIdle))
	if s := p.GetState(); s != StateIdleStr {
		t.Fatalf("idle -> %s", s)
	}
	p.state.Store(int32(StateRunning))
	if s := p.GetState(); s != StateRunningStr {
		t.Fatalf("running -> %s", s)
	}
	p.state.Store(int32(StatePaused))
	if s := p.GetState(); s != StatePausedStr {
		t.Fatalf("paused -> %s", s)
	}
	p.state.Store(int32(StateStopping))
	if s := p.GetState(); s != StateStoppingStr {
		t.Fatalf("stopping -> %s", s)
	}
	p.state.Store(int32(StateStopped))
	if s := p.GetState(); s != StateStoppedStr {
		t.Fatalf("stopped -> %s", s)
	}
	p.state.Store(999) // unknown
	if s := p.GetState(); s != "unknown" {
		t.Fatalf("unknown -> %s", s)
	}
}

// ---- Fakes to drive error branches ----

type drainErrRedis struct{}

func (r *drainErrRedis) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return nil
}
func (r *drainErrRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *drainErrRedis) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *drainErrRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *drainErrRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *drainErrRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *drainErrRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *drainErrRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *drainErrRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *drainErrRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *drainErrRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, _ string,
) (*ports.ConsumerGroupInfo, error) {
	return nil, errors.New("group info error")
}
func (r *drainErrRedis) GetConsumerName() string      { return "drain-err" }
func (r *drainErrRedis) Ping(_ context.Context) error { return nil }
func (r *drainErrRedis) Close() error                 { return nil }

type cleanupErrRedis struct {
	consumers []ports.ConsumerInfo
}

func (r *cleanupErrRedis) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return nil
}
func (r *cleanupErrRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *cleanupErrRedis) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *cleanupErrRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *cleanupErrRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *cleanupErrRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *cleanupErrRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return r.consumers, nil
}
func (r *cleanupErrRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return errors.New("remove error")
}
func (r *cleanupErrRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *cleanupErrRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *cleanupErrRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *cleanupErrRedis) GetConsumerName() string      { return "self" }
func (r *cleanupErrRedis) Ping(_ context.Context) error { return nil }
func (r *cleanupErrRedis) Close() error                 { return nil }

type mqttNil struct{}

func (m *mqttNil) Connect(_ context.Context) error { return nil }
func (m *mqttNil) Disconnect(_ time.Duration)      {}
func (m *mqttNil) IsConnected() bool               { return true }
func (m *mqttNil) GetUserPrefix() string           { return "" }
func (m *mqttNil) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return nil
}
func (m *mqttNil) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *mqttNil) Unsubscribe(_ context.Context, _ ...string) error { return nil }

type cbOKEdge struct{}

func (cb *cbOKEdge) Execute(fn func() error) error { return fn() }
func (cb *cbOKEdge) GetState() string              { return cbClosedState }
func (cb *cbOKEdge) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: cbClosedState}
}

func TestPerformDrain_GroupInfoErrorPath(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	r := &drainErrRedis{}
	p := NewStreamProcessor(cfg, r, &mqttNil{}, log, domain.NewMetrics(), &cbOKEdge{}, nil)
	// Should take error branch and return without panic
	p.performDrain()
}

func TestPerformConsumerCleanup_RemoveErrorPath(t *testing.T) {
	t.Helper()
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.ConsumerCleanupEnabled = true
	cfg.Redis.ConsumerIdleTimeout = 1 * time.Millisecond

	r := &cleanupErrRedis{
		consumers: []ports.ConsumerInfo{
			{Name: "self", Pending: 0, Idle: 0},
			{Name: "other", Pending: 0, Idle: 1 * time.Hour},
		},
	}
	p := NewStreamProcessor(cfg, r, &mqttNil{}, log, domain.NewMetrics(), &cbOKEdge{}, nil)
	p.performConsumerCleanup() // should hit remove error branch
}
