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

// ---- Fakes for Start subscribe error ----

type startOKRedis struct{}

func (r *startOKRedis) CreateConsumerGroup(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *startOKRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *startOKRedis) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *startOKRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *startOKRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *startOKRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *startOKRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *startOKRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *startOKRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *startOKRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *startOKRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *startOKRedis) GetConsumerName() string      { return "start-ok" }
func (r *startOKRedis) Ping(_ context.Context) error { return nil }
func (r *startOKRedis) Close() error                 { return nil }

type subErrMQTT struct{}

func (m *subErrMQTT) Connect(_ context.Context) error { return nil }
func (m *subErrMQTT) Disconnect(_ time.Duration)      {}
func (m *subErrMQTT) IsConnected() bool               { return true }
func (m *subErrMQTT) GetUserPrefix() string           { return "" }
func (m *subErrMQTT) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return nil
}
func (m *subErrMQTT) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return errors.New("subscribe fails")
}
func (m *subErrMQTT) Unsubscribe(_ context.Context, _ ...string) error { return nil }

type cbPassStart struct{}

func (cb *cbPassStart) Execute(fn func() error) error { return fn() }
func (cb *cbPassStart) GetState() string              { return cbClosedState }
func (cb *cbPassStart) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: "closed"}
}

func TestStart_SubscribeErrorRestoresIdle(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.MQTT.Topics.SubscribeTopic = ackTopicConst

	p := NewStreamProcessor(cfg, &startOKRedis{}, &subErrMQTT{}, log, domain.NewMetrics(), &cbPassStart{}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Start(ctx); err == nil {
		t.Fatal("expected Start to fail due to subscribe error")
	}
	if st := p.GetState(); st != "idle" {
		t.Fatalf("expected state idle after Start failure, got %s", st)
	}
}

func TestPauseResume_ErrorBranches(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	p := NewStreamProcessor(cfg, &startOKRedis{}, &subErrMQTT{}, log, domain.NewMetrics(), &cbPassStart{}, nil)

	// Pause when not running -> error
	if err := p.Pause(); err == nil {
		t.Fatal("expected Pause error when not running")
	}

	// Set to running, Pause should succeed
	p.state.Store(int32(StateRunning))
	if err := p.Pause(); err != nil {
		t.Fatalf("unexpected Pause error: %v", err)
	}

	// Resume error when not paused (set to running)
	p.state.Store(int32(StateRunning))
	if err := p.Resume(); err == nil {
		t.Fatal("expected Resume error when not paused")
	}
}
