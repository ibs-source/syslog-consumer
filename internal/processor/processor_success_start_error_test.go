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

// ---- Fakes for success path ----

type okMQTTPub struct {
	pubs []struct {
		Topic string
		Data  []byte
	}
}

func (m *okMQTTPub) Connect(_ context.Context) error { return nil }
func (m *okMQTTPub) Disconnect(_ time.Duration)      {}
func (m *okMQTTPub) IsConnected() bool               { return true }
func (m *okMQTTPub) GetUserPrefix() string           { return "" }
func (m *okMQTTPub) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *okMQTTPub) Unsubscribe(_ context.Context, _ ...string) error { return nil }
func (m *okMQTTPub) Publish(_ context.Context, topic string, _ byte, _ bool, payload []byte) error {
	m.pubs = append(m.pubs, struct {
		Topic string
		Data  []byte
	}{Topic: topic, Data: payload})
	return nil
}

type cbOK3 struct{}

func (cb *cbOK3) Execute(fn func() error) error { return fn() }
func (cb *cbOK3) GetState() string              { return cbClosedState }
func (cb *cbOK3) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: cbClosedState}
}

type nopRedis2 struct{}

func (r *nopRedis2) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return nil
}
func (r *nopRedis2) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *nopRedis2) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *nopRedis2) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *nopRedis2) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *nopRedis2) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *nopRedis2) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *nopRedis2) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *nopRedis2) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *nopRedis2) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *nopRedis2) GetConsumerGroupInfo(_ context.Context, _ string, group string) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *nopRedis2) GetConsumerName() string      { return "nop2" }
func (r *nopRedis2) Ping(_ context.Context) error { return nil }
func (r *nopRedis2) Close() error                 { return nil }

func TestProcessMessage_Success_PublishesAndSignals(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.MQTT.Topics.PublishTopic = "pub/topic"

	m := &okMQTTPub{}
	doneCh := make(chan struct{}, 1)
	p := NewStreamProcessor(cfg, &nopRedis2{}, m, log, domain.NewMetrics(), &cbOK3{}, doneCh)

	msg := &domain.Message{ID: "ok-1", Data: []byte(`{"k":"v"}`)}
	p.processMessage(msg)

	// Expect one publish recorded
	if len(m.pubs) != 1 || m.pubs[0].Topic != "pub/topic" {
		t.Fatalf("expected publish to pub/topic, got %#v", m.pubs)
	}

	// Expect metrics updated and channel signaled
	if p.metrics.MessagesPublished.Load() == 0 {
		t.Fatalf("expected MessagesPublished > 0")
	}

	select {
	case <-doneCh:
		// ok
	default:
		t.Fatalf("expected messageProcessedCh to receive")
	}
}

// ---- Start error path: CreateConsumerGroup failure should keep processor idle ----

type cgFailRedis struct{}

func (r *cgFailRedis) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return errors.New("xgroup create failed")
}
func (r *cgFailRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *cgFailRedis) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *cgFailRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *cgFailRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *cgFailRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *cgFailRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *cgFailRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *cgFailRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *cgFailRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *cgFailRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *cgFailRedis) GetConsumerName() string      { return "cg-fail" }
func (r *cgFailRedis) Ping(_ context.Context) error { return nil }
func (r *cgFailRedis) Close() error                 { return nil }

type cbOK4 struct{}

func (cb *cbOK4) Execute(fn func() error) error { return fn() }
func (cb *cbOK4) GetState() string              { return cbClosedState }
func (cb *cbOK4) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: cbClosedState}
}

func TestStart_ErrorOnCreateConsumerGroup_KeepsIdle(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()

	p := NewStreamProcessor(cfg, &cgFailRedis{}, &okMQTTPub{}, log, domain.NewMetrics(), &cbOK4{}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Start(ctx); err == nil {
		t.Fatalf("expected error from Start when CreateConsumerGroup fails")
	}

	if st := p.GetState(); st != StateIdleStr {
		t.Fatalf("expected state to remain idle after Start failure, got %s", st)
	}
}
