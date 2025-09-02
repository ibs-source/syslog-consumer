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

// ---- Local fakes (unique names to avoid collisions) ----

type negAckRedis struct {
	acked   []string
	deleted []string
}

func (r *negAckRedis) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return nil
}
func (r *negAckRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *negAckRedis) AckMessages(_ context.Context, _ string, _ string, ids ...string) error {
	r.acked = append(r.acked, ids...)
	return nil
}
func (r *negAckRedis) DeleteMessages(_ context.Context, _ string, ids ...string) error {
	r.deleted = append(r.deleted, ids...)
	return nil
}
func (r *negAckRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *negAckRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *negAckRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *negAckRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *negAckRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *negAckRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *negAckRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *negAckRedis) GetConsumerName() string      { return "neg-ack" }
func (r *negAckRedis) Ping(_ context.Context) error { return nil }
func (r *negAckRedis) Close() error                 { return nil }

type mqttOK struct{}

func (m *mqttOK) Connect(_ context.Context) error { return nil }
func (m *mqttOK) Disconnect(_ time.Duration)      {}
func (m *mqttOK) IsConnected() bool               { return true }
func (m *mqttOK) GetUserPrefix() string           { return "" }
func (m *mqttOK) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return nil
}
func (m *mqttOK) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *mqttOK) Unsubscribe(_ context.Context, _ ...string) error { return nil }

type cbOK2 struct{}

func (cb *cbOK2) Execute(fn func() error) error { return fn() }
func (cb *cbOK2) GetState() string              { return cbClosedState }
func (cb *cbOK2) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: cbClosedState}
}

// ---- Tests ----

func TestHandleAckMessage_NegativeAck_NoRedisOps(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.MQTT.Topics.SubscribeTopic = ackTopicConst

	r := &negAckRedis{}
	m := &mqttOK{}
	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), &cbOK2{}, nil)

	b := []byte(`{"id":"1-1","ack":false}`)

	p.handleAckMessage(cfg.MQTT.Topics.SubscribeTopic, b)

	if len(r.acked) != 0 || len(r.deleted) != 0 {
		t.Fatalf("negative ack should not ack/delete: acked=%#v deleted=%#v", r.acked, r.deleted)
	}
}

func TestRetryMessageAfter_DropsWhenBufferFull(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	// Small buffer to force full state
	cfg.Pipeline.BufferSize = 4

	p := NewStreamProcessor(cfg, &negAckRedis{}, &mqttOK{}, log, domain.NewMetrics(), &cbOK2{}, nil)

	// Fill buffer to capacity
	for i := 0; i < cfg.Pipeline.BufferSize; i++ {
		msg := &domain.Message{ID: "x"}
		_ = p.buffer.Put(msg)
	}

	before := p.metrics.MessagesDropped.Load()
	p.retryMessageAfter(0, &domain.Message{ID: "y"})
	after := p.metrics.MessagesDropped.Load()

	if after <= before {
		t.Fatalf("expected MessagesDropped to increase when buffer is full")
	}
}

type failMQTT2 struct{}

func (m *failMQTT2) Connect(_ context.Context) error { return nil }
func (m *failMQTT2) Disconnect(_ time.Duration)      {}
func (m *failMQTT2) IsConnected() bool               { return true }
func (m *failMQTT2) GetUserPrefix() string           { return "" }
func (m *failMQTT2) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *failMQTT2) Unsubscribe(_ context.Context, _ ...string) error { return nil }
func (m *failMQTT2) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return errors.New("publish fail")
}

func TestHandleMessageError_DropsWhenNoDLQAndRetriesDisabled(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Pipeline.Retry.Enabled = false
	cfg.Pipeline.DLQ.Enabled = false

	p := NewStreamProcessor(cfg, &negAckRedis{}, &failMQTT2{}, log, domain.NewMetrics(), &cbOK2{}, nil)

	msg := &domain.Message{ID: "z", Data: []byte(`{}`)}
	before := p.metrics.MessagesDropped.Load()
	p.handleMessageError(msg, errors.New("boom"))
	after := p.metrics.MessagesDropped.Load()

	if after <= before {
		t.Fatalf("expected drop metric to increase when retries disabled and DLQ disabled")
	}
}

func TestStateTransitions_FullCycle(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	m := &mqttOK{}
	r := &negAckRedis{}
	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), &cbOK2{}, nil)

	if s := p.GetState(); s != StateIdleStr {
		t.Fatalf("expected idle initially, got %s", s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("start error: %v", err)
	}
	if s := p.GetState(); s != StateRunningStr {
		t.Fatalf("expected running after Start, got %s", s)
	}

	if err := p.Pause(); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if s := p.GetState(); s != StatePausedStr {
		t.Fatalf("expected paused after Pause, got %s", s)
	}

	if err := p.Resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if s := p.GetState(); s != StateRunningStr {
		t.Fatalf("expected running after Resume, got %s", s)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := p.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if s := p.GetState(); s != StateStoppedStr {
		t.Fatalf("expected stopped after Stop, got %s", s)
	}
}
