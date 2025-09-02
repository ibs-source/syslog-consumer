package processor

import (
	"context"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

type unsubMQTT struct {
	subscribed   []string
	unsubscribed []string
}

func (m *unsubMQTT) Connect(_ context.Context) error { return nil }
func (m *unsubMQTT) Disconnect(_ time.Duration)      {}
func (m *unsubMQTT) IsConnected() bool               { return true }
func (m *unsubMQTT) GetUserPrefix() string           { return "" }
func (m *unsubMQTT) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return nil
}
func (m *unsubMQTT) Subscribe(_ context.Context, topic string, _ byte, _ ports.MessageHandler) error {
	m.subscribed = append(m.subscribed, topic)
	return nil
}
func (m *unsubMQTT) Unsubscribe(_ context.Context, topics ...string) error {
	m.unsubscribed = append(m.unsubscribed, topics...)
	return nil
}

type nopRedis struct{}

func (r *nopRedis) CreateConsumerGroup(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *nopRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *nopRedis) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *nopRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *nopRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *nopRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *nopRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *nopRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *nopRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *nopRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *nopRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *nopRedis) GetConsumerName() string      { return "unsub-consumer" }
func (r *nopRedis) Ping(_ context.Context) error { return nil }
func (r *nopRedis) Close() error                 { return nil }

type cbOk struct{}

func (cb *cbOk) Execute(fn func() error) error { return fn() }
func (cb *cbOk) GetState() string              { return cbClosedState }
func (cb *cbOk) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: "closed"}
}

func TestStartAndStop_UnsubscribesAckTopic(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.MQTT.Topics.SubscribeTopic = ackTopicConst

	m := &unsubMQTT{}
	r := &nopRedis{}
	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), &cbOk{}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start error: %v", err)
	}
	if len(m.subscribed) == 0 || m.subscribed[0] != ackTopicConst {
		t.Fatalf("expected to subscribe to ack/topic, got %#v", m.subscribed)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := p.Stop(stopCtx); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	if len(m.unsubscribed) == 0 || m.unsubscribed[0] != ackTopicConst {
		t.Fatalf("expected to unsubscribe ack/topic, got %#v", m.unsubscribed)
	}
}
