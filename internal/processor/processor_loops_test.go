package processor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

const cbClosedState = "closed"

// ---- Fakes for loops ----

type loopRedis struct {
	once    atomic.Bool
	msgOnce []*domain.Message
}

func (r *loopRedis) CreateConsumerGroup(
	_ context.Context, _ string, _ string, _ string,
) error {
	return nil
}
func (r *loopRedis) ReadMessages(
	ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	// First call returns messages, subsequent call returns error to hit error path
	if r.once.CompareAndSwap(false, true) {
		return r.msgOnce, nil
	}
	// Simulate timeout by sleeping a tad then returning error through context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return nil, context.DeadlineExceeded
	}
}
func (r *loopRedis) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (r *loopRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (r *loopRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *loopRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (r *loopRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (r *loopRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *loopRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (r *loopRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (r *loopRedis) GetConsumerGroupInfo(_ context.Context, _ string, group string) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (r *loopRedis) GetConsumerName() string      { return "loop-consumer" }
func (r *loopRedis) Ping(_ context.Context) error { return nil }
func (r *loopRedis) Close() error                 { return nil }

type mqttCount struct {
	count atomic.Int32
}

func (m *mqttCount) Connect(_ context.Context) error { return nil }
func (m *mqttCount) Disconnect(_ time.Duration)      {}
func (m *mqttCount) IsConnected() bool               { return true }
func (m *mqttCount) GetUserPrefix() string           { return "" }
func (m *mqttCount) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	m.count.Add(1)
	return nil
}
func (m *mqttCount) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *mqttCount) Unsubscribe(_ context.Context, _ ...string) error { return nil }

type cbOK5 struct{}

func (cb *cbOK5) Execute(fn func() error) error { return fn() }
func (cb *cbOK5) GetState() string              { return cbClosedState }
func (cb *cbOK5) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: cbClosedState}
}

// ---- Tests ----

func TestConsumeMessages_BuffersAndThenCancels(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.BlockTime = 5 * time.Millisecond

	msg := &domain.Message{ID: "cm-1", Data: []byte(`{}`)}

	r := &loopRedis{msgOnce: []*domain.Message{msg}}
	m := &mqttCount{}
	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), &cbOK5{}, nil)

	// Run consume loop
	go p.consumeMessages()

	// Wait until the buffer has the message
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if p.buffer.Size() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if p.buffer.Size() < 1 {
		t.Fatalf("expected buffer to have at least 1 message")
	}

	// Cancel processor context and ensure loop exits
	p.cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestProcessMessages_TickerFlushPublishes(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Pipeline.BufferSize = 16
	cfg.Pipeline.BatchSize = 10 // Ensure ticker path (flush < batch size)
	cfg.Pipeline.FlushInterval = 20 * time.Millisecond
	cfg.MQTT.Topics.PublishTopic = "pub/ticker"

	r := &loopRedis{}
	m := &mqttCount{}
	p := NewStreamProcessor(cfg, r, m, log, domain.NewMetrics(), &cbOK5{}, nil)

	// Need a worker pool running because processMessages submits to pool
	p.workerPool = NewWorkerPool(1, 1)
	p.workerPool.Start()
	defer p.workerPool.Stop()

	// Put a single message so ticker will flush it
	msg := &domain.Message{ID: "pm-1", Data: []byte(`{}`)}
	_ = p.buffer.Put(msg)

	// Run process loop
	done := make(chan struct{})
	go func() {
		p.processMessages()
		close(done)
	}()

	// Wait some time for the ticker-based flush and worker to process
	time.Sleep(80 * time.Millisecond)
	p.cancel()
	<-done

	if m.count.Load() == 0 {
		t.Fatalf("expected at least one publish via ticker flush, got 0")
	}
}
