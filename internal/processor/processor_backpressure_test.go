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

// backpressureFakeRedis implements only the methods used indirectly; all no-ops.
type backpressureFakeRedis struct{}

func (f *backpressureFakeRedis) CreateConsumerGroup(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (f *backpressureFakeRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (f *backpressureFakeRedis) AckMessages(_ context.Context, _ string, _ string, _ ...string) error {
	return nil
}
func (f *backpressureFakeRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (f *backpressureFakeRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (f *backpressureFakeRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (f *backpressureFakeRedis) GetConsumers(_ context.Context, _ string, _ string) ([]ports.ConsumerInfo, error) {
	return []ports.ConsumerInfo{}, nil
}
func (f *backpressureFakeRedis) RemoveConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (f *backpressureFakeRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (f *backpressureFakeRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: 0}, nil
}
func (f *backpressureFakeRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, group string,
) (*ports.ConsumerGroupInfo, error) {
	return &ports.ConsumerGroupInfo{Name: group, LastDeliveredID: "0-0"}, nil
}
func (f *backpressureFakeRedis) GetConsumerName() string      { return "bp-consumer" }
func (f *backpressureFakeRedis) Ping(_ context.Context) error { return nil }
func (f *backpressureFakeRedis) Close() error                 { return nil }

type noMQTT struct{}

func (m *noMQTT) Connect(_ context.Context) error { return nil }
func (m *noMQTT) Disconnect(_ time.Duration)      {}
func (m *noMQTT) IsConnected() bool               { return true }
func (m *noMQTT) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	return nil
}
func (m *noMQTT) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *noMQTT) Unsubscribe(_ context.Context, _ ...string) error { return nil }
func (m *noMQTT) GetUserPrefix() string                            { return "" }

type passCBCore struct{}

func (cb *passCBCore) Execute(fn func() error) error { return fn() }
func (cb *passCBCore) GetState() string              { return cbClosedState }
func (cb *passCBCore) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: "closed"}
}

func TestMonitorBackpressure_IncrementsMetrics(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	// Make buffer small and threshold very low to trigger quickly
	cfg.Pipeline.BufferSize = 8
	cfg.Pipeline.BackpressureThreshold = 0.25
	cfg.Pipeline.DropPolicy = "newest"

	p := NewStreamProcessor(cfg, &backpressureFakeRedis{}, &noMQTT{}, log, domain.NewMetrics(), &passCBCore{}, nil)

	// Fill buffer over threshold (usage > 0.25)
	for i := 0; i < 6; i++ {
		msg := &domain.Message{ID: "x", Data: []byte("{}")}
		_ = p.buffer.Put(msg)
	}

	// Run the monitor in background for one tick (~1s)
	done := make(chan struct{})
	go func() {
		p.monitorBackpressure()
		close(done)
	}()

	time.Sleep(1100 * time.Millisecond)
	p.cancel() // stop the monitor
	<-done

	if p.metrics.BackpressureDropped.Load() == 0 {
		t.Fatalf("expected BackpressureDropped > 0 after monitorBackpressure tick, got 0")
	}
}

type retryFailMQTT struct{ fails int }

func (m *retryFailMQTT) Connect(_ context.Context) error                  { return nil }
func (m *retryFailMQTT) Disconnect(_ time.Duration)                       {}
func (m *retryFailMQTT) IsConnected() bool                                { return true }
func (m *retryFailMQTT) GetUserPrefix() string                            { return "" }
func (m *retryFailMQTT) Unsubscribe(_ context.Context, _ ...string) error { return nil }
func (m *retryFailMQTT) Subscribe(_ context.Context, _ string, _ byte, _ ports.MessageHandler) error {
	return nil
}
func (m *retryFailMQTT) Publish(_ context.Context, _ string, _ byte, _ bool, _ []byte) error {
	m.fails++
	return errors.New("publish fails")
}

type passCB2 struct{}

func (cb *passCB2) Execute(fn func() error) error { return fn() }
func (cb *passCB2) GetState() string              { return cbClosedState }
func (cb *passCB2) GetStats() ports.CircuitBreakerStats {
	return ports.CircuitBreakerStats{State: "closed"}
}

func TestRetry_RebuffersOnFailure(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	// Enable retry with short backoff
	cfg.Pipeline.Retry.Enabled = true
	cfg.Pipeline.Retry.MaxAttempts = 2
	cfg.Pipeline.Retry.InitialBackoff = 20 * time.Millisecond
	cfg.Pipeline.Retry.MaxBackoff = 20 * time.Millisecond
	cfg.Pipeline.Retry.Multiplier = 1

	m := &retryFailMQTT{}
	p := NewStreamProcessor(cfg, &backpressureFakeRedis{}, m, log, domain.NewMetrics(), &passCB2{}, nil)

	msg := &domain.Message{ID: "r-1", Data: []byte(`{}`)}
	// Directly invoke processMessage to hit retry logic
	p.processMessage(msg)

	// Give time for retry goroutine to re-buffer
	time.Sleep(50 * time.Millisecond)

	if p.buffer.Size() == 0 {
		t.Fatalf("expected message to be re-buffered after retry backoff")
	}
	if m.fails == 0 {
		t.Fatalf("expected publish to have been attempted and failed")
	}
}
