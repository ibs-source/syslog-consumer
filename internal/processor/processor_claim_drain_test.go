package processor

import (
	"context"
	"testing"
	"time"

	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/logger"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
)

type claimDrainFakeRedis struct {
	claimBatches [][]*domain.Message

	groupInfo   ports.ConsumerGroupInfo
	streamBatch []*domain.Message

	consumers    []ports.ConsumerInfo
	removed      []string
	consumerName string
}

func (f *claimDrainFakeRedis) CreateConsumerGroup(_ context.Context, _, _, _ string) error {
	return nil
}
func (f *claimDrainFakeRedis) ReadMessages(
	_ context.Context, _ string, _ string, _ string, _ int64, _ time.Duration,
) ([]*domain.Message, error) {
	return []*domain.Message{}, nil
}
func (f *claimDrainFakeRedis) AckMessages(_ context.Context, _, _ string, _ ...string) error {
	return nil
}
func (f *claimDrainFakeRedis) DeleteMessages(_ context.Context, _ string, _ ...string) error {
	return nil
}
func (f *claimDrainFakeRedis) ClaimPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ time.Duration, _ int64,
) ([]*domain.Message, error) {
	if len(f.claimBatches) == 0 {
		return []*domain.Message{}, nil
	}
	b := f.claimBatches[0]
	f.claimBatches = f.claimBatches[1:]
	return b, nil
}
func (f *claimDrainFakeRedis) GetPendingMessages(
	_ context.Context, _ string, _ string, _ string, _ string, _ int64,
) ([]ports.PendingMessage, error) {
	return []ports.PendingMessage{}, nil
}
func (f *claimDrainFakeRedis) GetConsumers(_ context.Context, _, _ string) ([]ports.ConsumerInfo, error) {
	return f.consumers, nil
}
func (f *claimDrainFakeRedis) RemoveConsumer(_ context.Context, _, _, consumer string) error {
	f.removed = append(f.removed, consumer)
	return nil
}
func (f *claimDrainFakeRedis) ReadStreamMessages(
	_ context.Context, _ string, _ string, _ int64,
) ([]*domain.Message, error) {
	return f.streamBatch, nil
}
func (f *claimDrainFakeRedis) GetStreamInfo(_ context.Context, _ string) (*ports.StreamInfo, error) {
	return &ports.StreamInfo{Length: int64(len(f.streamBatch))}, nil
}
func (f *claimDrainFakeRedis) GetConsumerGroupInfo(
	_ context.Context, _ string, _ string,
) (*ports.ConsumerGroupInfo, error) {
	c := f.groupInfo // copy
	return &c, nil
}
func (f *claimDrainFakeRedis) GetConsumerName() string {
	if f.consumerName != "" {
		return f.consumerName
	}
	return "claim-consumer"
}
func (f *claimDrainFakeRedis) Ping(_ context.Context) error { return nil }
func (f *claimDrainFakeRedis) Close() error                 { return nil }

func TestPerformSingleClaim_BuffersClaimedMessages(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.ClaimBatchSize = 10
	cfg.Redis.ClaimMinIdleTime = 1 * time.Millisecond

	f := &claimDrainFakeRedis{
		claimBatches: [][]*domain.Message{
			{
				{ID: "1-1", Data: []byte("{}")},
				{ID: "1-2", Data: []byte("{}")},
			},
		},
	}
	p := NewStreamProcessor(cfg, f, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)

	// Should buffer two messages
	p.performSingleClaim()
	if got := p.buffer.Size(); got != 2 {
		t.Fatalf("expected 2 claimed messages buffered, got %d", got)
	}
}

func TestPerformAggressiveClaim_ExhaustsBatches(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.ClaimBatchSize = 10
	cfg.Redis.ClaimMinIdleTime = 1 * time.Millisecond
	cfg.Redis.ClaimCycleDelay = 0

	f := &claimDrainFakeRedis{
		claimBatches: [][]*domain.Message{
			{{ID: "2-1", Data: []byte("{}")}},
			{{ID: "2-2", Data: []byte("{}")}},
			{}, // stop condition
		},
	}
	p := NewStreamProcessor(cfg, f, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)

	p.performAggressiveClaim()
	if got := p.buffer.Size(); got != 2 {
		t.Fatalf("expected 2 total claimed messages buffered, got %d", got)
	}
}

func TestPerformDrain_BuffersStreamMessages(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.DrainEnabled = true
	cfg.Redis.DrainBatchSize = 10

	f := &claimDrainFakeRedis{
		groupInfo: ports.ConsumerGroupInfo{
			Name:            "group",
			LastDeliveredID: "0-0",
		},
		streamBatch: []*domain.Message{
			{ID: "s-1", Data: []byte("{}")},
			{ID: "s-2", Data: []byte("{}")},
		},
	}
	p := NewStreamProcessor(cfg, f, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)

	p.performDrain()
	if got := p.buffer.Size(); got != 2 {
		t.Fatalf("expected 2 drained messages buffered, got %d", got)
	}
}

func TestPerformConsumerCleanup_RemovesIdle(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	cfg.Redis.ConsumerCleanupEnabled = true
	cfg.Redis.ConsumerIdleTimeout = 10 * time.Millisecond

	f := &claimDrainFakeRedis{
		consumers: []ports.ConsumerInfo{
			{Name: "keep-me", Pending: 0, Idle: 1 * time.Millisecond},
			{Name: "remove-me", Pending: 0, Idle: 1 * time.Hour},
		},
		consumerName: "self",
	}
	p := NewStreamProcessor(cfg, f, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)

	p.performConsumerCleanup()
	if len(f.removed) != 1 || f.removed[0] != "remove-me" {
		t.Fatalf("expected remove-me to be removed, got %#v", f.removed)
	}
}

func TestSubmitMessageTask_ErrorPathIncrementsDropMetric(t *testing.T) {
	log, _ := logger.NewLogrusLogger("error", "json")
	cfg := minimalConfig()
	p := NewStreamProcessor(cfg, &claimDrainFakeRedis{}, &noopMQTT{}, log, domain.NewMetrics(), &passCB{}, nil)

	// Replace with a stopped worker pool to force Submit error
	p.workerPool = NewWorkerPool(1, 1)
	p.workerPool.Start()
	p.workerPool.Stop()

	before := p.metrics.MessagesDropped.Load()
	p.submitMessageTask(&domain.Message{ID: "x", Data: []byte("{}")})
	after := p.metrics.MessagesDropped.Load()

	if after <= before {
		t.Fatalf("expected MessagesDropped to increase on submit error")
	}
}
