package hotpath

import (
	"context"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/message"
)

// mockRedis implements redis.StreamClient for testing.
type mockRedis struct {
	readBatchFn    func(ctx context.Context) (message.Batch, error)
	claimIdleFn    func(ctx context.Context) (message.Batch, error)
	ackAndDeleteFn func(ctx context.Context, ids []string, stream string) error
	cleanupFn      func(ctx context.Context, idle time.Duration) error
	refreshFn      func(ctx context.Context) (int, error)
	closeFn        func() error
}

func (m *mockRedis) ReadBatch(ctx context.Context) (message.Batch, error) {
	if m.readBatchFn != nil {
		return m.readBatchFn(ctx)
	}
	return message.Batch{}, nil
}

func (m *mockRedis) ClaimIdle(ctx context.Context) (message.Batch, error) {
	if m.claimIdleFn != nil {
		return m.claimIdleFn(ctx)
	}
	return message.Batch{}, nil
}

func (m *mockRedis) AckAndDeleteBatch(ctx context.Context, ids []string, stream string) error {
	if m.ackAndDeleteFn != nil {
		return m.ackAndDeleteFn(ctx, ids, stream)
	}
	return nil
}

func (m *mockRedis) CleanupDeadConsumers(ctx context.Context, idle time.Duration) error {
	if m.cleanupFn != nil {
		return m.cleanupFn(ctx, idle)
	}
	return nil
}

func (m *mockRedis) RefreshStreams(ctx context.Context) (int, error) {
	if m.refreshFn != nil {
		return m.refreshFn(ctx)
	}
	return 0, nil
}

func (m *mockRedis) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

// mockPublisher implements mqtt.Publisher for testing.
type mockPublisher struct {
	publishFn      func(ctx context.Context, payload message.Payload) error
	subscribeAckFn func(handler func(message.AckMessage)) error
	closeFn        func() error
}

func (m *mockPublisher) Publish(ctx context.Context, payload message.Payload) error {
	if m.publishFn != nil {
		return m.publishFn(ctx, payload)
	}
	return nil
}

func (m *mockPublisher) SubscribeAck(handler func(message.AckMessage)) error {
	if m.subscribeAckFn != nil {
		return m.subscribeAckFn(handler)
	}
	return nil
}

func (m *mockPublisher) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}
