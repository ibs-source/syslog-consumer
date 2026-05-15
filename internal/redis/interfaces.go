// Package redis provides Redis stream client and interface abstractions.
package redis

import (
	"context"
	"io"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/message"
)

// StreamClient is the surface area the hot path needs from Redis streams.
// Implemented by *Client and by test mocks.
type StreamClient interface {
	ReadBatch(ctx context.Context) (message.Batch, error)
	ClaimIdle(ctx context.Context) (message.Batch, error)
	// AckAndDeleteBatch issues XACK + XDEL in a single pipeline round-trip.
	AckAndDeleteBatch(ctx context.Context, ids []string, stream string) error
	CleanupDeadConsumers(ctx context.Context, idleTimeout time.Duration) error
	// RefreshStreams rediscovers streams in multi-stream mode and returns the
	// number of newly discovered ones.
	RefreshStreams(ctx context.Context) (int, error)
	io.Closer
}

var _ StreamClient = (*Client)(nil)
