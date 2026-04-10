// Package redis provides Redis stream client and interface abstractions.
package redis

import (
	"context"
	"io"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/message"
)

// StreamClient combines all Redis stream operations required by the hot path.
// It is satisfied by *Client and by test mocks.
type StreamClient interface {
	// ReadBatch fetches the next batch of messages from Redis streams.
	ReadBatch(ctx context.Context) (message.Batch, error)

	// ClaimIdle reclaims stale pending messages from other consumers.
	ClaimIdle(ctx context.Context) (message.Batch, error)

	// AckAndDeleteBatch acknowledges and deletes a batch of messages from a Redis stream
	// using a single pipeline round-trip.
	AckAndDeleteBatch(ctx context.Context, ids []string, stream string) error

	// CleanupDeadConsumers removes consumers that have been idle longer than idleTimeout.
	CleanupDeadConsumers(ctx context.Context, idleTimeout time.Duration) error

	// RefreshStreams rediscovers streams in multi-stream mode.
	// Returns the number of newly discovered streams.
	RefreshStreams(ctx context.Context) (int, error)

	// Close releases the Redis connection.
	io.Closer
}

// compile-time interface satisfaction check
var _ StreamClient = (*Client)(nil)
