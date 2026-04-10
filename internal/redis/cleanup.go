// Package redis provides Redis stream operations and consumer group management.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/metrics"
)

// CleanupDeadConsumers removes inactive consumers from the consumer group.
func (c *Client) CleanupDeadConsumers(ctx context.Context, idleTimeout time.Duration) error {
	totalRemovedCount := 0

	c.mu.RLock()
	streams := c.streams
	c.mu.RUnlock()

	for _, stream := range streams {
		removedCount, err := c.cleanupDeadConsumersForStream(ctx, stream, idleTimeout)
		if err != nil {
			c.log.Warn("failed to cleanup dead consumers for stream %s: %v", stream, err)
			continue
		}
		totalRemovedCount += removedCount
	}

	if totalRemovedCount > 0 {
		c.log.Info("Cleaned up %d dead consumers", totalRemovedCount)
		metrics.DeadConsumersRemoved.Add(int64(totalRemovedCount))
	}

	return nil
}

func (c *Client) cleanupDeadConsumersForStream(
	ctx context.Context, stream string, idleTimeout time.Duration,
) (int, error) {
	consumers, err := c.rdb.XInfoConsumers(ctx, stream, c.groupName).Result()
	if err != nil {
		// Stream or group was deleted; recreate the group so it's ready for future use
		if isNoGroupError(err) {
			c.log.Warn("Consumer group missing for stream '%s' during cleanup, recreating", stream)
			if gerr := c.ensureGroups(ctx, []string{stream}); gerr != nil {
				c.log.Warn("Failed to recreate group for stream '%s': %v", stream, gerr)
			}
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get consumers info: %w", err)
	}

	var removedCount int

	for _, consumer := range consumers {
		if consumer.Name == c.consumer {
			continue
		}

		if consumer.Idle > idleTimeout {
			c.log.Info("Removing dead consumer %s from stream %s (idle for %s)", consumer.Name, stream, consumer.Idle)

			// XGroupDelConsumer returns the number of pending messages the
			// deleted consumer had; the consumer is always removed on success.
			deleted, err := c.rdb.XGroupDelConsumer(ctx, stream, c.groupName, consumer.Name).Result()
			if err != nil {
				c.log.Error("Failed to delete consumer %s from stream %s: %v", consumer.Name, stream, err)
				continue
			}

			c.log.Info("Deleted consumer %s from stream %s (%d pending messages released)", consumer.Name, stream, deleted)
			removedCount++
		} else {
			c.log.Debug("Consumer %s on stream %s is active (idle for %s)", consumer.Name, stream, consumer.Idle)
		}
	}

	return removedCount, nil
}
