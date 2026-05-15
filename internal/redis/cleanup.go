// Package redis provides Redis stream operations and consumer group management.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/metrics"
)

// CleanupDeadConsumers drops every consumer (other than this one) whose idle
// time exceeds idleTimeout, releasing their pending entries back to the group.
func (c *Client) CleanupDeadConsumers(ctx context.Context, idleTimeout time.Duration) error {
	totalRemovedCount := 0

	c.mu.RLock()
	streams := c.streams
	c.mu.RUnlock()

	for _, stream := range streams {
		removedCount, err := c.cleanupDeadConsumersForStream(ctx, stream, idleTimeout)
		if err != nil {
			c.log.Warnf(ctx, "failed to cleanup dead consumers for stream %s: %v", stream, err)
			continue
		}
		totalRemovedCount += removedCount
	}

	if totalRemovedCount > 0 {
		c.log.Infof(ctx, "Cleaned up %d dead consumers", totalRemovedCount)
		metrics.DeadConsumersRemoved.Add(int64(totalRemovedCount))
	}

	return nil
}

func (c *Client) cleanupDeadConsumersForStream(
	ctx context.Context, stream string, idleTimeout time.Duration,
) (int, error) {
	consumers, err := c.rdb.XInfoConsumers(ctx, stream, c.groupName).Result()
	if err != nil {
		if isNoGroupError(err) {
			c.log.Warnf(ctx, "Consumer group missing for stream '%s' during cleanup, recreating", stream)
			if gerr := c.ensureGroups(ctx, []string{stream}); gerr != nil {
				c.log.Warnf(ctx, "Failed to recreate group for stream '%s': %v", stream, gerr)
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
			c.log.Infof(ctx, "Removing dead consumer %s from stream %s (idle for %s)", consumer.Name, stream, consumer.Idle)

			deleted, err := c.rdb.XGroupDelConsumer(ctx, stream, c.groupName, consumer.Name).Result()
			if err != nil {
				c.log.Errorf(ctx, "Failed to delete consumer %s from stream %s: %v", consumer.Name, stream, err)
				continue
			}

			c.log.Infof(ctx, "Deleted consumer %s from stream %s (%d pending messages released)", consumer.Name, stream, deleted)
			removedCount++
		} else {
			c.log.Debugf(ctx, "Consumer %s on stream %s is active (idle for %s)", consumer.Name, stream, consumer.Idle)
		}
	}

	return removedCount, nil
}
