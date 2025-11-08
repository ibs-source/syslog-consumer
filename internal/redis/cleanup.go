// Package redis provides Redis stream operations and consumer group management.
package redis

import (
	"context"
	"fmt"
	"time"
)

// CleanupDeadConsumers removes inactive consumers from the consumer group
func (c *Client) CleanupDeadConsumers(ctx context.Context, idleTimeout time.Duration) error {
	now := time.Now()
	totalRemovedCount := 0

	for _, stream := range c.streams {
		removedCount, err := c.cleanupDeadConsumersForStream(ctx, stream, idleTimeout)
		if err != nil {
			c.log.Warn("failed to cleanup dead consumers for stream %s: %v", stream, err)
			continue
		}
		totalRemovedCount += removedCount
	}

	if totalRemovedCount > 0 {
		c.log.Info("Cleaned up %d dead consumers at %s", totalRemovedCount, now.Format(time.RFC3339))
	}

	return nil
}

func (c *Client) cleanupDeadConsumersForStream(
	ctx context.Context, stream string, idleTimeout time.Duration,
) (int, error) {
	consumers, err := c.rdb.XInfoConsumers(ctx, stream, c.groups[stream]).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get consumers info: %w", err)
	}

	var removedCount int

	for _, consumer := range consumers {
		if consumer.Name == c.consumer {
			continue
		}

		if consumer.Idle > idleTimeout {
			c.log.Info("Removing dead consumer %s from stream %s (idle for %s)", consumer.Name, stream, consumer.Idle)

			deleted, err := c.rdb.XGroupDelConsumer(ctx, stream, c.groups[stream], consumer.Name).Result()
			if err != nil {
				c.log.Error("Failed to delete consumer %s from stream %s: %v", consumer.Name, stream, err)
				continue
			}

			if deleted > 0 {
				c.log.Info("Deleted consumer %s from stream %s (had %d pending messages)", consumer.Name, stream, deleted)
				removedCount++
			}
		} else {
			c.log.Debug("Consumer %s on stream %s is active (idle for %s)", consumer.Name, stream, consumer.Idle)
		}
	}

	return removedCount, nil
}
