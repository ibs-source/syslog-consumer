// Package redis provides a Redis Streams client implementation with conversion helpers and retry logic.
package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ibs-source/syslog/consumer/golang/internal/config"
	"github.com/ibs-source/syslog/consumer/golang/internal/domain"
	"github.com/ibs-source/syslog/consumer/golang/internal/ports"
	"github.com/ibs-source/syslog/consumer/golang/pkg/jsonx"
	goredis "github.com/redis/go-redis/v9"
)

// client implements ports.RedisClient using go-redis v9
type client struct {
	client       goredis.UniversalClient
	cfg          *config.RedisConfig
	logger       ports.Logger
	consumerName string
}

// AckAndDeleteBatch pipelines XACK followed by XDEL for the provided IDs.
// Semantics:
//   - If ids is empty, this is a no-op and returns nil.
//   - Treat redis.Nil as non-error (no data).
//   - Treat NOGROUP as already-cleaned (consistent with AckMessages behavior).
func (c *client) AckAndDeleteBatch(ctx context.Context, stream, group string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	return c.executeWithRetry(ctx, "AckAndDeleteBatch", func(ctx context.Context) error {
		pipe := c.client.Pipeline()
		ackCmd := pipe.XAck(ctx, stream, group, ids...)
		delCmd := pipe.XDel(ctx, stream, ids...)

		_, err := pipe.Exec(ctx)
		if err != nil {
			// Go-redis may return a combined error; handle special cases.
			if errors.Is(err, goredis.Nil) {
				return nil
			}
			if strings.Contains(err.Error(), "NOGROUP") {
				return nil
			}
			return err
		}

		// Check individual command errors post-Exec
		if aerr := ackCmd.Err(); aerr != nil && !errors.Is(aerr, goredis.Nil) && !strings.Contains(aerr.Error(), "NOGROUP") {
			return aerr
		}
		if derr := delCmd.Err(); derr != nil && !errors.Is(derr, goredis.Nil) {
			return derr
		}
		return nil
	})
}

// NewClient creates a new Redis client using the application config
func NewClient(cfg *config.Config, logger ports.Logger) (ports.RedisClient, error) {
	return newClient(&cfg.Redis, logger)
}

// newClient creates a new Redis client using the redis-specific config
func newClient(cfg *config.RedisConfig, logger ports.Logger) (*client, error) {
	c := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:           cfg.Addresses,
		Username:        cfg.Username,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		PoolTimeout:     cfg.PoolTimeout,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		DialTimeout:     cfg.ConnectTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		MasterName:      cfg.MasterName, // for sentinel
	})

	consumerName := fmt.Sprintf("consumer-%s", uuid.New().String())

	return &client{
		client:       c,
		cfg:          cfg,
		logger:       logger.WithFields(ports.Field{Key: "component", Value: "redis-client"}),
		consumerName: consumerName,
	}, nil
}

// CreateConsumerGroup creates a new consumer group if it doesn't exist
func (c *client) CreateConsumerGroup(ctx context.Context, stream, group, start string) error {
	// XGROUP CREATE creates the stream if it doesn't exist
	// We can ignore the "BUSYGROUP" error
	return c.executeWithRetry(ctx, "CreateConsumerGroup", func(ctx context.Context) error {
		err := c.client.XGroupCreateMkStream(ctx, stream, group, start).Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return err
		}
		return nil
	})
}

// ReadMessages reads messages from a stream for a specific consumer
func (c *client) ReadMessages(
	ctx context.Context,
	group, consumer, stream string,
	count int64,
	block time.Duration,
) ([]*domain.Message, error) {
	var messages []*domain.Message

	err := c.executeWithRetry(ctx, "ReadMessages", func(ctx context.Context) error {
		streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"}, // ">" means new messages only
			Count:    int64(count),
			Block:    block,
			NoAck:    false,
		}).Result()

		if err != nil {
			// Treat redis.Nil as "no new messages" (not an error)
			if errors.Is(err, goredis.Nil) {
				messages = []*domain.Message{}
				return nil
			}
			// Handle missing group after Redis restart: auto-create and continue
			if strings.Contains(err.Error(), "NOGROUP") {
				cgErr := c.client.XGroupCreateMkStream(ctx, stream, group, "0-0").Err()
				if cgErr != nil && !strings.Contains(cgErr.Error(), "BUSYGROUP") {
					return cgErr
				}
				messages = []*domain.Message{}
				return nil
			}
			return err
		}

		messages = c.convertXMessages(streams)
		return nil
	})

	return messages, err
}

// AckMessages acknowledges messages in a stream
func (c *client) AckMessages(ctx context.Context, stream, group string, ids ...string) error {
	return c.executeWithRetry(ctx, "AckMessages", func(ctx context.Context) error {
		err := c.client.XAck(ctx, stream, group, ids...).Err()
		if err != nil && strings.Contains(err.Error(), "NOGROUP") {
			// Group missing (e.g., after Redis restart). Treat as already acked/cleaned up.
			return nil
		}
		return err
	})
}

// DeleteMessages deletes messages from a stream
func (c *client) DeleteMessages(ctx context.Context, stream string, ids ...string) error {
	return c.executeWithRetry(ctx, "DeleteMessages", func(ctx context.Context) error {
		return c.client.XDel(ctx, stream, ids...).Err()
	})
}

// GetPendingMessages retrieves the list of pending messages for a consumer
func (c *client) GetPendingMessages(
	ctx context.Context,
	stream, group string,
	start, end string,
	count int64,
) ([]ports.PendingMessage, error) {
	var pendingMessages []ports.PendingMessage

	err := c.executeWithRetry(ctx, "GetPendingMessages", func(ctx context.Context) error {
		pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
			Stream:   stream,
			Group:    group,
			Idle:     0,  // All pending messages
			Consumer: "", // Not filtering by consumer
			Start:    start,
			End:      end,
			Count:    count,
		}).Result()
		if err != nil {
			return err
		}

		pendingMessages = make([]ports.PendingMessage, len(pending))
		for i, p := range pending {
			pendingMessages[i] = ports.PendingMessage{
				ID:         p.ID,
				Consumer:   p.Consumer,
				Idle:       p.Idle,
				RetryCount: p.RetryCount,
			}
		}
		return nil
	})

	return pendingMessages, err
}

// ClaimPendingMessages claims pending messages from other consumers
func (c *client) ClaimPendingMessages(
	ctx context.Context,
	stream, group, consumer string,
	minIdleTime time.Duration,
	count int64,
) ([]*domain.Message, error) {
	var messages []*domain.Message

	err := c.executeWithRetry(ctx, "ClaimPendingMessages", func(ctx context.Context) error {
		// First, get the list of pending messages
		pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
			Stream: stream,
			Group:  group,
			Idle:   minIdleTime,
			Start:  "-",
			End:    "+",
			Count:  count,
		}).Result()
		if err != nil {
			return err
		}

		if len(pending) == 0 {
			return nil
		}

		// Extract message IDs to claim
		ids := make([]string, len(pending))
		for i, p := range pending {
			ids[i] = p.ID
		}

		// Claim the messages
		xmsgs, err := c.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: consumer,
			MinIdle:  minIdleTime,
			Messages: ids,
		}).Result()
		if err != nil {
			return err
		}

		messages = c.convertXMessages([]goredis.XStream{{Stream: stream, Messages: xmsgs}})
		return nil
	})

	return messages, err
}

// GetConsumers returns a list of consumers in a group
func (c *client) GetConsumers(ctx context.Context, stream, group string) ([]ports.ConsumerInfo, error) {
	var consumers []ports.ConsumerInfo

	err := c.executeWithRetry(ctx, "GetConsumers", func(ctx context.Context) error {
		info, err := c.client.XInfoConsumers(ctx, stream, group).Result()
		if err != nil {
			return err
		}

		consumers = make([]ports.ConsumerInfo, len(info))
		for i, consumer := range info {
			consumers[i] = ports.ConsumerInfo{
				Name:    consumer.Name,
				Pending: consumer.Pending,
				Idle:    consumer.Idle,
			}
		}
		return nil
	})

	return consumers, err
}

// RemoveConsumer removes a consumer from a group
func (c *client) RemoveConsumer(ctx context.Context, stream, group, consumer string) error {
	return c.executeWithRetry(ctx, "RemoveConsumer", func(ctx context.Context) error {
		return c.client.XGroupDelConsumer(ctx, stream, group, consumer).Err()
	})
}

// ReadStreamMessages reads messages from a stream (not a consumer group)
func (c *client) ReadStreamMessages(ctx context.Context, stream, start string, count int64) ([]*domain.Message, error) {
	var messages []*domain.Message

	err := c.executeWithRetry(ctx, "ReadStreamMessages", func(ctx context.Context) error {
		streams, err := c.client.XRead(ctx, &goredis.XReadArgs{
			Streams: []string{stream, start},
			Count:   count,
		}).Result()
		if err != nil {
			// Treat redis.Nil as "no messages available"
			if errors.Is(err, goredis.Nil) {
				messages = []*domain.Message{}
				return nil
			}
			return err
		}

		messages = c.convertXMessages(streams)
		return nil
	})

	return messages, err
}

// GetConsumerGroupInfo returns information about a consumer group
func (c *client) GetConsumerGroupInfo(ctx context.Context, stream, group string) (*ports.ConsumerGroupInfo, error) {
	var groupInfo *ports.ConsumerGroupInfo

	err := c.executeWithRetry(ctx, "GetConsumerGroupInfo", func(ctx context.Context) error {
		groups, err := c.client.XInfoGroups(ctx, stream).Result()
		if err != nil {
			return err
		}

		for _, g := range groups {
			if g.Name == group {
				groupInfo = &ports.ConsumerGroupInfo{
					Name:            g.Name,
					Consumers:       g.Consumers,
					Pending:         g.Pending,
					LastDeliveredID: g.LastDeliveredID,
				}
				return nil
			}
		}

		return fmt.Errorf("consumer group not found: %s", group)
	})

	return groupInfo, err
}

// Ping checks the connection to Redis
func (c *client) Ping(ctx context.Context) error {
	return c.executeWithRetry(ctx, "Ping", func(ctx context.Context) error {
		return c.client.Ping(ctx).Err()
	})
}

// Close closes the Redis client
func (c *client) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// GetConsumerName returns the name of the consumer
func (c *client) GetConsumerName() string {
	return c.consumerName
}

// GetStreamInfo returns stream info
func (c *client) GetStreamInfo(ctx context.Context, stream string) (*ports.StreamInfo, error) {
	var streamInfo *ports.StreamInfo

	err := c.executeWithRetry(ctx, "GetStreamInfo", func(ctx context.Context) error {
		info, err := c.client.XInfoStream(ctx, stream).Result()
		if err != nil {
			return err
		}

		streamInfo = &ports.StreamInfo{
			Length:          info.Length,
			RadixTreeKeys:   info.RadixTreeKeys,
			RadixTreeNodes:  info.RadixTreeNodes,
			Groups:          info.Groups,
			LastGeneratedID: info.LastGeneratedID,
			FirstEntry:      nil, // Not directly available from XInfoStream
			LastEntry:       nil, // Not directly available from XInfoStream
		}
		return nil
	})

	return streamInfo, err
}

// convertXMessages converts goredis.XMessage to domain.Message with minimal allocations.
// Preference: if a "payload" field looks like already-encoded JSON (starts with '{' or '['),
// forward it as-is (zero-copy for []byte). Otherwise, JSON-encode just once.
func (c *client) convertXMessages(streams []goredis.XStream) []*domain.Message {
	now := time.Now()
	// Preallocate a small capacity to reduce reslices under load
	messages := make([]*domain.Message, 0, 128)

	for _, stream := range streams {
		for _, xmsg := range stream.Messages {
			data := buildPayload(xmsg.Values)

			messages = append(messages, &domain.Message{
				ID:        xmsg.ID,
				Timestamp: now,
				Data:      data,
				Attempts:  0,
			})
		}
	}
	return messages
}

func buildPayload(values map[string]any) []byte {
	if raw, ok := values["payload"]; ok {
		switch v := raw.(type) {
		case []byte:
			if jsonx.IsLikelyJSONBytes(v) {
				return v
			}
			b, _ := jsonx.Marshal(string(v))
			return b
		case string:
			if jsonx.IsLikelyJSONString(v) {
				return []byte(v)
			}
			b, _ := jsonx.Marshal(v)
			return b
		default:
			b, _ := jsonx.Marshal(v)
			return b
		}
	}
	b, err := jsonx.Marshal(values)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// executeWithRetry is a minimal wrapper; can be extended to add backoff/retries
func (c *client) executeWithRetry(ctx context.Context, _ string, fn func(ctx context.Context) error) error {
	var attempt int
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		// Do not retry on redis.Nil (treated as "no data")
		if errors.Is(err, goredis.Nil) {
			return nil
		}

		if !isTransientRedisError(err) || attempt >= c.cfg.MaxRetries {
			return err
		}
		attempt++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.cfg.RetryInterval):
		}
	}
}

// isTransientRedisError reports whether err appears to be a transient connection/loading issue.
func isTransientRedisError(err error) bool {
	if err == nil {
		return false
	}
	es := err.Error()
	return strings.Contains(es, "LOADING") ||
		strings.Contains(es, "connect: connection refused") ||
		strings.Contains(es, "i/o timeout") ||
		strings.Contains(es, "EOF") ||
		strings.Contains(es, "read: connection reset")
}
