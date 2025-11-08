package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/ibs-source/syslog-consumer/pkg/jsonfast"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Client manages Redis stream operations
type Client struct {
	rdb             *redis.Client
	streams         []string
	groups          map[string]string
	multiStreamMode bool
	consumer        string
	batchSize       int64
	blockTimeout    time.Duration
	claimIdle       time.Duration
	log             *log.Logger
}

// NewClient creates a new Redis client
func NewClient(cfg *config.RedisConfig, logger *log.Logger) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Address,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		// Explicitly disable maintenance notifications
		// This prevents the client from sending extra commands to Redis
		// which can add unnecessary load.
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	client := &Client{
		rdb:          rdb,
		consumer:     cfg.Consumer,
		batchSize:    int64(cfg.BatchSize),
		blockTimeout: cfg.BlockTimeout,
		claimIdle:    cfg.ClaimIdle,
		groups:       make(map[string]string),
		log:          logger,
	}

	// Determine mode: single-stream or multi-stream
	if cfg.Stream == "" {
		// Multi-stream mode: discover all streams
		logger.Info("Multi-stream mode enabled: discovering Redis streams")
		streams, err := client.DiscoverStreams(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to discover streams: %w", err)
		}

		if len(streams) == 0 {
			logger.Warn("No streams found in Redis, will retry on next refresh")
		} else {
			logger.Info("Discovered %d streams: %v", len(streams), streams)
		}

		client.streams = streams
		client.multiStreamMode = true
	} else {
		// Single-stream mode: treat as multi-stream with one stream
		logger.Info("Single-stream mode: consuming from stream '%s'", cfg.Stream)
		client.streams = []string{cfg.Stream}
		client.multiStreamMode = false
	}

	// Create consumer groups for all streams (1 or more)
	if err := client.ensureGroups(ctx, client.streams); err != nil {
		return nil, err
	}

	return client, nil
}

// DiscoverStreams discovers all Redis streams using KEYS command
func (c *Client) DiscoverStreams(ctx context.Context) ([]string, error) {
	// Get all keys matching any pattern
	keys, err := c.rdb.Keys(ctx, "*").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	// Filter only stream-type keys
	streams := make([]string, 0)
	for _, key := range keys {
		keyType, err := c.rdb.Type(ctx, key).Result()
		if err != nil {
			c.log.Warn("failed to get type for key %s: %v", key, err)
			continue
		}
		if keyType == "stream" {
			streams = append(streams, key)
		}
	}

	return streams, nil
}

func (c *Client) ensureGroups(ctx context.Context, streams []string) error {
	for _, stream := range streams {
		groupName := "group-" + stream
		c.groups[stream] = groupName

		err := c.rdb.XGroupCreateMkStream(ctx, stream, groupName, "0").Err()
		if err != nil {
			if err.Error() == "BUSYGROUP Consumer Group name already exists" {
				c.log.Info("Consumer group '%s' already exists for stream '%s', joining existing group", groupName, stream)
				continue
			}
			return fmt.Errorf("failed to create consumer group for stream %s: %w", stream, err)
		}
		c.log.Info("Created consumer group '%s' for stream '%s'", groupName, stream)
	}
	return nil
}

// ReadBatch fetches messages using XREADGROUP with zero-copy payload extraction
func (c *Client) ReadBatch(ctx context.Context) (message.Batch[message.Payload], error) {
	// Handle empty stream list (multi-stream mode with no streams discovered yet)
	if len(c.streams) == 0 {
		return message.Batch[message.Payload]{}, nil
	}

	streamsArg := make([]string, 0, len(c.streams)*2)
	for _, stream := range c.streams {
		streamsArg = append(streamsArg, stream, ">")
	}

	groupName := c.groups[c.streams[0]]

	result, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: c.consumer,
		Streams:  streamsArg,
		Count:    c.batchSize,
		Block:    c.blockTimeout,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			// No messages available
			return message.Batch[message.Payload]{}, nil
		}
		return message.Batch[message.Payload]{}, fmt.Errorf("xreadgroup failed: %w", err)
	}

	if len(result) == 0 {
		return message.Batch[message.Payload]{}, nil
	}

	// Aggregate messages from all streams
	messages := make([]message.Redis[message.Payload], 0)

	for _, streamResult := range result {
		for _, msg := range streamResult.Messages {
			// Serialize entire message values (all fields) as JSON payload
			payload, err := serializeMessageValues(msg.Values)
			if err != nil {
				c.log.Warn("Failed to serialize message %s values: %v", msg.ID, err)
				continue
			}

			messages = append(messages, message.Redis[message.Payload]{
				ID:     msg.ID,
				Stream: streamResult.Stream, // Track which stream this message came from
				Body:   payload,
			})
		}
	}

	return message.Batch[message.Payload]{
		Items: messages,
	}, nil
}

// ClaimIdle rebalances stale pending messages
func (c *Client) ClaimIdle(ctx context.Context) (message.Batch[message.Payload], error) {
	var allMessages []message.Redis[message.Payload]

	// Claim from all streams (works for both single-stream with 1 element and multi-stream with N elements)
	for _, stream := range c.streams {
		pending, err := c.getPendingMessages(ctx, stream)
		if err != nil {
			c.log.Warn("failed to get pending messages for stream %s: %v", stream, err)
			continue
		}

		if len(pending) == 0 {
			continue
		}

		claimed, err := c.claimMessages(ctx, stream, pending)
		if err != nil {
			c.log.Warn("failed to claim messages for stream %s: %v", stream, err)
			continue
		}

		messages := c.buildClaimedMessages(stream, claimed)
		allMessages = append(allMessages, messages...)
	}

	return message.Batch[message.Payload]{Items: allMessages}, nil
}

func (c *Client) getPendingMessages(ctx context.Context, stream string) ([]redis.XPendingExt, error) {
	pending, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  c.groups[stream],
		Idle:   c.claimIdle,
		Start:  "-",
		End:    "+",
		Count:  c.batchSize,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xpending failed: %w", err)
	}

	return pending, nil
}

func (c *Client) claimMessages(
	ctx context.Context, stream string, pending []redis.XPendingExt,
) ([]redis.XMessage, error) {
	ids := make([]string, len(pending))
	for i, p := range pending {
		ids[i] = p.ID
	}

	claimed, err := c.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    c.groups[stream],
		Consumer: c.consumer,
		MinIdle:  c.claimIdle,
		Messages: ids,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("xclaim failed: %w", err)
	}

	return claimed, nil
}

func (c *Client) buildClaimedMessages(
	stream string,
	claimed []redis.XMessage,
) []message.Redis[message.Payload] {
	messages := make([]message.Redis[message.Payload], 0, len(claimed))

	for _, msg := range claimed {
		// Serialize entire message values (all fields) as JSON payload
		payload, err := serializeMessageValues(msg.Values)
		if err != nil {
			c.log.Warn("Failed to serialize message %s values: %v", msg.ID, err)
			continue
		}

		messages = append(messages, message.Redis[message.Payload]{
			ID:     msg.ID,
			Stream: stream,
			Body:   payload,
		})
	}

	return messages
}

// RefreshStreams rediscovers Redis streams and updates the streams list (multi-stream mode only)
// Returns the number of new streams discovered
func (c *Client) RefreshStreams(ctx context.Context) (int, error) {
	if !c.multiStreamMode {
		return 0, nil // No-op in single-stream mode
	}

	// Discover current streams
	discoveredStreams, err := c.DiscoverStreams(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to discover streams: %w", err)
	}

	// Build a set of existing streams for comparison
	existingStreams := make(map[string]bool)
	for _, stream := range c.streams {
		existingStreams[stream] = true
	}

	// Find new streams
	var newStreams []string
	for _, stream := range discoveredStreams {
		if !existingStreams[stream] {
			newStreams = append(newStreams, stream)
		}
	}

	// If we have new streams, create consumer groups and update the list
	if len(newStreams) > 0 {
		c.log.Info("Discovered %d new streams: %v", len(newStreams), newStreams)
		if err := c.ensureGroups(ctx, newStreams); err != nil {
			return 0, fmt.Errorf("failed to create groups for new streams: %w", err)
		}
	}

	// Update the streams list
	c.streams = discoveredStreams

	// Log if streams were removed
	if len(discoveredStreams) < len(existingStreams) {
		c.log.Info("Stream count decreased from %d to %d", len(existingStreams), len(discoveredStreams))
	}

	return len(newStreams), nil
}

// AckAndDelete acknowledges and deletes a message from Redis stream
func (c *Client) AckAndDelete(ctx context.Context, msg message.Redis[message.Payload]) error {
	stream := msg.Stream
	if stream == "" {
		stream = c.streams[0]
	}

	if err := c.rdb.XAck(ctx, stream, c.groups[stream], msg.ID).Err(); err != nil {
		return fmt.Errorf("xack failed for message %s in stream %s: %w", msg.ID, stream, err)
	}

	if err := c.rdb.XDel(ctx, stream, msg.ID).Err(); err != nil {
		return fmt.Errorf("xdel failed for message %s in stream %s: %w", msg.ID, stream, err)
	}

	return nil
}

// serializeMessageValues serializes all Redis message fields to JSON using jsonfast builder
// If the "object" field contains a JSON string, it will be included as raw JSON (not escaped)
func serializeMessageValues(values map[string]interface{}) ([]byte, error) {
	// Estimate capacity: ~100 bytes base + field content
	builder := jsonfast.New(512)
	builder.BeginObject()

	// Iterate through all fields
	for k, v := range values {
		switch k {
		case "object":
			// Special handling: if it's a string containing JSON, include it as raw JSON
			if str, ok := v.(string); ok && str != "" && isJSON(str) {
				builder.AddRawJSONField(k, []byte(str))
			} else if str, ok := v.(string); ok {
				builder.AddStringField(k, str)
			}
		default:
			// All other fields are treated as strings
			if str, ok := v.(string); ok {
				builder.AddStringField(k, str)
			}
		}
	}

	builder.EndObject()

	// Return a copy of the buffer to avoid aliasing issues
	result := make([]byte, len(builder.Bytes()))
	copy(result, builder.Bytes())
	return result, nil
}

// isJSON quickly checks if a string might be valid JSON (starts with { or [)
func isJSON(s string) bool {
	if len(s) == 0 {
		return false
	}
	// Trim leading whitespace
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		return c == '{' || c == '['
	}
	return false
}

// Close closes the Redis client connection
func (c *Client) Close() error {
	if c.rdb != nil {
		return c.rdb.Close()
	}
	return nil
}
