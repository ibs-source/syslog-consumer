package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/ibs-source/syslog-consumer/internal/metrics"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// isNoGroupError reports whether err is a Redis NOGROUP (stream or
// consumer group deleted). Redis prefixes such errors with "NOGROUP".
func isNoGroupError(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "NOGROUP")
}

// Client manages Redis stream operations.
type Client struct {
	rdb                *redis.Client
	log                *log.Logger
	batchPool          sync.Pool
	claimPool          sync.Pool
	consumer           string
	groupName          string
	streams            []string
	streamsArg         []string
	mu                 sync.RWMutex // protects streams, streamsArg
	batchSize          int64
	blockTimeout       time.Duration
	claimIdle          time.Duration
	discoveryScanCount int64
	multiStreamMode    bool
	streamsArgDirty    atomic.Bool // true when streams list changed; forces streamsArg rebuild
}

func newBatchSlicePool(capacity int) sync.Pool {
	return sync.Pool{
		New: func() any {
			s := make([]message.Redis, 0, capacity)
			return &s
		},
	}
}

// NewClient creates a new Redis client
func NewClient(cfg *config.RedisConfig, logger *log.Logger) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:            cfg.Address,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
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
		rdb:                rdb,
		consumer:           cfg.Consumer,
		groupName:          cfg.GroupName,
		batchSize:          int64(cfg.BatchSize),
		blockTimeout:       cfg.BlockTimeout,
		claimIdle:          cfg.ClaimIdle,
		discoveryScanCount: int64(cfg.DiscoveryScanCount),
		log:                logger,
		batchPool:          newBatchSlicePool(cfg.BatchSize),
		claimPool:          newBatchSlicePool(cfg.BatchSize),
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
		client.streamsArgDirty.Store(true)
	} else {
		// Single-stream mode: treat as multi-stream with one stream
		logger.Info("Single-stream mode: consuming from stream '%s'", cfg.Stream)
		client.streams = []string{cfg.Stream}
		client.multiStreamMode = false
		client.streamsArgDirty.Store(true)
	}

	// Create consumer groups for all streams (1 or more)
	if err := client.ensureGroups(ctx, client.streams); err != nil {
		return nil, err
	}

	return client, nil
}

// DiscoverStreams discovers all Redis streams using SCAN with server-side TYPE filter.
// Each SCAN call is O(1); the TYPE filter avoids per-key round-trips.
func (c *Client) DiscoverStreams(ctx context.Context) ([]string, error) {
	streams := make([]string, 0, c.discoveryScanCount)
	var cursor uint64

	for {
		keys, nextCursor, err := c.rdb.ScanType(ctx, cursor, "*", c.discoveryScanCount, "stream").Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan keys: %w", err)
		}

		streams = append(streams, keys...)

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return streams, nil
}

func (c *Client) ensureGroups(ctx context.Context, streams []string) error {
	for _, stream := range streams {
		err := c.rdb.XGroupCreateMkStream(ctx, stream, c.groupName, "0").Err()
		if err != nil {
			if strings.Contains(err.Error(), "BUSYGROUP") {
				c.log.Info("Consumer group '%s' already exists for stream '%s', joining existing group", c.groupName, stream)
				continue
			}
			return fmt.Errorf("failed to create consumer group for stream %s: %w", stream, err)
		}
		c.log.Info("Created consumer group '%s' for stream '%s'", c.groupName, stream)
	}
	return nil
}

// ReadBatch fetches messages via XREADGROUP. streamsArg is only touched
// by this function, which runs in a single fetchLoop goroutine.
func (c *Client) ReadBatch(ctx context.Context) (message.Batch, error) {
	c.mu.RLock()
	streams := c.streams
	c.mu.RUnlock()

	if len(streams) == 0 {
		return message.Batch{}, nil
	}

	if c.streamsArgDirty.CompareAndSwap(true, false) {
		n := len(streams)
		c.streamsArg = c.streamsArg[:0]
		c.streamsArg = append(c.streamsArg, streams...)
		for range n {
			c.streamsArg = append(c.streamsArg, ">")
		}
	}

	result, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.groupName,
		Consumer: c.consumer,
		Streams:  c.streamsArg,
		Count:    c.batchSize,
		Block:    c.blockTimeout,
	}).Result()

	if err != nil {
		return message.Batch{},
			c.handleReadError(ctx, err)
	}

	if len(result) == 0 {
		return message.Batch{}, nil
	}

	pv := c.batchPool.Get()
	bp, ok := pv.(*[]message.Redis)
	if !ok {
		s := make([]message.Redis, 0, c.batchSize)
		bp = &s
	}
	messages := (*bp)[:0]

	for si := range result {
		sr := &result[si]
		for i := range sr.Messages {
			object, raw := extractFields(sr.Messages[i].Values)
			messages = append(messages, message.Redis{
				ID:     sr.Messages[i].ID,
				Stream: sr.Stream,
				Object: object,
				Raw:    raw,
			})
		}
	}

	return message.NewPooledBatch(messages, bp, &c.batchPool), nil
}

// handleReadError handles XREADGROUP errors, recreating groups on NOGROUP.
// Returns nil when the error was recovered (caller returns empty batch).
func (c *Client) handleReadError(ctx context.Context, err error) error {
	if errors.Is(err, redis.Nil) {
		return nil
	}
	c.mu.RLock()
	currentStreams := c.streams
	c.mu.RUnlock()
	if isNoGroupError(err) {
		c.log.Warn("Consumer group missing, recreating groups")
		if grpErr := c.ensureGroups(ctx, currentStreams); grpErr != nil {
			return fmt.Errorf(
				"xreadgroup NOGROUP and recreate failed: %w", grpErr)
		}
		return nil
	}
	return fmt.Errorf("xreadgroup failed: %w", err)
}

// ClaimIdle rebalances stale pending messages.
func (c *Client) ClaimIdle(ctx context.Context) (message.Batch, error) {
	c.mu.RLock()
	streams := c.streams
	c.mu.RUnlock()

	// Borrow a pooled slice to avoid allocating per claim cycle.
	pv := c.claimPool.Get()
	bp, ok := pv.(*[]message.Redis)
	if !ok {
		s := make([]message.Redis, 0, c.batchSize)
		bp = &s
	}
	allMessages := (*bp)[:0]

	// Claim from all streams (works for both single-stream with 1 element and multi-stream with N elements)
	for _, stream := range streams {
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

		for _, msg := range claimed {
			object, raw := extractFields(msg.Values)
			allMessages = append(allMessages, message.Redis{
				ID:     msg.ID,
				Stream: stream,
				Object: object,
				Raw:    raw,
			})
		}
	}

	return message.NewPooledBatch(allMessages, bp, &c.claimPool), nil
}

func (c *Client) getPendingMessages(ctx context.Context, stream string) ([]redis.XPendingExt, error) {
	pending, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  c.groupName,
		Idle:   c.claimIdle,
		Start:  "-",
		End:    "+",
		Count:  c.batchSize,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		// Stream or group was deleted; recreate the group and return empty (no pending messages in a fresh group)
		if isNoGroupError(err) {
			c.log.Warn("Consumer group missing for stream '%s', recreating", stream)
			if grpErr := c.ensureGroups(ctx, []string{stream}); grpErr != nil {
				return nil, fmt.Errorf(
					"xpending NOGROUP and recreate failed for %s: %w",
					stream, grpErr)
			}
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
		Group:    c.groupName,
		Consumer: c.consumer,
		MinIdle:  c.claimIdle,
		Messages: ids,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("xclaim failed: %w", err)
	}

	return claimed, nil
}

// RefreshStreams rediscovers Redis streams and updates the streams
// list. Called only from refreshLoop (single goroutine) so the
// RLock/Lock split is safe. Returns the number of new streams added.
func (c *Client) RefreshStreams(ctx context.Context) (int, error) {
	if !c.multiStreamMode {
		return 0, nil
	}

	discoveredStreams, err := c.DiscoverStreams(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to discover streams: %w", err)
	}

	c.mu.RLock()
	prevCount := len(c.streams)
	existingStreams := make(map[string]struct{}, prevCount)
	for _, stream := range c.streams {
		existingStreams[stream] = struct{}{}
	}
	c.mu.RUnlock()

	// Find new streams
	var newStreams []string
	for _, stream := range discoveredStreams {
		if _, ok := existingStreams[stream]; !ok {
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

	// Atomically update the streams list and mark streamsArg for rebuild
	c.mu.Lock()
	c.streams = discoveredStreams
	c.mu.Unlock()
	c.streamsArgDirty.Store(true)

	metrics.StreamsActive.Set(int64(len(discoveredStreams)))
	metrics.StreamsDiscovered.Add(int64(len(newStreams)))

	// Log if streams were removed
	if len(discoveredStreams) < prevCount {
		c.log.Info("Stream count decreased from %d to %d", prevCount, len(discoveredStreams))
	}

	return len(newStreams), nil
}

// AckAndDeleteBatch acknowledges and deletes a batch of messages from a Redis stream
// using a single pipeline round-trip (one XACK + one XDEL for all IDs).
func (c *Client) AckAndDeleteBatch(ctx context.Context, ids []string, stream string) error {
	if stream == "" {
		return fmt.Errorf("cannot ACK messages: stream name is empty")
	}
	if len(ids) == 0 {
		return nil
	}

	pipe := c.rdb.Pipeline()
	pipe.XAck(ctx, stream, c.groupName, ids...)
	pipe.XDel(ctx, stream, ids...)

	_, err := pipe.Exec(ctx)
	if err != nil {
		if isNoGroupError(err) {
			c.log.Warn("Consumer group missing for stream '%s' during batch ACK, recreating", stream)
			if gerr := c.ensureGroups(ctx, []string{stream}); gerr != nil {
				c.log.Warn("Failed to recreate group for stream '%s': %v", stream, gerr)
			}
			return nil
		}
		return fmt.Errorf("ack+del pipeline failed for %d messages in stream %s: %w", len(ids), stream, err)
	}

	return nil
}

// Close closes the Redis client connection
func (c *Client) Close() error {
	if c.rdb != nil {
		return c.rdb.Close()
	}
	return nil
}

// extractFields pulls the "object" and "raw" values from the go-redis
// field map using a single iteration. Redis stream entries typically have
// only these two fields, so one range loop is faster than two hash lookups.
func extractFields(m map[string]interface{}) (object, raw string) {
	for k, v := range m {
		switch k {
		case "object":
			if s, ok := v.(string); ok {
				object = s
			}
		case "raw":
			if s, ok := v.(string); ok {
				raw = s
			}
		}
	}
	return
}

// Ping checks the Redis connection is alive.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}
