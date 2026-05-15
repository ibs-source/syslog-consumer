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

// isNoGroupError matches the "NOGROUP" prefix Redis uses when the stream or
// consumer group has been deleted.
func isNoGroupError(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "NOGROUP")
}

// Client is the Redis stream consumer used by the hot path.
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
	streamsArgDirty    atomic.Bool // forces streamsArg rebuild when streams list changed
}

func newBatchSlicePool(capacity int) sync.Pool {
	return sync.Pool{
		New: func() any {
			s := make([]message.Redis, 0, capacity)
			return &s
		},
	}
}

// NewClient dials Redis with cfg.PingTimeout and discovers streams or pins
// to cfg.Stream depending on whether cfg.Stream is empty.
func NewClient(ctx context.Context, cfg *config.RedisConfig, logger *log.Logger) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:            cfg.Address,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		// Maintenance notifications add extra commands and load we don't need.
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})

	pingCtx, cancel := context.WithTimeout(ctx, cfg.PingTimeout)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
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

	if cfg.Stream == "" {
		logger.Infof(ctx, "Multi-stream mode enabled: discovering Redis streams")
		streams, err := client.DiscoverStreams(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to discover streams: %w", err)
		}

		if len(streams) == 0 {
			logger.Warnf(ctx, "No streams found in Redis, will retry on next refresh")
		} else {
			logger.Infof(ctx, "Discovered %d streams: %v", len(streams), streams)
		}

		client.streams = streams
		client.multiStreamMode = true
		client.streamsArgDirty.Store(true)
	} else {
		logger.Infof(ctx, "Single-stream mode: consuming from stream '%s'", cfg.Stream)
		client.streams = []string{cfg.Stream}
		client.multiStreamMode = false
		client.streamsArgDirty.Store(true)
	}

	if err := client.ensureGroups(ctx, client.streams); err != nil {
		return nil, err
	}

	return client, nil
}

// DiscoverStreams lists every Redis key of type stream using SCAN with the
// server-side TYPE filter to avoid per-key round-trips.
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
				c.log.Infof(ctx, "Consumer group '%s' already exists for stream '%s', joining existing group", c.groupName, stream)
				continue
			}
			return fmt.Errorf("failed to create consumer group for stream %s: %w", stream, err)
		}
		c.log.Infof(ctx, "Created consumer group '%s' for stream '%s'", c.groupName, stream)
	}
	return nil
}

// ReadBatch must only be called from a single goroutine: streamsArg is not
// guarded by the mutex.
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

// handleReadError returns nil when the error was recovered (caller returns
// an empty batch).
func (c *Client) handleReadError(ctx context.Context, err error) error {
	if errors.Is(err, redis.Nil) {
		return nil
	}
	c.mu.RLock()
	currentStreams := c.streams
	c.mu.RUnlock()
	if isNoGroupError(err) {
		c.log.Warnf(ctx, "Consumer group missing, recreating groups")
		if grpErr := c.ensureGroups(ctx, currentStreams); grpErr != nil {
			return fmt.Errorf(
				"xreadgroup NOGROUP and recreate failed: %w", grpErr)
		}
		return nil
	}
	return fmt.Errorf("xreadgroup failed: %w", err)
}

// ClaimIdle reclaims pending messages whose owner has been idle longer than
// the configured ClaimIdle threshold.
func (c *Client) ClaimIdle(ctx context.Context) (message.Batch, error) {
	c.mu.RLock()
	streams := c.streams
	c.mu.RUnlock()

	pv := c.claimPool.Get()
	bp, ok := pv.(*[]message.Redis)
	if !ok {
		s := make([]message.Redis, 0, c.batchSize)
		bp = &s
	}
	allMessages := (*bp)[:0]

	for _, stream := range streams {
		pending, err := c.getPendingMessages(ctx, stream)
		if err != nil {
			c.log.Warnf(ctx, "failed to get pending messages for stream %s: %v", stream, err)
			continue
		}

		if len(pending) == 0 {
			continue
		}

		claimed, err := c.claimMessages(ctx, stream, pending)
		if err != nil {
			c.log.Warnf(ctx, "failed to claim messages for stream %s: %v", stream, err)
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
		if isNoGroupError(err) {
			c.log.Warnf(ctx, "Consumer group missing for stream '%s', recreating", stream)
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

// RefreshStreams must only be called from refreshLoop (single goroutine);
// the RLock/Lock split relies on that. Returns the number of new streams added.
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

	var newStreams []string
	for _, stream := range discoveredStreams {
		if _, ok := existingStreams[stream]; !ok {
			newStreams = append(newStreams, stream)
		}
	}

	if len(newStreams) > 0 {
		c.log.Infof(ctx, "Discovered %d new streams: %v", len(newStreams), newStreams)
		if err := c.ensureGroups(ctx, newStreams); err != nil {
			return 0, fmt.Errorf("failed to create groups for new streams: %w", err)
		}
	}

	c.mu.Lock()
	c.streams = discoveredStreams
	c.mu.Unlock()
	c.streamsArgDirty.Store(true)

	metrics.StreamsActive.Set(int64(len(discoveredStreams)))
	metrics.StreamsDiscovered.Add(int64(len(newStreams)))

	if len(discoveredStreams) < prevCount {
		c.log.Infof(ctx, "Stream count decreased from %d to %d", prevCount, len(discoveredStreams))
	}

	return len(newStreams), nil
}

// AckAndDeleteBatch issues XACK + XDEL in a single pipeline round-trip.
func (c *Client) AckAndDeleteBatch(ctx context.Context, ids []string, stream string) error {
	if stream == "" {
		return errors.New("cannot ACK messages: stream name is empty")
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
			c.log.Warnf(ctx, "Consumer group missing for stream '%s' during batch ACK, recreating", stream)
			if gerr := c.ensureGroups(ctx, []string{stream}); gerr != nil {
				c.log.Warnf(ctx, "Failed to recreate group for stream '%s': %v", stream, gerr)
			}
			return nil
		}
		return fmt.Errorf("ack+del pipeline failed for %d messages in stream %s: %w", len(ids), stream, err)
	}

	return nil
}

// Close releases the underlying Redis connection pool; safe on a nil-backed
// Client (e.g. ones built for tests without an rdb).
func (c *Client) Close() error {
	if c.rdb != nil {
		return c.rdb.Close()
	}
	return nil
}

// extractFields scans the field map once; Redis stream entries normally hold
// only "object" and "raw", so one range beats two map lookups.
func extractFields(m map[string]any) (object, raw string) {
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

// Ping verifies the connection; used by the health endpoint.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}
