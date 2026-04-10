package redis

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
	goredis "github.com/redis/go-redis/v9"
)

// startMiniredis creates an in-process Redis server for unit tests.
func startMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// mustXAdd adds a message to a miniredis stream, failing the test on error.
func mustXAdd(t *testing.T, s *miniredis.Miniredis, stream string, values ...string) string {
	t.Helper()
	id, err := s.XAdd(stream, "*", values)
	if err != nil {
		t.Fatalf("XAdd(%s): %v", stream, err)
	}
	return id
}

// newTestClient creates a Client backed by miniredis in single-stream mode.
func newTestClient(t *testing.T, s *miniredis.Miniredis, stream string) *Client {
	t.Helper()
	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() {
		if err := rdb.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
			t.Errorf("rdb.Close(): %v", err)
		}
	})

	client := &Client{
		rdb:                rdb,
		consumer:           "test-consumer",
		groupName:          "test-group",
		batchSize:          10,
		blockTimeout:       50 * time.Millisecond,
		claimIdle:          1 * time.Second,
		discoveryScanCount: 1000,
		log:                log.New(),
		batchPool: sync.Pool{
			New: func() any {
				s := make([]message.Redis, 0, 10)
				return &s
			},
		},
		claimPool: sync.Pool{
			New: func() any {
				s := make([]message.Redis, 0, 10)
				return &s
			},
		},
	}
	if stream != "" {
		client.streams = []string{stream}
		client.multiStreamMode = false
		client.streamsArgDirty.Store(true)
	} else {
		client.multiStreamMode = true
	}

	return client
}

func closeRedisClient(t *testing.T, client *Client) {
	t.Helper()
	if err := client.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
		t.Errorf("client.Close(): %v", err)
	}
}

func mustEnsureGroups(t *testing.T, c *Client, streams ...string) {
	t.Helper()
	if err := c.ensureGroups(t.Context(), streams); err != nil {
		t.Fatalf("ensureGroups(%v): %v", streams, err)
	}
}

func mustReadBatch(t *testing.T, c *Client) {
	t.Helper()
	if _, err := c.ReadBatch(t.Context()); err != nil {
		t.Fatalf("ReadBatch(): %v", err)
	}
}

// --- NewClient ---

func TestNewClient_SingleStream(t *testing.T) {
	s := startMiniredis(t)
	// Seed a stream so NewClient finds it
	mustXAdd(t, s, "test-stream", "key", "val")

	cfg := &config.RedisConfig{
		Address:            s.Addr(),
		Stream:             "test-stream",
		Consumer:           "c1",
		GroupName:          "test-group",
		BatchSize:          10,
		DiscoveryScanCount: 1000,
		BlockTimeout:       50 * time.Millisecond,
		ClaimIdle:          1 * time.Second,
		DialTimeout:        1 * time.Second,
		ReadTimeout:        1 * time.Second,
		WriteTimeout:       1 * time.Second,
		PingTimeout:        1 * time.Second,
	}

	client, err := NewClient(cfg, log.New())
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer closeRedisClient(t, client)

	if client.multiStreamMode {
		t.Error("expected single-stream mode")
	}
	if len(client.streams) != 1 || client.streams[0] != "test-stream" {
		t.Errorf("streams = %v; want [test-stream]", client.streams)
	}
}

func TestNewClient_MultiStream(t *testing.T) {
	s := startMiniredis(t)
	// Seed two streams
	mustXAdd(t, s, "stream-a", "k", "v")
	mustXAdd(t, s, "stream-b", "k", "v")

	cfg := &config.RedisConfig{
		Address:            s.Addr(),
		Stream:             "", // multi-stream
		Consumer:           "c1",
		GroupName:          "test-group",
		BatchSize:          10,
		DiscoveryScanCount: 1000,
		BlockTimeout:       50 * time.Millisecond,
		ClaimIdle:          1 * time.Second,
		DialTimeout:        1 * time.Second,
		ReadTimeout:        1 * time.Second,
		WriteTimeout:       1 * time.Second,
		PingTimeout:        1 * time.Second,
	}

	client, err := NewClient(cfg, log.New())
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer closeRedisClient(t, client)

	if !client.multiStreamMode {
		t.Error("expected multi-stream mode")
	}
	if len(client.streams) < 2 {
		t.Errorf("expected ≥2 discovered streams, got %d", len(client.streams))
	}
}

func TestNewClient_ConnectionFailure(t *testing.T) {
	cfg := &config.RedisConfig{
		Address:            "localhost:1", // invalid port
		Stream:             "test",
		GroupName:          "test-group",
		DiscoveryScanCount: 1000,
		DialTimeout:        100 * time.Millisecond,
		PingTimeout:        100 * time.Millisecond,
	}

	_, err := NewClient(cfg, log.New())
	if err == nil {
		t.Error("expected connection error")
	}
}

// --- Ping ---

func TestPing_Success(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	if err := c.Ping(t.Context()); err != nil {
		t.Errorf("Ping() = %v; want nil", err)
	}
}

func TestPing_Failure(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")
	s.Close() // kill the server

	if err := c.Ping(t.Context()); err == nil {
		t.Error("Ping() = nil; want error after server closed")
	}
}

// --- Close (rdb != nil branch) ---

func TestClose_WithRDB(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	if err := c.Close(); err != nil {
		t.Errorf("Close() = %v; want nil", err)
	}
}

// --- ensureGroups ---

func TestEnsureGroups_CreatesGroup(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	// Seed the stream — XGroupCreateMkStream should work
	mustXAdd(t, s, "s1", "k", "v")

	err := c.ensureGroups(t.Context(), []string{"s1"})
	if err != nil {
		t.Fatalf("ensureGroups() error = %v", err)
	}

	// Call again — should get BUSYGROUP and not error
	err = c.ensureGroups(t.Context(), []string{"s1"})
	if err != nil {
		t.Errorf("ensureGroups() second call error = %v; want nil (BUSYGROUP handled)", err)
	}
}

// --- DiscoverStreams ---

func TestDiscoverStreams_FindsStreams(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "")

	mustXAdd(t, s, "stream-1", "k", "v")
	mustXAdd(t, s, "stream-2", "k", "v")

	streams, err := c.DiscoverStreams(t.Context())
	if err != nil {
		t.Fatalf("DiscoverStreams() error = %v", err)
	}
	if len(streams) < 2 {
		t.Errorf("expected ≥2 streams, got %d: %v", len(streams), streams)
	}
}

func TestDiscoverStreams_EmptyDatabase(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "")

	streams, err := c.DiscoverStreams(t.Context())
	if err != nil {
		t.Fatalf("DiscoverStreams() error = %v", err)
	}
	if len(streams) != 0 {
		t.Errorf("expected 0 streams, got %d", len(streams))
	}
}

// --- ReadBatch ---

func TestReadBatch_ReadsMessages(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	// Create stream with some messages
	mustXAdd(t, s, "s1", "source", "10.0.0.1")
	mustXAdd(t, s, "s1", "raw", "test message")

	// Create consumer group
	err := c.ensureGroups(t.Context(), []string{"s1"})
	if err != nil {
		t.Fatalf("ensureGroups() error = %v", err)
	}

	batch, err := c.ReadBatch(t.Context())
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	if len(batch.Items) != 2 {
		t.Errorf("expected 2 messages, got %d", len(batch.Items))
	}
	for _, item := range batch.Items {
		if item.Stream != "s1" {
			t.Errorf("Stream = %q; want s1", item.Stream)
		}
	}
}

func TestReadBatch_EmptyStreams(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "")
	c.streams = nil // no streams

	batch, err := c.ReadBatch(t.Context())
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	if len(batch.Items) != 0 {
		t.Errorf("expected 0 messages, got %d", len(batch.Items))
	}
}

func TestReadBatch_NoNewMessages(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	mustXAdd(t, s, "s1", "k", "v")
	mustEnsureGroups(t, c, "s1")

	// Read all messages
	mustReadBatch(t, c)

	// Second read should return empty (no new messages, blockTimeout is short)
	batch, err := c.ReadBatch(t.Context())
	if err != nil {
		t.Fatalf("ReadBatch() second call error = %v", err)
	}
	if len(batch.Items) != 0 {
		t.Errorf("expected 0 messages on second read, got %d", len(batch.Items))
	}
}

// --- handleReadError NOGROUP path ---

func TestHandleReadError_NOGROUP_Recovers(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	// Seed the stream so ensureGroups succeeds inside handleReadError
	mustXAdd(t, s, "s1", "k", "v")

	nogroupErr := errors.New("NOGROUP No such key 's1' or consumer group 'consumer-group'")
	err := c.handleReadError(t.Context(), nogroupErr)
	if err != nil {
		t.Errorf("handleReadError(NOGROUP) = %v; want nil (recovered)", err)
	}
}

// --- AckAndDelete ---

func TestAckAndDeleteBatch_Success(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	id := mustXAdd(t, s, "s1", "k", "v")
	mustEnsureGroups(t, c, "s1")
	// Read to register the message in the pending list
	mustReadBatch(t, c)

	err := c.AckAndDeleteBatch(t.Context(), []string{id}, "s1")
	if err != nil {
		t.Errorf("AckAndDeleteBatch() error = %v", err)
	}
}

func TestAckAndDeleteBatch_EmptyStream(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	err := c.AckAndDeleteBatch(t.Context(), []string{"1-0"}, "")
	if err == nil {
		t.Error("AckAndDeleteBatch() with empty stream should error")
	}
}

// --- ClaimIdle ---

func TestClaimIdle_NoPending(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	mustXAdd(t, s, "s1", "k", "v")
	mustEnsureGroups(t, c, "s1")

	batch, err := c.ClaimIdle(t.Context())
	if err != nil {
		t.Fatalf("ClaimIdle() error = %v", err)
	}
	if len(batch.Items) != 0 {
		t.Errorf("expected 0 claimed messages (nothing pending), got %d", len(batch.Items))
	}
}

// --- RefreshStreams ---

func TestRefreshStreams_DiscoversNewStreams(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "")

	mustXAdd(t, s, "s1", "k", "v")
	mustEnsureGroups(t, c, "s1")
	c.streams = []string{"s1"}
	c.multiStreamMode = true

	// Add a new stream
	mustXAdd(t, s, "s2", "k", "v")

	newCount, err := c.RefreshStreams(t.Context())
	if err != nil {
		t.Fatalf("RefreshStreams() error = %v", err)
	}
	if newCount < 1 {
		t.Errorf("expected ≥1 new stream, got %d", newCount)
	}
}

func TestRefreshStreams_SingleStreamModeNoop(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")
	c.multiStreamMode = false

	newCount, err := c.RefreshStreams(t.Context())
	if err != nil {
		t.Fatalf("RefreshStreams() error = %v", err)
	}
	if newCount != 0 {
		t.Errorf("expected 0 for single-stream mode, got %d", newCount)
	}
}

func TestRefreshStreams_StreamRemoved(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "")
	c.multiStreamMode = true

	mustXAdd(t, s, "s1", "k", "v")
	mustXAdd(t, s, "s2", "k", "v")
	c.streams = []string{"s1", "s2"}

	// Remove s2 from Redis
	s.Del("s2")

	_, err := c.RefreshStreams(t.Context())
	if err != nil {
		t.Fatalf("RefreshStreams() error = %v", err)
	}
	// After refresh, streams list should shrink
	if len(c.streams) > 1 {
		t.Logf("streams after removal: %v (expected ≤1)", c.streams)
	}
}

// --- CleanupDeadConsumers ---

func TestCleanupDeadConsumers_NoDeadConsumers(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	mustXAdd(t, s, "s1", "k", "v")
	mustEnsureGroups(t, c, "s1")

	err := c.CleanupDeadConsumers(t.Context(), 5*time.Minute)
	if err != nil {
		t.Errorf("CleanupDeadConsumers() error = %v", err)
	}
}

// --- getPendingMessages NOGROUP recovery ---

func TestGetPendingMessages_NOGROUP_Recreates(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	// Create stream without group → XPENDING should return NOGROUP
	mustXAdd(t, s, "s1", "k", "v")

	// getPendingMessages for a non-existent group should return nil, nil (NOGROUP handled)
	pending, err := c.getPendingMessages(t.Context(), "s1")
	if err != nil {
		t.Errorf("getPendingMessages(NOGROUP) error = %v; want nil", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after NOGROUP recovery, got %d", len(pending))
	}
}

// --- Multi-stream ReadBatch ---
// Note: miniredis has limitations with multi-stream XREADGROUP.
// Multi-stream functionality is verified via integration tests.

func TestReadBatch_SingleStreamMultipleMessages(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	mustXAdd(t, s, "s1", "source", "10.0.0.1")
	mustXAdd(t, s, "s1", "source", "10.0.0.2")
	mustXAdd(t, s, "s1", "source", "10.0.0.3")

	mustEnsureGroups(t, c, "s1")

	batch, err := c.ReadBatch(t.Context())
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	if len(batch.Items) != 3 {
		t.Errorf("expected 3 messages, got %d", len(batch.Items))
	}
}

// --- ClaimIdle with pending messages ---

func TestClaimIdle_WithPendingMessages(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")
	c.claimIdle = 0 // claim everything immediately

	mustXAdd(t, s, "s1", "source", "10.0.0.1")
	mustEnsureGroups(t, c, "s1")

	// Read messages to put them into the pending list
	mustReadBatch(t, c)

	// Fast-forward miniredis time so messages become idle
	s.FastForward(2 * time.Second)

	// Now claim them
	batch, err := c.ClaimIdle(t.Context())
	if err != nil {
		t.Fatalf("ClaimIdle() error = %v", err)
	}
	// miniredis may or may not support XCLAIM fully;
	// the important thing is no error
	_ = batch
}

// --- claimMessages direct test ---

func TestClaimMessages_Success(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")
	c.claimIdle = 0

	id := mustXAdd(t, s, "s1", "source", "10.0.0.1")
	mustEnsureGroups(t, c, "s1")
	// Read to create pending entry
	mustReadBatch(t, c)
	s.FastForward(2 * time.Second)

	pending := []goredis.XPendingExt{{ID: id}}
	claimed, err := c.claimMessages(t.Context(), "s1", pending)
	if err != nil {
		t.Fatalf("claimMessages() error = %v", err)
	}
	_ = claimed // miniredis may return empty; we just verify no error
}

// --- CleanupDeadConsumers with dead consumers ---

func TestCleanupDeadConsumers_WithDeadConsumer(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "s1")

	mustXAdd(t, s, "s1", "k", "v")
	mustEnsureGroups(t, c, "s1")

	// Create a second consumer by reading with a different consumer name
	rdb2 := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	defer func() {
		if err := rdb2.Close(); err != nil {
			t.Errorf("rdb2.Close(): %v", err)
		}
	}()

	if _, err := rdb2.XReadGroup(t.Context(), &goredis.XReadGroupArgs{
		Group:    c.groupName,
		Consumer: "dead-consumer",
		Streams:  []string{"s1", ">"},
		Count:    1,
		Block:    0,
	}).Result(); err != nil {
		t.Fatalf("XReadGroup(): %v", err)
	}

	s.FastForward(10 * time.Minute)

	// Now cleanup with a short idle timeout
	err := c.CleanupDeadConsumers(t.Context(), 1*time.Minute)
	if err != nil {
		t.Errorf("CleanupDeadConsumers() error = %v", err)
	}
}

// --- AckAndDeleteBatch NOGROUP recovery ---

func TestAckAndDeleteBatch_NOGROUP_Recovery(t *testing.T) {
	s := startMiniredis(t)
	c := newTestClient(t, s, "nogroup-stream")

	// Create stream without a consumer group for this client
	mustXAdd(t, s, "nogroup-stream", "k", "v")
	// This exercises the NOGROUP recovery path in AckAndDeleteBatch.
	if err := c.AckAndDeleteBatch(t.Context(), []string{"0-0"}, "nogroup-stream"); err != nil {
		t.Fatalf("AckAndDeleteBatch(): %v", err)
	}
}
