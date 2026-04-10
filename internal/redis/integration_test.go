//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/redis/go-redis/v9"
)

func setupRedisConfig(t *testing.T) *config.RedisConfig {
	t.Helper()

	t.Setenv("REDIS_ADDRESS", "localhost:6379")
	t.Setenv("REDIS_STREAM", "test-stream")
	t.Setenv("REDIS_CONSUMER", "test-consumer")
	t.Setenv("REDIS_CLAIM_IDLE", "100ms")

	fullCfg, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	return &fullCfg.Redis
}

func TestIntegration_RedisConnection(t *testing.T) {
	cfg := setupRedisConfig(t)
	logger := log.New()

	t.Run("Connect", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skipf("Skipping Redis test: %v (Redis not available?)", err)
			return
		}
		defer func() { _ = client.Close() }()

		t.Log("Successfully connected to Redis")
	})

	t.Run("EnsureGroups", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skip("Redis not available")
			return
		}
		defer func() { _ = client.Close() }()

		ctx := t.Context()
		err = client.ensureGroups(ctx, []string{cfg.Stream})
		if err != nil {
			t.Fatalf("Failed to ensure groups: %v", err)
		}

		t.Log("Successfully ensured consumer groups exist")
	})
}

// TestIntegration_RedisOperations tests Redis stream operations
func TestIntegration_RedisOperations(t *testing.T) {
	cfg := setupRedisConfig(t)
	cfg.Stream = "test-stream-ops"
	logger := log.New()

	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Skip("Redis not available")
		return
	}
	defer func() { _ = client.Close() }()

	ctx := t.Context()
	defer client.rdb.Del(ctx, cfg.Stream)

	t.Run("ReadBatch_Empty", func(t *testing.T) { testReadBatchEmpty(t, client) })
	t.Run("AddAndReadMessage", func(t *testing.T) { testAddAndReadMessage(t, client, cfg) })
	t.Run("ClaimIdle", func(t *testing.T) { testClaimIdle(t, client, cfg) })
	t.Run("AckAndDelete_Multiple", func(t *testing.T) { testAckAndDeleteMultiple(t, client, cfg) })
}

func testReadBatchEmpty(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	batch, err := client.ReadBatch(ctx)
	if err != nil {
		t.Fatalf("ReadBatch failed: %v", err)
	}
	if len(batch.Items) != 0 {
		t.Errorf("Expected empty batch, got %d items", len(batch.Items))
	}
	t.Log("Successfully read empty batch")
}

func testAddAndReadMessage(t *testing.T, client *Client, cfg *config.RedisConfig) {
	t.Helper()
	msgID := addTestMessage(t, client, cfg)
	t.Logf("Added message: %s", msgID)

	batch := readMessageBatch(t, client)
	item := parsePayload(t, batch)

	verifyPayloadFields(t, item)
	verifyObjectField(t, item)

	t.Log("Successfully read message with all fields (including parsed object) from stream")

	ackAndDeleteMessage(t, client, batch)
	t.Log("Successfully acknowledged and deleted message")
}

func addTestMessage(t *testing.T, client *Client, cfg *config.RedisConfig) string {
	t.Helper()
	ctx := t.Context()
	msgID, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.Stream,
		Values: map[string]any{
			"source":    "10.0.0.1",
			"timestamp": "1234567890",
			"object":    `{"message":"test payload","severity":5}`,
			"raw":       "raw data",
		},
	}).Result()
	if err != nil {
		t.Fatalf("Failed to add message: %v", err)
	}
	return msgID
}

func readMessageBatch(t *testing.T, client *Client) message.Batch {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	batch, err := client.ReadBatch(ctx)
	if err != nil {
		t.Fatalf("ReadBatch failed: %v", err)
	}
	if len(batch.Items) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(batch.Items))
	}
	return batch
}

func parsePayload(t *testing.T, batch message.Batch) message.Redis {
	t.Helper()
	return batch.Items[0]
}

func verifyPayloadFields(t *testing.T, item message.Redis) {
	t.Helper()
	if item.Raw != "raw data" {
		t.Errorf("Expected Raw 'raw data', got '%v'", item.Raw)
	}
}

func verifyObjectField(t *testing.T, item message.Redis) {
	t.Helper()
	var objectField map[string]any
	if err := json.Unmarshal([]byte(item.Object), &objectField); err != nil {
		t.Fatalf("Failed to unmarshal Object field: %v", err)
	}
	if objectField["message"] != "test payload" {
		t.Errorf("Expected object.message 'test payload', got '%v'", objectField["message"])
	}
	if objectField["severity"] != float64(5) {
		t.Errorf("Expected object.severity 5, got '%v'", objectField["severity"])
	}
}

func ackAndDeleteMessage(t *testing.T, client *Client, batch message.Batch) {
	t.Helper()
	ctx := t.Context()
	item := batch.Items[0]
	if err := client.AckAndDelete(ctx, item.ID, item.Stream); err != nil {
		t.Fatalf("AckAndDelete failed: %v", err)
	}
}

func testClaimIdle(t *testing.T, client *Client, cfg *config.RedisConfig) {
	t.Helper()
	ctx := t.Context()

	_, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.Stream,
		Values: map[string]any{"object": "idle test"},
	}).Result()
	if err != nil {
		t.Fatalf("Failed to add message: %v", err)
	}

	readCtx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	batch, err := client.ReadBatch(readCtx)
	cancel()
	if err != nil {
		t.Fatalf("ReadBatch failed: %v", err)
	}
	if len(batch.Items) == 0 {
		t.Skip("No messages to test ClaimIdle")
		return
	}

	time.Sleep(cfg.ClaimIdle + 10*time.Millisecond)

	claimCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	claimed, err := client.ClaimIdle(claimCtx)
	if err != nil {
		t.Fatalf("ClaimIdle failed: %v", err)
	}
	t.Logf("Claimed %d idle messages", len(claimed.Items))

	if len(claimed.Items) > 0 {
		for _, msg := range claimed.Items {
			_ = client.AckAndDelete(ctx, msg)
		}
	}
}

func testAckAndDeleteMultiple(t *testing.T, client *Client, cfg *config.RedisConfig) {
	t.Helper()
	ctx := t.Context()

	ids := make([]string, 3)
	for i := range 3 {
		msgID, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: cfg.Stream,
			Values: map[string]any{"object": "multi test"},
		}).Result()
		if err != nil {
			t.Fatalf("Failed to add message %d: %v", i, err)
		}
		ids[i] = msgID
	}

	readCtx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	batch, err := client.ReadBatch(readCtx)
	cancel()
	if err != nil {
		t.Fatalf("ReadBatch failed: %v", err)
	}
	if len(batch.Items) != 3 {
		t.Fatalf("Expected 3 messages, got %d", len(batch.Items))
	}

	for _, msg := range batch.Items {
		err = client.AckAndDelete(ctx, msg)
		if err != nil {
			t.Fatalf("AckAndDelete failed: %v", err)
		}
	}
	t.Log("Successfully acknowledged and deleted multiple messages")
}

// TestIntegration_RedisClose tests client cleanup
func TestIntegration_RedisClose(t *testing.T) {
	cfg := setupRedisConfig(t)
	cfg.Stream = "test-stream-close"
	logger := log.New()

	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Skip("Redis not available")
		return
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Close again may return error but should not panic
	err = client.Close()
	if err != nil {
		t.Logf("Second Close returned expected error: %v", err)
	}

	t.Log("Successfully closed client")
}

// TestIntegration_RedisCleanup tests cleanup functions
func TestIntegration_RedisCleanup(t *testing.T) {
	cfg := setupRedisConfig(t)
	cfg.Stream = "test-stream-cleanup"
	logger := log.New()

	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Skip("Redis not available")
		return
	}
	defer func() { _ = client.Close() }()
	defer func() { _ = client.rdb.Del(t.Context(), cfg.Stream) }()

	t.Run("CleanupDeadConsumers", func(t *testing.T) { testCleanupDeadConsumers(t, client, cfg) })
}

func testCleanupDeadConsumers(t *testing.T, client *Client, cfg *config.RedisConfig) {
	t.Helper()
	ctx := t.Context()
	deadConsumerName := "dead-consumer-test"

	// Create a dead consumer
	msgID := createDeadConsumerForTest(t, client, cfg, deadConsumerName)

	// Ensure our consumer is also registered by reading a message
	ensureOurConsumerExists(t, client, cfg)

	time.Sleep(100 * time.Millisecond)

	err := client.CleanupDeadConsumers(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CleanupDeadConsumers failed: %v", err)
	}

	verifyCleanupResults(t, client, cfg, deadConsumerName)
	t.Log("Successfully cleaned up dead consumer and preserved self")
	_ = client.rdb.XDel(ctx, cfg.Stream, msgID)
}

func ensureOurConsumerExists(t *testing.T, client *Client, cfg *config.RedisConfig) {
	t.Helper()
	// Read a batch to ensure our consumer is registered
	readCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	_, _ = client.ReadBatch(readCtx)
	cancel()
}

func createDeadConsumerForTest(
	t *testing.T, client *Client, cfg *config.RedisConfig, consumerName string,
) string {
	t.Helper()
	ctx := t.Context()
	msgID, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.Stream,
		Values: map[string]any{"object": "cleanup test"},
	}).Result()
	if err != nil {
		t.Fatalf("Failed to add message: %v", err)
	}

	groupForTest := cfg.GroupName
	_, err = client.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupForTest,
		Consumer: consumerName,
		Streams:  []string{cfg.Stream, ">"},
		Count:    1,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("Failed to read with dead consumer: %v", err)
	}

	return msgID
}

func verifyCleanupResults(
	t *testing.T, client *Client, cfg *config.RedisConfig, deadConsumerName string,
) {
	t.Helper()
	consumers, err := client.rdb.XInfoConsumers(t.Context(), cfg.Stream, cfg.GroupName).Result()
	if err != nil {
		t.Fatalf("Failed to get consumers: %v", err)
	}

	foundDead := false
	foundSelf := false
	for _, c := range consumers {
		if c.Name == deadConsumerName {
			foundDead = true
		}
		if c.Name == cfg.Consumer {
			foundSelf = true
		}
	}

	if foundDead {
		t.Error("Dead consumer was not removed")
	}
	if !foundSelf {
		t.Error("CleanupDeadConsumers removed ourselves!")
	}
}

// TestIntegration_RedisErrors tests error scenarios
func TestIntegration_RedisErrors(t *testing.T) {
	cfg := setupRedisConfig(t)
	logger := log.New()

	t.Run("InvalidAddress", func(t *testing.T) {
		badCfg := *cfg
		badCfg.Address = "invalid:99999"

		_, err := NewClient(&badCfg, logger)
		if err == nil {
			t.Error("Expected error for invalid address, got nil")
		}

		t.Logf("Correctly handled invalid address: %v", err)
	})

	t.Run("ConnectionTimeout", func(t *testing.T) {
		badCfg := *cfg
		badCfg.Address = "10.255.255.1:6379"
		badCfg.PingTimeout = 100 * time.Millisecond

		_, err := NewClient(&badCfg, logger)
		if err == nil {
			t.Error("Expected timeout error, got nil")
		}

		t.Logf("Correctly handled connection timeout: %v", err)
	})
}
