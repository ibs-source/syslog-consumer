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

		ctx := context.Background()
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

	ctx := context.Background()
	defer client.rdb.Del(ctx, cfg.Stream)

	t.Run("ReadBatch_Empty", func(t *testing.T) { testReadBatchEmpty(t, client) })
	t.Run("AddAndReadMessage", func(t *testing.T) { testAddAndReadMessage(t, client, cfg) })
	t.Run("ClaimIdle", func(t *testing.T) { testClaimIdle(t, client, cfg) })
	t.Run("AckAndDelete_Multiple", func(t *testing.T) { testAckAndDeleteMultiple(t, client, cfg) })
}

func testReadBatchEmpty(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
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
	payload := parsePayload(t, batch)

	verifyPayloadFields(t, payload)
	verifyObjectField(t, payload)

	t.Log("Successfully read message with all fields (including parsed object) from stream")

	ackAndDeleteMessage(t, client, batch)
	t.Log("Successfully acknowledged and deleted message")
}

func addTestMessage(t *testing.T, client *Client, cfg *config.RedisConfig) string {
	t.Helper()
	ctx := context.Background()
	msgID, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.Stream,
		Values: map[string]interface{}{
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

func readMessageBatch(t *testing.T, client *Client) message.Batch[message.Payload] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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

func parsePayload(t *testing.T, batch message.Batch[message.Payload]) map[string]interface{} {
	t.Helper()
	var payload map[string]interface{}
	if err := json.Unmarshal(batch.Items[0].Body, &payload); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}
	return payload
}

func verifyPayloadFields(t *testing.T, payload map[string]interface{}) {
	t.Helper()
	if payload["source"] != "10.0.0.1" {
		t.Errorf("Expected source '10.0.0.1', got '%v'", payload["source"])
	}
	if payload["timestamp"] != "1234567890" {
		t.Errorf("Expected timestamp '1234567890', got '%v'", payload["timestamp"])
	}
	if payload["raw"] != "raw data" {
		t.Errorf("Expected raw 'raw data', got '%v'", payload["raw"])
	}
}

func verifyObjectField(t *testing.T, payload map[string]interface{}) {
	t.Helper()
	objectField, ok := payload["object"].(map[string]interface{})
	if !ok {
		t.Errorf("Expected 'object' to be a JSON object, got type %T: %v", payload["object"], payload["object"])
		return
	}
	if objectField["message"] != "test payload" {
		t.Errorf("Expected object.message 'test payload', got '%v'", objectField["message"])
	}
	if objectField["severity"] != float64(5) {
		t.Errorf("Expected object.severity 5, got '%v'", objectField["severity"])
	}
}

func ackAndDeleteMessage(t *testing.T, client *Client, batch message.Batch[message.Payload]) {
	t.Helper()
	ctx := context.Background()
	if err := client.AckAndDelete(ctx, batch.Items[0]); err != nil {
		t.Fatalf("AckAndDelete failed: %v", err)
	}
}

func testClaimIdle(t *testing.T, client *Client, cfg *config.RedisConfig) {
	t.Helper()
	ctx := context.Background()

	_, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.Stream,
		Values: map[string]interface{}{"object": "idle test"},
	}).Result()
	if err != nil {
		t.Fatalf("Failed to add message: %v", err)
	}

	readCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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

	claimCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
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
	ctx := context.Background()

	ids := make([]string, 3)
	for i := 0; i < 3; i++ {
		msgID, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: cfg.Stream,
			Values: map[string]interface{}{"object": "multi test"},
		}).Result()
		if err != nil {
			t.Fatalf("Failed to add message %d: %v", i, err)
		}
		ids[i] = msgID
	}

	readCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
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
	defer func() { _ = client.rdb.Del(context.Background(), cfg.Stream) }()

	t.Run("CleanupDeadConsumers", func(t *testing.T) { testCleanupDeadConsumers(t, client, cfg) })
}

func testCleanupDeadConsumers(t *testing.T, client *Client, cfg *config.RedisConfig) {
	t.Helper()
	ctx := context.Background()
	deadConsumerName := "dead-consumer-test"

	// Create a dead consumer
	msgID := createDeadConsumerForTest(t, ctx, client, cfg, deadConsumerName)

	// Ensure our consumer is also registered by reading a message
	ensureOurConsumerExists(t, ctx, client, cfg)

	time.Sleep(100 * time.Millisecond)

	err := client.CleanupDeadConsumers(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CleanupDeadConsumers failed: %v", err)
	}

	verifyCleanupResults(t, ctx, client, cfg, deadConsumerName)
	t.Log("Successfully cleaned up dead consumer and preserved self")
	_ = client.rdb.XDel(ctx, cfg.Stream, msgID)
}

//nolint:revive // test helper with t *testing.T as first param is idiomatic
func ensureOurConsumerExists(t *testing.T, ctx context.Context, client *Client, cfg *config.RedisConfig) {
	t.Helper()
	// Read a batch to ensure our consumer is registered
	readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _ = client.ReadBatch(readCtx)
	cancel()
}

//nolint:revive // test helper with t *testing.T as first param is idiomatic
func createDeadConsumerForTest(
	t *testing.T, ctx context.Context, client *Client, cfg *config.RedisConfig, consumerName string,
) string {
	t.Helper()
	msgID, err := client.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: cfg.Stream,
		Values: map[string]interface{}{"object": "cleanup test"},
	}).Result()
	if err != nil {
		t.Fatalf("Failed to add message: %v", err)
	}

	groupName := client.groups[cfg.Stream]
	_, err = client.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{cfg.Stream, ">"},
		Count:    1,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("Failed to read with dead consumer: %v", err)
	}

	return msgID
}

//nolint:revive // test helper with t *testing.T as first param is idiomatic
func verifyCleanupResults(
	t *testing.T, ctx context.Context, client *Client, cfg *config.RedisConfig, deadConsumerName string,
) {
	t.Helper()
	groupName := client.groups[cfg.Stream]
	consumers, err := client.rdb.XInfoConsumers(ctx, cfg.Stream, groupName).Result()
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
