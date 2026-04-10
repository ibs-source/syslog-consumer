//go:build integration

package redis

import (
	"testing"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/log"
)

// TestCleanupDeadConsumers_Integration verifies CleanupDeadConsumers against a
// live Redis instance. Skipped when Redis is unavailable or -short is set.
func TestCleanupDeadConsumers_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Setenv("REDIS_ADDRESS", "localhost:6379")
	t.Setenv("REDIS_STREAM", "test-cleanup-integration")
	t.Setenv("REDIS_CONSUMER", "test-consumer")
	t.Setenv("REDIS_CLAIM_IDLE", "100ms")

	fullCfg, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	cfg := &fullCfg.Redis

	logger := log.New()
	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Skip("Redis not available, skipping integration test")
		return
	}
	defer func() { _ = client.Close() }()

	ctx := t.Context()
	defer client.rdb.Del(ctx, cfg.Stream)

	err = client.CleanupDeadConsumers(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CleanupDeadConsumers failed: %v", err)
	}
}
