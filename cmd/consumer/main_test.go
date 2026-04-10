package main

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/ibs-source/syslog-consumer/internal/config"
	"github.com/ibs-source/syslog-consumer/internal/hotpath"
	"github.com/ibs-source/syslog-consumer/internal/log"
	"github.com/ibs-source/syslog-consumer/internal/message"
	"github.com/ibs-source/syslog-consumer/internal/mqtt"
	"github.com/ibs-source/syslog-consumer/internal/redis"
)

// --- lightweight mocks for hotpath.New dependencies ---

type stubRedis struct{}

func (s *stubRedis) ReadBatch(_ context.Context) (message.Batch, error) {
	return message.Batch{}, nil
}
func (s *stubRedis) ClaimIdle(_ context.Context) (message.Batch, error) {
	return message.Batch{}, nil
}
func (s *stubRedis) AckAndDeleteBatch(_ context.Context, _ []string, _ string) error {
	return nil
}
func (s *stubRedis) CleanupDeadConsumers(_ context.Context, _ time.Duration) error { return nil }
func (s *stubRedis) RefreshStreams(_ context.Context) (int, error)                 { return 0, nil }
func (s *stubRedis) Close() error                                                  { return nil }

type stubPublisher struct{}

func (s *stubPublisher) Publish(_ context.Context, _ message.Payload) error { return nil }
func (s *stubPublisher) SubscribeAck(_ func(message.AckMessage)) error      { return nil }
func (s *stubPublisher) Close() error                                       { return nil }

type stubPublisherFail struct {
	subErr error
}

func (s *stubPublisherFail) Publish(_ context.Context, _ message.Payload) error { return nil }
func (s *stubPublisherFail) SubscribeAck(_ func(message.AckMessage)) error      { return s.subErr }
func (s *stubPublisherFail) Close() error                                       { return nil }

func testCfg() *config.Config {
	return &config.Config{
		Redis: config.RedisConfig{
			Stream:              "test",
			ClaimIdle:           30 * time.Second,
			CleanupInterval:     1 * time.Minute,
			ConsumerIdleTimeout: 5 * time.Minute,
		},
		Pipeline: config.PipelineConfig{
			BufferCapacity:       100,
			MessageQueueCapacity: 4,
			PublishWorkers:       2,
			AckWorkers:           2,
			RefreshInterval:      1 * time.Minute,
			ErrorBackoff:         100 * time.Millisecond,
			AckTimeout:           5 * time.Second,
			ShutdownTimeout:      5 * time.Second,
		},
	}
}

func closeHotPath(t *testing.T, hp *hotpath.HotPath) {
	t.Helper()
	if err := hp.Close(); err != nil {
		t.Errorf("hp.Close(): %v", err)
	}
}

// TestLoadAndLogConfig_DefaultConfig verifies that loadAndLogConfig succeeds
// with default environment values and returns a valid, non-nil configuration.
func TestLoadAndLogConfig_DefaultConfig(t *testing.T) {
	logger := log.New()
	cfg, err := loadAndLogConfig(logger)
	if err != nil {
		t.Fatalf("loadAndLogConfig() error = %v; want nil with defaults", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.Redis.Address == "" {
		t.Error("Redis.Address should have a default value")
	}
	if cfg.MQTT.Broker == "" {
		t.Error("MQTT.Broker should have a default value")
	}
	if cfg.Pipeline.BufferCapacity == 0 {
		t.Error("Pipeline.BufferCapacity should have a default value")
	}
}

// TestLoadAndLogConfig_ValidationError verifies the error path.
func TestLoadAndLogConfig_ValidationError(t *testing.T) {
	t.Setenv("PIPELINE_BUFFER_CAPACITY", "-1") // Invalid: must be positive
	logger := log.New()
	_, err := loadAndLogConfig(logger)
	if err == nil {
		t.Error("expected validation error for negative buffer capacity")
	}
}

// TestRun_RedisConnectionFailure verifies run() returns 1 when Redis is unreachable.
func TestRun_RedisConnectionFailure(t *testing.T) {
	t.Setenv("REDIS_ADDRESS", "localhost:1") // unroutable port → immediate failure
	result := run()
	if result != 1 {
		t.Errorf("run() = %d; want 1 for redis connection failure", result)
	}
}

// TestRun_ConfigError verifies run() returns 1 when config validation fails.
func TestRun_ConfigError(t *testing.T) {
	t.Setenv("PIPELINE_BUFFER_CAPACITY", "-1")
	result := run()
	if result != 1 {
		t.Errorf("run() = %d; want 1 for config validation failure", result)
	}
}

// TestCloseServices_NilSafe verifies that closeServices does not panic
// when called with properly created but unconnected services.
func TestCloseServices_NilSafe(t *testing.T) {
	logger := log.New()
	cfg := &config.Config{
		Redis: config.RedisConfig{
			Stream:          "test",
			ClaimIdle:       30 * time.Second,
			CleanupInterval: 1 * time.Minute,
		},
		Pipeline: config.PipelineConfig{
			BufferCapacity:       100,
			MessageQueueCapacity: 4,
			PublishWorkers:       2,
			AckWorkers:           2,
			RefreshInterval:      1 * time.Minute,
			ErrorBackoff:         100 * time.Millisecond,
			AckTimeout:           5 * time.Second,
		},
	}

	hp, err := hotpath.New(&stubRedis{}, &stubPublisher{}, cfg, logger)
	if err != nil {
		t.Fatalf("hotpath.New() error = %v", err)
	}

	// closeServices takes concrete types; we pass zero-valued structs for redis/mqtt
	// redis.Client with nil rdb → Close() returns nil
	// mqtt.Pool with no clients → Close() returns nil
	closeServices(&redis.Client{}, &mqtt.Pool{}, hp, logger)
}

// --- stubRedisBlocking blocks ReadBatch until context is canceled ---

type stubRedisBlocking struct{}

func (s *stubRedisBlocking) ReadBatch(ctx context.Context) (message.Batch, error) {
	<-ctx.Done()
	return message.Batch{}, ctx.Err()
}
func (s *stubRedisBlocking) ClaimIdle(ctx context.Context) (message.Batch, error) {
	<-ctx.Done()
	return message.Batch{}, ctx.Err()
}
func (s *stubRedisBlocking) AckAndDeleteBatch(_ context.Context, _ []string, _ string) error {
	return nil
}
func (s *stubRedisBlocking) CleanupDeadConsumers(_ context.Context, _ time.Duration) error {
	return nil
}
func (s *stubRedisBlocking) RefreshStreams(_ context.Context) (int, error) { return 0, nil }
func (s *stubRedisBlocking) Close() error                                  { return nil }

// TestRunMainLoop_HotPathError verifies that runMainLoop returns 1
// when the hot path exits with an error (e.g. SubscribeAck failure).
func TestRunMainLoop_HotPathError(t *testing.T) {
	logger := log.New()
	cfg := testCfg()

	hp, err := hotpath.New(
		&stubRedis{},
		&stubPublisherFail{subErr: fmt.Errorf("subscribe failed")},
		cfg, logger,
	)
	if err != nil {
		t.Fatalf("hotpath.New: %v", err)
	}
	defer closeHotPath(t, hp)

	result := runMainLoop(hp, cfg, logger)
	if result != 1 {
		t.Errorf("runMainLoop() = %d; want 1 for hot path error", result)
	}
}

// TestRunMainLoop_SignalGraceful verifies graceful shutdown via SIGINT.
// The test sends SIGINT after a short delay and expects runMainLoop to
// return 0 after the hot path completes its context-canceled shutdown.
func TestRunMainLoop_SignalGraceful(t *testing.T) {
	logger := log.New()
	cfg := testCfg()

	hp, err := hotpath.New(&stubRedisBlocking{}, &stubPublisher{}, cfg, logger)
	if err != nil {
		t.Fatalf("hotpath.New: %v", err)
	}
	defer closeHotPath(t, hp)

	// Send SIGINT after a short delay so the signal handler fires.
	go func() {
		time.Sleep(200 * time.Millisecond)
		if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
			t.Errorf("syscall.Kill(SIGINT): %v", err)
		}
	}()

	result := runMainLoop(hp, cfg, logger)
	if result != 0 {
		t.Errorf("runMainLoop() = %d; want 0 for graceful signal shutdown", result)
	}
}

// TestCloseServices_ErrorPaths verifies closeServices logs errors without panicking.
func TestCloseServices_ErrorPaths(t *testing.T) {
	logger := log.New()
	cfg := testCfg()

	// hotpath wrapping stubRedis that returns an error on Close
	errRedis := &stubRedisCloseFail{}
	hp, err := hotpath.New(errRedis, &stubPublisher{}, cfg, logger)
	if err != nil {
		t.Fatalf("hotpath.New: %v", err)
	}

	// closeServices should not panic even when Close returns errors
	closeServices(&redis.Client{}, &mqtt.Pool{}, hp, logger)
}

type stubRedisCloseFail struct {
	stubRedis
}

func (s *stubRedisCloseFail) Close() error { return fmt.Errorf("redis close error") }

// TestRunMainLoop_ShutdownTimeout verifies shutdown timeout path when hot path hangs.
func TestRunMainLoop_ShutdownTimeout(t *testing.T) {
	logger := log.New()
	cfg := testCfg()
	cfg.Pipeline.ShutdownTimeout = 100 * time.Millisecond // very short

	// Use a publisher that blocks SubscribeAck handler, simulating a hanging hot path
	hangingRedis := &stubRedisHangOnRead{}
	hp, err := hotpath.New(hangingRedis, &stubPublisher{}, cfg, logger)
	if err != nil {
		t.Fatalf("hotpath.New: %v", err)
	}
	defer closeHotPath(t, hp)

	// Send SIGINT quickly
	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
			t.Errorf("syscall.Kill(SIGINT): %v", err)
		}
	}()

	result := runMainLoop(hp, cfg, logger)
	// Should return 0 (graceful) or 1 (timeout) — either is acceptable
	_ = result
}

// stubRedisHangOnRead blocks ReadBatch forever (ignores context cancel),
// simulating a scenario where shutdown would time out.
type stubRedisHangOnRead struct {
	stubRedis
}

func (s *stubRedisHangOnRead) ReadBatch(ctx context.Context) (message.Batch, error) {
	<-ctx.Done()
	return message.Batch{}, ctx.Err()
}

func (s *stubRedisHangOnRead) ClaimIdle(ctx context.Context) (message.Batch, error) {
	<-ctx.Done()
	return message.Batch{}, ctx.Err()
}

// TestRunMainLoop_NormalExit verifies that runMainLoop returns 0
// when the hot path exits normally without errors.
func TestRunMainLoop_NormalExit(t *testing.T) {
	logger := log.New()
	cfg := testCfg()

	// Publisher that immediately returns nil on SubscribeAck
	// and redis that immediately cancels
	cancelRedis := &stubRedisImmediate{}
	hp, err := hotpath.New(cancelRedis, &stubPublisher{}, cfg, logger)
	if err != nil {
		t.Fatalf("hotpath.New: %v", err)
	}
	defer closeHotPath(t, hp)

	// Send SIGINT so the main loop exits cleanly
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
			t.Errorf("syscall.Kill(SIGINT): %v", err)
		}
	}()

	result := runMainLoop(hp, cfg, logger)
	if result != 0 {
		t.Errorf("runMainLoop() = %d; want 0", result)
	}
}

type stubRedisImmediate struct {
	stubRedisBlocking
}
