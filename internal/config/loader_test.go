package config

import (
	"flag"
	"os"
	"testing"
	"time"
)

const (
	testRedisAddr  = "localhost:6379"
	testMQTTBroker = "tcp://localhost:1883"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear environment and reset flags
	clearTestEnv(t)
	resetTestFlags(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Verify Redis defaults
	if cfg.Redis.Address != testRedisAddr {
		t.Errorf("Redis.Address = %s; want %s", cfg.Redis.Address, testRedisAddr)
	}
	if cfg.Redis.Stream != "syslog-stream" {
		t.Errorf("Redis.Stream = %s; want syslog-stream", cfg.Redis.Stream)
	}
	if cfg.Redis.BatchSize != 500 {
		t.Errorf("Redis.BatchSize = %d; want 500", cfg.Redis.BatchSize)
	}

	// Verify MQTT defaults
	if cfg.MQTT.Broker != testMQTTBroker {
		t.Errorf("MQTT.Broker = %s; want %s", cfg.MQTT.Broker, testMQTTBroker)
	}
	if cfg.MQTT.PoolSize != 10 {
		t.Errorf("MQTT.PoolSize = %d; want 10", cfg.MQTT.PoolSize)
	}

	// Verify Pipeline defaults
	if cfg.Pipeline.BufferCapacity != 10000 {
		t.Errorf("Pipeline.BufferCapacity = %d; want 10000", cfg.Pipeline.BufferCapacity)
	}
	if cfg.Pipeline.PublishWorkers != 20 {
		t.Errorf("Pipeline.PublishWorkers = %d; want 20", cfg.Pipeline.PublishWorkers)
	}
}

func TestLoad_EnvironmentVariables(t *testing.T) {
	// Clear and set environment
	clearTestEnv(t)
	resetTestFlags(t)

	t.Setenv("REDIS_ADDRESS", "redis-env:6379")
	t.Setenv("REDIS_STREAM", "env-stream")
	t.Setenv("REDIS_BATCH_SIZE", "100")
	t.Setenv("MQTT_BROKER", "tcp://mqtt-env:1883")
	t.Setenv("MQTT_POOL_SIZE", "5")
	t.Setenv("PIPELINE_BUFFER_CAPACITY", "500")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Verify environment variables were applied
	if cfg.Redis.Address != "redis-env:6379" {
		t.Errorf("Redis.Address = %s; want redis-env:6379", cfg.Redis.Address)
	}
	if cfg.Redis.Stream != "env-stream" {
		t.Errorf("Redis.Stream = %s; want env-stream", cfg.Redis.Stream)
	}
	if cfg.Redis.BatchSize != 100 {
		t.Errorf("Redis.BatchSize = %d; want 100", cfg.Redis.BatchSize)
	}
	if cfg.MQTT.Broker != "tcp://mqtt-env:1883" {
		t.Errorf("MQTT.Broker = %s; want tcp://mqtt-env:1883", cfg.MQTT.Broker)
	}
	if cfg.MQTT.PoolSize != 5 {
		t.Errorf("MQTT.PoolSize = %d; want 5", cfg.MQTT.PoolSize)
	}
	if cfg.Pipeline.BufferCapacity != 500 {
		t.Errorf("Pipeline.BufferCapacity = %d; want 500", cfg.Pipeline.BufferCapacity)
	}
}

func TestLoad_FlagsPrecedence(t *testing.T) {
	// Set environment variables
	clearTestEnv(t)
	t.Setenv("REDIS_ADDRESS", "redis-env:6379")
	t.Setenv("MQTT_BROKER", "tcp://mqtt-env:1883")

	// Set command line flags (should override environment)
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{
		"test",
		"-redis-address=redis-flag:6379",
		"-mqtt-broker=tcp://mqtt-flag:1883",
	}

	// Reset and parse flags
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Flags should override environment variables
	if cfg.Redis.Address != "redis-flag:6379" {
		t.Errorf("Redis.Address = %s; want redis-flag:6379", cfg.Redis.Address)
	}
	if cfg.MQTT.Broker != "tcp://mqtt-flag:1883" {
		t.Errorf("MQTT.Broker = %s; want tcp://mqtt-flag:1883", cfg.MQTT.Broker)
	}
}

func TestLoad_ValidationError(t *testing.T) {
	clearTestEnv(t)
	resetTestFlags(t)

	// Set invalid batch size (negative value will be applied and fail validation)
	t.Setenv("REDIS_BATCH_SIZE", "-1")

	_, err := Load()
	if err == nil {
		t.Error("Load() error = nil; want validation error")
	}
}

func TestLoad_CompleteConfiguration(t *testing.T) {
	clearTestEnv(t)
	resetTestFlags(t)
	setCompleteEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	verifyRedisConfig(t, cfg)
	verifyMQTTConfig(t, cfg)
	verifyPipelineConfig(t, cfg)
}

func setCompleteEnv(t *testing.T) {
	t.Helper()
	// Set comprehensive environment variables
	t.Setenv("REDIS_ADDRESS", "redis:6379")
	t.Setenv("REDIS_STREAM", "test-stream")
	t.Setenv("REDIS_CONSUMER", "test-consumer")
	t.Setenv("REDIS_BATCH_SIZE", "100")
	t.Setenv("REDIS_BLOCK_TIMEOUT", "5s")

	t.Setenv("MQTT_BROKER", "tcp://mqtt:1883")
	t.Setenv("MQTT_CLIENT_ID", "test-client")
	t.Setenv("MQTT_PUBLISH_TOPIC", "test/pub")
	t.Setenv("MQTT_ACK_TOPIC", "test/ack")
	t.Setenv("MQTT_QOS", "1")
	t.Setenv("MQTT_POOL_SIZE", "5")

	t.Setenv("PIPELINE_BUFFER_CAPACITY", "1000")
	t.Setenv("PIPELINE_PUBLISH_WORKERS", "10")
	t.Setenv("PIPELINE_SHUTDOWN_TIMEOUT", "30s")
}

func verifyRedisConfig(t *testing.T, cfg *Config) {
	t.Helper()
	if cfg.Redis.Address != "redis:6379" {
		t.Errorf("Redis.Address = %s; want redis:6379", cfg.Redis.Address)
	}
	if cfg.Redis.BatchSize != 100 {
		t.Errorf("Redis.BatchSize = %d; want 100", cfg.Redis.BatchSize)
	}
	if cfg.Redis.BlockTimeout != 5*time.Second {
		t.Errorf("Redis.BlockTimeout = %v; want 5s", cfg.Redis.BlockTimeout)
	}
}

func verifyMQTTConfig(t *testing.T, cfg *Config) {
	t.Helper()
	if cfg.MQTT.Broker != "tcp://mqtt:1883" {
		t.Errorf("MQTT.Broker = %s; want tcp://mqtt:1883", cfg.MQTT.Broker)
	}
	if cfg.MQTT.QoS != 1 {
		t.Errorf("MQTT.QoS = %d; want 1", cfg.MQTT.QoS)
	}
	if cfg.MQTT.PoolSize != 5 {
		t.Errorf("MQTT.PoolSize = %d; want 5", cfg.MQTT.PoolSize)
	}
}

func verifyPipelineConfig(t *testing.T, cfg *Config) {
	t.Helper()
	if cfg.Pipeline.BufferCapacity != 1000 {
		t.Errorf("Pipeline.BufferCapacity = %d; want 1000", cfg.Pipeline.BufferCapacity)
	}
	if cfg.Pipeline.PublishWorkers != 10 {
		t.Errorf("Pipeline.PublishWorkers = %d; want 10", cfg.Pipeline.PublishWorkers)
	}
	if cfg.Pipeline.ShutdownTimeout != 30*time.Second {
		t.Errorf("Pipeline.ShutdownTimeout = %v; want 30s", cfg.Pipeline.ShutdownTimeout)
	}
}

// Helper functions for tests

func clearTestEnv(t *testing.T) {
	t.Helper()
	envVars := []string{
		"REDIS_ADDRESS", "REDIS_STREAM", "REDIS_CONSUMER",
		"REDIS_BATCH_SIZE", "REDIS_BLOCK_TIMEOUT", "REDIS_CLAIM_IDLE",
		"REDIS_CONSUMER_IDLE_TIMEOUT", "REDIS_CLEANUP_INTERVAL",
		"REDIS_DIAL_TIMEOUT", "REDIS_READ_TIMEOUT", "REDIS_WRITE_TIMEOUT", "REDIS_PING_TIMEOUT",
		"MQTT_BROKER", "MQTT_CLIENT_ID", "MQTT_PUBLISH_TOPIC", "MQTT_ACK_TOPIC",
		"MQTT_QOS", "MQTT_CONNECT_TIMEOUT", "MQTT_WRITE_TIMEOUT", "MQTT_POOL_SIZE",
		"MQTT_MAX_RECONNECT_INTERVAL", "MQTT_SUBSCRIBE_TIMEOUT", "MQTT_DISCONNECT_TIMEOUT",
		"MQTT_TLS_ENABLED", "MQTT_CA_CERT", "MQTT_CLIENT_CERT", "MQTT_CLIENT_KEY",
		"MQTT_TLS_INSECURE_SKIP", "MQTT_USE_CERT_CN_PREFIX",
		"PIPELINE_BUFFER_CAPACITY", "PIPELINE_SHUTDOWN_TIMEOUT",
		"PIPELINE_ERROR_BACKOFF", "PIPELINE_ACK_TIMEOUT", "PIPELINE_PUBLISH_WORKERS",
	}
	for _, v := range envVars {
		_ = os.Unsetenv(v)
	}
}

func resetTestFlags(t *testing.T) {
	t.Helper()
	// Save old args
	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })

	// Reset to minimal args
	os.Args = []string{"test"}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
}
