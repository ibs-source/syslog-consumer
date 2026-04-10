package config

import (
	"testing"
	"time"
)

func TestLoadRedisFromEnv(t *testing.T) {
	// Start with defaults
	cfg := defaultRedisConfig()

	// Set environment variables
	t.Setenv("REDIS_ADDRESS", "redis-test:6379")
	t.Setenv("REDIS_STREAM", "test-stream")
	t.Setenv("REDIS_CONSUMER", "test-consumer")
	t.Setenv("REDIS_BATCH_SIZE", "100")
	t.Setenv("REDIS_BLOCK_TIMEOUT", "3s")
	t.Setenv("REDIS_CLAIM_IDLE", "20s")
	t.Setenv("REDIS_CONSUMER_IDLE_TIMEOUT", "3m")
	t.Setenv("REDIS_CLEANUP_INTERVAL", "2m")
	t.Setenv("REDIS_DIAL_TIMEOUT", "5s")
	t.Setenv("REDIS_READ_TIMEOUT", "7s")
	t.Setenv("REDIS_WRITE_TIMEOUT", "3s")
	t.Setenv("REDIS_PING_TIMEOUT", "2s")

	// Load from environment
	loadRedisFromEnv(&cfg)

	// Verify
	tests := []struct {
		got  any
		want any
		name string
	}{
		{cfg.Address, "redis-test:6379", "Address"},
		{cfg.Stream, "test-stream", "Stream"},
		{cfg.Consumer, "test-consumer", "Consumer"},
		{cfg.BatchSize, 100, "BatchSize"},
		{cfg.BlockTimeout, 3 * time.Second, "BlockTimeout"},
		{cfg.ClaimIdle, 20 * time.Second, "ClaimIdle"},
		{cfg.ConsumerIdleTimeout, 3 * time.Minute, "ConsumerIdleTimeout"},
		{cfg.CleanupInterval, 2 * time.Minute, "CleanupInterval"},
		{cfg.DialTimeout, 5 * time.Second, "DialTimeout"},
		{cfg.ReadTimeout, 7 * time.Second, "ReadTimeout"},
		{cfg.WriteTimeout, 3 * time.Second, "WriteTimeout"},
		{cfg.PingTimeout, 2 * time.Second, "PingTimeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("loadRedisFromEnv() %s = %v; want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestLoadMQTTFromEnv(t *testing.T) {
	// Start with defaults
	cfg := defaultMQTTConfig()

	// Set environment variables
	t.Setenv("MQTT_BROKER", "tcp://mqtt-test:1883")
	t.Setenv("MQTT_CLIENT_ID", "test-client")
	t.Setenv("MQTT_PUBLISH_TOPIC", "test/pub")
	t.Setenv("MQTT_ACK_TOPIC", "test/ack")
	t.Setenv("MQTT_QOS", "1")
	t.Setenv("MQTT_CONNECT_TIMEOUT", "5s")
	t.Setenv("MQTT_WRITE_TIMEOUT", "20s")
	t.Setenv("MQTT_POOL_SIZE", "3")
	t.Setenv("MQTT_MAX_RECONNECT_INTERVAL", "5s")
	t.Setenv("MQTT_SUBSCRIBE_TIMEOUT", "5s")
	t.Setenv("MQTT_DISCONNECT_TIMEOUT", "500ms")
	t.Setenv("MQTT_CA_CERT", "/path/ca.crt")
	t.Setenv("MQTT_CLIENT_CERT", "/path/client.crt")
	t.Setenv("MQTT_CLIENT_KEY", "/path/client.key")
	t.Setenv("MQTT_TLS_ENABLED", "true")
	t.Setenv("MQTT_TLS_INSECURE_SKIP", "true")
	t.Setenv("MQTT_USE_CERT_CN_PREFIX", "true")

	// Load from environment
	loadMQTTFromEnv(&cfg)

	// Verify
	tests := []struct {
		got  any
		want any
		name string
	}{
		{cfg.Broker, "tcp://mqtt-test:1883", "Broker"},
		{cfg.ClientID, "test-client", "ClientID"},
		{cfg.PublishTopic, "test/pub", "PublishTopic"},
		{cfg.AckTopic, "test/ack", "AckTopic"},
		{cfg.QoS, byte(1), "QoS"},
		{cfg.ConnectTimeout, 5 * time.Second, "ConnectTimeout"},
		{cfg.WriteTimeout, 20 * time.Second, "WriteTimeout"},
		{cfg.PoolSize, 3, "PoolSize"},
		{cfg.MaxReconnectInterval, 5 * time.Second, "MaxReconnectInterval"},
		{cfg.SubscribeTimeout, 5 * time.Second, "SubscribeTimeout"},
		{cfg.DisconnectTimeout, 500 * time.Millisecond, "DisconnectTimeout"},
		{cfg.CACert, "/path/ca.crt", "CACert"},
		{cfg.ClientCert, "/path/client.crt", "ClientCert"},
		{cfg.ClientKey, "/path/client.key", "ClientKey"},
		{cfg.TLSEnabled, true, "TLSEnabled"},
		{cfg.InsecureSkip, true, "InsecureSkip"},
		{cfg.UseCertCNPrefix, true, "UseCertCNPrefix"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("loadMQTTFromEnv() %s = %v; want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestLoadPipelineFromEnv(t *testing.T) {
	// Start with defaults
	cfg := defaultPipelineConfig()

	// Set environment variables
	t.Setenv("PIPELINE_BUFFER_CAPACITY", "500")
	t.Setenv("PIPELINE_SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("PIPELINE_ERROR_BACKOFF", "2s")
	t.Setenv("PIPELINE_ACK_TIMEOUT", "3s")
	t.Setenv("PIPELINE_PUBLISH_WORKERS", "10")
	t.Setenv("PIPELINE_REFRESH_INTERVAL", "2m")
	t.Setenv("PIPELINE_HEALTH_ADDR", ":9090")

	// Load from environment
	loadPipelineFromEnv(&cfg)

	// Verify
	tests := []struct {
		got  any
		want any
		name string
	}{
		{cfg.BufferCapacity, 500, "BufferCapacity"},
		{cfg.ShutdownTimeout, 15 * time.Second, "ShutdownTimeout"},
		{cfg.ErrorBackoff, 2 * time.Second, "ErrorBackoff"},
		{cfg.AckTimeout, 3 * time.Second, "AckTimeout"},
		{cfg.PublishWorkers, 10, "PublishWorkers"},
		{cfg.RefreshInterval, 2 * time.Minute, "RefreshInterval"},
		{cfg.HealthAddr, ":9090", "HealthAddr"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("loadPipelineFromEnv() %s = %v; want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestGetEnvHelpers(t *testing.T) {
	t.Run("getEnvString", testGetEnvString)
	t.Run("getEnvInt", testGetEnvInt)
	t.Run("getEnvDuration", testGetEnvDuration)
	t.Run("lookupEnvBool", testLookupEnvBool)
}

func testGetEnvString(t *testing.T) {
	t.Setenv("TEST_STRING", "hello")
	if got := getEnvString("TEST_STRING"); got != "hello" {
		t.Errorf("getEnvString() = %s; want hello", got)
	}
	if got := getEnvString("NONEXISTENT"); got != "" {
		t.Errorf("getEnvString() = %s; want empty string", got)
	}
}

func testGetEnvInt(t *testing.T) {
	t.Setenv("TEST_INT", "42")
	if got := getEnvInt("TEST_INT"); got != 42 {
		t.Errorf("getEnvInt() = %d; want 42", got)
	}
	if got := getEnvInt("NONEXISTENT"); got != 0 {
		t.Errorf("getEnvInt() = %d; want 0", got)
	}
	t.Setenv("TEST_INT_INVALID", "not-a-number")
	if got := getEnvInt("TEST_INT_INVALID"); got != 0 {
		t.Errorf("getEnvInt() with invalid value = %d; want 0", got)
	}
}

func testGetEnvDuration(t *testing.T) {
	t.Setenv("TEST_DURATION", "5s")
	if got := getEnvDuration("TEST_DURATION"); got != 5*time.Second {
		t.Errorf("getEnvDuration() = %v; want 5s", got)
	}
	if got := getEnvDuration("NONEXISTENT"); got != 0 {
		t.Errorf("getEnvDuration() = %v; want 0", got)
	}
	t.Setenv("TEST_DURATION_INVALID", "not-a-duration")
	if got := getEnvDuration("TEST_DURATION_INVALID"); got != 0 {
		t.Errorf("getEnvDuration() with invalid value = %v; want 0", got)
	}
}

func testLookupEnvBool(t *testing.T) {
	t.Run("true_values", testLookupEnvBoolTrue)
	t.Run("false_values", testLookupEnvBoolFalse)
	t.Run("missing", testLookupEnvBoolMissing)
	t.Run("invalid", testLookupEnvBoolInvalid)
}

func testLookupEnvBoolTrue(t *testing.T) {
	t.Setenv("TEST_BOOL_TRUE", "true")
	if v, ok := lookupEnvBool("TEST_BOOL_TRUE"); !ok || !v {
		t.Error("lookupEnvBool(true) = false; want true")
	}
	t.Setenv("TEST_BOOL_1", "1")
	if v, ok := lookupEnvBool("TEST_BOOL_1"); !ok || !v {
		t.Error("lookupEnvBool(1) = false; want true")
	}
}

func testLookupEnvBoolFalse(t *testing.T) {
	t.Setenv("TEST_BOOL_FALSE", "false")
	if v, ok := lookupEnvBool("TEST_BOOL_FALSE"); !ok || v {
		t.Error("lookupEnvBool(false): want (false, true)")
	}
	t.Setenv("TEST_BOOL_0", "0")
	if v, ok := lookupEnvBool("TEST_BOOL_0"); !ok || v {
		t.Error("lookupEnvBool(0): want (false, true)")
	}
}

func testLookupEnvBoolMissing(t *testing.T) {
	if _, ok := lookupEnvBool("NONEXISTENT"); ok {
		t.Error("lookupEnvBool(NONEXISTENT): want (_, false)")
	}
}

func testLookupEnvBoolInvalid(t *testing.T) {
	t.Setenv("TEST_BOOL_INVALID", "not-a-bool")
	if _, ok := lookupEnvBool("TEST_BOOL_INVALID"); ok {
		t.Error("lookupEnvBool(not-a-bool): want (_, false)")
	}
}

func TestLoadRedisFromEnv_PartialOverride(t *testing.T) {
	// Start with defaults
	cfg := defaultRedisConfig()
	originalStream := cfg.Stream

	// Only override address
	t.Setenv("REDIS_ADDRESS", "custom:6379")

	loadRedisFromEnv(&cfg)

	// Address should be overridden
	if cfg.Address != "custom:6379" {
		t.Errorf("Address = %s; want custom:6379", cfg.Address)
	}

	// Stream should remain default
	if cfg.Stream != originalStream {
		t.Errorf("Stream = %s; want %s", cfg.Stream, originalStream)
	}
}
