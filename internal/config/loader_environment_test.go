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
		name string
		got  interface{}
		want interface{}
	}{
		{"Address", cfg.Address, "redis-test:6379"},
		{"Stream", cfg.Stream, "test-stream"},
		{"Consumer", cfg.Consumer, "test-consumer"},
		{"BatchSize", cfg.BatchSize, 100},
		{"BlockTimeout", cfg.BlockTimeout, 3 * time.Second},
		{"ClaimIdle", cfg.ClaimIdle, 20 * time.Second},
		{"ConsumerIdleTimeout", cfg.ConsumerIdleTimeout, 3 * time.Minute},
		{"CleanupInterval", cfg.CleanupInterval, 2 * time.Minute},
		{"DialTimeout", cfg.DialTimeout, 5 * time.Second},
		{"ReadTimeout", cfg.ReadTimeout, 7 * time.Second},
		{"WriteTimeout", cfg.WriteTimeout, 3 * time.Second},
		{"PingTimeout", cfg.PingTimeout, 2 * time.Second},
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
	t.Setenv("MQTT_DISCONNECT_TIMEOUT", "500")
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
		name string
		got  interface{}
		want interface{}
	}{
		{"Broker", cfg.Broker, "tcp://mqtt-test:1883"},
		{"ClientID", cfg.ClientID, "test-client"},
		{"PublishTopic", cfg.PublishTopic, "test/pub"},
		{"AckTopic", cfg.AckTopic, "test/ack"},
		{"QoS", cfg.QoS, byte(1)},
		{"ConnectTimeout", cfg.ConnectTimeout, 5 * time.Second},
		{"WriteTimeout", cfg.WriteTimeout, 20 * time.Second},
		{"PoolSize", cfg.PoolSize, 3},
		{"MaxReconnectInterval", cfg.MaxReconnectInterval, 5 * time.Second},
		{"SubscribeTimeout", cfg.SubscribeTimeout, 5 * time.Second},
		{"DisconnectTimeout", cfg.DisconnectTimeout, uint(500)},
		{"CACert", cfg.CACert, "/path/ca.crt"},
		{"ClientCert", cfg.ClientCert, "/path/client.crt"},
		{"ClientKey", cfg.ClientKey, "/path/client.key"},
		{"TLSEnabled", cfg.TLSEnabled, true},
		{"InsecureSkip", cfg.InsecureSkip, true},
		{"UseCertCNPrefix", cfg.UseCertCNPrefix, true},
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

	// Load from environment
	loadPipelineFromEnv(&cfg)

	// Verify
	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"BufferCapacity", cfg.BufferCapacity, 500},
		{"ShutdownTimeout", cfg.ShutdownTimeout, 15 * time.Second},
		{"ErrorBackoff", cfg.ErrorBackoff, 2 * time.Second},
		{"AckTimeout", cfg.AckTimeout, 3 * time.Second},
		{"PublishWorkers", cfg.PublishWorkers, 10},
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
	t.Run("getEnvBool", testGetEnvBool)
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

func testGetEnvBool(t *testing.T) {
	t.Setenv("TEST_BOOL_TRUE", "true")
	if got := getEnvBool("TEST_BOOL_TRUE"); !got {
		t.Error("getEnvBool() = false; want true")
	}
	t.Setenv("TEST_BOOL_FALSE", "false")
	if got := getEnvBool("TEST_BOOL_FALSE"); got {
		t.Error("getEnvBool() = true; want false")
	}
	if got := getEnvBool("NONEXISTENT"); got {
		t.Error("getEnvBool() = true; want false")
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
