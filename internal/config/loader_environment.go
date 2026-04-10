package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// loadLogFromEnv loads logging configuration from environment variables.
func loadLogFromEnv(cfg *LogConfig) {
	if v := getEnvString("LOG_LEVEL"); v != "" {
		cfg.Level = v
	}
}

// loadRedisFromEnv loads Redis configuration from environment variables.
func loadRedisFromEnv(cfg *RedisConfig) {
	loadRedisStrings(cfg)
	loadRedisInts(cfg)
	loadRedisTimeouts(cfg)
}

func loadRedisStrings(cfg *RedisConfig) {
	if v := getEnvString("REDIS_ADDRESS"); v != "" {
		cfg.Address = v
	}
	// Use LookupEnv so that REDIS_STREAM="" explicitly sets empty string
	// (required for multi-stream mode). LookupEnv distinguishes "not set" from "set to empty".
	if v, ok := os.LookupEnv("REDIS_STREAM"); ok {
		cfg.Stream = v
	}
	if v := getEnvString("REDIS_CONSUMER"); v != "" {
		cfg.Consumer = v
	}
	if v := getEnvString("REDIS_GROUP_NAME"); v != "" {
		cfg.GroupName = v
	}
}

func loadRedisInts(cfg *RedisConfig) {
	if v := getEnvInt("REDIS_BATCH_SIZE"); v != 0 {
		cfg.BatchSize = v
	}
	if v := getEnvInt("REDIS_POOL_SIZE"); v != 0 {
		cfg.PoolSize = v
	}
	if v := getEnvInt("REDIS_MIN_IDLE_CONNS"); v != 0 {
		cfg.MinIdleConns = v
	}
	if v := getEnvInt("REDIS_DISCOVERY_SCAN_COUNT"); v != 0 {
		cfg.DiscoveryScanCount = v
	}
}

func loadRedisTimeouts(cfg *RedisConfig) {
	if v := getEnvDuration("REDIS_BLOCK_TIMEOUT"); v != 0 {
		cfg.BlockTimeout = v
	}
	if v := getEnvDuration("REDIS_CLAIM_IDLE"); v != 0 {
		cfg.ClaimIdle = v
	}
	if v := getEnvDuration("REDIS_CONSUMER_IDLE_TIMEOUT"); v != 0 {
		cfg.ConsumerIdleTimeout = v
	}
	if v := getEnvDuration("REDIS_CLEANUP_INTERVAL"); v != 0 {
		cfg.CleanupInterval = v
	}
	if v := getEnvDuration("REDIS_DIAL_TIMEOUT"); v != 0 {
		cfg.DialTimeout = v
	}
	if v := getEnvDuration("REDIS_READ_TIMEOUT"); v != 0 {
		cfg.ReadTimeout = v
	}
	if v := getEnvDuration("REDIS_WRITE_TIMEOUT"); v != 0 {
		cfg.WriteTimeout = v
	}
	if v := getEnvDuration("REDIS_PING_TIMEOUT"); v != 0 {
		cfg.PingTimeout = v
	}
}

// loadMQTTFromEnv loads MQTT configuration from environment variables
func loadMQTTFromEnv(cfg *MQTTConfig) {
	loadMQTTStrings(cfg)
	loadMQTTInts(cfg)
	loadMQTTTimeouts(cfg)
	loadMQTTTLS(cfg)
	loadMQTTBools(cfg)
}

func loadMQTTStrings(cfg *MQTTConfig) {
	if v := getEnvString("MQTT_BROKER"); v != "" {
		cfg.Broker = v
	}
	if v := getEnvString("MQTT_CLIENT_ID"); v != "" {
		cfg.ClientID = v
	}
	if v := getEnvString("MQTT_PUBLISH_TOPIC"); v != "" {
		cfg.PublishTopic = v
	}
	if v := getEnvString("MQTT_ACK_TOPIC"); v != "" {
		cfg.AckTopic = v
	}
}

func loadMQTTInts(cfg *MQTTConfig) {
	if raw, ok := os.LookupEnv("MQTT_QOS"); ok && raw != "" {
		v, err := strconv.Atoi(raw)
		if err == nil && v >= 0 && v <= 2 {
			cfg.QoS = byte(min(max(v, 0), 2))
		}
	}
	if v := getEnvInt("MQTT_POOL_SIZE"); v != 0 {
		cfg.PoolSize = v
	}
	if v := getEnvDuration("MQTT_DISCONNECT_TIMEOUT"); v != 0 {
		cfg.DisconnectTimeout = v
	}
	if v := getEnvUint("MQTT_MESSAGE_CHANNEL_DEPTH"); v != 0 {
		cfg.MessageChannelDepth = v
	}
	if v := getEnvInt("MQTT_MAX_RESUME_PUB_IN_FLIGHT"); v != 0 {
		cfg.MaxResumePubInFlight = v
	}
}

func loadMQTTTimeouts(cfg *MQTTConfig) {
	if v := getEnvDuration("MQTT_CONNECT_TIMEOUT"); v != 0 {
		cfg.ConnectTimeout = v
	}
	if v := getEnvDuration("MQTT_WRITE_TIMEOUT"); v != 0 {
		cfg.WriteTimeout = v
	}
	if v := getEnvDuration("MQTT_MAX_RECONNECT_INTERVAL"); v != 0 {
		cfg.MaxReconnectInterval = v
	}
	if v := getEnvDuration("MQTT_SUBSCRIBE_TIMEOUT"); v != 0 {
		cfg.SubscribeTimeout = v
	}
	if v := getEnvDuration("MQTT_KEEP_ALIVE"); v != 0 {
		cfg.KeepAlive = v
	}
	if v := getEnvDuration("MQTT_PING_TIMEOUT"); v != 0 {
		cfg.PingTimeout = v
	}
	if v := getEnvDuration("MQTT_CONNECT_RETRY_DELAY"); v != 0 {
		cfg.ConnectRetryDelay = v
	}
}

func loadMQTTTLS(cfg *MQTTConfig) {
	if v := getEnvString("MQTT_CA_CERT"); v != "" {
		cfg.CACert = v
	}
	if v := getEnvString("MQTT_CLIENT_CERT"); v != "" {
		cfg.ClientCert = v
	}
	if v := getEnvString("MQTT_CLIENT_KEY"); v != "" {
		cfg.ClientKey = v
	}
}

func loadMQTTBools(cfg *MQTTConfig) {
	if v, ok := lookupEnvBool("MQTT_TLS_ENABLED"); ok {
		cfg.TLSEnabled = v
	}
	if v, ok := lookupEnvBool("MQTT_TLS_INSECURE_SKIP"); ok {
		cfg.InsecureSkip = v
	}
	if v, ok := lookupEnvBool("MQTT_USE_CERT_CN_PREFIX"); ok {
		cfg.UseCertCNPrefix = v
	}
}

// loadPipelineFromEnv loads Pipeline configuration from environment variables.
func loadPipelineFromEnv(cfg *PipelineConfig) {
	loadPipelineIntsFromEnv(cfg)
	loadPipelineDurationsFromEnv(cfg)
	if v := getEnvString("PIPELINE_HEALTH_ADDR"); v != "" {
		cfg.HealthAddr = v
	}
}

func loadPipelineIntsFromEnv(cfg *PipelineConfig) {
	if v := getEnvInt("PIPELINE_BUFFER_CAPACITY"); v != 0 {
		cfg.BufferCapacity = v
	}
	if v := getEnvInt("PIPELINE_PUBLISH_WORKERS"); v != 0 {
		cfg.PublishWorkers = v
	}
	if v := getEnvInt("PIPELINE_ACK_BATCH_SIZE"); v != 0 {
		cfg.AckBatchSize = v
	}
	if v := getEnvInt("PIPELINE_MESSAGE_QUEUE_CAPACITY"); v != 0 {
		cfg.MessageQueueCapacity = v
	}
	if v := getEnvInt("PIPELINE_ACK_WORKERS"); v != 0 {
		cfg.AckWorkers = v
	}
}

func loadPipelineDurationsFromEnv(cfg *PipelineConfig) {
	if v := getEnvDuration("PIPELINE_SHUTDOWN_TIMEOUT"); v != 0 {
		cfg.ShutdownTimeout = v
	}
	if v := getEnvDuration("PIPELINE_ERROR_BACKOFF"); v != 0 {
		cfg.ErrorBackoff = v
	}
	if v := getEnvDuration("PIPELINE_ACK_TIMEOUT"); v != 0 {
		cfg.AckTimeout = v
	}
	if v := getEnvDuration("PIPELINE_REFRESH_INTERVAL"); v != 0 {
		cfg.RefreshInterval = v
	}
	if v := getEnvDuration("PIPELINE_ACK_FLUSH_INTERVAL"); v != 0 {
		cfg.AckFlushInterval = v
	}
	if v := getEnvDuration("PIPELINE_HEALTH_PING_TIMEOUT"); v != 0 {
		cfg.HealthPingTimeout = v
	}
	if v := getEnvDuration("PIPELINE_HEALTH_READ_HEADER_TIMEOUT"); v != 0 {
		cfg.HealthReadHeaderTimeout = v
	}
}

// Helper functions for reading environment variables

func getEnvString(key string) string {
	return os.Getenv(key)
}

func getEnvInt(key string) int {
	value := os.Getenv(key)
	if value == "" {
		return 0
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: invalid integer for %s=%q, using default\n", key, value)
		return 0
	}
	return intValue
}

func getEnvUint(key string) uint {
	value := os.Getenv(key)
	if value == "" {
		return 0
	}
	v, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: invalid unsigned integer for %s=%q, using default\n", key, value)
		return 0
	}
	return uint(v)
}

func getEnvDuration(key string) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return 0
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: invalid duration for %s=%q, using default\n", key, value)
		return 0
	}
	return duration
}

// lookupEnvBool returns (value, true) if the env var is set, allowing
// the caller to distinguish "not set" from "explicitly set to false".
func lookupEnvBool(key string) (value, ok bool) {
	rawValue, ok := os.LookupEnv(key)
	if !ok || rawValue == "" {
		return false, false
	}
	value, err := strconv.ParseBool(rawValue)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "WARNING: invalid boolean for %s=%q, using default\n", key, rawValue)
		return false, false
	}
	return value, true
}
