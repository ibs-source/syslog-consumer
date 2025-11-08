package config

import (
	"os"
	"strconv"
	"time"
)

// loadRedisFromEnv loads Redis configuration from environment variables
func loadRedisFromEnv(cfg *RedisConfig) {
	loadRedisStrings(cfg)
	loadRedisInts(cfg)
	loadRedisTimeouts(cfg)
}

func loadRedisStrings(cfg *RedisConfig) {
	if v := getEnvString("REDIS_ADDRESS"); v != "" {
		cfg.Address = v
	}
	if v := getEnvString("REDIS_STREAM"); v != "" {
		cfg.Stream = v
	}
	if v := getEnvString("REDIS_CONSUMER"); v != "" {
		cfg.Consumer = v
	}
}

func loadRedisInts(cfg *RedisConfig) {
	if v := getEnvInt("REDIS_BATCH_SIZE"); v != 0 {
		cfg.BatchSize = v
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
	if v := getEnvInt("MQTT_QOS"); v != 0 && v >= 0 && v <= 2 {
		cfg.QoS = byte(v) // #nosec G115 - validated range 0-2
	}
	if v := getEnvInt("MQTT_POOL_SIZE"); v != 0 {
		cfg.PoolSize = v
	}
	if v := getEnvInt("MQTT_DISCONNECT_TIMEOUT"); v != 0 {
		cfg.DisconnectTimeout = uint(v) // #nosec G115 - config values are non-negative
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
	if v := getEnvBool("MQTT_TLS_ENABLED"); v {
		cfg.TLSEnabled = v
	}
	if v := getEnvBool("MQTT_TLS_INSECURE_SKIP"); v {
		cfg.InsecureSkip = v
	}
	if v := getEnvBool("MQTT_USE_CERT_CN_PREFIX"); v {
		cfg.UseCertCNPrefix = v
	}
}

// loadPipelineFromEnv loads Pipeline configuration from environment variables
func loadPipelineFromEnv(cfg *PipelineConfig) {
	if v := getEnvInt("PIPELINE_BUFFER_CAPACITY"); v != 0 {
		cfg.BufferCapacity = v
	}
	if v := getEnvDuration("PIPELINE_SHUTDOWN_TIMEOUT"); v != 0 {
		cfg.ShutdownTimeout = v
	}
	if v := getEnvDuration("PIPELINE_ERROR_BACKOFF"); v != 0 {
		cfg.ErrorBackoff = v
	}
	if v := getEnvDuration("PIPELINE_ACK_TIMEOUT"); v != 0 {
		cfg.AckTimeout = v
	}
	if v := getEnvInt("PIPELINE_PUBLISH_WORKERS"); v != 0 {
		cfg.PublishWorkers = v
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
		return 0
	}
	return intValue
}

func getEnvDuration(key string) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return 0
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return duration
}

func getEnvBool(key string) bool {
	value := os.Getenv(key)
	return value == "true"
}
