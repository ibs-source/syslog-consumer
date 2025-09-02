package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// LoadFromEnvironment loads configuration from environment variables
func LoadFromEnvironment(cfg *Config) {
	applyAppEnv(cfg)

	applyRedisEnv(cfg)
	applyMQTTEnv(cfg)
	applyResourceEnv(cfg)

	applyMetricsEnv(cfg)
	applyHealthEnv(cfg)

	applyPipelineEnv(cfg)
	applyRetryEnv(cfg)
	applyDLQEnv(cfg)

	applyCircuitBreakerEnv(cfg)
}

// --- App ---

func applyAppEnv(cfg *Config) {
	if val := os.Getenv("APP_NAME"); val != "" {
		cfg.App.Name = val
	}
	if val := os.Getenv("APP_ENV"); val != "" {
		cfg.App.Environment = val
	}
	if val := os.Getenv("LOG_LEVEL"); val != "" {
		cfg.App.LogLevel = val
	}
	if val := os.Getenv("LOG_FORMAT"); val != "" {
		cfg.App.LogFormat = val
	}
	if val := getEnvDuration("APP_SHUTDOWN_TIMEOUT"); val != 0 {
		cfg.App.ShutdownTimeout = val
	}
	if val := getEnvDuration("APP_PENDING_OPS_GRACE"); val != 0 {
		cfg.App.PendingOpsGrace = val
	}
}

// --- Redis (split to reduce complexity) ---

func applyRedisEnv(cfg *Config) {
	applyRedisBasicsEnv(cfg)
	applyRedisTimeoutsEnv(cfg)
	applyRedisPoolEnv(cfg)
	applyRedisClaimEnv(cfg)
	applyRedisAdvancedEnv(cfg)
}

func applyRedisBasicsEnv(cfg *Config) {
	if val := getEnvStringSlice("REDIS_ADDRESSES"); len(val) > 0 {
		cfg.Redis.Addresses = val
	}
	if val := os.Getenv("REDIS_PASSWORD"); val != "" {
		cfg.Redis.Password = val
	}
	if val := getEnvInt("REDIS_DB"); val >= 0 {
		cfg.Redis.DB = val
	}
	if val := os.Getenv("REDIS_STREAM"); val != "" {
		cfg.Redis.StreamName = val
	}
	if val := os.Getenv("REDIS_CONSUMER_GROUP"); val != "" {
		cfg.Redis.ConsumerGroup = val
	}
	if val := getEnvInt("REDIS_MAX_RETRIES"); val > 0 {
		cfg.Redis.MaxRetries = val
	}
	if val := getEnvDuration("REDIS_RETRY_INTERVAL"); val != 0 {
		cfg.Redis.RetryInterval = val
	}
	// General batch/read block time
	if val := getEnvInt64("REDIS_BATCH_SIZE"); val > 0 {
		cfg.Redis.BatchSize = val
	}
	if val := getEnvDuration("REDIS_BLOCK_TIME"); val != 0 {
		cfg.Redis.BlockTime = val
	}
}

func applyRedisTimeoutsEnv(cfg *Config) {
	if val := getEnvDuration("REDIS_CONNECT_TIMEOUT"); val != 0 {
		cfg.Redis.ConnectTimeout = val
	}
	if val := getEnvDuration("REDIS_READ_TIMEOUT"); val != 0 {
		cfg.Redis.ReadTimeout = val
	}
	if val := getEnvDuration("REDIS_WRITE_TIMEOUT"); val != 0 {
		cfg.Redis.WriteTimeout = val
	}
}

func applyRedisPoolEnv(cfg *Config) {
	if val := getEnvInt("REDIS_POOL_SIZE"); val > 0 {
		cfg.Redis.PoolSize = val
	}
	if val := getEnvInt("REDIS_MIN_IDLE_CONNS"); val > 0 {
		cfg.Redis.MinIdleConns = val
	}
	if val := getEnvDuration("REDIS_MAX_CONN_AGE"); val != 0 {
		cfg.Redis.ConnMaxLifetime = val
	}
	if val := getEnvDuration("REDIS_IDLE_TIMEOUT"); val != 0 {
		cfg.Redis.ConnMaxIdleTime = val
	}
}

func applyRedisClaimEnv(cfg *Config) {
	if val := getEnvDuration("REDIS_CLAIM_MIN_IDLE"); val != 0 {
		cfg.Redis.ClaimMinIdleTime = val
	}
	if val := getEnvInt("REDIS_CLAIM_BATCH_SIZE"); val > 0 {
		cfg.Redis.ClaimBatchSize = val
	}
	if val := getEnvDuration("REDIS_PENDING_CHECK_INTERVAL"); val != 0 {
		cfg.Redis.PendingCheckInterval = val
	}
	if val := getEnvDuration("REDIS_CLAIM_INTERVAL"); val != 0 {
		cfg.Redis.ClaimInterval = val
	}
}

func applyRedisAdvancedEnv(cfg *Config) {
	if val := os.Getenv("REDIS_AGGRESSIVE_CLAIM"); val != "" {
		cfg.Redis.AggressiveClaim = getEnvBool("REDIS_AGGRESSIVE_CLAIM")
	}
	if val := getEnvDuration("REDIS_CLAIM_CYCLE_DELAY"); val != 0 {
		cfg.Redis.ClaimCycleDelay = val
	}
	if val := os.Getenv("REDIS_DRAIN_ENABLED"); val != "" {
		cfg.Redis.DrainEnabled = getEnvBool("REDIS_DRAIN_ENABLED")
	}
	if val := getEnvDuration("REDIS_DRAIN_INTERVAL"); val != 0 {
		cfg.Redis.DrainInterval = val
	}
	if val := getEnvInt64("REDIS_DRAIN_BATCH_SIZE"); val > 0 {
		cfg.Redis.DrainBatchSize = val
	}
	if val := os.Getenv("REDIS_CONSUMER_CLEANUP_ENABLED"); val != "" {
		cfg.Redis.ConsumerCleanupEnabled = getEnvBool("REDIS_CONSUMER_CLEANUP_ENABLED")
	}
	if val := getEnvDuration("REDIS_CONSUMER_IDLE_TIMEOUT"); val != 0 {
		cfg.Redis.ConsumerIdleTimeout = val
	}
	if val := getEnvDuration("REDIS_CONSUMER_CLEANUP_INTERVAL"); val != 0 {
		cfg.Redis.ConsumerCleanupInterval = val
	}
}

// --- MQTT (split to reduce complexity) ---

func applyMQTTEnv(cfg *Config) {
	applyMQTTBasicsEnv(cfg)
	applyMQTTSecurityTLSEnv(cfg)
	applyMQTTTopicsEnv(cfg)
	applyMQTTPerformanceEnv(cfg)
}

func applyMQTTBasicsEnv(cfg *Config) {
	if val := getEnvStringSlice("MQTT_BROKERS"); len(val) > 0 {
		cfg.MQTT.Brokers = val
	}
	if val := os.Getenv("MQTT_CLIENT_ID"); val != "" {
		cfg.MQTT.ClientID = val
	}
	if val := getEnvInt("MQTT_QOS"); val >= 0 && val <= 2 {
		cfg.MQTT.QoS = byte(val)
	}
	if val := getEnvDuration("MQTT_KEEP_ALIVE"); val != 0 {
		cfg.MQTT.KeepAlive = val
	}
	if val := getEnvDuration("MQTT_CONNECT_TIMEOUT"); val != 0 {
		cfg.MQTT.ConnectTimeout = val
	}
	if val := getEnvDuration("MQTT_MAX_RECONNECT_DELAY"); val != 0 {
		cfg.MQTT.MaxReconnectDelay = val
	}
	if val := os.Getenv("MQTT_CLEAN_SESSION"); val != "" {
		cfg.MQTT.CleanSession = getEnvBool("MQTT_CLEAN_SESSION")
	}
	if val := os.Getenv("MQTT_ORDER_MATTERS"); val != "" {
		cfg.MQTT.OrderMatters = getEnvBool("MQTT_ORDER_MATTERS")
	}
}

func applyMQTTSecurityTLSEnv(cfg *Config) {
	if val := os.Getenv("MQTT_TLS_ENABLED"); val != "" {
		cfg.MQTT.TLS.Enabled = getEnvBool("MQTT_TLS_ENABLED")
	}
	if val := os.Getenv("MQTT_CA_CERT"); val != "" {
		cfg.MQTT.TLS.CACertFile = val
	}
	if val := os.Getenv("MQTT_CLIENT_CERT"); val != "" {
		cfg.MQTT.TLS.ClientCertFile = val
	}
	if val := os.Getenv("MQTT_CLIENT_KEY"); val != "" {
		cfg.MQTT.TLS.ClientKeyFile = val
	}
	if val := os.Getenv("MQTT_TLS_INSECURE"); val != "" {
		cfg.MQTT.TLS.InsecureSkipVerify = getEnvBool("MQTT_TLS_INSECURE")
	}
	if val := os.Getenv("MQTT_TLS_SERVER_NAME"); val != "" {
		cfg.MQTT.TLS.ServerName = val
	}
	if val := os.Getenv("MQTT_TLS_MIN_VERSION"); val != "" {
		cfg.MQTT.TLS.MinVersion = val
	}
	if val := getEnvStringSlice("MQTT_TLS_CIPHER_SUITES"); len(val) > 0 {
		cfg.MQTT.TLS.CipherSuites = val
	}
	if val := os.Getenv("MQTT_TLS_PREFER_SERVER_CIPHERS"); val != "" {
		cfg.MQTT.TLS.PreferServerCiphers = getEnvBool("MQTT_TLS_PREFER_SERVER_CIPHERS")
	}
}

func applyMQTTTopicsEnv(cfg *Config) {
	if val := os.Getenv("MQTT_PUBLISH_TOPIC"); val != "" {
		cfg.MQTT.Topics.PublishTopic = val
	}
	if val := os.Getenv("MQTT_SUBSCRIBE_TOPIC"); val != "" {
		cfg.MQTT.Topics.SubscribeTopic = val
	}
	if val := os.Getenv("MQTT_USE_USER_PREFIX"); val != "" {
		cfg.MQTT.Topics.UseUserPrefix = getEnvBool("MQTT_USE_USER_PREFIX")
	}
	if val := os.Getenv("MQTT_CUSTOM_PREFIX"); val != "" {
		cfg.MQTT.Topics.CustomPrefix = val
	}
}

func applyMQTTPerformanceEnv(cfg *Config) {
	if val := getEnvInt("MQTT_PUBLISHER_POOL_SIZE"); val > 0 {
		cfg.MQTT.PublisherPoolSize = val
	}
	if val := getEnvInt("MQTT_MAX_INFLIGHT"); val > 0 {
		cfg.MQTT.MaxInflight = val
	}
	if val := getEnvInt("MQTT_MESSAGE_CHANNEL_DEPTH"); val > 0 {
		cfg.MQTT.MessageChannelDepth = val
	}
	if val := getEnvDuration("MQTT_WRITE_TIMEOUT"); val != 0 {
		cfg.MQTT.WriteTimeout = val
	}
}

// --- Resource (split) ---

func applyResourceEnv(cfg *Config) {
	applyResourceThresholdsEnv(cfg)
	applyResourceDurationsEnv(cfg)
	applyResourceCountsEnv(cfg)
}

func applyResourceThresholdsEnv(cfg *Config) {
	if val := getEnvFloat64("RESOURCE_CPU_HIGH"); val > 0 {
		cfg.Resource.CPUThresholdHigh = val
	}
	if val := getEnvFloat64("RESOURCE_CPU_LOW"); val > 0 {
		cfg.Resource.CPUThresholdLow = val
	}
	if val := getEnvFloat64("RESOURCE_MEM_HIGH"); val > 0 {
		cfg.Resource.MemoryThresholdHigh = val
	}
	if val := getEnvFloat64("RESOURCE_MEM_LOW"); val > 0 {
		cfg.Resource.MemoryThresholdLow = val
	}
}

func applyResourceDurationsEnv(cfg *Config) {
	if val := getEnvDuration("RESOURCE_CHECK_INTERVAL"); val != 0 {
		cfg.Resource.CheckInterval = val
	}
	if val := getEnvDuration("RESOURCE_SCALE_UP_COOLDOWN"); val != 0 {
		cfg.Resource.ScaleUpCooldown = val
	}
	if val := getEnvDuration("RESOURCE_SCALE_DOWN_COOLDOWN"); val != 0 {
		cfg.Resource.ScaleDownCooldown = val
	}
	if val := getEnvDuration("RESOURCE_PREDICTION_HORIZON"); val != 0 {
		cfg.Resource.PredictionHorizon = val
	}
}

func applyResourceCountsEnv(cfg *Config) {
	if val := getEnvInt("RESOURCE_MIN_WORKERS"); val > 0 {
		cfg.Resource.MinWorkers = val
	}
	if val := getEnvInt("RESOURCE_MAX_WORKERS"); val > 0 {
		cfg.Resource.MaxWorkers = val
	}
	if val := getEnvInt("RESOURCE_WORKER_STEP"); val > 0 {
		cfg.Resource.WorkerStepSize = val
	}
	if val := os.Getenv("RESOURCE_PREDICTIVE_SCALING"); val != "" {
		cfg.Resource.EnablePredictiveScaling = getEnvBool("RESOURCE_PREDICTIVE_SCALING")
	}
	if val := getEnvInt("RESOURCE_HISTORY_WINDOW"); val > 0 {
		cfg.Resource.HistoryWindowSize = val
	}
}

// --- Metrics ---

func applyMetricsEnv(cfg *Config) {
	if val := os.Getenv("METRICS_ENABLED"); val != "" {
		cfg.Metrics.Enabled = getEnvBool("METRICS_ENABLED")
	}
	if val := getEnvInt("METRICS_PORT"); val > 0 {
		cfg.Metrics.PrometheusPort = val
	}
	if val := getEnvDuration("METRICS_COLLECT_INTERVAL"); val != 0 {
		cfg.Metrics.CollectInterval = val
	}
	if val := os.Getenv("METRICS_NAMESPACE"); val != "" {
		cfg.Metrics.Namespace = val
	}
	if val := os.Getenv("METRICS_SUBSYSTEM"); val != "" {
		cfg.Metrics.Subsystem = val
	}
	if val := getEnvFloatSlice("METRICS_HISTOGRAM_BUCKETS"); len(val) > 0 {
		cfg.Metrics.HistogramBuckets = val
	}
}

// --- Health ---

func applyHealthEnv(cfg *Config) {
	if val := os.Getenv("HEALTH_ENABLED"); val != "" {
		cfg.Health.Enabled = getEnvBool("HEALTH_ENABLED")
	}
	if val := getEnvInt("HEALTH_PORT"); val > 0 {
		cfg.Health.Port = val
	}
	if val := getEnvDuration("HEALTH_READ_TIMEOUT"); val != 0 {
		cfg.Health.ReadTimeout = val
	}
	if val := getEnvDuration("HEALTH_WRITE_TIMEOUT"); val != 0 {
		cfg.Health.WriteTimeout = val
	}
	if val := getEnvDuration("HEALTH_CHECK_INTERVAL"); val != 0 {
		cfg.Health.CheckInterval = val
	}
	if val := getEnvDuration("HEALTH_REDIS_TIMEOUT"); val != 0 {
		cfg.Health.RedisTimeout = val
	}
	if val := getEnvDuration("HEALTH_MQTT_TIMEOUT"); val != 0 {
		cfg.Health.MQTTTimeout = val
	}
}

// --- Pipeline (+ Retry/DLQ are separate helpers) ---

func applyPipelineEnv(cfg *Config) {
	applyPipelineCoreEnv(cfg)
	applyPipelineTogglesEnv(cfg)
	applyPipelineScheduleEnv(cfg)
}

func applyPipelineCoreEnv(cfg *Config) {
	if val := getEnvInt("PIPELINE_BUFFER_SIZE"); val > 0 {
		cfg.Pipeline.BufferSize = nextPowerOf2(val)
	}
	if val := getEnvInt("PIPELINE_BATCH_SIZE"); val > 0 {
		cfg.Pipeline.BatchSize = val
	}
	if val := getEnvDuration("PIPELINE_BATCH_TIMEOUT"); val != 0 {
		cfg.Pipeline.BatchTimeout = val
	}
	if val := getEnvDuration("PIPELINE_PROCESSING_TIMEOUT"); val != 0 {
		cfg.Pipeline.ProcessingTimeout = val
	}
}

func applyPipelineTogglesEnv(cfg *Config) {
	if val := os.Getenv("PIPELINE_ZERO_COPY"); val != "" {
		cfg.Pipeline.UseZeroCopy = getEnvBool("PIPELINE_ZERO_COPY")
	}
	if val := os.Getenv("PIPELINE_PREALLOCATE"); val != "" {
		cfg.Pipeline.PreallocateBuffers = getEnvBool("PIPELINE_PREALLOCATE")
	}
	if val := os.Getenv("PIPELINE_NUMA_AWARE"); val != "" {
		cfg.Pipeline.NumaAware = getEnvBool("PIPELINE_NUMA_AWARE")
	}
}

func applyPipelineScheduleEnv(cfg *Config) {
	if val := getEnvIntSlice("PIPELINE_CPU_AFFINITY"); len(val) > 0 {
		cfg.Pipeline.CPUAffinity = val
	}
	if val := getEnvFloat64("PIPELINE_BACKPRESSURE_THRESHOLD"); val > 0 {
		cfg.Pipeline.BackpressureThreshold = val
	}
	if val := os.Getenv("PIPELINE_DROP_POLICY"); val != "" {
		cfg.Pipeline.DropPolicy = val
	}
	if val := getEnvDuration("PIPELINE_FLUSH_INTERVAL"); val != 0 {
		cfg.Pipeline.FlushInterval = val
	}
	if val := getEnvDuration("PIPELINE_BACKPRESSURE_POLL_INTERVAL"); val != 0 {
		cfg.Pipeline.BackpressurePollInterval = val
	}
	if val := getEnvDuration("PIPELINE_IDLE_POLL_SLEEP"); val != 0 {
		cfg.Pipeline.IdlePollSleep = val
	}
}

func applyRetryEnv(cfg *Config) {
	if val := os.Getenv("PIPELINE_RETRY_ENABLED"); val != "" {
		cfg.Pipeline.Retry.Enabled = getEnvBool("PIPELINE_RETRY_ENABLED")
	}
	if val := getEnvInt("PIPELINE_RETRY_MAX_ATTEMPTS"); val > 0 {
		cfg.Pipeline.Retry.MaxAttempts = val
	}
	if val := getEnvDuration("PIPELINE_RETRY_INITIAL_BACKOFF"); val != 0 {
		cfg.Pipeline.Retry.InitialBackoff = val
	}
	if val := getEnvDuration("PIPELINE_RETRY_MAX_BACKOFF"); val != 0 {
		cfg.Pipeline.Retry.MaxBackoff = val
	}
	if val := getEnvFloat64("PIPELINE_RETRY_MULTIPLIER"); val > 0 {
		cfg.Pipeline.Retry.Multiplier = val
	}
}

func applyDLQEnv(cfg *Config) {
	if val := os.Getenv("PIPELINE_DLQ_ENABLED"); val != "" {
		cfg.Pipeline.DLQ.Enabled = getEnvBool("PIPELINE_DLQ_ENABLED")
	}
	if val := os.Getenv("PIPELINE_DLQ_TOPIC"); val != "" {
		cfg.Pipeline.DLQ.Topic = val
	}
}

// --- Circuit Breaker ---

func applyCircuitBreakerEnv(cfg *Config) {
	if val := os.Getenv("CIRCUIT_BREAKER_ENABLED"); val != "" {
		cfg.CircuitBreaker.Enabled = getEnvBool("CIRCUIT_BREAKER_ENABLED")
	}
	if val := getEnvFloat64("CIRCUIT_BREAKER_ERROR_THRESHOLD"); val > 0 {
		cfg.CircuitBreaker.ErrorThreshold = val
	}
	if val := getEnvInt("CIRCUIT_BREAKER_SUCCESS_THRESHOLD"); val > 0 {
		cfg.CircuitBreaker.SuccessThreshold = val
	}
	if val := getEnvDuration("CIRCUIT_BREAKER_TIMEOUT"); val != 0 {
		cfg.CircuitBreaker.Timeout = val
	}
	if val := getEnvInt("CIRCUIT_BREAKER_MAX_CONCURRENT"); val > 0 {
		cfg.CircuitBreaker.MaxConcurrentCalls = val
	}
	if val := getEnvInt("CIRCUIT_BREAKER_REQUEST_VOLUME"); val > 0 {
		cfg.CircuitBreaker.RequestVolumeThreshold = val
	}
}

// Helper functions

func getEnvInt(key string) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return -1
}

func getEnvInt64(key string) int64 {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
	}
	return 0
}

func getEnvFloat64(key string) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return 0
}

func getEnvBool(key string) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return false
}

func getEnvDuration(key string) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return 0
}

func getEnvStringSlice(key string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return nil
}

func getEnvIntSlice(key string) []int {
	if value := os.Getenv(key); value != "" {
		parts := strings.Split(value, ",")
		result := make([]int, 0, len(parts))
		for _, part := range parts {
			if intVal, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
				result = append(result, intVal)
			}
		}
		return result
	}
	return nil
}

func getEnvFloatSlice(key string) []float64 {
	if value := os.Getenv(key); value != "" {
		parts := strings.Split(value, ",")
		result := make([]float64, 0, len(parts))
		for _, part := range parts {
			if floatVal, err := strconv.ParseFloat(strings.TrimSpace(part), 64); err == nil {
				result = append(result, floatVal)
			}
		}
		return result
	}
	return nil
}
