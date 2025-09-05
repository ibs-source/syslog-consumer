// Package config loads, merges, and validates application configuration from defaults, environment, and flags.
package config

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration
type Config struct {
	App            AppConfig
	Redis          RedisConfig
	MQTT           MQTTConfig
	Resource       ResourceConfig
	Metrics        MetricsConfig
	Health         HealthConfig
	Pipeline       PipelineConfig
	CircuitBreaker CircuitBreakerConfig
}

// AppConfig holds application-level configuration
type AppConfig struct {
	Name            string
	Environment     string
	LogLevel        string
	LogFormat       string
	ShutdownTimeout time.Duration
	PendingOpsGrace time.Duration
}

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Addresses            []string
	Username             string
	Password             string
	DB                   int
	MasterName           string
	StreamName           string
	ConsumerGroup        string
	MaxRetries           int
	RetryInterval        time.Duration
	ConnectTimeout       time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	PoolSize             int
	MinIdleConns         int
	ConnMaxLifetime      time.Duration
	PoolTimeout          time.Duration
	ConnMaxIdleTime      time.Duration
	ClaimMinIdleTime     time.Duration
	ClaimBatchSize       int
	PendingCheckInterval time.Duration
	ClaimInterval        time.Duration
	BatchSize            int64
	BlockTime            time.Duration

	// Advanced claim and drain settings
	AggressiveClaim         bool          // Enable aggressive claim until exhaustion
	ClaimCycleDelay         time.Duration // Delay between aggressive claim cycles
	DrainEnabled            bool          // Enable draining unassigned messages
	DrainInterval           time.Duration // Interval for drain operations
	DrainBatchSize          int64         // Batch size for drain operations
	ConsumerCleanupEnabled  bool          // Enable automatic consumer cleanup
	ConsumerIdleTimeout     time.Duration // Timeout to consider a consumer idle
	ConsumerCleanupInterval time.Duration // Interval for consumer cleanup checks
}

// MQTTConfig holds MQTT configuration
type MQTTConfig struct {
	// Connection settings
	Brokers           []string      // List of MQTT broker addresses
	ClientID          string        // MQTT client identifier
	QoS               byte          // Quality of Service level (0, 1, 2)
	KeepAlive         time.Duration // Keep alive interval for MQTT connection
	ConnectTimeout    time.Duration // Connection timeout
	MaxReconnectDelay time.Duration // Maximum delay between reconnection attempts
	CleanSession      bool          // Whether to start a clean session
	OrderMatters      bool          // Whether message order must be preserved

	// Security configuration
	TLS TLSConfig // TLS/SSL configuration

	// Topic configuration
	Topics TopicConfig // Topic settings for publish/subscribe

	// Performance settings
	PublisherPoolSize   int           // Number of concurrent publishers
	MaxInflight         int           // Maximum number of inflight messages
	MessageChannelDepth int           // Depth of internal message channel in client
	WriteTimeout        time.Duration // Timeout for write operations
}

// TLSConfig holds TLS/SSL configuration for MQTT
type TLSConfig struct {
	Enabled            bool   // Enable TLS/SSL connection
	CACertFile         string // Path to CA certificate file
	ClientCertFile     string // Path to client certificate file
	ClientKeyFile      string // Path to client private key file
	InsecureSkipVerify bool   // Skip certificate verification (DANGEROUS - only for testing)

	// Advanced TLS settings
	ServerName          string   // Expected server name for verification
	MinVersion          string   // Minimum TLS version (e.g., "TLS1.2", "TLS1.3")
	CipherSuites        []string // Allowed cipher suites (empty = use defaults)
	PreferServerCiphers bool     // Whether to prefer server cipher suites
}

// TopicConfig holds MQTT topic configuration
type TopicConfig struct {
	// Base topics (without user prefix)
	PublishTopic   string // Topic for publishing syslog messages
	SubscribeTopic string // Topic for receiving acknowledgments

	// Topic prefix settings
	UseUserPrefix bool   // Whether to prepend user prefix from certificate
	CustomPrefix  string // Custom prefix to use if UseUserPrefix is false

	// Advanced topic settings
	QoSOverride    map[string]byte // Override QoS for specific topics
	RetainMessages bool            // Whether to retain published messages
}

// ResourceConfig holds resource management configuration
type ResourceConfig struct {
	CPUThresholdHigh    float64
	CPUThresholdLow     float64
	MemoryThresholdHigh float64
	MemoryThresholdLow  float64
	CheckInterval       time.Duration
	ScaleUpCooldown     time.Duration
	ScaleDownCooldown   time.Duration
	MinWorkers          int
	MaxWorkers          int
	WorkerStepSize      int

	// Advanced resource management
	EnablePredictiveScaling bool
	HistoryWindowSize       int
	PredictionHorizon       time.Duration
}

// MetricsConfig holds metrics configuration
type MetricsConfig struct {
	Enabled          bool
	PrometheusPort   int
	CollectInterval  time.Duration
	Namespace        string
	Subsystem        string
	HistogramBuckets []float64
}

// HealthConfig holds health check configuration
type HealthConfig struct {
	Enabled       bool
	Port          int
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	CheckInterval time.Duration

	// Component-specific timeouts
	RedisTimeout time.Duration
	MQTTTimeout  time.Duration
}

// PipelineConfig holds pipeline configuration
type PipelineConfig struct {
	// Ring buffer configuration
	BufferSize        int
	BatchSize         int
	BatchTimeout      time.Duration
	ProcessingTimeout time.Duration

	// Performance tuning
	NumaAware   bool
	CPUAffinity []int

	// Backpressure
	BackpressureThreshold    float64
	DropPolicy               string // "oldest", "newest", "none"
	FlushInterval            time.Duration
	BackpressurePollInterval time.Duration

	// Scheduler tuning
	IdlePollSleep time.Duration

	// Retry and DLQ
	Retry RetryConfig
	DLQ   DLQConfig
}

// RetryConfig holds retry policy configuration
type RetryConfig struct {
	Enabled        bool
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// DLQConfig holds Dead Letter Queue configuration
type DLQConfig struct {
	Enabled bool
	Topic   string
}

// CircuitBreakerConfig holds circuit breaker configuration
type CircuitBreakerConfig struct {
	Enabled                bool
	ErrorThreshold         float64
	SuccessThreshold       int
	Timeout                time.Duration
	MaxConcurrentCalls     int
	RequestVolumeThreshold int
}

// Load loads configuration from environment variables and defaults
func Load() (*Config, error) {
	// First, register command-line flags
	RegisterFlags()

	// Start with default configuration
	cfg := GetDefaults()

	// Apply environment variables (they override defaults)
	LoadFromEnvironment(cfg)

	// Apply command-line flags (they override environment variables)
	ApplyFlags(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

func loadAppConfig() AppConfig {
	return AppConfig{
		Name:            getEnv("APP_NAME", "syslog-consumer"),
		Environment:     getEnv("APP_ENV", "production"),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		LogFormat:       getEnv("LOG_FORMAT", "json"),
		ShutdownTimeout: getDurationEnv("APP_SHUTDOWN_TIMEOUT", 30*time.Second),
		PendingOpsGrace: getDurationEnv("APP_PENDING_OPS_GRACE", 500*time.Millisecond),
	}
}

func loadRedisConfig() RedisConfig {
	addresses := getEnvSlice("REDIS_ADDRESSES", []string{"localhost:6379"})

	return RedisConfig{
		Addresses:            addresses,
		Password:             getEnv("REDIS_PASSWORD", ""),
		DB:                   getIntEnv("REDIS_DB", 0),
		StreamName:           getEnv("REDIS_STREAM", "syslog-stream"),
		ConsumerGroup:        getEnv("REDIS_CONSUMER_GROUP", "syslog-group"),
		MaxRetries:           getIntEnv("REDIS_MAX_RETRIES", 5),
		RetryInterval:        getDurationEnv("REDIS_RETRY_INTERVAL", 1*time.Second),
		ConnectTimeout:       getDurationEnv("REDIS_CONNECT_TIMEOUT", 5*time.Second),
		ReadTimeout:          getDurationEnv("REDIS_READ_TIMEOUT", 3*time.Second),
		WriteTimeout:         getDurationEnv("REDIS_WRITE_TIMEOUT", 3*time.Second),
		PoolSize:             getIntEnv("REDIS_POOL_SIZE", runtime.NumCPU()*10),
		MinIdleConns:         getIntEnv("REDIS_MIN_IDLE_CONNS", runtime.NumCPU()),
		ConnMaxLifetime:      getDurationEnv("REDIS_MAX_CONN_AGE", 30*time.Minute),
		ConnMaxIdleTime:      getDurationEnv("REDIS_IDLE_TIMEOUT", 5*time.Minute),
		ClaimMinIdleTime:     getDurationEnv("REDIS_CLAIM_MIN_IDLE", 1*time.Minute),
		ClaimBatchSize:       getIntEnv("REDIS_CLAIM_BATCH_SIZE", 1000),
		PendingCheckInterval: getDurationEnv("REDIS_PENDING_CHECK_INTERVAL", 30*time.Second),
		ClaimInterval:        getDurationEnv("REDIS_CLAIM_INTERVAL", 30*time.Second),
		BatchSize:            int64(getIntEnv("REDIS_BATCH_SIZE", 100)),
		BlockTime:            getDurationEnv("REDIS_BLOCK_TIME", 5*time.Second),

		// Advanced claim and drain settings
		AggressiveClaim:         getBoolEnv("REDIS_AGGRESSIVE_CLAIM", true),
		ClaimCycleDelay:         getDurationEnv("REDIS_CLAIM_CYCLE_DELAY", 1*time.Second),
		DrainEnabled:            getBoolEnv("REDIS_DRAIN_ENABLED", true),
		DrainInterval:           getDurationEnv("REDIS_DRAIN_INTERVAL", 1*time.Minute),
		DrainBatchSize:          int64(getIntEnv("REDIS_DRAIN_BATCH_SIZE", 1000)),
		ConsumerCleanupEnabled:  getBoolEnv("REDIS_CONSUMER_CLEANUP_ENABLED", true),
		ConsumerIdleTimeout:     getDurationEnv("REDIS_CONSUMER_IDLE_TIMEOUT", 5*time.Minute),
		ConsumerCleanupInterval: getDurationEnv("REDIS_CONSUMER_CLEANUP_INTERVAL", 1*time.Minute),
	}
}

func loadMQTTConfig() MQTTConfig {
	brokers := getEnvSlice("MQTT_BROKERS", []string{"tcp://localhost:1883"})

	return MQTTConfig{
		// Connection settings
		Brokers:           brokers,
		ClientID:          getEnv("MQTT_CLIENT_ID", generateClientID()),
		QoS:               byte(getIntEnv("MQTT_QOS", 2)),
		KeepAlive:         getDurationEnv("MQTT_KEEP_ALIVE", 30*time.Second),
		ConnectTimeout:    getDurationEnv("MQTT_CONNECT_TIMEOUT", 10*time.Second),
		MaxReconnectDelay: getDurationEnv("MQTT_MAX_RECONNECT_DELAY", 2*time.Minute),
		CleanSession:      getBoolEnv("MQTT_CLEAN_SESSION", true),
		OrderMatters:      getBoolEnv("MQTT_ORDER_MATTERS", true),

		// TLS Configuration
		TLS: TLSConfig{
			Enabled:             getBoolEnv("MQTT_TLS_ENABLED", true),
			CACertFile:          getEnv("MQTT_CA_CERT", ""),
			ClientCertFile:      getEnv("MQTT_CLIENT_CERT", ""),
			ClientKeyFile:       getEnv("MQTT_CLIENT_KEY", ""),
			InsecureSkipVerify:  getBoolEnv("MQTT_TLS_INSECURE", false),
			ServerName:          getEnv("MQTT_TLS_SERVER_NAME", ""),
			MinVersion:          getEnv("MQTT_TLS_MIN_VERSION", "TLS1.2"),
			CipherSuites:        getEnvSlice("MQTT_TLS_CIPHER_SUITES", []string{}),
			PreferServerCiphers: getBoolEnv("MQTT_TLS_PREFER_SERVER_CIPHERS", false),
		},

		// Topic Configuration
		Topics: TopicConfig{
			PublishTopic:   getEnv("MQTT_PUBLISH_TOPIC", "syslog"),
			SubscribeTopic: getEnv("MQTT_SUBSCRIBE_TOPIC", "syslog/acknowledgement"),
			UseUserPrefix:  getBoolEnv("MQTT_USE_USER_PREFIX", true),
			CustomPrefix:   getEnv("MQTT_CUSTOM_PREFIX", ""),
		},

		// Performance settings
		PublisherPoolSize:   getIntEnv("MQTT_PUBLISHER_POOL_SIZE", 1), // Default to 1 like Node-RED
		MaxInflight:         getIntEnv("MQTT_MAX_INFLIGHT", 1000),
		MessageChannelDepth: getIntEnv("MQTT_MESSAGE_CHANNEL_DEPTH", 0),
		WriteTimeout:        getDurationEnv("MQTT_WRITE_TIMEOUT", 5*time.Second),
	}
}

func loadResourceConfig() ResourceConfig {
	return ResourceConfig{
		CPUThresholdHigh:        getFloatEnv("RESOURCE_CPU_HIGH", 80.0),
		CPUThresholdLow:         getFloatEnv("RESOURCE_CPU_LOW", 30.0),
		MemoryThresholdHigh:     getFloatEnv("RESOURCE_MEM_HIGH", 80.0),
		MemoryThresholdLow:      getFloatEnv("RESOURCE_MEM_LOW", 30.0),
		CheckInterval:           getDurationEnv("RESOURCE_CHECK_INTERVAL", 5*time.Second),
		ScaleUpCooldown:         getDurationEnv("RESOURCE_SCALE_UP_COOLDOWN", 30*time.Second),
		ScaleDownCooldown:       getDurationEnv("RESOURCE_SCALE_DOWN_COOLDOWN", 2*time.Minute),
		MinWorkers:              getIntEnv("RESOURCE_MIN_WORKERS", 1),
		MaxWorkers:              getIntEnv("RESOURCE_MAX_WORKERS", runtime.NumCPU()*4),
		WorkerStepSize:          getIntEnv("RESOURCE_WORKER_STEP", 2),
		EnablePredictiveScaling: getBoolEnv("RESOURCE_PREDICTIVE_SCALING", true),
		HistoryWindowSize:       getIntEnv("RESOURCE_HISTORY_WINDOW", 100),
		PredictionHorizon:       getDurationEnv("RESOURCE_PREDICTION_HORIZON", 30*time.Second),
	}
}

func loadMetricsConfig() MetricsConfig {
	return MetricsConfig{
		Enabled:         getBoolEnv("METRICS_ENABLED", true),
		PrometheusPort:  getIntEnv("METRICS_PORT", 9090),
		CollectInterval: getDurationEnv("METRICS_COLLECT_INTERVAL", 10*time.Second),
		Namespace:       getEnv("METRICS_NAMESPACE", "syslog_consumer"),
		Subsystem:       getEnv("METRICS_SUBSYSTEM", ""),
		HistogramBuckets: getFloatSliceEnv("METRICS_HISTOGRAM_BUCKETS",
			[]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}),
	}
}

func loadHealthConfig() HealthConfig {
	return HealthConfig{
		Enabled:       getBoolEnv("HEALTH_ENABLED", true),
		Port:          getIntEnv("HEALTH_PORT", 8080),
		ReadTimeout:   getDurationEnv("HEALTH_READ_TIMEOUT", 5*time.Second),
		WriteTimeout:  getDurationEnv("HEALTH_WRITE_TIMEOUT", 5*time.Second),
		CheckInterval: getDurationEnv("HEALTH_CHECK_INTERVAL", 10*time.Second),
		RedisTimeout:  getDurationEnv("HEALTH_REDIS_TIMEOUT", 2*time.Second),
		MQTTTimeout:   getDurationEnv("HEALTH_MQTT_TIMEOUT", 2*time.Second),
	}
}

func loadPipelineConfig() PipelineConfig {
	bufferSize := getIntEnv("PIPELINE_BUFFER_SIZE", 1048576) // 1M messages
	// Ensure buffer size is power of 2
	bufferSize = nextPowerOf2(bufferSize)

	return PipelineConfig{
		BufferSize:               bufferSize,
		BatchSize:                getIntEnv("PIPELINE_BATCH_SIZE", 1000),
		BatchTimeout:             getDurationEnv("PIPELINE_BATCH_TIMEOUT", 100*time.Millisecond),
		ProcessingTimeout:        getDurationEnv("PIPELINE_PROCESSING_TIMEOUT", 5*time.Second),
		NumaAware:                getBoolEnv("PIPELINE_NUMA_AWARE", false),
		CPUAffinity:              getIntSliceEnv("PIPELINE_CPU_AFFINITY", []int{}),
		BackpressureThreshold:    getFloatEnv("PIPELINE_BACKPRESSURE_THRESHOLD", 0.8),
		DropPolicy:               getEnv("PIPELINE_DROP_POLICY", "oldest"),
		FlushInterval:            getDurationEnv("PIPELINE_FLUSH_INTERVAL", 100*time.Millisecond),
		BackpressurePollInterval: getDurationEnv("PIPELINE_BACKPRESSURE_POLL_INTERVAL", 1*time.Second),
		IdlePollSleep:            getDurationEnv("PIPELINE_IDLE_POLL_SLEEP", 1*time.Millisecond),
	}
}

func loadCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		Enabled:                getBoolEnv("CIRCUIT_BREAKER_ENABLED", true),
		ErrorThreshold:         getFloatEnv("CIRCUIT_BREAKER_ERROR_THRESHOLD", 50.0),
		SuccessThreshold:       getIntEnv("CIRCUIT_BREAKER_SUCCESS_THRESHOLD", 5),
		Timeout:                getDurationEnv("CIRCUIT_BREAKER_TIMEOUT", 30*time.Second),
		MaxConcurrentCalls:     getIntEnv("CIRCUIT_BREAKER_MAX_CONCURRENT", 100),
		RequestVolumeThreshold: getIntEnv("CIRCUIT_BREAKER_REQUEST_VOLUME", 20),
	}
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getFloatEnv(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return defaultValue
}

func getBoolEnv(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnvSlice(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return defaultValue
}

func getIntSliceEnv(key string, defaultValue []int) []int {
	if value := os.Getenv(key); value != "" {
		parts := strings.Split(value, ",")
		result := make([]int, 0, len(parts))
		for _, part := range parts {
			if intVal, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
				result = append(result, intVal)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultValue
}

func getFloatSliceEnv(key string, defaultValue []float64) []float64 {
	if value := os.Getenv(key); value != "" {
		parts := strings.Split(value, ",")
		result := make([]float64, 0, len(parts))
		for _, part := range parts {
			if floatVal, err := strconv.ParseFloat(strings.TrimSpace(part), 64); err == nil {
				result = append(result, floatVal)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultValue
}

func generateClientID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("syslog-consumer-%s-%d", hostname, os.Getpid())
}

func nextPowerOf2(n int) int {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}
