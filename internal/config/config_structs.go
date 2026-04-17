// Package config provides configuration loading and validation from environment variables and command line flags.
package config

import "time"

// Config holds the complete configuration
type Config struct {
	Log      LogConfig
	MQTT     MQTTConfig
	Pipeline PipelineConfig
	Redis    RedisConfig
	Compress CompressConfig
}

// CompressConfig holds compression/decompression tuning parameters.
type CompressConfig struct {
	FreelistSize       int // channel buffer capacity for decoder reuse
	MaxDecompressBytes int // hard cap for a single decompressed payload
	WarmupCount        int // decoders pre-created at init to avoid cold-start latency
}

// LogConfig holds application logging configuration.
type LogConfig struct {
	Level string
}

// RedisConfig holds Redis stream consumer configuration
type RedisConfig struct {
	Address             string
	Stream              string
	Consumer            string
	GroupName           string
	BatchSize           int
	DiscoveryScanCount  int
	BlockTimeout        time.Duration
	ClaimIdle           time.Duration
	ConsumerIdleTimeout time.Duration
	CleanupInterval     time.Duration
	DialTimeout         time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	PingTimeout         time.Duration
	PoolSize            int
	MinIdleConns        int
}

// MQTTConfig holds MQTT client configuration
type MQTTConfig struct {
	Broker               string
	ClientID             string
	PublishTopic         string
	AckTopic             string
	CACert               string
	ClientCert           string
	ClientKey            string
	ConnectTimeout       time.Duration
	WriteTimeout         time.Duration
	MaxReconnectInterval time.Duration
	SubscribeTimeout     time.Duration
	DisconnectTimeout    time.Duration
	KeepAlive            time.Duration // Interval between PINGREQ packets
	PingTimeout          time.Duration // Max wait for PINGRESP before reconnect
	ConnectRetryDelay    time.Duration // Pause between connection retry attempts
	PoolSize             int           // Number of connections in the pool
	MessageChannelDepth  uint          // Internal paho outgoing message queue depth
	MaxResumePubInFlight int           // Unacknowledged publishes resumed after reconnect
	QoS                  byte
	TLSEnabled           bool
	InsecureSkip         bool
	UseCertCNPrefix      bool // If true, prefix topics with cert CN for ACL constraints
}

// PipelineConfig holds pipeline orchestration settings
type PipelineConfig struct {
	HealthAddr              string        // Address for health/metrics HTTP server (e.g. ":9980")
	HealthPingTimeout       time.Duration // Timeout for Redis ping in health check
	HealthReadHeaderTimeout time.Duration // HTTP ReadHeaderTimeout for the health server
	ShutdownTimeout         time.Duration
	ErrorBackoff            time.Duration // Backoff on errors
	AckTimeout              time.Duration // Timeout for ACK operations
	RefreshInterval         time.Duration // Interval for multi-stream discovery refresh
	AckFlushInterval        time.Duration // Timer interval for flushing batched ACKs
	BufferCapacity          int           // ACK queue depth
	MessageQueueCapacity    int           // Redis batch queue depth between fetch and publish workers
	PublishWorkers          int           // Number of concurrent publish workers
	AckWorkers              int           // Number of concurrent ACK workers
	AckBatchSize            int           // Threshold for immediate flush of accumulated ACKs
}
