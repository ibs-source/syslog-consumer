// Package config provides configuration loading and validation from
// environment variables and command line flags.
package config

import "time"

// Config aggregates every subsystem's configuration.
type Config struct {
	Log      LogConfig
	MQTT     MQTTConfig
	Pipeline PipelineConfig
	Redis    RedisConfig
	Compress CompressConfig
}

// CompressConfig tunes the zstd encoder/decoder freelists.
type CompressConfig struct {
	FreelistSize       int
	MaxDecompressBytes int
	WarmupCount        int
}

// LogConfig is a placeholder for future logging knobs; currently only Level.
type LogConfig struct {
	Level string
}

// RedisConfig drives the Redis stream consumer and its connection pool.
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
	// ConnMaxIdleTime recycles pooled connections that have been idle longer
	// than this. Protects against silently-dead TCP connections (NAT/conntrack
	// eviction) the client would otherwise reuse and fail on. Zero disables.
	ConnMaxIdleTime time.Duration
	// ConnMaxLifetime rotates every connection past this age regardless of
	// activity. Zero disables.
	ConnMaxLifetime time.Duration
	PoolSize        int
	MinIdleConns    int
}

// MQTTConfig captures broker connection, TLS, and pool settings.
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
	KeepAlive            time.Duration
	PingTimeout          time.Duration
	ConnectRetryDelay    time.Duration
	PoolSize             int
	MessageChannelDepth  uint
	MaxResumePubInFlight int
	QoS                  byte
	TLSEnabled           bool
	InsecureSkip         bool
	// UseCertCNPrefix prepends the client cert CN to publish and ACK topics
	// to satisfy broker ACL constraints.
	UseCertCNPrefix bool
}

// PipelineConfig sizes the worker pools, queues, and timeouts that govern
// the fetch → publish → ACK flow and the health endpoint.
type PipelineConfig struct {
	HealthAddr              string
	HealthPingTimeout       time.Duration
	HealthReadHeaderTimeout time.Duration
	ShutdownTimeout         time.Duration
	ErrorBackoff            time.Duration
	AckTimeout              time.Duration
	RefreshInterval         time.Duration
	AckFlushInterval        time.Duration
	BufferCapacity          int
	MessageQueueCapacity    int
	PublishWorkers          int
	AckWorkers              int
	AckBatchSize            int
}
