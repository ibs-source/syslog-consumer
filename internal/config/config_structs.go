// Package config provides configuration loading and validation from environment variables and command line flags.
package config

import "time"

// Payload is the canonical alias for raw message body
type Payload = []byte

// Config holds the complete configuration
type Config struct {
	Redis    RedisConfig
	MQTT     MQTTConfig
	Pipeline PipelineConfig
}

// RedisConfig holds Redis stream consumer configuration
type RedisConfig struct {
	Address             string
	Stream              string
	Consumer            string
	BatchSize           int
	BlockTimeout        time.Duration
	ClaimIdle           time.Duration
	ConsumerIdleTimeout time.Duration
	CleanupInterval     time.Duration
	DialTimeout         time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	PingTimeout         time.Duration
}

// MQTTConfig holds MQTT client configuration
type MQTTConfig struct {
	Broker               string
	ClientID             string
	PublishTopic         string
	AckTopic             string
	QoS                  byte
	ConnectTimeout       time.Duration
	WriteTimeout         time.Duration
	PoolSize             int // Number of connections for high throughput
	MaxReconnectInterval time.Duration
	SubscribeTimeout     time.Duration
	DisconnectTimeout    uint // Milliseconds for graceful disconnect
	// TLS Configuration
	TLSEnabled      bool
	CACert          string
	ClientCert      string
	ClientKey       string
	InsecureSkip    bool
	UseCertCNPrefix bool // If true, prefix topics with cert CN for ACL constraints
}

// PipelineConfig holds pipeline orchestration settings
type PipelineConfig struct {
	BufferCapacity  int
	ShutdownTimeout time.Duration
	ErrorBackoff    time.Duration // Backoff on errors
	AckTimeout      time.Duration // Timeout for ACK operations
	PublishWorkers  int           // Number of concurrent publish workers
}
