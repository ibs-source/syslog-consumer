package config

import (
	"flag"
)

// Command line flags (have precedence over environment variables)
var (
	// Redis flags
	flagRedisAddress         = flag.String("redis-address", "", "Redis address")
	flagRedisStream          = flag.String("redis-stream", "", "Redis stream name (empty for multi-stream mode)")
	flagRedisConsumer        = flag.String("redis-consumer", "", "Redis consumer name")
	flagRedisBatchSize       = flag.Int("redis-batch-size", 0, "Redis batch size")
	flagRedisBlockTimeout    = flag.Duration("redis-block-timeout", 0, "Redis block timeout")
	flagRedisClaimIdle       = flag.Duration("redis-claim-idle", 0, "Redis claim idle time")
	flagRedisConsumerIdle    = flag.Duration("redis-consumer-idle-timeout", 0, "Redis consumer idle timeout")
	flagRedisCleanupInterval = flag.Duration("redis-cleanup-interval", 0, "Redis cleanup interval")
	flagRedisDialTimeout     = flag.Duration("redis-dial-timeout", 0, "Redis dial timeout")
	flagRedisReadTimeout     = flag.Duration("redis-read-timeout", 0, "Redis read timeout")
	flagRedisWriteTimeout    = flag.Duration("redis-write-timeout", 0, "Redis write timeout")
	flagRedisPingTimeout     = flag.Duration("redis-ping-timeout", 0, "Redis ping timeout")

	// MQTT flags
	flagMQTTBroker            = flag.String("mqtt-broker", "", "MQTT broker URL")
	flagMQTTClientID          = flag.String("mqtt-client-id", "", "MQTT client ID")
	flagMQTTPublishTopic      = flag.String("mqtt-publish-topic", "", "MQTT publish topic")
	flagMQTTAckTopic          = flag.String("mqtt-ack-topic", "", "MQTT ACK topic")
	flagMQTTQoS               = flag.Int("mqtt-qos", -1, "MQTT QoS (0, 1, or 2)")
	flagMQTTConnectTimeout    = flag.Duration("mqtt-connect-timeout", 0, "MQTT connect timeout")
	flagMQTTWriteTimeout      = flag.Duration("mqtt-write-timeout", 0, "MQTT write timeout")
	flagMQTTPoolSize          = flag.Int("mqtt-pool-size", 0, "MQTT connection pool size")
	flagMQTTMaxReconnect      = flag.Duration("mqtt-max-reconnect-interval", 0, "MQTT max reconnect interval")
	flagMQTTSubscribeTimeout  = flag.Duration("mqtt-subscribe-timeout", 0, "MQTT subscribe timeout")
	flagMQTTDisconnectTimeout = flag.Int("mqtt-disconnect-timeout", 0, "MQTT disconnect timeout (ms)")
	flagMQTTTLSEnabled        = flag.Bool("mqtt-tls-enabled", false, "Enable MQTT TLS")
	flagMQTTCACert            = flag.String("mqtt-ca-cert", "", "MQTT CA certificate path")
	flagMQTTClientCert        = flag.String("mqtt-client-cert", "", "MQTT client certificate path")
	flagMQTTClientKey         = flag.String("mqtt-client-key", "", "MQTT client key path")
	flagMQTTTLSInsecureSkip   = flag.Bool("mqtt-tls-insecure-skip", false, "Skip MQTT TLS verification")
	// Prefix topics with client cert CN (for ACL constraints)
	flagMQTTUseCertCNPrefix = flag.Bool("mqtt-use-cert-cn-prefix", false, "Prefix topics with client cert CN")

	// Pipeline flags
	flagPipelineBufferCapacity  = flag.Int("pipeline-buffer-capacity", 0, "Pipeline buffer capacity")
	flagPipelineShutdownTimeout = flag.Duration("pipeline-shutdown-timeout", 0, "Pipeline shutdown timeout")
	flagPipelineErrorBackoff    = flag.Duration("pipeline-error-backoff", 0, "Pipeline error backoff")
	flagPipelineAckTimeout      = flag.Duration("pipeline-ack-timeout", 0, "Pipeline ACK timeout")
	flagPipelinePublishWorkers  = flag.Int("pipeline-publish-workers", 0, "Number of concurrent publish workers")
)

// applyRedisFlags applies command line flags to Redis configuration
func applyRedisFlags(cfg *RedisConfig) {
	applyRedisFlagStrings(cfg)
	applyRedisFlagInts(cfg)
	applyRedisFlagTimeouts(cfg)
}

func applyRedisFlagStrings(cfg *RedisConfig) {
	if *flagRedisAddress != "" {
		cfg.Address = *flagRedisAddress
	}
	if *flagRedisStream != "" {
		cfg.Stream = *flagRedisStream
	}
	if *flagRedisConsumer != "" {
		cfg.Consumer = *flagRedisConsumer
	}
}

func applyRedisFlagInts(cfg *RedisConfig) {
	if *flagRedisBatchSize != 0 {
		cfg.BatchSize = *flagRedisBatchSize
	}
}

func applyRedisFlagTimeouts(cfg *RedisConfig) {
	if *flagRedisBlockTimeout != 0 {
		cfg.BlockTimeout = *flagRedisBlockTimeout
	}
	if *flagRedisClaimIdle != 0 {
		cfg.ClaimIdle = *flagRedisClaimIdle
	}
	if *flagRedisConsumerIdle != 0 {
		cfg.ConsumerIdleTimeout = *flagRedisConsumerIdle
	}
	if *flagRedisCleanupInterval != 0 {
		cfg.CleanupInterval = *flagRedisCleanupInterval
	}
	if *flagRedisDialTimeout != 0 {
		cfg.DialTimeout = *flagRedisDialTimeout
	}
	if *flagRedisReadTimeout != 0 {
		cfg.ReadTimeout = *flagRedisReadTimeout
	}
	if *flagRedisWriteTimeout != 0 {
		cfg.WriteTimeout = *flagRedisWriteTimeout
	}
	if *flagRedisPingTimeout != 0 {
		cfg.PingTimeout = *flagRedisPingTimeout
	}
}

// applyMQTTFlags applies command line flags to MQTT configuration
func applyMQTTFlags(cfg *MQTTConfig) {
	applyMQTTFlagStrings(cfg)
	applyMQTTFlagInts(cfg)
	applyMQTTFlagTimeouts(cfg)
	applyMQTTFlagTLS(cfg)
	applyMQTTFlagBools(cfg)
}

func applyMQTTFlagStrings(cfg *MQTTConfig) {
	if *flagMQTTBroker != "" {
		cfg.Broker = *flagMQTTBroker
	}
	if *flagMQTTClientID != "" {
		cfg.ClientID = *flagMQTTClientID
	}
	if *flagMQTTPublishTopic != "" {
		cfg.PublishTopic = *flagMQTTPublishTopic
	}
	if *flagMQTTAckTopic != "" {
		cfg.AckTopic = *flagMQTTAckTopic
	}
}

func applyMQTTFlagInts(cfg *MQTTConfig) {
	if *flagMQTTQoS != -1 && *flagMQTTQoS >= 0 && *flagMQTTQoS <= 2 {
		cfg.QoS = byte(*flagMQTTQoS) // #nosec G115 - validated range 0-2
	}
	if *flagMQTTPoolSize != 0 {
		cfg.PoolSize = *flagMQTTPoolSize
	}
	if *flagMQTTDisconnectTimeout != 0 {
		cfg.DisconnectTimeout = uint(*flagMQTTDisconnectTimeout) // #nosec G115 - config values are non-negative
	}
}

func applyMQTTFlagTimeouts(cfg *MQTTConfig) {
	if *flagMQTTConnectTimeout != 0 {
		cfg.ConnectTimeout = *flagMQTTConnectTimeout
	}
	if *flagMQTTWriteTimeout != 0 {
		cfg.WriteTimeout = *flagMQTTWriteTimeout
	}
	if *flagMQTTMaxReconnect != 0 {
		cfg.MaxReconnectInterval = *flagMQTTMaxReconnect
	}
	if *flagMQTTSubscribeTimeout != 0 {
		cfg.SubscribeTimeout = *flagMQTTSubscribeTimeout
	}
}

func applyMQTTFlagTLS(cfg *MQTTConfig) {
	if *flagMQTTCACert != "" {
		cfg.CACert = *flagMQTTCACert
	}
	if *flagMQTTClientCert != "" {
		cfg.ClientCert = *flagMQTTClientCert
	}
	if *flagMQTTClientKey != "" {
		cfg.ClientKey = *flagMQTTClientKey
	}
}

func applyMQTTFlagBools(cfg *MQTTConfig) {
	// Handle bool flags - check if explicitly set
	if isFlagSet("mqtt-tls-enabled") {
		cfg.TLSEnabled = *flagMQTTTLSEnabled
	}
	if isFlagSet("mqtt-tls-insecure-skip") {
		cfg.InsecureSkip = *flagMQTTTLSInsecureSkip
	}
	if isFlagSet("mqtt-use-cert-cn-prefix") {
		cfg.UseCertCNPrefix = *flagMQTTUseCertCNPrefix
	}
}

// applyPipelineFlags applies command line flags to Pipeline configuration
func applyPipelineFlags(cfg *PipelineConfig) {
	if *flagPipelineBufferCapacity != 0 {
		cfg.BufferCapacity = *flagPipelineBufferCapacity
	}
	if *flagPipelineShutdownTimeout != 0 {
		cfg.ShutdownTimeout = *flagPipelineShutdownTimeout
	}
	if *flagPipelineErrorBackoff != 0 {
		cfg.ErrorBackoff = *flagPipelineErrorBackoff
	}
	if *flagPipelineAckTimeout != 0 {
		cfg.AckTimeout = *flagPipelineAckTimeout
	}
	if *flagPipelinePublishWorkers != 0 {
		cfg.PublishWorkers = *flagPipelinePublishWorkers
	}
}

// isFlagSet checks if a flag was explicitly set on the command line
func isFlagSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
