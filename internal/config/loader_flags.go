package config

import (
	"flag"
)

// Command line flags (have precedence over environment variables)
var (
	// Log flags
	flagLogLevel = flag.String("log-level", "", "Log level (trace, debug, info, warn, error, fatal, panic)")

	// Redis flags
	flagRedisAddress         = flag.String("redis-address", "", "Redis address")
	flagRedisStream          = flag.String("redis-stream", "", "Redis stream name (empty for multi-stream mode)")
	flagRedisConsumer        = flag.String("redis-consumer", "", "Redis consumer name")
	flagRedisGroupName       = flag.String("redis-group-name", "", "Redis consumer group name")
	flagRedisBatchSize       = flag.Int("redis-batch-size", 0, "Redis batch size")
	flagRedisBlockTimeout    = flag.Duration("redis-block-timeout", 0, "Redis block timeout")
	flagRedisClaimIdle       = flag.Duration("redis-claim-idle", 0, "Redis claim idle time")
	flagRedisConsumerIdle    = flag.Duration("redis-consumer-idle-timeout", 0, "Redis consumer idle timeout")
	flagRedisCleanupInterval = flag.Duration("redis-cleanup-interval", 0, "Redis cleanup interval")
	flagRedisDialTimeout     = flag.Duration("redis-dial-timeout", 0, "Redis dial timeout")
	flagRedisReadTimeout     = flag.Duration("redis-read-timeout", 0, "Redis read timeout")
	flagRedisWriteTimeout    = flag.Duration("redis-write-timeout", 0, "Redis write timeout")
	flagRedisPingTimeout     = flag.Duration("redis-ping-timeout", 0, "Redis ping timeout")
	flagRedisConnMaxIdleTime = flag.Duration(
		"redis-conn-max-idle-time", -1,
		"Max idle time before a pooled connection is recycled (0 disables)",
	)
	flagRedisConnMaxLifetime = flag.Duration(
		"redis-conn-max-lifetime", -1,
		"Max lifetime of a pooled connection (0 disables)",
	)
	flagRedisPoolSize           = flag.Int("redis-pool-size", 0, "Redis connection pool size")
	flagRedisMinIdleConns       = flag.Int("redis-min-idle-conns", 0, "Redis minimum idle connections")
	flagRedisDiscoveryScanCount = flag.Int("redis-discovery-scan-count", 0, "Redis SCAN count hint for stream discovery")

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
	flagMQTTDisconnectTimeout = flag.Duration("mqtt-disconnect-timeout", 0, "MQTT disconnect timeout")
	flagMQTTTLSEnabled        = flag.Bool("mqtt-tls-enabled", false, "Enable MQTT TLS")
	flagMQTTCACert            = flag.String("mqtt-ca-cert", "", "MQTT CA certificate path")
	flagMQTTClientCert        = flag.String("mqtt-client-cert", "", "MQTT client certificate path")
	flagMQTTClientKey         = flag.String("mqtt-client-key", "", "MQTT client key path")
	flagMQTTTLSInsecureSkip   = flag.Bool("mqtt-tls-insecure-skip", false, "Skip MQTT TLS verification")
	// Prefix topics with client cert CN (for ACL constraints)
	flagMQTTUseCertCNPrefix      = flag.Bool("mqtt-use-cert-cn-prefix", false, "Prefix topics with client cert CN")
	flagMQTTKeepAlive            = flag.Duration("mqtt-keep-alive", 0, "MQTT keep-alive interval")
	flagMQTTPingTimeout          = flag.Duration("mqtt-ping-timeout", 0, "MQTT ping response timeout")
	flagMQTTConnectRetryDelay    = flag.Duration("mqtt-connect-retry-delay", 0, "MQTT connect retry delay")
	flagMQTTMessageChannelDepth  = flag.Int("mqtt-message-channel-depth", 0, "MQTT internal message queue depth")
	flagMQTTMaxResumePubInFlight = flag.Int("mqtt-max-resume-pub-in-flight", 0, "MQTT max resumed unacked publishes")

	// Compress flags
	flagCompressFreelistSize       = flag.Int("compress-freelist-size", 0, "Decoder freelist channel capacity")
	flagCompressMaxDecompressBytes = flag.Int("max-decompress-bytes", 0, "Max decompressed payload size in bytes")
	flagCompressWarmupCount        = flag.Int("compress-warmup-count", 0, "Decoders pre-created at init")

	// Pipeline flags
	flagPipelineBufferCapacity  = flag.Int("pipeline-buffer-capacity", 0, "Pipeline buffer capacity")
	flagPipelineShutdownTimeout = flag.Duration("pipeline-shutdown-timeout", 0, "Pipeline shutdown timeout")
	flagPipelineErrorBackoff    = flag.Duration("pipeline-error-backoff", 0, "Pipeline error backoff")
	flagPipelineAckTimeout      = flag.Duration("pipeline-ack-timeout", 0, "Pipeline ACK timeout")
	flagPipelinePublishWorkers  = flag.Int(
		"pipeline-publish-workers", 0, "Number of concurrent publish workers",
	)
	flagPipelineRefreshInterval = flag.Duration(
		"pipeline-refresh-interval", 0, "Multi-stream discovery refresh interval",
	)
	flagPipelineHealthAddr = flag.String(
		"pipeline-health-addr", "", "Health/metrics HTTP address (e.g. :9980)",
	)
	flagPipelineAckFlushInterval = flag.Duration(
		"pipeline-ack-flush-interval", 0, "ACK batch flush interval",
	)
	flagPipelineAckBatchSize = flag.Int(
		"pipeline-ack-batch-size", 0, "ACK batch flush threshold",
	)
	flagPipelineAckWorkers = flag.Int(
		"pipeline-ack-workers", 0, "Number of concurrent ACK workers",
	)
	flagPipelineMessageQueueCapacity = flag.Int(
		"pipeline-message-queue-capacity", 0, "Fetch→publish queue capacity",
	)
	flagPipelineHealthPingTimeout = flag.Duration(
		"pipeline-health-ping-timeout", 0, "Health check Redis ping timeout",
	)
	flagPipelineHealthReadHeaderTimeout = flag.Duration(
		"pipeline-health-read-header-timeout", 0, "Health server ReadHeaderTimeout",
	)
)

// applyLogFlags applies command line flags to logging configuration.
func applyLogFlags(cfg *LogConfig) {
	if *flagLogLevel != "" {
		cfg.Level = *flagLogLevel
	}
}

// applyRedisFlags applies command line flags to Redis configuration
func applyRedisFlags(cfg *RedisConfig) {
	applyRedisFlagStrings(cfg)
	applyRedisFlagInts(cfg)
	applyRedisFlagTimeouts(cfg)
	applyRedisFlagPoolLifecycle(cfg)
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
	if *flagRedisGroupName != "" {
		cfg.GroupName = *flagRedisGroupName
	}
}

func applyRedisFlagInts(cfg *RedisConfig) {
	if *flagRedisBatchSize != 0 {
		cfg.BatchSize = *flagRedisBatchSize
	}
	if *flagRedisPoolSize != 0 {
		cfg.PoolSize = *flagRedisPoolSize
	}
	if *flagRedisMinIdleConns != 0 {
		cfg.MinIdleConns = *flagRedisMinIdleConns
	}
	if *flagRedisDiscoveryScanCount != 0 {
		cfg.DiscoveryScanCount = *flagRedisDiscoveryScanCount
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

// applyRedisFlagPoolLifecycle applies connection-pool recycling flags.
// The -1 sentinel distinguishes "not set" from the valid value 0, which the
// user may pass to disable proactive recycling.
func applyRedisFlagPoolLifecycle(cfg *RedisConfig) {
	if *flagRedisConnMaxIdleTime >= 0 {
		cfg.ConnMaxIdleTime = *flagRedisConnMaxIdleTime
	}
	if *flagRedisConnMaxLifetime >= 0 {
		cfg.ConnMaxLifetime = *flagRedisConnMaxLifetime
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
		cfg.QoS = byte(min(max(*flagMQTTQoS, 0), 2))
	}
	if *flagMQTTPoolSize != 0 {
		cfg.PoolSize = *flagMQTTPoolSize
	}
	if *flagMQTTDisconnectTimeout != 0 {
		cfg.DisconnectTimeout = *flagMQTTDisconnectTimeout
	}
	if *flagMQTTMessageChannelDepth != 0 {
		cfg.MessageChannelDepth = uint(max(*flagMQTTMessageChannelDepth, 0))
	}
	if *flagMQTTMaxResumePubInFlight != 0 {
		cfg.MaxResumePubInFlight = *flagMQTTMaxResumePubInFlight
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
	if *flagMQTTKeepAlive != 0 {
		cfg.KeepAlive = *flagMQTTKeepAlive
	}
	if *flagMQTTPingTimeout != 0 {
		cfg.PingTimeout = *flagMQTTPingTimeout
	}
	if *flagMQTTConnectRetryDelay != 0 {
		cfg.ConnectRetryDelay = *flagMQTTConnectRetryDelay
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

// applyCompressFlags applies command line flags to compression configuration.
func applyCompressFlags(cfg *CompressConfig) {
	if *flagCompressFreelistSize != 0 {
		cfg.FreelistSize = *flagCompressFreelistSize
	}
	if *flagCompressMaxDecompressBytes != 0 {
		cfg.MaxDecompressBytes = *flagCompressMaxDecompressBytes
	}
	if *flagCompressWarmupCount != 0 {
		cfg.WarmupCount = *flagCompressWarmupCount
	}
}

// applyPipelineFlags applies command line flags to Pipeline configuration.
func applyPipelineFlags(cfg *PipelineConfig) {
	applyPipelineFlagInts(cfg)
	applyPipelineFlagDurations(cfg)
	if *flagPipelineHealthAddr != "" {
		cfg.HealthAddr = *flagPipelineHealthAddr
	}
}

func applyPipelineFlagInts(cfg *PipelineConfig) {
	if *flagPipelineBufferCapacity != 0 {
		cfg.BufferCapacity = *flagPipelineBufferCapacity
	}
	if *flagPipelinePublishWorkers != 0 {
		cfg.PublishWorkers = *flagPipelinePublishWorkers
	}
	if *flagPipelineAckBatchSize != 0 {
		cfg.AckBatchSize = *flagPipelineAckBatchSize
	}
	if *flagPipelineAckWorkers != 0 {
		cfg.AckWorkers = *flagPipelineAckWorkers
	}
	if *flagPipelineMessageQueueCapacity != 0 {
		cfg.MessageQueueCapacity = *flagPipelineMessageQueueCapacity
	}
}

func applyPipelineFlagDurations(cfg *PipelineConfig) {
	if *flagPipelineShutdownTimeout != 0 {
		cfg.ShutdownTimeout = *flagPipelineShutdownTimeout
	}
	if *flagPipelineErrorBackoff != 0 {
		cfg.ErrorBackoff = *flagPipelineErrorBackoff
	}
	if *flagPipelineAckTimeout != 0 {
		cfg.AckTimeout = *flagPipelineAckTimeout
	}
	if *flagPipelineRefreshInterval != 0 {
		cfg.RefreshInterval = *flagPipelineRefreshInterval
	}
	if *flagPipelineAckFlushInterval != 0 {
		cfg.AckFlushInterval = *flagPipelineAckFlushInterval
	}
	if *flagPipelineHealthPingTimeout != 0 {
		cfg.HealthPingTimeout = *flagPipelineHealthPingTimeout
	}
	if *flagPipelineHealthReadHeaderTimeout != 0 {
		cfg.HealthReadHeaderTimeout = *flagPipelineHealthReadHeaderTimeout
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
