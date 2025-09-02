package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// RegisterFlags registers all command-line flags
func RegisterFlags() {
	// Avoid redefining flags if already registered (tests may call multiple times)
	if flag.Lookup("redis-addr") != nil {
		return
	}

	registerRedisFlags()
	registerAdvancedRedisFlags()
	registerMQTTFlags()
	registerLogFlags()
	registerPipelineRetryFlags()
	registerPipelineDLQFlags()
	registerResourceFlags()
	registerHealthFlags()

	// Additional coverage to ensure defaults → env → flags precedence for all configurable params
	registerAppFlags()
	registerPipelineCoreFlags()
	registerMetricsFlags()
	registerCircuitBreakerFlags()
	registerMQTTAdvancedFlags()
	registerHealthExtraFlags()
}

// ApplyFlags applies command-line flag values to the configuration
func ApplyFlags(cfg *Config) {
	// Parse flags if not already parsed
	if !flag.Parsed() {
		flag.Parse()
	}

	applyRedisFlags(cfg)
	applyAdvancedRedisFlags(cfg)
	applyMQTTFlags(cfg)
	applyLogFlags(cfg)
	applyPipelineRetryFlags(cfg)
	applyPipelineDLQFlags(cfg)
	applyResourceFlags(cfg)
	applyHealthFlags(cfg)

	// Apply extended coverage
	applyAppFlags(cfg)
	applyPipelineCoreFlags(cfg)
	applyMetricsFlags(cfg)
	applyCircuitBreakerFlags(cfg)
	applyMQTTAdvancedFlags(cfg)
	applyHealthExtraFlags(cfg)
}

func registerRedisFlags() {
	// Redis flags
	flag.String("redis-addr", "", "Redis server address")
	flag.String("redis-password", "", "Redis server password")
	flag.Int("redis-db", -1, "Redis database")
	flag.String("redis-stream", "", "Redis stream name")
	flag.String("redis-group", "", "Redis consumer group name")
	flag.Int("redis-workers", -1, "Number of Redis stream workers (0 for number of CPUs)")
	flag.Int("redis-batch-size", -1, "Number of messages to read from Redis in one batch")
	flag.Int("redis-batch-timeout", -1, "Timeout in seconds for Redis XReadGroup block")
	flag.Int("redis-client-retries", -1, "Number of retries for Redis client connection")
	flag.Int("redis-client-retry-interval", -1, "Interval in seconds between Redis client connection retries")
}

func registerAdvancedRedisFlags() {
	// Advanced Redis claim and drain flags
	flag.Bool("redis-aggressive-claim", true, "Enable aggressive claim until exhaustion")
	flag.Int("redis-claim-cycle-delay", -1, "Delay in seconds between aggressive claim cycles")
	flag.Bool("redis-drain-enabled", true, "Enable draining unassigned messages")
	flag.Int("redis-drain-interval", -1, "Interval in seconds for drain operations")
	flag.Int("redis-drain-batch-size", -1, "Batch size for drain operations")
	flag.Bool("redis-consumer-cleanup-enabled", true, "Enable automatic consumer cleanup")
	flag.Int("redis-consumer-idle-timeout", -1, "Timeout in seconds to consider a consumer idle")
	flag.Int("redis-consumer-cleanup-interval", -1, "Interval in seconds for consumer cleanup checks")
}

func registerMQTTFlags() {
	// MQTT flags
	flag.String("mqtt-broker", "", "MQTT broker address")
	flag.String("mqtt-client-id", "", "MQTT client ID")
	flag.String("mqtt-publish-topic", "", "MQTT topic for publishing messages")
	flag.String("mqtt-subscribe-topic", "", "MQTT topic for receiving acknowledgments")
	flag.Bool("mqtt-use-user-prefix", true, "Whether to prepend user prefix from certificate")
	flag.String("mqtt-custom-prefix", "", "Custom prefix to use if user prefix is disabled")
	flag.Int("mqtt-qos", -1, "MQTT QoS level")
	flag.String("mqtt-ca-cert", "", "Path to MQTT CA certificate file")
	flag.String("mqtt-client-cert", "", "Path to MQTT client certificate file")
	flag.String("mqtt-client-key", "", "Path to MQTT client key file")
	flag.Int("mqtt-publishers", -1, "Number of MQTT publishers (0 for number of CPUs)")
	flag.Bool("mqtt-clean-session", false, "MQTT clean session")
}

func registerLogFlags() {
	// Log flags
	flag.String("log-level", "", "Log level (debug, info, warn, error)")
	flag.String("log-format", "", "Log format (text, json)")
}

func registerPipelineRetryFlags() {
	// Pipeline retry flags
	flag.Bool("pipeline-retry-enabled", true, "Enable message retry logic")
	flag.Int("pipeline-retry-max-attempts", -1, "Maximum number of retry attempts for a message")
	flag.Int("pipeline-retry-initial-backoff", -1, "Initial backoff duration in seconds for message retries")
	flag.Int("pipeline-retry-max-backoff", -1, "Maximum backoff duration in seconds for message retries")
	flag.Float64("pipeline-retry-multiplier", -1, "Multiplier for exponential backoff in message retries")
}

func registerPipelineDLQFlags() {
	flag.Bool("pipeline-dlq-enabled", true, "Enable Dead Letter Queue for failed messages")
	flag.String("pipeline-dlq-topic", "", "MQTT topic for Dead Letter Queue")
}

func registerResourceFlags() {
	// Resource management flags
	flag.Float64("resource-upper-cpu", -1, "Upper CPU threshold for resource management")
	flag.Float64("resource-lower-cpu", -1, "Lower CPU threshold for resource management")
	flag.Float64("resource-upper-mem", -1, "Upper memory threshold for resource management")
	flag.Float64("resource-lower-mem", -1, "Lower memory threshold for resource management")
}

func registerHealthFlags() {
	// Health check flags
	flag.Bool("health-enabled", true, "Enable health check server")
	flag.Int("health-port", -1, "Health check server port")
	flag.Int("health-read-timeout", -1, "Health check read timeout in seconds")
	flag.Int("health-write-timeout", -1, "Health check write timeout in seconds")
}

func applyRedisFlags(cfg *Config) {
	applyRedisAddrAuthFlags(cfg)
	applyRedisStreamFlags(cfg)
	applyRedisWorkerBatchFlags(cfg)
	applyRedisClientRetryFlags(cfg)
}

func applyRedisAddrAuthFlags(cfg *Config) {
	if val := getFlagString("redis-addr"); val != "" {
		cfg.Redis.Addresses = []string{val}
	}
	if val := getFlagString("redis-password"); val != "" {
		cfg.Redis.Password = val
	}
	if val := getFlagInt("redis-db"); val >= 0 {
		cfg.Redis.DB = val
	}
}

func applyRedisStreamFlags(cfg *Config) {
	if val := getFlagString("redis-stream"); val != "" {
		cfg.Redis.StreamName = val
	}
	if val := getFlagString("redis-group"); val != "" {
		cfg.Redis.ConsumerGroup = val
	}
}

func applyRedisWorkerBatchFlags(cfg *Config) {
	if val := getFlagInt("redis-workers"); val >= 0 {
		if val > 0 {
			cfg.Resource.MaxWorkers = val
		}
	}
	if val := getFlagInt("redis-batch-size"); val > 0 {
		cfg.Redis.BatchSize = int64(val)
	}
	if val := getFlagInt("redis-batch-timeout"); val > 0 {
		cfg.Redis.BlockTime = time.Duration(val) * time.Second
	}
}

func applyRedisClientRetryFlags(cfg *Config) {
	if val := getFlagInt("redis-client-retries"); val >= 0 {
		cfg.Redis.MaxRetries = val
	}
	if val := getFlagInt("redis-client-retry-interval"); val > 0 {
		cfg.Redis.RetryInterval = time.Duration(val) * time.Second
	}
}

func applyAdvancedRedisFlags(cfg *Config) {
	// Advanced Redis flags
	if f := flag.Lookup("redis-aggressive-claim"); f != nil {
		cfg.Redis.AggressiveClaim = getFlagBool("redis-aggressive-claim")
	}
	if val := getFlagInt("redis-claim-cycle-delay"); val > 0 {
		cfg.Redis.ClaimCycleDelay = time.Duration(val) * time.Second
	}
	if f := flag.Lookup("redis-drain-enabled"); f != nil {
		cfg.Redis.DrainEnabled = getFlagBool("redis-drain-enabled")
	}
	if val := getFlagInt("redis-drain-interval"); val > 0 {
		cfg.Redis.DrainInterval = time.Duration(val) * time.Second
	}
	if val := getFlagInt("redis-drain-batch-size"); val > 0 {
		cfg.Redis.DrainBatchSize = int64(val)
	}
	if f := flag.Lookup("redis-consumer-cleanup-enabled"); f != nil {
		cfg.Redis.ConsumerCleanupEnabled = getFlagBool("redis-consumer-cleanup-enabled")
	}
	if val := getFlagInt("redis-consumer-idle-timeout"); val > 0 {
		cfg.Redis.ConsumerIdleTimeout = time.Duration(val) * time.Second
	}
	if val := getFlagInt("redis-consumer-cleanup-interval"); val > 0 {
		cfg.Redis.ConsumerCleanupInterval = time.Duration(val) * time.Second
	}
}

func applyMQTTFlags(cfg *Config) {
	applyMQTTBrokerFlag(cfg)
	applyMQTTClientAndTopicsFlags(cfg)
	applyMQTTQoSAndTLSFilesFlags(cfg)
	applyMQTTPublisherAndSessionFlags(cfg)
}

func applyMQTTBrokerFlag(cfg *Config) {
	// MQTT flags
	if val := getFlagString("mqtt-broker"); val != "" {
		cfg.MQTT.Brokers = []string{val}
		// Auto-enable TLS if port 8883 is used
		if strings.Contains(val, ":8883") {
			cfg.MQTT.TLS.Enabled = true
		}
	}
}

func applyMQTTClientAndTopicsFlags(cfg *Config) {
	if val := getFlagString("mqtt-client-id"); val != "" {
		cfg.MQTT.ClientID = val
	}
	if val := getFlagString("mqtt-publish-topic"); val != "" {
		cfg.MQTT.Topics.PublishTopic = val
	}
	if val := getFlagString("mqtt-subscribe-topic"); val != "" {
		cfg.MQTT.Topics.SubscribeTopic = val
	}
	// Check mqtt-use-user-prefix flag
	if f := flag.Lookup("mqtt-use-user-prefix"); f != nil {
		cfg.MQTT.Topics.UseUserPrefix = getFlagBool("mqtt-use-user-prefix")
	}
	if val := getFlagString("mqtt-custom-prefix"); val != "" {
		cfg.MQTT.Topics.CustomPrefix = val
	}
}

func applyMQTTQoSAndTLSFilesFlags(cfg *Config) {
	if val := getFlagInt("mqtt-qos"); val >= 0 && val <= 2 {
		cfg.MQTT.QoS = byte(val)
	}
	if val := getFlagString("mqtt-ca-cert"); val != "" {
		cfg.MQTT.TLS.CACertFile = val
		cfg.MQTT.TLS.Enabled = true
	}
	if val := getFlagString("mqtt-client-cert"); val != "" {
		cfg.MQTT.TLS.ClientCertFile = val
		cfg.MQTT.TLS.Enabled = true
	}
	if val := getFlagString("mqtt-client-key"); val != "" {
		cfg.MQTT.TLS.ClientKeyFile = val
		cfg.MQTT.TLS.Enabled = true
	}
}

func applyMQTTPublisherAndSessionFlags(cfg *Config) {
	if val := getFlagInt("mqtt-publishers"); val > 0 {
		cfg.MQTT.PublisherPoolSize = val
	}
	// Check clean session flag
	if f := flag.Lookup("mqtt-clean-session"); f != nil {
		if val := getFlagBool("mqtt-clean-session"); val {
			cfg.MQTT.CleanSession = val
		}
	}
}

func applyLogFlags(cfg *Config) {
	// Log flags
	if val := getFlagString("log-level"); val != "" {
		cfg.App.LogLevel = val
	}
	if val := getFlagString("log-format"); val != "" {
		cfg.App.LogFormat = val
	}
}

func applyPipelineRetryFlags(cfg *Config) {
	// Pipeline retry flags
	if f := flag.Lookup("pipeline-retry-enabled"); f != nil {
		cfg.Pipeline.Retry.Enabled = getFlagBool("pipeline-retry-enabled")
	}
	if val := getFlagInt("pipeline-retry-max-attempts"); val > 0 {
		cfg.Pipeline.Retry.MaxAttempts = val
	}
	if val := getFlagInt("pipeline-retry-initial-backoff"); val > 0 {
		cfg.Pipeline.Retry.InitialBackoff = time.Duration(val) * time.Second
	}
	if val := getFlagInt("pipeline-retry-max-backoff"); val > 0 {
		cfg.Pipeline.Retry.MaxBackoff = time.Duration(val) * time.Second
	}
	if val := getFlagFloat64("pipeline-retry-multiplier"); val > 0 {
		cfg.Pipeline.Retry.Multiplier = val
	}
}

func applyPipelineDLQFlags(cfg *Config) {
	if f := flag.Lookup("pipeline-dlq-enabled"); f != nil {
		cfg.Pipeline.DLQ.Enabled = getFlagBool("pipeline-dlq-enabled")
	}
	if val := getFlagString("pipeline-dlq-topic"); val != "" {
		cfg.Pipeline.DLQ.Topic = val
	}
}

func applyResourceFlags(cfg *Config) {
	// Resource management flags
	if val := getFlagFloat64("resource-upper-cpu"); val >= 0 {
		cfg.Resource.CPUThresholdHigh = val
	}
	if val := getFlagFloat64("resource-lower-cpu"); val >= 0 {
		cfg.Resource.CPUThresholdLow = val
	}
	if val := getFlagFloat64("resource-upper-mem"); val >= 0 {
		cfg.Resource.MemoryThresholdHigh = val
	}
	if val := getFlagFloat64("resource-lower-mem"); val >= 0 {
		cfg.Resource.MemoryThresholdLow = val
	}
}

func applyHealthFlags(cfg *Config) {
	// Health check flags
	if f := flag.Lookup("health-enabled"); f != nil {
		cfg.Health.Enabled = getFlagBool("health-enabled")
	}
	if val := getFlagInt("health-port"); val > 0 {
		cfg.Health.Port = val
	}
	if val := getFlagInt("health-read-timeout"); val > 0 {
		cfg.Health.ReadTimeout = time.Duration(val) * time.Second
	}
	if val := getFlagInt("health-write-timeout"); val > 0 {
		cfg.Health.WriteTimeout = time.Duration(val) * time.Second
	}
}

func getFlagString(name string) string {
	f := flag.Lookup(name)
	if f == nil {
		return ""
	}
	return f.Value.String()
}

func getFlagInt(name string) int {
	f := flag.Lookup(name)
	if f == nil {
		return -1
	}
	// Type assertion to get the actual int value
	if getter, ok := f.Value.(flag.Getter); ok {
		if val, ok := getter.Get().(int); ok {
			return val
		}
	}
	return -1
}

func getFlagFloat64(name string) float64 {
	f := flag.Lookup(name)
	if f == nil {
		return -1
	}
	// Type assertion to get the actual float64 value
	if getter, ok := f.Value.(flag.Getter); ok {
		if val, ok := getter.Get().(float64); ok {
			return val
		}
	}
	return -1
}

func getFlagBool(name string) bool {
	f := flag.Lookup(name)
	if f == nil {
		return false
	}
	// Type assertion to get the actual bool value
	if getter, ok := f.Value.(flag.Getter); ok {
		if val, ok := getter.Get().(bool); ok {
			return val
		}
	}
	return false
}

/********************* Additional Flag Registration *********************/

func registerAppFlags() {
	flag.String("app-name", "", "Application name")
	flag.String("app-env", "", "Application environment (production, staging, etc.)")
	flag.Int("app-shutdown-timeout", -1, "Application shutdown timeout in seconds")
	flag.Int("app-pending-ops-grace-ms", -1, "Grace period for pending ops in milliseconds")
}

func applyAppFlags(cfg *Config) {
	if v := getFlagString("app-name"); v != "" {
		cfg.App.Name = v
	}
	if v := getFlagString("app-env"); v != "" {
		cfg.App.Environment = v
	}
	if v := getFlagInt("app-shutdown-timeout"); v > 0 {
		cfg.App.ShutdownTimeout = time.Duration(v) * time.Second
	}
	if v := getFlagInt("app-pending-ops-grace-ms"); v > 0 {
		cfg.App.PendingOpsGrace = time.Duration(v) * time.Millisecond
	}
}

func registerPipelineCoreFlags() {
	flag.Int("pipeline-buffer-size", -1, "Ring buffer size (will be rounded up to next power of 2)")
	flag.Int("pipeline-batch-size", -1, "Pipeline batch size")
	flag.Int("pipeline-batch-timeout", -1, "Batch timeout in seconds")
	flag.Int("pipeline-processing-timeout", -1, "Processing timeout in seconds")
	flag.Bool("pipeline-zero-copy", true, "Enable zero-copy pipeline")
	flag.Bool("pipeline-preallocate", true, "Preallocate pipeline buffers")
	flag.Bool("pipeline-numa-aware", false, "Enable NUMA awareness")
	flag.String("pipeline-cpu-affinity", "", "Comma-separated CPU ids for affinity (e.g., 0,1,2)")
	flag.Float64("pipeline-backpressure-threshold", -1, "Backpressure threshold (0.0-1.0)")
	flag.String("pipeline-drop-policy", "", "Drop policy (oldest|newest|none)")
	flag.Int("pipeline-flush-interval-ms", -1, "Flush interval in milliseconds")
	flag.Int("pipeline-backpressure-poll-interval", -1, "Backpressure poll interval in seconds")
	flag.Int("pipeline-idle-poll-sleep-ms", -1, "Idle poll sleep in milliseconds when no messages are available")
}

func applyPipelineCoreFlags(cfg *Config) {
	applyPipelineCoreSizes(cfg)
	applyPipelineCoreTimeouts(cfg)
	applyPipelineCoreToggles(cfg)
	applyPipelineCoreAffinityAndBackpressure(cfg)
}

func applyPipelineCoreSizes(cfg *Config) {
	if v := getFlagInt("pipeline-buffer-size"); v > 0 {
		cfg.Pipeline.BufferSize = nextPowerOf2(v)
	}
	if v := getFlagInt("pipeline-batch-size"); v > 0 {
		cfg.Pipeline.BatchSize = v
	}
}

func applyPipelineCoreTimeouts(cfg *Config) {
	if v := getFlagInt("pipeline-batch-timeout"); v > 0 {
		cfg.Pipeline.BatchTimeout = time.Duration(v) * time.Second
	}
	if v := getFlagInt("pipeline-processing-timeout"); v > 0 {
		cfg.Pipeline.ProcessingTimeout = time.Duration(v) * time.Second
	}
	if v := getFlagInt("pipeline-flush-interval-ms"); v > 0 {
		cfg.Pipeline.FlushInterval = time.Duration(v) * time.Millisecond
	}
	if v := getFlagInt("pipeline-backpressure-poll-interval"); v > 0 {
		cfg.Pipeline.BackpressurePollInterval = time.Duration(v) * time.Second
	}
	if v := getFlagInt("pipeline-idle-poll-sleep-ms"); v > 0 {
		cfg.Pipeline.IdlePollSleep = time.Duration(v) * time.Millisecond
	}
}

func applyPipelineCoreToggles(cfg *Config) {
	if f := flag.Lookup("pipeline-zero-copy"); f != nil {
		cfg.Pipeline.UseZeroCopy = getFlagBool("pipeline-zero-copy")
	}
	if f := flag.Lookup("pipeline-preallocate"); f != nil {
		cfg.Pipeline.PreallocateBuffers = getFlagBool("pipeline-preallocate")
	}
	if f := flag.Lookup("pipeline-numa-aware"); f != nil {
		cfg.Pipeline.NumaAware = getFlagBool("pipeline-numa-aware")
	}
	if s := getFlagString("pipeline-drop-policy"); s != "" {
		cfg.Pipeline.DropPolicy = s
	}
}

func applyPipelineCoreAffinityAndBackpressure(cfg *Config) {
	if s := getFlagString("pipeline-cpu-affinity"); s != "" {
		parts := strings.Split(s, ",")
		aff := make([]int, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if n, err := strconv.Atoi(p); err == nil {
				aff = append(aff, n)
			}
		}
		if len(aff) > 0 {
			cfg.Pipeline.CPUAffinity = aff
		}
	}
	if v := getFlagFloat64("pipeline-backpressure-threshold"); v > 0 {
		cfg.Pipeline.BackpressureThreshold = v
	}
}

func registerMetricsFlags() {
	flag.Bool("metrics-enabled", true, "Enable metrics")
	flag.Int("metrics-port", -1, "Prometheus port")
	flag.Int("metrics-collect-interval", -1, "Metrics collection interval in seconds")
	flag.String("metrics-namespace", "", "Metrics namespace")
	flag.String("metrics-subsystem", "", "Metrics subsystem")
	flag.String("metrics-histogram-buckets", "", "CSV list of histogram buckets (e.g., 0.1,0.2,0.3)")
}

func applyMetricsFlags(cfg *Config) {
	if f := flag.Lookup("metrics-enabled"); f != nil {
		cfg.Metrics.Enabled = getFlagBool("metrics-enabled")
	}
	if v := getFlagInt("metrics-port"); v > 0 {
		cfg.Metrics.PrometheusPort = v
	}
	if v := getFlagInt("metrics-collect-interval"); v > 0 {
		cfg.Metrics.CollectInterval = time.Duration(v) * time.Second
	}
	if s := getFlagString("metrics-namespace"); s != "" {
		cfg.Metrics.Namespace = s
	}
	if s := getFlagString("metrics-subsystem"); s != "" {
		cfg.Metrics.Subsystem = s
	}
	if s := getFlagString("metrics-histogram-buckets"); s != "" {
		parts := strings.Split(s, ",")
		out := make([]float64, 0, len(parts))
		for _, p := range parts {
			if v, err := strconv.ParseFloat(strings.TrimSpace(p), 64); err == nil {
				out = append(out, v)
			}
		}
		if len(out) > 0 {
			cfg.Metrics.HistogramBuckets = out
		}
	}
}

func registerCircuitBreakerFlags() {
	flag.Bool("cb-enabled", true, "Enable circuit breaker")
	flag.Float64("cb-error-threshold", -1, "Error threshold percentage for circuit breaker")
	flag.Int("cb-success-threshold", -1, "Success threshold to close circuit")
	flag.Int("cb-timeout", -1, "Circuit breaker timeout in seconds")
	flag.Int("cb-max-concurrent", -1, "Max concurrent calls allowed")
	flag.Int("cb-request-volume", -1, "Request volume threshold")
}

func applyCircuitBreakerFlags(cfg *Config) {
	if f := flag.Lookup("cb-enabled"); f != nil {
		cfg.CircuitBreaker.Enabled = getFlagBool("cb-enabled")
	}
	if v := getFlagFloat64("cb-error-threshold"); v > 0 {
		cfg.CircuitBreaker.ErrorThreshold = v
	}
	if v := getFlagInt("cb-success-threshold"); v > 0 {
		cfg.CircuitBreaker.SuccessThreshold = v
	}
	if v := getFlagInt("cb-timeout"); v > 0 {
		cfg.CircuitBreaker.Timeout = time.Duration(v) * time.Second
	}
	if v := getFlagInt("cb-max-concurrent"); v > 0 {
		cfg.CircuitBreaker.MaxConcurrentCalls = v
	}
	if v := getFlagInt("cb-request-volume"); v > 0 {
		cfg.CircuitBreaker.RequestVolumeThreshold = v
	}
}

func registerMQTTAdvancedFlags() {
	flag.Int("mqtt-keep-alive", -1, "MQTT keep alive in seconds")
	flag.Int("mqtt-connect-timeout", -1, "MQTT connect timeout in seconds")
	flag.Int("mqtt-max-reconnect-delay", -1, "MQTT max reconnect delay in seconds")
	flag.Bool("mqtt-order-matters", true, "Whether MQTT message order matters")
	flag.Int("mqtt-message-channel-depth", -1, "MQTT client message channel depth")
	flag.Int("mqtt-max-inflight", -1, "MQTT max inflight messages")
	flag.Int("mqtt-write-timeout", -1, "MQTT write timeout in seconds")
	flag.Bool("mqtt-tls-insecure", false, "MQTT TLS insecure skip verify (DANGEROUS)")
	flag.String("mqtt-tls-server-name", "", "MQTT TLS server name")
	flag.String("mqtt-tls-min-version", "", "MQTT TLS minimum version (TLS1.2|TLS1.3)")
	flag.String("mqtt-tls-cipher-suites", "", "CSV list of MQTT TLS cipher suites")
	flag.Bool("mqtt-tls-prefer-server-ciphers", false, "Prefer server ciphers for MQTT TLS")
}

func applyMQTTAdvancedFlags(cfg *Config) {
	applyMQTTAdvancedDurations(cfg)
	applyMQTTAdvancedBooleans(cfg)
	applyMQTTAdvancedCounts(cfg)
	applyMQTTAdvancedTLS(cfg)
}

func applyMQTTAdvancedDurations(cfg *Config) {
	if v := getFlagInt("mqtt-keep-alive"); v > 0 {
		cfg.MQTT.KeepAlive = time.Duration(v) * time.Second
	}
	if v := getFlagInt("mqtt-connect-timeout"); v > 0 {
		cfg.MQTT.ConnectTimeout = time.Duration(v) * time.Second
	}
	if v := getFlagInt("mqtt-max-reconnect-delay"); v > 0 {
		cfg.MQTT.MaxReconnectDelay = time.Duration(v) * time.Second
	}
	if v := getFlagInt("mqtt-write-timeout"); v > 0 {
		cfg.MQTT.WriteTimeout = time.Duration(v) * time.Second
	}
}

func applyMQTTAdvancedBooleans(cfg *Config) {
	if f := flag.Lookup("mqtt-order-matters"); f != nil {
		cfg.MQTT.OrderMatters = getFlagBool("mqtt-order-matters")
	}
	if f := flag.Lookup("mqtt-tls-insecure"); f != nil {
		cfg.MQTT.TLS.InsecureSkipVerify = getFlagBool("mqtt-tls-insecure")
	}
	if f := flag.Lookup("mqtt-tls-prefer-server-ciphers"); f != nil {
		cfg.MQTT.TLS.PreferServerCiphers = getFlagBool("mqtt-tls-prefer-server-ciphers")
	}
}

func applyMQTTAdvancedCounts(cfg *Config) {
	if v := getFlagInt("mqtt-message-channel-depth"); v > 0 {
		cfg.MQTT.MessageChannelDepth = v
	}
	if v := getFlagInt("mqtt-max-inflight"); v > 0 {
		cfg.MQTT.MaxInflight = v
	}
}

func applyMQTTAdvancedTLS(cfg *Config) {
	if s := getFlagString("mqtt-tls-server-name"); s != "" {
		cfg.MQTT.TLS.ServerName = s
	}
	if s := getFlagString("mqtt-tls-min-version"); s != "" {
		cfg.MQTT.TLS.MinVersion = s
	}
	if s := getFlagString("mqtt-tls-cipher-suites"); s != "" {
		parts := strings.Split(s, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		if len(out) > 0 {
			cfg.MQTT.TLS.CipherSuites = out
		}
	}
}

func registerHealthExtraFlags() {
	flag.Int("health-check-interval", -1, "Health check interval in seconds")
	flag.Int("health-redis-timeout", -1, "Health Redis timeout in seconds")
	flag.Int("health-mqtt-timeout", -1, "Health MQTT timeout in seconds")
}

func applyHealthExtraFlags(cfg *Config) {
	if v := getFlagInt("health-check-interval"); v > 0 {
		cfg.Health.CheckInterval = time.Duration(v) * time.Second
	}
	if v := getFlagInt("health-redis-timeout"); v > 0 {
		cfg.Health.RedisTimeout = time.Duration(v) * time.Second
	}
	if v := getFlagInt("health-mqtt-timeout"); v > 0 {
		cfg.Health.MQTTTimeout = time.Duration(v) * time.Second
	}
}

// PrintUsage prints the usage information for all flags.
func PrintUsage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}
