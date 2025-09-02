package config

import (
	"fmt"
	"math"
	"os"
)

// Validate validates the configuration
func (c *Config) Validate() error {
	if err := validateApp(c); err != nil {
		return err
	}
	if err := validateRedis(c); err != nil {
		return err
	}
	if err := validateMQTT(c); err != nil {
		return err
	}
	if err := validateResource(c); err != nil {
		return err
	}
	if err := validateMetrics(c); err != nil {
		return err
	}
	if err := validateHealth(c); err != nil {
		return err
	}
	if err := validatePipeline(c); err != nil {
		return err
	}
	if err := validateCircuitBreaker(c); err != nil {
		return err
	}
	return nil
}

// --- App ---

func validateApp(c *Config) error {
	if c.App.Name == "" {
		return fmt.Errorf("app name cannot be empty")
	}
	if !isValidLogLevel(c.App.LogLevel) {
		return fmt.Errorf("invalid log level: %s", c.App.LogLevel)
	}
	if !isValidLogFormat(c.App.LogFormat) {
		return fmt.Errorf("invalid log format: %s", c.App.LogFormat)
	}
	if c.App.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown timeout must be positive")
	}
	return nil
}

func isValidLogLevel(level string) bool {
	switch level {
	case "trace", "debug", "info", "warn", "error":
		return true
	default:
		return false
	}
}

func isValidLogFormat(format string) bool {
	switch format {
	case "json", "text":
		return true
	default:
		return false
	}
}

// --- Redis ---

func validateRedis(c *Config) error {
	if len(c.Redis.Addresses) == 0 {
		return fmt.Errorf("at least one redis address is required")
	}
	if c.Redis.DB < 0 {
		return fmt.Errorf("redis db must be non-negative")
	}
	if c.Redis.StreamName == "" {
		return fmt.Errorf("redis stream name cannot be empty")
	}
	if c.Redis.ConsumerGroup == "" {
		return fmt.Errorf("redis consumer group cannot be empty")
	}
	if c.Redis.MaxRetries < 0 {
		return fmt.Errorf("redis max retries must be non-negative")
	}
	if c.Redis.PoolSize <= 0 {
		return fmt.Errorf("redis pool size must be positive")
	}
	if c.Redis.ClaimBatchSize <= 0 {
		return fmt.Errorf("redis claim batch size must be positive")
	}
	return nil
}

// --- MQTT + TLS ---

func validateMQTT(c *Config) error {
	if len(c.MQTT.Brokers) == 0 {
		return fmt.Errorf("at least one mqtt broker is required")
	}
	if c.MQTT.ClientID == "" {
		return fmt.Errorf("mqtt client id cannot be empty")
	}
	// SA4003: QoS is an unsigned byte; only upper bound check is meaningful
	if c.MQTT.QoS > 2 {
		return fmt.Errorf("mqtt qos must be 0, 1, or 2")
	}
	if c.MQTT.PublisherPoolSize <= 0 {
		return fmt.Errorf("mqtt publisher pool size must be positive")
	}
	if c.MQTT.MaxInflight <= 0 {
		return fmt.Errorf("mqtt max inflight must be positive")
	}

	if c.MQTT.TLS.Enabled {
		if err := validateTLS(&c.MQTT.TLS); err != nil {
			return err
		}
	}
	return nil
}

func validateTLS(tls *TLSConfig) error {
	if tls.CACertFile == "" {
		return fmt.Errorf("ca certificate file is required when tls is enabled")
	}
	if tls.ClientCertFile == "" {
		return fmt.Errorf("client certificate file is required when tls is enabled")
	}
	if tls.ClientKeyFile == "" {
		return fmt.Errorf("client key file is required when tls is enabled")
	}

	// Check if files exist
	if _, err := os.Stat(tls.CACertFile); err != nil {
		return fmt.Errorf("ca certificate file not found: %w", err)
	}
	if _, err := os.Stat(tls.ClientCertFile); err != nil {
		return fmt.Errorf("client certificate file not found: %w", err)
	}
	if _, err := os.Stat(tls.ClientKeyFile); err != nil {
		return fmt.Errorf("client key file not found: %w", err)
	}
	return nil
}

// --- Resource ---

func validateResource(c *Config) error {
	if err := validateResourceThresholds(c); err != nil {
		return err
	}
	if err := validateResourceWorkers(c); err != nil {
		return err
	}
	return nil
}

func validateResourceThresholds(c *Config) error {
	if c.Resource.CPUThresholdHigh <= c.Resource.CPUThresholdLow {
		return fmt.Errorf("cpu high threshold must be greater than low threshold")
	}
	if c.Resource.CPUThresholdLow <= 0 || c.Resource.CPUThresholdHigh > 100 {
		return fmt.Errorf("cpu thresholds must be between 0 and 100")
	}
	if c.Resource.MemoryThresholdHigh <= c.Resource.MemoryThresholdLow {
		return fmt.Errorf("memory high threshold must be greater than low threshold")
	}
	if c.Resource.MemoryThresholdLow <= 0 || c.Resource.MemoryThresholdHigh > 100 {
		return fmt.Errorf("memory thresholds must be between 0 and 100")
	}
	return nil
}

func validateResourceWorkers(c *Config) error {
	if c.Resource.MinWorkers <= 0 {
		return fmt.Errorf("minimum workers must be positive")
	}
	if c.Resource.MaxWorkers < c.Resource.MinWorkers {
		return fmt.Errorf("maximum workers must be greater than or equal to minimum workers")
	}
	if c.Resource.WorkerStepSize <= 0 {
		return fmt.Errorf("worker step size must be positive")
	}
	if c.Resource.HistoryWindowSize <= 0 {
		return fmt.Errorf("history window size must be positive")
	}
	return nil
}

// --- Metrics ---

func validateMetrics(c *Config) error {
	if !c.Metrics.Enabled {
		return nil
	}
	if c.Metrics.PrometheusPort <= 0 || c.Metrics.PrometheusPort > 65535 {
		return fmt.Errorf("prometheus port must be between 1 and 65535")
	}
	if c.Metrics.Namespace == "" {
		return fmt.Errorf("metrics namespace cannot be empty")
	}
	if len(c.Metrics.HistogramBuckets) == 0 {
		return fmt.Errorf("at least one histogram bucket is required")
	}
	return nil
}

// --- Health ---

func validateHealth(c *Config) error {
	if !c.Health.Enabled {
		return nil
	}
	if c.Health.Port <= 0 || c.Health.Port > 65535 {
		return fmt.Errorf("health port must be between 1 and 65535")
	}
	if c.Health.Port == c.Metrics.PrometheusPort {
		return fmt.Errorf("health port and metrics port cannot be the same")
	}
	return nil
}

// --- Pipeline ---

func validatePipeline(c *Config) error {
	if c.Pipeline.BufferSize <= 0 {
		return fmt.Errorf("pipeline buffer size must be positive")
	}
	// Enforce 32-bit limit for ring-buffer capacity conversions
	if c.Pipeline.BufferSize > int(math.MaxUint32) {
		return fmt.Errorf("pipeline buffer size exceeds 32-bit limit")
	}
	if !isPowerOfTwo(c.Pipeline.BufferSize) {
		return fmt.Errorf("pipeline buffer size must be a power of 2")
	}
	if c.Pipeline.BatchSize <= 0 {
		return fmt.Errorf("pipeline batch size must be positive")
	}
	if c.Pipeline.BatchSize > c.Pipeline.BufferSize {
		return fmt.Errorf("pipeline batch size cannot exceed buffer size")
	}
	if c.Pipeline.BackpressureThreshold <= 0 || c.Pipeline.BackpressureThreshold > 1 {
		return fmt.Errorf("backpressure threshold must be between 0 and 1")
	}
	if !isValidDropPolicy(c.Pipeline.DropPolicy) {
		return fmt.Errorf("invalid drop policy: %s", c.Pipeline.DropPolicy)
	}
	return nil
}

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

func isValidDropPolicy(policy string) bool {
	switch policy {
	case "oldest", "newest", "none":
		return true
	default:
		return false
	}
}

// --- Circuit Breaker ---

func validateCircuitBreaker(c *Config) error {
	if !c.CircuitBreaker.Enabled {
		return nil
	}
	if c.CircuitBreaker.ErrorThreshold <= 0 || c.CircuitBreaker.ErrorThreshold > 100 {
		return fmt.Errorf("circuit breaker error threshold must be between 0 and 100")
	}
	if c.CircuitBreaker.SuccessThreshold <= 0 {
		return fmt.Errorf("circuit breaker success threshold must be positive")
	}
	if c.CircuitBreaker.MaxConcurrentCalls <= 0 {
		return fmt.Errorf("circuit breaker max concurrent calls must be positive")
	}
	if c.CircuitBreaker.RequestVolumeThreshold <= 0 {
		return fmt.Errorf("circuit breaker request volume threshold must be positive")
	}
	return nil
}
