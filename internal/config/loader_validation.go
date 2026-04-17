package config

import "fmt"

// Validate checks configuration constraints
func Validate(cfg *Config) error {
	if err := validateLog(&cfg.Log); err != nil {
		return err
	}
	if err := validateRedis(&cfg.Redis); err != nil {
		return err
	}
	if err := validateMQTT(&cfg.MQTT); err != nil {
		return err
	}
	if err := validatePipeline(&cfg.Pipeline); err != nil {
		return err
	}
	return validateCompress(&cfg.Compress)
}

func validateLog(cfg *LogConfig) error {
	switch cfg.Level {
	case "trace", "debug", "info", "warn", "warning", "error", "fatal", "panic":
		return nil
	default:
		return fmt.Errorf("log level must be one of trace, debug, info, warn, error, fatal, panic")
	}
}

// validateRedis validates Redis configuration
func validateRedis(cfg *RedisConfig) error {
	if cfg.Address == "" {
		return fmt.Errorf("redis address cannot be empty")
	}
	if cfg.Consumer == "" {
		return fmt.Errorf("redis consumer name cannot be empty")
	}
	if cfg.GroupName == "" {
		return fmt.Errorf("redis group name cannot be empty")
	}
	if cfg.BatchSize < 1 {
		return fmt.Errorf("redis batch size must be positive")
	}
	if cfg.DiscoveryScanCount < 1 {
		return fmt.Errorf("redis discovery scan count must be positive")
	}
	return nil
}

// validateMQTT validates MQTT configuration
func validateMQTT(cfg *MQTTConfig) error {
	if cfg.Broker == "" {
		return fmt.Errorf("mqtt broker cannot be empty")
	}
	if cfg.ClientID == "" {
		return fmt.Errorf("mqtt client ID cannot be empty")
	}
	if cfg.PoolSize < 1 {
		return fmt.Errorf("mqtt pool size must be positive")
	}
	if cfg.PublishTopic == "" {
		return fmt.Errorf("mqtt publish topic cannot be empty")
	}
	if cfg.AckTopic == "" {
		return fmt.Errorf("mqtt ack topic cannot be empty")
	}
	return nil
}

// validateCompress validates compression configuration.
func validateCompress(cfg *CompressConfig) error {
	if cfg.FreelistSize < 1 {
		return fmt.Errorf("compress freelist size must be positive")
	}
	if cfg.MaxDecompressBytes < 1 {
		return fmt.Errorf("compress max decompress bytes must be positive")
	}
	if cfg.WarmupCount < 0 || cfg.WarmupCount > cfg.FreelistSize {
		return fmt.Errorf("compress warmup count must be between 0 and freelist size")
	}
	return nil
}

// validatePipeline validates Pipeline configuration
func validatePipeline(cfg *PipelineConfig) error {
	if cfg.BufferCapacity < 1 {
		return fmt.Errorf("pipeline buffer capacity must be positive")
	}
	if cfg.MessageQueueCapacity < 1 {
		return fmt.Errorf("pipeline message queue capacity must be positive")
	}
	if cfg.PublishWorkers < 1 {
		return fmt.Errorf("pipeline publish workers must be positive")
	}
	if cfg.AckWorkers < 1 {
		return fmt.Errorf("pipeline ack workers must be positive")
	}
	if cfg.AckBatchSize < 1 {
		return fmt.Errorf("pipeline ack batch size must be positive")
	}
	if cfg.HealthPingTimeout <= 0 {
		return fmt.Errorf("pipeline health ping timeout must be positive")
	}
	if cfg.HealthReadHeaderTimeout <= 0 {
		return fmt.Errorf("pipeline health read header timeout must be positive")
	}
	return nil
}
