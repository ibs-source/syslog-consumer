package config

import "errors"

// Validate enforces the subsystem invariants assumed by the rest of the code.
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
		return errors.New("log level must be one of trace, debug, info, warn, error, fatal, panic")
	}
}

func validateRedis(cfg *RedisConfig) error {
	if cfg.Address == "" {
		return errors.New("redis address cannot be empty")
	}
	if cfg.Consumer == "" {
		return errors.New("redis consumer name cannot be empty")
	}
	if cfg.GroupName == "" {
		return errors.New("redis group name cannot be empty")
	}
	if cfg.BatchSize < 1 {
		return errors.New("redis batch size must be positive")
	}
	if cfg.DiscoveryScanCount < 1 {
		return errors.New("redis discovery scan count must be positive")
	}
	return nil
}

func validateMQTT(cfg *MQTTConfig) error {
	if cfg.Broker == "" {
		return errors.New("mqtt broker cannot be empty")
	}
	if cfg.ClientID == "" {
		return errors.New("mqtt client ID cannot be empty")
	}
	if cfg.PoolSize < 1 {
		return errors.New("mqtt pool size must be positive")
	}
	if cfg.PublishTopic == "" {
		return errors.New("mqtt publish topic cannot be empty")
	}
	if cfg.AckTopic == "" {
		return errors.New("mqtt ack topic cannot be empty")
	}
	return nil
}

func validateCompress(cfg *CompressConfig) error {
	if cfg.FreelistSize < 1 {
		return errors.New("compress freelist size must be positive")
	}
	if cfg.MaxDecompressBytes < 1 {
		return errors.New("compress max decompress bytes must be positive")
	}
	if cfg.WarmupCount < 0 || cfg.WarmupCount > cfg.FreelistSize {
		return errors.New("compress warmup count must be between 0 and freelist size")
	}
	return nil
}

func validatePipeline(cfg *PipelineConfig) error {
	if cfg.BufferCapacity < 1 {
		return errors.New("pipeline buffer capacity must be positive")
	}
	if cfg.MessageQueueCapacity < 1 {
		return errors.New("pipeline message queue capacity must be positive")
	}
	if cfg.PublishWorkers < 1 {
		return errors.New("pipeline publish workers must be positive")
	}
	if cfg.AckWorkers < 1 {
		return errors.New("pipeline ack workers must be positive")
	}
	if cfg.AckBatchSize < 1 {
		return errors.New("pipeline ack batch size must be positive")
	}
	if cfg.HealthPingTimeout <= 0 {
		return errors.New("pipeline health ping timeout must be positive")
	}
	if cfg.HealthReadHeaderTimeout <= 0 {
		return errors.New("pipeline health read header timeout must be positive")
	}
	return nil
}
