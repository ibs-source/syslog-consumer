package config

import "fmt"

// Validate checks configuration constraints
func Validate(cfg *Config) error {
	if err := validateRedis(&cfg.Redis); err != nil {
		return err
	}
	if err := validateMQTT(&cfg.MQTT); err != nil {
		return err
	}
	return validatePipeline(&cfg.Pipeline)
}

// validateRedis validates Redis configuration
func validateRedis(cfg *RedisConfig) error {
	if cfg.Address == "" {
		return fmt.Errorf("redis address cannot be empty")
	}
	if cfg.Consumer == "" {
		return fmt.Errorf("redis consumer name cannot be empty")
	}
	if cfg.BatchSize < 1 {
		return fmt.Errorf("redis batch size must be positive")
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

// validatePipeline validates Pipeline configuration
func validatePipeline(cfg *PipelineConfig) error {
	if cfg.BufferCapacity < 1 {
		return fmt.Errorf("pipeline buffer capacity must be positive")
	}
	if cfg.PublishWorkers < 1 {
		return fmt.Errorf("pipeline publish workers must be positive")
	}
	return nil
}
