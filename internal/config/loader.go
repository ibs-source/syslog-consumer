package config

import (
	"flag"
	"fmt"
)

// Load resolves the configuration with precedence
// defaults < environment < command-line flags, then validates it.
func Load() (*Config, error) {
	if !flag.Parsed() {
		flag.Parse()
	}

	cfg := defaultConfig()

	loadLogFromEnv(&cfg.Log)
	loadRedisFromEnv(&cfg.Redis)
	loadMQTTFromEnv(&cfg.MQTT)
	loadPipelineFromEnv(&cfg.Pipeline)
	loadCompressFromEnv(&cfg.Compress)

	applyLogFlags(&cfg.Log)
	applyRedisFlags(&cfg.Redis)
	applyMQTTFlags(&cfg.MQTT)
	applyPipelineFlags(&cfg.Pipeline)
	applyCompressFlags(&cfg.Compress)

	if err := applyRuntimeValidation(cfg); err != nil {
		return nil, err
	}

	if err := Validate(cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}
