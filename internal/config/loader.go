package config

import (
	"flag"
	"fmt"
)

// Load loads configuration with precedence: defaults → environment variables → command line flags
// It performs validation and runtime transformations before returning the configuration.
func Load() (*Config, error) {
	// Parse command line flags if not already parsed
	if !flag.Parsed() {
		flag.Parse()
	}

	// Step 1: Start with defaults
	cfg := defaultConfig()

	// Step 2: Apply environment variables
	loadRedisFromEnv(&cfg.Redis)
	loadMQTTFromEnv(&cfg.MQTT)
	loadPipelineFromEnv(&cfg.Pipeline)

	// Step 3: Apply command line flags (highest precedence)
	applyRedisFlags(&cfg.Redis)
	applyMQTTFlags(&cfg.MQTT)
	applyPipelineFlags(&cfg.Pipeline)

	// Step 4: Apply runtime validations and transformations
	if err := applyRuntimeValidation(cfg); err != nil {
		return nil, err
	}

	// Step 5: Validate the final configuration
	if err := Validate(cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}
