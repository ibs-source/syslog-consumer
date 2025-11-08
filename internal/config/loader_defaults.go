package config

import "time"

// defaultRedisConfig returns the default Redis configuration
func defaultRedisConfig() RedisConfig {
	return RedisConfig{
		Address:             "localhost:6379",
		Stream:              "syslog-stream",
		Consumer:            "consumer-1",
		BatchSize:           500,
		BlockTimeout:        5 * time.Second,
		ClaimIdle:           30 * time.Second,
		ConsumerIdleTimeout: 5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		DialTimeout:         10 * time.Second,
		ReadTimeout:         10 * time.Second,
		WriteTimeout:        5 * time.Second,
		PingTimeout:         5 * time.Second,
	}
}

// defaultMQTTConfig returns the default MQTT configuration
func defaultMQTTConfig() MQTTConfig {
	return MQTTConfig{
		Broker:               "tcp://localhost:1883",
		ClientID:             "syslog-consumer",
		PublishTopic:         "syslog/remote/messages",
		AckTopic:             "syslog/remote/acknowledgement",
		QoS:                  0,
		ConnectTimeout:       10 * time.Second,
		WriteTimeout:         30 * time.Second,
		PoolSize:             10,
		MaxReconnectInterval: 10 * time.Second,
		SubscribeTimeout:     10 * time.Second,
		DisconnectTimeout:    1000,
		TLSEnabled:           false,
		CACert:               "",
		ClientCert:           "",
		ClientKey:            "",
		InsecureSkip:         false,
		UseCertCNPrefix:      false,
	}
}

// defaultPipelineConfig returns the default pipeline configuration
func defaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		BufferCapacity:  10000,
		ShutdownTimeout: 30 * time.Second,
		ErrorBackoff:    1 * time.Second,
		AckTimeout:      5 * time.Second,
		PublishWorkers:  20,
	}
}

// defaultConfig returns a complete configuration with all default values
func defaultConfig() *Config {
	return &Config{
		Redis:    defaultRedisConfig(),
		MQTT:     defaultMQTTConfig(),
		Pipeline: defaultPipelineConfig(),
	}
}
