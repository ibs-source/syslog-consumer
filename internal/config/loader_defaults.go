package config

import "time"

// defaultRedisConfig returns the default Redis configuration.
func defaultRedisConfig() RedisConfig {
	return RedisConfig{
		Address:             "localhost:6379",
		Stream:              "syslog-stream",
		Consumer:            "consumer-1",
		GroupName:           "consumer-group",
		BatchSize:           20000,            // large batches to amortize XREADGROUP overhead
		DiscoveryScanCount:  1000,             // Redis SCAN COUNT hint for stream discovery
		BlockTimeout:        1 * time.Second,  // react within 1s when stream has new data
		ClaimIdle:           10 * time.Second, // faster reclaim of stuck messages
		ConsumerIdleTimeout: 5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		DialTimeout:         5 * time.Second, // tighter than 10s but safe
		ReadTimeout:         3 * time.Second, // must be > BlockTimeout
		WriteTimeout:        3 * time.Second,
		PingTimeout:         3 * time.Second,
		PoolSize:            50, // enough for batch + claim + ack concurrency
		MinIdleConns:        10, // keep warm connections ready
	}
}

// defaultLogConfig returns the default logging configuration.
func defaultLogConfig() LogConfig {
	return LogConfig{Level: "info"}
}

// defaultMQTTConfig returns the default MQTT configuration.
func defaultMQTTConfig() MQTTConfig {
	return MQTTConfig{
		Broker:               "tcp://localhost:1883",
		ClientID:             "syslog-consumer",
		PublishTopic:         "syslog/remote",
		AckTopic:             "syslog/remote/acknowledgement",
		QoS:                  0,
		ConnectTimeout:       10 * time.Second,
		WriteTimeout:         5 * time.Second, // reduced from 30s — with QoS 0 publish is fast
		PoolSize:             25,              // 25 connections to match publish workers ratio (2 workers/conn)
		MaxReconnectInterval: 5 * time.Second, // faster reconnect under load
		SubscribeTimeout:     10 * time.Second,
		DisconnectTimeout:    1000 * time.Millisecond,
		KeepAlive:            60 * time.Second, // PINGREQ interval for NAT/firewall idle timeouts
		PingTimeout:          10 * time.Second, // max wait for PINGRESP before reconnect
		ConnectRetryDelay:    2 * time.Second,  // pause between connection retry attempts
		MessageChannelDepth:  10000,            // internal paho outgoing queue depth
		MaxResumePubInFlight: 1000,             // unacknowledged publishes resumed after reconnect
		TLSEnabled:           false,
		CACert:               "",
		ClientCert:           "",
		ClientKey:            "",
		InsecureSkip:         false,
		UseCertCNPrefix:      false,
	}
}

// defaultPipelineConfig returns the default pipeline configuration.
func defaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		BufferCapacity:          10000,                 // ACK channel depth: ~0.5s at 20k ACK/s
		MessageQueueCapacity:    500,                   // fetch→publish queue depth; absorbs fetch bursts
		ShutdownTimeout:         10 * time.Second,      // faster drain on shutdown
		ErrorBackoff:            50 * time.Millisecond, // minimal pause, lose ~1000 msgs max
		AckTimeout:              5 * time.Second,
		PublishWorkers:          50, // 50 workers / 25 pool connections = 2 workers/conn
		AckWorkers:              50, // keep ACK throughput aligned with publish throughput
		RefreshInterval:         1 * time.Minute,
		AckFlushInterval:        10 * time.Millisecond, // timer interval for batched ACK flush
		AckBatchSize:            256,                   // immediate flush threshold
		HealthPingTimeout:       2 * time.Second,       // timeout for Redis ping in health check
		HealthReadHeaderTimeout: 5 * time.Second,
		HealthAddr:              ":9980",
	}
}

// defaultCompressConfig returns the default compression configuration.
func defaultCompressConfig() CompressConfig {
	return CompressConfig{
		FreelistSize:       128,               // sized for max concurrent decode workers
		MaxDecompressBytes: 256 * 1024 * 1024, // 256 MiB — broker limit is 128 MiB
		WarmupCount:        4,                 // pre-created decoders to avoid cold-start latency
	}
}

// defaultConfig returns a complete configuration with all default values.
func defaultConfig() *Config {
	return &Config{
		Log:      defaultLogConfig(),
		Redis:    defaultRedisConfig(),
		MQTT:     defaultMQTTConfig(),
		Pipeline: defaultPipelineConfig(),
		Compress: defaultCompressConfig(),
	}
}
