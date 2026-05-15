package config

import "time"

const (
	defaultRedisAddress     = "localhost:6379"
	defaultStreamName       = "syslog-stream"
	defaultRedisConsumer    = "consumer-1"
	defaultRedisGroup       = "consumer-group"
	defaultLogLevel         = "info"
	defaultMQTTBroker       = "tcp://localhost:1883"
	defaultMQTTClientID     = "syslog-consumer"
	defaultMQTTPublishTopic = "syslog/remote"
	defaultMQTTAckTopic     = "syslog/remote/acknowledgement"
	defaultHealthAddr       = ":9980"
)

func defaultRedisConfig() RedisConfig {
	return RedisConfig{
		Address:             defaultRedisAddress,
		Stream:              defaultStreamName,
		Consumer:            defaultRedisConsumer,
		GroupName:           defaultRedisGroup,
		BatchSize:           20000,
		DiscoveryScanCount:  1000,
		BlockTimeout:        1 * time.Second,
		ClaimIdle:           10 * time.Second,
		ConsumerIdleTimeout: 5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		DialTimeout:         5 * time.Second,
		// ReadTimeout must stay greater than BlockTimeout.
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PingTimeout:  3 * time.Second,
		// Recycle idle connections before NAT/conntrack drops them.
		ConnMaxIdleTime: 5 * time.Minute,
		// Lifetime rotation left disabled: synchronized expirations of all
		// boot-time connections cause "pool.go: was not able to get a healthy
		// connection" log spam. Idle recycling already covers stale connections.
		ConnMaxLifetime: 0,
		PoolSize:        50,
		MinIdleConns:    10,
	}
}

func defaultLogConfig() LogConfig {
	return LogConfig{Level: defaultLogLevel}
}

func defaultMQTTConfig() MQTTConfig {
	return MQTTConfig{
		Broker:               defaultMQTTBroker,
		ClientID:             defaultMQTTClientID,
		PublishTopic:         defaultMQTTPublishTopic,
		AckTopic:             defaultMQTTAckTopic,
		QoS:                  0,
		ConnectTimeout:       10 * time.Second,
		WriteTimeout:         5 * time.Second,
		PoolSize:             25,
		MaxReconnectInterval: 5 * time.Second,
		SubscribeTimeout:     10 * time.Second,
		DisconnectTimeout:    1000 * time.Millisecond,
		KeepAlive:            60 * time.Second,
		PingTimeout:          10 * time.Second,
		ConnectRetryDelay:    2 * time.Second,
		MessageChannelDepth:  10000,
		MaxResumePubInFlight: 1000,
		TLSEnabled:           false,
		CACert:               "",
		ClientCert:           "",
		ClientKey:            "",
		InsecureSkip:         false,
		UseCertCNPrefix:      false,
	}
}

func defaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		BufferCapacity:          10000,
		MessageQueueCapacity:    500,
		ShutdownTimeout:         10 * time.Second,
		ErrorBackoff:            50 * time.Millisecond,
		AckTimeout:              5 * time.Second,
		PublishWorkers:          25,
		AckWorkers:              50,
		RefreshInterval:         1 * time.Minute,
		AckFlushInterval:        10 * time.Millisecond,
		AckBatchSize:            256,
		HealthPingTimeout:       2 * time.Second,
		HealthReadHeaderTimeout: 5 * time.Second,
		HealthAddr:              defaultHealthAddr,
	}
}

func defaultCompressConfig() CompressConfig {
	return CompressConfig{
		FreelistSize:       128,
		MaxDecompressBytes: 256 * 1024 * 1024,
		WarmupCount:        4,
	}
}

func defaultConfig() *Config {
	return &Config{
		Log:      defaultLogConfig(),
		Redis:    defaultRedisConfig(),
		MQTT:     defaultMQTTConfig(),
		Pipeline: defaultPipelineConfig(),
		Compress: defaultCompressConfig(),
	}
}
