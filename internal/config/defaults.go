package config

import (
	"fmt"
	"os"
	"runtime"
	"time"
)

// GetDefaults returns a Config with all default values
func GetDefaults() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		App:            defaultApp(),
		Redis:          defaultRedis(),
		MQTT:           defaultMQTT(hostname),
		Resource:       defaultResource(),
		Metrics:        defaultMetrics(),
		Health:         defaultHealth(),
		Pipeline:       defaultPipeline(),
		CircuitBreaker: defaultCircuitBreaker(),
	}
}

func defaultApp() AppConfig {
	return AppConfig{
		Name:            "syslog-consumer",
		Environment:     "production",
		LogLevel:        "info",
		LogFormat:       "text",
		ShutdownTimeout: 30 * time.Second,
		PendingOpsGrace: 500 * time.Millisecond,
	}
}

func defaultRedis() RedisConfig {
	return RedisConfig{
		Addresses:               []string{"localhost:6379"},
		Password:                "",
		DB:                      0,
		StreamName:              "syslog-stream",
		ConsumerGroup:           "syslog-group",
		MaxRetries:              5,
		RetryInterval:           1 * time.Second,
		ConnectTimeout:          5 * time.Second,
		ReadTimeout:             3 * time.Second,
		WriteTimeout:            3 * time.Second,
		PoolSize:                runtime.NumCPU() * 10,
		MinIdleConns:            runtime.NumCPU(),
		ConnMaxLifetime:         30 * time.Minute,
		PoolTimeout:             5 * time.Minute,
		ConnMaxIdleTime:         5 * time.Minute,
		ClaimMinIdleTime:        1 * time.Minute,
		ClaimBatchSize:          10000,
		PendingCheckInterval:    30 * time.Second,
		ClaimInterval:           30 * time.Second,
		BatchSize:               100,
		BlockTime:               5 * time.Second,
		AggressiveClaim:         true,
		ClaimCycleDelay:         1 * time.Second,
		DrainEnabled:            true,
		DrainInterval:           1 * time.Minute,
		DrainBatchSize:          10000,
		ConsumerCleanupEnabled:  true,
		ConsumerIdleTimeout:     5 * time.Minute,
		ConsumerCleanupInterval: 1 * time.Minute,
	}
}

func defaultMQTT(hostname string) MQTTConfig {
	return MQTTConfig{
		Brokers:           []string{"tcp://localhost:1883"},
		ClientID:          fmt.Sprintf("syslog-consumer-%s-%d", hostname, os.Getpid()),
		QoS:               2,
		KeepAlive:         30 * time.Second,
		ConnectTimeout:    10 * time.Second,
		MaxReconnectDelay: 2 * time.Minute,
		CleanSession:      true,
		OrderMatters:      true,
		TLS: TLSConfig{
			Enabled:             false,
			CACertFile:          "",
			ClientCertFile:      "",
			ClientKeyFile:       "",
			InsecureSkipVerify:  false,
			ServerName:          "",
			MinVersion:          "TLS1.2",
			CipherSuites:        []string{},
			PreferServerCiphers: false,
		},
		Topics: TopicConfig{
			PublishTopic:   "syslog",
			SubscribeTopic: "syslog/acknowledgement",
			UseUserPrefix:  true,
			CustomPrefix:   "",
		},
		PublisherPoolSize:   1,
		MaxInflight:         1000,
		MessageChannelDepth: 0,
		WriteTimeout:        5 * time.Second,
	}
}

func defaultResource() ResourceConfig {
	return ResourceConfig{
		CPUThresholdHigh:        80.0,
		CPUThresholdLow:         30.0,
		MemoryThresholdHigh:     80.0,
		MemoryThresholdLow:      30.0,
		CheckInterval:           5 * time.Second,
		ScaleUpCooldown:         30 * time.Second,
		ScaleDownCooldown:       2 * time.Minute,
		MinWorkers:              1,
		MaxWorkers:              runtime.NumCPU() * 4,
		WorkerStepSize:          2,
		EnablePredictiveScaling: true,
		HistoryWindowSize:       100,
		PredictionHorizon:       30 * time.Second,
	}
}

func defaultMetrics() MetricsConfig {
	return MetricsConfig{
		Enabled:          true,
		PrometheusPort:   9090,
		CollectInterval:  10 * time.Second,
		Namespace:        "syslog_consumer",
		Subsystem:        "",
		HistogramBuckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}
}

func defaultHealth() HealthConfig {
	return HealthConfig{
		Enabled:       true,
		Port:          8080,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		CheckInterval: 10 * time.Second,
		RedisTimeout:  2 * time.Second,
		MQTTTimeout:   2 * time.Second,
	}
}

func defaultPipeline() PipelineConfig {
	return PipelineConfig{
		BufferSize:               nextPowerOf2(1048576), // 1M messages
		BatchSize:                1000,
		BatchTimeout:             100 * time.Millisecond,
		ProcessingTimeout:        5 * time.Second,
		UseZeroCopy:              true,
		PreallocateBuffers:       true,
		NumaAware:                false,
		CPUAffinity:              []int{},
		BackpressureThreshold:    0.8,
		DropPolicy:               "oldest",
		FlushInterval:            100 * time.Millisecond,
		BackpressurePollInterval: 1 * time.Second,
		IdlePollSleep:            1 * time.Millisecond,
		Retry: RetryConfig{
			Enabled:        true,
			MaxAttempts:    5,
			InitialBackoff: 1 * time.Second,
			MaxBackoff:     30 * time.Second,
			Multiplier:     2.0,
		},
		DLQ: DLQConfig{
			Enabled: true,
			Topic:   "syslog/dlq",
		},
	}
}

func defaultCircuitBreaker() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		Enabled:                true,
		ErrorThreshold:         50.0,
		SuccessThreshold:       5,
		Timeout:                30 * time.Second,
		MaxConcurrentCalls:     100,
		RequestVolumeThreshold: 20,
	}
}
