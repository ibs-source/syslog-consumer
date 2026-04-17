package config

import (
	"testing"
	"time"
)

func TestConfigStructs_ZeroValue(t *testing.T) {
	var cfg Config
	if cfg.Log.Level != "" {
		t.Errorf("expected empty LogLevel, got %q", cfg.Log.Level)
	}
	if cfg.Redis.Address != "" {
		t.Error("expected empty Redis.Address")
	}
	if cfg.MQTT.Broker != "" {
		t.Error("expected empty MQTT.Broker")
	}
	if cfg.Pipeline.HealthAddr != "" {
		t.Error("expected empty Pipeline.HealthAddr")
	}
}

func TestRedisConfig_Fields(t *testing.T) {
	got := RedisConfig{
		Address:             "localhost:6379",
		Stream:              "syslog-stream",
		Consumer:            "c1",
		GroupName:           "grp",
		BatchSize:           100,
		DiscoveryScanCount:  500,
		BlockTimeout:        2 * time.Second,
		ClaimIdle:           30 * time.Second,
		ConsumerIdleTimeout: 5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		DialTimeout:         5 * time.Second,
		ReadTimeout:         3 * time.Second,
		WriteTimeout:        3 * time.Second,
		PingTimeout:         2 * time.Second,
		PoolSize:            10,
		MinIdleConns:        2,
	}
	want := RedisConfig{
		Address:             "localhost:6379",
		Stream:              "syslog-stream",
		Consumer:            "c1",
		GroupName:           "grp",
		BatchSize:           100,
		DiscoveryScanCount:  500,
		BlockTimeout:        2 * time.Second,
		ClaimIdle:           30 * time.Second,
		ConsumerIdleTimeout: 5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		DialTimeout:         5 * time.Second,
		ReadTimeout:         3 * time.Second,
		WriteTimeout:        3 * time.Second,
		PingTimeout:         2 * time.Second,
		PoolSize:            10,
		MinIdleConns:        2,
	}
	if got != want {
		t.Errorf("RedisConfig mismatch\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestMQTTConfig_Fields(t *testing.T) {
	got := MQTTConfig{
		Broker:               "ssl://broker:8883",
		ClientID:             "client-1",
		PublishTopic:         "pub/topic",
		AckTopic:             "ack/topic",
		CACert:               "/path/ca.pem",
		ClientCert:           "/path/cert.pem",
		ClientKey:            "/path/key.pem",
		ConnectTimeout:       10 * time.Second,
		WriteTimeout:         5 * time.Second,
		MaxReconnectInterval: 30 * time.Second,
		SubscribeTimeout:     10 * time.Second,
		DisconnectTimeout:    5 * time.Second,
		KeepAlive:            60 * time.Second,
		PingTimeout:          10 * time.Second,
		ConnectRetryDelay:    2 * time.Second,
		PoolSize:             3,
		MessageChannelDepth:  100,
		MaxResumePubInFlight: 10,
		QoS:                  1,
		TLSEnabled:           true,
		InsecureSkip:         false,
		UseCertCNPrefix:      true,
	}
	want := MQTTConfig{
		Broker:               "ssl://broker:8883",
		ClientID:             "client-1",
		PublishTopic:         "pub/topic",
		AckTopic:             "ack/topic",
		CACert:               "/path/ca.pem",
		ClientCert:           "/path/cert.pem",
		ClientKey:            "/path/key.pem",
		ConnectTimeout:       10 * time.Second,
		WriteTimeout:         5 * time.Second,
		MaxReconnectInterval: 30 * time.Second,
		SubscribeTimeout:     10 * time.Second,
		DisconnectTimeout:    5 * time.Second,
		KeepAlive:            60 * time.Second,
		PingTimeout:          10 * time.Second,
		ConnectRetryDelay:    2 * time.Second,
		PoolSize:             3,
		MessageChannelDepth:  100,
		MaxResumePubInFlight: 10,
		QoS:                  1,
		TLSEnabled:           true,
		InsecureSkip:         false,
		UseCertCNPrefix:      true,
	}
	if got != want {
		t.Errorf("MQTTConfig mismatch\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestPipelineConfig_Fields(t *testing.T) {
	got := PipelineConfig{
		HealthAddr:              ":9980",
		HealthPingTimeout:       2 * time.Second,
		HealthReadHeaderTimeout: 5 * time.Second,
		ShutdownTimeout:         30 * time.Second,
		ErrorBackoff:            1 * time.Second,
		AckTimeout:              5 * time.Second,
		RefreshInterval:         1 * time.Minute,
		AckFlushInterval:        500 * time.Millisecond,
		BufferCapacity:          1000,
		MessageQueueCapacity:    8,
		PublishWorkers:          4,
		AckWorkers:              2,
		AckBatchSize:            50,
	}
	want := PipelineConfig{
		HealthAddr:              ":9980",
		HealthPingTimeout:       2 * time.Second,
		HealthReadHeaderTimeout: 5 * time.Second,
		ShutdownTimeout:         30 * time.Second,
		ErrorBackoff:            1 * time.Second,
		AckTimeout:              5 * time.Second,
		RefreshInterval:         1 * time.Minute,
		AckFlushInterval:        500 * time.Millisecond,
		BufferCapacity:          1000,
		MessageQueueCapacity:    8,
		PublishWorkers:          4,
		AckWorkers:              2,
		AckBatchSize:            50,
	}
	if got != want {
		t.Errorf("PipelineConfig mismatch\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestLogConfig_Fields(t *testing.T) {
	lc := LogConfig{Level: "debug"}
	if lc.Level != "debug" {
		t.Errorf("Level = %q", lc.Level)
	}
}

func TestConfig_Composite(t *testing.T) {
	cfg := Config{
		Log:  LogConfig{Level: "info"},
		MQTT: MQTTConfig{Broker: "tcp://localhost:1883"},
		Redis: RedisConfig{
			Address: "localhost:6379",
			Stream:  "stream",
		},
		Pipeline: PipelineConfig{
			PublishWorkers: 2,
			AckWorkers:     1,
		},
	}

	if cfg.Log.Level != "info" {
		t.Errorf("Log.Level = %q", cfg.Log.Level)
	}
	if cfg.MQTT.Broker != "tcp://localhost:1883" {
		t.Errorf("MQTT.Broker = %q", cfg.MQTT.Broker)
	}
	if cfg.Redis.Address != "localhost:6379" {
		t.Errorf("Redis.Address = %q", cfg.Redis.Address)
	}
	if cfg.Pipeline.PublishWorkers != 2 {
		t.Errorf("Pipeline.PublishWorkers = %d", cfg.Pipeline.PublishWorkers)
	}
}
