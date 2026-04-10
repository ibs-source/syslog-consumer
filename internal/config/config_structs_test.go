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
	rc := RedisConfig{
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
	if rc.Address != "localhost:6379" {
		t.Errorf("Address = %q", rc.Address)
	}
	if rc.BatchSize != 100 {
		t.Errorf("BatchSize = %d", rc.BatchSize)
	}
	if rc.ClaimIdle != 30*time.Second {
		t.Errorf("ClaimIdle = %v", rc.ClaimIdle)
	}
	if rc.PoolSize != 10 {
		t.Errorf("PoolSize = %d", rc.PoolSize)
	}
	if rc.DiscoveryScanCount != 500 {
		t.Errorf("DiscoveryScanCount = %d", rc.DiscoveryScanCount)
	}
}

func TestMQTTConfig_Fields(t *testing.T) {
	mc := MQTTConfig{
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
	if mc.Broker != "ssl://broker:8883" {
		t.Errorf("Broker = %q", mc.Broker)
	}
	if mc.QoS != 1 {
		t.Errorf("QoS = %d", mc.QoS)
	}
	if !mc.TLSEnabled {
		t.Error("TLSEnabled should be true")
	}
	if mc.InsecureSkip {
		t.Error("InsecureSkip should be false")
	}
	if !mc.UseCertCNPrefix {
		t.Error("UseCertCNPrefix should be true")
	}
	if mc.PoolSize != 3 {
		t.Errorf("PoolSize = %d", mc.PoolSize)
	}
	if mc.MessageChannelDepth != 100 {
		t.Errorf("MessageChannelDepth = %d", mc.MessageChannelDepth)
	}
}

func TestPipelineConfig_Fields(t *testing.T) {
	pc := PipelineConfig{
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
	if pc.HealthAddr != ":9980" {
		t.Errorf("HealthAddr = %q", pc.HealthAddr)
	}
	if pc.PublishWorkers != 4 {
		t.Errorf("PublishWorkers = %d", pc.PublishWorkers)
	}
	if pc.AckWorkers != 2 {
		t.Errorf("AckWorkers = %d", pc.AckWorkers)
	}
	if pc.AckBatchSize != 50 {
		t.Errorf("AckBatchSize = %d", pc.AckBatchSize)
	}
	if pc.MessageQueueCapacity != 8 {
		t.Errorf("MessageQueueCapacity = %d", pc.MessageQueueCapacity)
	}
	if pc.BufferCapacity != 1000 {
		t.Errorf("BufferCapacity = %d", pc.BufferCapacity)
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
