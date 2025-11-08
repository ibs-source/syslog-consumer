package config

import (
	"testing"
	"time"
)

func TestConfig_Structure(t *testing.T) {
	cfg := &Config{
		Redis: RedisConfig{
			Address:   "localhost:6379",
			Stream:    "test-stream",
			Consumer:  "consumer-1",
			BatchSize: 100,
		},
		MQTT: MQTTConfig{
			Broker:       "tcp://localhost:1883",
			ClientID:     "test-client",
			PublishTopic: "test/publish",
			AckTopic:     "test/ack",
			PoolSize:     5,
		},
		Pipeline: PipelineConfig{
			BufferCapacity: 1000,
			PublishWorkers: 10,
		},
	}

	if cfg.Redis.Address != "localhost:6379" {
		t.Errorf("Redis.Address = %s; want localhost:6379", cfg.Redis.Address)
	}
	if cfg.MQTT.Broker != "tcp://localhost:1883" {
		t.Errorf("MQTT.Broker = %s; want tcp://localhost:1883", cfg.MQTT.Broker)
	}
	if cfg.Pipeline.BufferCapacity != 1000 {
		t.Errorf("Pipeline.BufferCapacity = %d; want 1000", cfg.Pipeline.BufferCapacity)
	}
}

func TestRedisConfig_Fields(t *testing.T) {
	rc := RedisConfig{
		Address:             "redis:6379",
		Stream:              "stream",
		Consumer:            "consumer",
		BatchSize:           50,
		BlockTimeout:        5 * time.Second,
		ClaimIdle:           30 * time.Second,
		ConsumerIdleTimeout: 5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		DialTimeout:         10 * time.Second,
		ReadTimeout:         10 * time.Second,
		WriteTimeout:        5 * time.Second,
		PingTimeout:         5 * time.Second,
	}

	if rc.Address != "redis:6379" {
		t.Errorf("Address = %s; want redis:6379", rc.Address)
	}
	if rc.BatchSize != 50 {
		t.Errorf("BatchSize = %d; want 50", rc.BatchSize)
	}
	if rc.BlockTimeout != 5*time.Second {
		t.Errorf("BlockTimeout = %v; want 5s", rc.BlockTimeout)
	}
}

func TestMQTTConfig_Fields(t *testing.T) {
	mc := MQTTConfig{
		Broker:               "tcp://mqtt:1883",
		ClientID:             "client",
		PublishTopic:         "pub/topic",
		AckTopic:             "ack/topic",
		QoS:                  1,
		ConnectTimeout:       10 * time.Second,
		WriteTimeout:         30 * time.Second,
		PoolSize:             5,
		MaxReconnectInterval: 10 * time.Second,
		SubscribeTimeout:     10 * time.Second,
		DisconnectTimeout:    1000,
		TLSEnabled:           true,
		CACert:               "/path/to/ca.crt",
		ClientCert:           "/path/to/client.crt",
		ClientKey:            "/path/to/client.key",
		InsecureSkip:         false,
		UseCertCNPrefix:      true,
	}

	if mc.Broker != "tcp://mqtt:1883" {
		t.Errorf("Broker = %s; want tcp://mqtt:1883", mc.Broker)
	}
	if mc.QoS != 1 {
		t.Errorf("QoS = %d; want 1", mc.QoS)
	}
	if mc.PoolSize != 5 {
		t.Errorf("PoolSize = %d; want 5", mc.PoolSize)
	}
	if !mc.TLSEnabled {
		t.Error("TLSEnabled = false; want true")
	}
	if !mc.UseCertCNPrefix {
		t.Error("UseCertCNPrefix = false; want true")
	}
}

func TestPipelineConfig_Fields(t *testing.T) {
	pc := PipelineConfig{
		BufferCapacity:  1000,
		ShutdownTimeout: 30 * time.Second,
		ErrorBackoff:    1 * time.Second,
		AckTimeout:      5 * time.Second,
		PublishWorkers:  20,
	}

	if pc.BufferCapacity != 1000 {
		t.Errorf("BufferCapacity = %d; want 1000", pc.BufferCapacity)
	}
	if pc.PublishWorkers != 20 {
		t.Errorf("PublishWorkers = %d; want 20", pc.PublishWorkers)
	}
	if pc.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v; want 30s", pc.ShutdownTimeout)
	}
}
