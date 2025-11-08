package config

import (
	"testing"
	"time"
)

func TestDefaultRedisConfig(t *testing.T) {
	cfg := defaultRedisConfig()

	tests := []struct {
		name     string
		got      interface{}
		want     interface{}
		typeName string
	}{
		{"Address", cfg.Address, "localhost:6379", "string"},
		{"Stream", cfg.Stream, "syslog-stream", "string"},
		{"Consumer", cfg.Consumer, "consumer-1", "string"},
		{"BatchSize", cfg.BatchSize, 500, "int"},
		{"BlockTimeout", cfg.BlockTimeout, 5 * time.Second, "duration"},
		{"ClaimIdle", cfg.ClaimIdle, 30 * time.Second, "duration"},
		{"ConsumerIdleTimeout", cfg.ConsumerIdleTimeout, 5 * time.Minute, "duration"},
		{"CleanupInterval", cfg.CleanupInterval, 1 * time.Minute, "duration"},
		{"DialTimeout", cfg.DialTimeout, 10 * time.Second, "duration"},
		{"ReadTimeout", cfg.ReadTimeout, 10 * time.Second, "duration"},
		{"WriteTimeout", cfg.WriteTimeout, 5 * time.Second, "duration"},
		{"PingTimeout", cfg.PingTimeout, 5 * time.Second, "duration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("defaultRedisConfig().%s = %v; want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestDefaultMQTTConfig(t *testing.T) {
	cfg := defaultMQTTConfig()

	tests := []struct {
		name     string
		got      interface{}
		want     interface{}
		typeName string
	}{
		{"Broker", cfg.Broker, "tcp://localhost:1883", "string"},
		{"ClientID", cfg.ClientID, "syslog-consumer", "string"},
		{"PublishTopic", cfg.PublishTopic, "syslog/remote/messages", "string"},
		{"AckTopic", cfg.AckTopic, "syslog/remote/acknowledgement", "string"},
		{"QoS", cfg.QoS, byte(0), "byte"},
		{"ConnectTimeout", cfg.ConnectTimeout, 10 * time.Second, "duration"},
		{"WriteTimeout", cfg.WriteTimeout, 30 * time.Second, "duration"},
		{"PoolSize", cfg.PoolSize, 10, "int"},
		{"MaxReconnectInterval", cfg.MaxReconnectInterval, 10 * time.Second, "duration"},
		{"SubscribeTimeout", cfg.SubscribeTimeout, 10 * time.Second, "duration"},
		{"DisconnectTimeout", cfg.DisconnectTimeout, uint(1000), "uint"},
		{"TLSEnabled", cfg.TLSEnabled, false, "bool"},
		{"CACert", cfg.CACert, "", "string"},
		{"ClientCert", cfg.ClientCert, "", "string"},
		{"ClientKey", cfg.ClientKey, "", "string"},
		{"InsecureSkip", cfg.InsecureSkip, false, "bool"},
		{"UseCertCNPrefix", cfg.UseCertCNPrefix, false, "bool"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("defaultMQTTConfig().%s = %v; want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestDefaultPipelineConfig(t *testing.T) {
	cfg := defaultPipelineConfig()

	tests := []struct {
		name     string
		got      interface{}
		want     interface{}
		typeName string
	}{
		{"BufferCapacity", cfg.BufferCapacity, 10000, "int"},
		{"ShutdownTimeout", cfg.ShutdownTimeout, 30 * time.Second, "duration"},
		{"ErrorBackoff", cfg.ErrorBackoff, 1 * time.Second, "duration"},
		{"AckTimeout", cfg.AckTimeout, 5 * time.Second, "duration"},
		{"PublishWorkers", cfg.PublishWorkers, 20, "int"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("defaultPipelineConfig().%s = %v; want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()

	if cfg == nil {
		t.Fatal("defaultConfig() returned nil")
	}

	// Verify Redis defaults
	if cfg.Redis.Address != "localhost:6379" {
		t.Errorf("defaultConfig().Redis.Address = %s; want localhost:6379", cfg.Redis.Address)
	}
	if cfg.Redis.BatchSize != 500 {
		t.Errorf("defaultConfig().Redis.BatchSize = %d; want 500", cfg.Redis.BatchSize)
	}

	// Verify MQTT defaults
	if cfg.MQTT.Broker != "tcp://localhost:1883" {
		t.Errorf("defaultConfig().MQTT.Broker = %s; want tcp://localhost:1883", cfg.MQTT.Broker)
	}
	if cfg.MQTT.PoolSize != 10 {
		t.Errorf("defaultConfig().MQTT.PoolSize = %d; want 10", cfg.MQTT.PoolSize)
	}

	// Verify Pipeline defaults
	if cfg.Pipeline.BufferCapacity != 10000 {
		t.Errorf("defaultConfig().Pipeline.BufferCapacity = %d; want 10000", cfg.Pipeline.BufferCapacity)
	}
	if cfg.Pipeline.PublishWorkers != 20 {
		t.Errorf("defaultConfig().Pipeline.PublishWorkers = %d; want 20", cfg.Pipeline.PublishWorkers)
	}
}
