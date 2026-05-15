package config

import (
	"testing"
	"time"
)

func TestDefaultRedisConfig(t *testing.T) {
	cfg := defaultRedisConfig()

	tests := []struct {
		got  any
		want any
		name string
	}{
		{cfg.Address, defaultRedisAddress, "Address"},
		{cfg.Stream, defaultStreamName, "Stream"},
		{cfg.Consumer, defaultRedisConsumer, "Consumer"},
		{cfg.BatchSize, 20000, "BatchSize"},
		{cfg.BlockTimeout, 1 * time.Second, "BlockTimeout"},
		{cfg.ClaimIdle, 10 * time.Second, "ClaimIdle"},
		{cfg.ConsumerIdleTimeout, 5 * time.Minute, "ConsumerIdleTimeout"},
		{cfg.CleanupInterval, 1 * time.Minute, "CleanupInterval"},
		{cfg.DialTimeout, 5 * time.Second, "DialTimeout"},
		{cfg.ReadTimeout, 3 * time.Second, "ReadTimeout"},
		{cfg.WriteTimeout, 3 * time.Second, tcWriteTimeout},
		{cfg.PingTimeout, 3 * time.Second, "PingTimeout"},
		{cfg.ConnMaxIdleTime, 5 * time.Minute, "ConnMaxIdleTime"},
		{cfg.ConnMaxLifetime, time.Duration(0), "ConnMaxLifetime"},
		{cfg.PoolSize, 50, "PoolSize"},
		{cfg.MinIdleConns, 10, "MinIdleConns"},
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
		got  any
		want any
		name string
	}{
		{cfg.Broker, defaultMQTTBroker, "Broker"},
		{cfg.ClientID, defaultMQTTClientID, "ClientID"},
		{cfg.PublishTopic, defaultMQTTPublishTopic, "PublishTopic"},
		{cfg.AckTopic, defaultMQTTAckTopic, "AckTopic"},
		{cfg.QoS, byte(0), "QoS"},
		{cfg.ConnectTimeout, 10 * time.Second, "ConnectTimeout"},
		{cfg.WriteTimeout, 5 * time.Second, tcWriteTimeout},
		{cfg.PoolSize, 25, "PoolSize"},
		{cfg.MaxReconnectInterval, 5 * time.Second, "MaxReconnectInterval"},
		{cfg.SubscribeTimeout, 10 * time.Second, "SubscribeTimeout"},
		{cfg.DisconnectTimeout, 1000 * time.Millisecond, "DisconnectTimeout"},
		{cfg.TLSEnabled, false, "TLSEnabled"},
		{cfg.CACert, "", "CACert"},
		{cfg.ClientCert, "", "ClientCert"},
		{cfg.ClientKey, "", "ClientKey"},
		{cfg.InsecureSkip, false, "InsecureSkip"},
		{cfg.UseCertCNPrefix, false, "UseCertCNPrefix"},
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
		got  any
		want any
		name string
	}{
		{cfg.BufferCapacity, 10000, "BufferCapacity"},
		{cfg.ShutdownTimeout, 10 * time.Second, "ShutdownTimeout"},
		{cfg.ErrorBackoff, 50 * time.Millisecond, "ErrorBackoff"},
		{cfg.AckTimeout, 5 * time.Second, "AckTimeout"},
		{cfg.PublishWorkers, 25, "PublishWorkers"},
		{cfg.RefreshInterval, 1 * time.Minute, "RefreshInterval"},
		{cfg.HealthAddr, defaultHealthAddr, "HealthAddr"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("defaultPipelineConfig().%s = %v; want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestDefaultCompressConfig(t *testing.T) {
	cfg := defaultCompressConfig()

	tests := []struct {
		got  any
		want any
		name string
	}{
		{cfg.FreelistSize, 128, "FreelistSize"},
		{cfg.MaxDecompressBytes, 256 * 1024 * 1024, "MaxDecompressBytes"},
		{cfg.WarmupCount, 4, "WarmupCount"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("defaultCompressConfig().%s = %v; want %v", tt.name, tt.got, tt.want)
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
	if cfg.Redis.Address != defaultRedisAddress {
		t.Errorf("defaultConfig().Redis.Address = %s; want %s", cfg.Redis.Address, defaultRedisAddress)
	}
	if cfg.Redis.BatchSize != 20000 {
		t.Errorf("defaultConfig().Redis.BatchSize = %d; want 20000", cfg.Redis.BatchSize)
	}

	// Verify MQTT defaults
	if cfg.MQTT.Broker != defaultMQTTBroker {
		t.Errorf("defaultConfig().MQTT.Broker = %s; want %s", cfg.MQTT.Broker, defaultMQTTBroker)
	}
	if cfg.MQTT.PoolSize != 25 {
		t.Errorf("defaultConfig().MQTT.PoolSize = %d; want 25", cfg.MQTT.PoolSize)
	}

	// Verify Pipeline defaults
	if cfg.Pipeline.BufferCapacity != 10000 {
		t.Errorf("defaultConfig().Pipeline.BufferCapacity = %d; want 10000", cfg.Pipeline.BufferCapacity)
	}
	if cfg.Pipeline.PublishWorkers != 25 {
		t.Errorf("defaultConfig().Pipeline.PublishWorkers = %d; want 25", cfg.Pipeline.PublishWorkers)
	}

	// Compress defaults covered by TestDefaultCompressConfig.
}
