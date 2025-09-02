package config

import (
	"os"
	"testing"
)

const nopePath = "nope"

func TestGetDefaultsAndValidate_Succeeds(t *testing.T) {
	cfg := GetDefaults()
	if cfg == nil {
		t.Fatal("GetDefaults returned nil")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults should validate, got error: %v", err)
	}
}

func TestValidate_AppErrors(t *testing.T) {
	cfg := GetDefaults()
	cfg.App.Name = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty app name")
	}

	cfg = GetDefaults()
	cfg.App.LogLevel = "bad"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid log level")
	}

	cfg = GetDefaults()
	cfg.App.LogFormat = "badfmt"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid log format")
	}

	cfg = GetDefaults()
	cfg.App.ShutdownTimeout = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive shutdown timeout")
	}
}

func TestValidate_RedisErrors(t *testing.T) {
	cfg := GetDefaults()
	cfg.Redis.Addresses = []string{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty redis addresses")
	}

	cfg = GetDefaults()
	cfg.Redis.DB = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative redis db")
	}

	cfg = GetDefaults()
	cfg.Redis.StreamName = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty redis stream")
	}

	cfg = GetDefaults()
	cfg.Redis.ConsumerGroup = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty consumer group")
	}

	cfg = GetDefaults()
	cfg.Redis.MaxRetries = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative max retries")
	}

	cfg = GetDefaults()
	cfg.Redis.PoolSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive pool size")
	}

	cfg = GetDefaults()
	cfg.Redis.ClaimBatchSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive claim batch size")
	}
}

func TestValidate_MQTT_Errors(t *testing.T) {
	cfg := GetDefaults()
	cfg.MQTT.Brokers = []string{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty brokers")
	}

	cfg = GetDefaults()
	cfg.MQTT.ClientID = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty client id")
	}

	cfg = GetDefaults()
	cfg.MQTT.QoS = 3
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid qos")
	}

	cfg = GetDefaults()
	cfg.MQTT.PublisherPoolSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive publisher pool size")
	}

	cfg = GetDefaults()
	cfg.MQTT.MaxInflight = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive max inflight")
	}
}

func TestValidate_TLS_WhenEnabledRequiresFiles(t *testing.T) {
	cfg := GetDefaults()
	cfg.MQTT.TLS.Enabled = true
	cfg.MQTT.TLS.CACertFile = nopePath
	cfg.MQTT.TLS.ClientCertFile = nopePath
	cfg.MQTT.TLS.ClientKeyFile = nopePath

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-existing TLS files")
	}

	// Create temp files to satisfy existence checks
	ca, _ := os.CreateTemp(t.TempDir(), "ca.pem")
	cert, _ := os.CreateTemp(t.TempDir(), "cert.pem")
	key, _ := os.CreateTemp(t.TempDir(), "key.pem")
	cfg.MQTT.TLS.CACertFile = ca.Name()
	cfg.MQTT.TLS.ClientCertFile = cert.Name()
	cfg.MQTT.TLS.ClientKeyFile = key.Name()

	if err := cfg.Validate(); err != nil {
		// The files exist, other TLS semantics are not validated here.
		t.Fatalf("expected TLS validation to pass file checks, got: %v", err)
	}
}

func TestValidate_Resource_Errors(t *testing.T) {
	cfg := GetDefaults()
	cfg.Resource.CPUThresholdHigh = cfg.Resource.CPUThresholdLow
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for cpu high <= low")
	}

	cfg = GetDefaults()
	cfg.Resource.CPUThresholdLow = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for cpu thresholds out of range")
	}

	cfg = GetDefaults()
	cfg.Resource.MemoryThresholdHigh = cfg.Resource.MemoryThresholdLow
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for mem high <= low")
	}

	cfg = GetDefaults()
	cfg.Resource.MinWorkers = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for min workers <= 0")
	}

	cfg = GetDefaults()
	cfg.Resource.MaxWorkers = cfg.Resource.MinWorkers - 1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for max < min")
	}

	cfg = GetDefaults()
	cfg.Resource.WorkerStepSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive step size")
	}

	cfg = GetDefaults()
	cfg.Resource.HistoryWindowSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive history window")
	}
}

func TestValidate_MetricsAndHealth_Errors(t *testing.T) {
	cfg := GetDefaults()
	cfg.Metrics.Enabled = true
	cfg.Metrics.PrometheusPort = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid metrics port")
	}

	cfg = GetDefaults()
	cfg.Metrics.Enabled = true
	cfg.Metrics.Namespace = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty metrics namespace")
	}

	cfg = GetDefaults()
	cfg.Metrics.Enabled = true
	cfg.Metrics.HistogramBuckets = []float64{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty buckets")
	}

	cfg = GetDefaults()
	cfg.Health.Enabled = true
	cfg.Health.Port = 70000
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid health port")
	}

	cfg = GetDefaults()
	cfg.Health.Enabled = true
	cfg.Health.Port = cfg.Metrics.PrometheusPort
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for conflicting ports")
	}
}

func TestValidate_PipelineErrors(t *testing.T) {
	cfg := GetDefaults()
	cfg.Pipeline.BufferSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive buffer size")
	}

	cfg = GetDefaults()
	cfg.Pipeline.BufferSize = 3 // not power of 2
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-power-of-two buffer size")
	}

	cfg = GetDefaults()
	cfg.Pipeline.BatchSize = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive batch size")
	}

	cfg = GetDefaults()
	cfg.Pipeline.BatchSize = cfg.Pipeline.BufferSize + 1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for batch size > buffer size")
	}

	cfg = GetDefaults()
	cfg.Pipeline.BackpressureThreshold = 2
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid backpressure threshold")
	}

	cfg = GetDefaults()
	cfg.Pipeline.DropPolicy = "bad"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid drop policy")
	}
}

func TestValidate_CircuitBreakerErrors(t *testing.T) {
	cfg := GetDefaults()
	cfg.CircuitBreaker.Enabled = true
	cfg.CircuitBreaker.ErrorThreshold = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid cb error threshold")
	}

	cfg = GetDefaults()
	cfg.CircuitBreaker.Enabled = true
	cfg.CircuitBreaker.SuccessThreshold = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid cb success threshold")
	}

	cfg = GetDefaults()
	cfg.CircuitBreaker.Enabled = true
	cfg.CircuitBreaker.MaxConcurrentCalls = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid cb max concurrent")
	}

	cfg = GetDefaults()
	cfg.CircuitBreaker.Enabled = true
	cfg.CircuitBreaker.RequestVolumeThreshold = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid cb request volume")
	}
}

func TestNextPowerOf2(t *testing.T) {
	if got := nextPowerOf2(3); got != 4 {
		t.Fatalf("expected 4, got %d", got)
	}
	if got := nextPowerOf2(8); got != 8 {
		t.Fatalf("expected 8, got %d", got)
	}
}

func TestLoad_ValidateApplied(t *testing.T) {
	// ensure flags/env won't break basic load, using minimal env
	t.Setenv("APP_NAME", "syslog-consumer")
	t.Setenv("MQTT_BROKERS", "tcp://localhost:1883")
	t.Setenv("MQTT_CLIENT_ID", "cid")
	t.Setenv("REDIS_ADDRESSES", "localhost:6379")
	t.Setenv("REDIS_STREAM", "stream")
	t.Setenv("REDIS_CONSUMER_GROUP", "group")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.App.Name == "" || len(cfg.MQTT.Brokers) == 0 {
		t.Fatalf("unexpected config after Load: %+v", cfg)
	}
}
