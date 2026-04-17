package config

import (
	"flag"
	"os"
	"testing"
	"time"
)

func TestApplyRedisFlags(t *testing.T) {
	// Save original command line args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Set command line args
	os.Args = []string{
		"test",
		"-redis-address=flag-redis:6379",
		"-redis-stream=flag-stream",
		"-redis-consumer=flag-consumer",
		"-redis-batch-size=200",
		"-redis-block-timeout=8s",
	}

	// Reset flags and parse
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	// Start with defaults
	cfg := defaultRedisConfig()

	// Apply flags
	applyRedisFlags(&cfg)

	// Verify
	if cfg.Address != "flag-redis:6379" {
		t.Errorf("Address = %s; want flag-redis:6379", cfg.Address)
	}
	if cfg.Stream != "flag-stream" {
		t.Errorf("Stream = %s; want flag-stream", cfg.Stream)
	}
	if cfg.Consumer != "flag-consumer" {
		t.Errorf("Consumer = %s; want flag-consumer", cfg.Consumer)
	}
	if cfg.BatchSize != 200 {
		t.Errorf("BatchSize = %d; want 200", cfg.BatchSize)
	}
	if cfg.BlockTimeout != 8*time.Second {
		t.Errorf("BlockTimeout = %v; want 8s", cfg.BlockTimeout)
	}
}

func TestApplyMQTTFlags(t *testing.T) {
	// Save original command line args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Set command line args
	os.Args = []string{
		"test",
		"-mqtt-broker=tcp://flag-mqtt:1883",
		"-mqtt-client-id=flag-client",
		"-mqtt-qos=2",
		"-mqtt-pool-size=15",
		"-mqtt-tls-enabled=true",
	}

	// Reset flags and parse
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	// Start with defaults
	cfg := defaultMQTTConfig()

	// Apply flags
	applyMQTTFlags(&cfg)

	// Verify
	if cfg.Broker != "tcp://flag-mqtt:1883" {
		t.Errorf("Broker = %s; want tcp://flag-mqtt:1883", cfg.Broker)
	}
	if cfg.ClientID != "flag-client" {
		t.Errorf("ClientID = %s; want flag-client", cfg.ClientID)
	}
	if cfg.QoS != 2 {
		t.Errorf("QoS = %d; want 2", cfg.QoS)
	}
	if cfg.PoolSize != 15 {
		t.Errorf("PoolSize = %d; want 15", cfg.PoolSize)
	}
	if !cfg.TLSEnabled {
		t.Error("TLSEnabled = false; want true")
	}
}

func TestApplyPipelineFlags(t *testing.T) {
	// Save original command line args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Set command line args
	os.Args = []string{
		"test",
		"-pipeline-buffer-capacity=2000",
		"-pipeline-shutdown-timeout=45s",
		"-pipeline-publish-workers=15",
	}

	// Reset flags and parse
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	// Start with defaults
	cfg := defaultPipelineConfig()

	// Apply flags
	applyPipelineFlags(&cfg)

	// Verify
	if cfg.BufferCapacity != 2000 {
		t.Errorf("BufferCapacity = %d; want 2000", cfg.BufferCapacity)
	}
	if cfg.ShutdownTimeout != 45*time.Second {
		t.Errorf("ShutdownTimeout = %v; want 45s", cfg.ShutdownTimeout)
	}
	if cfg.PublishWorkers != 15 {
		t.Errorf("PublishWorkers = %d; want 15", cfg.PublishWorkers)
	}
}

func TestIsFlagSet(t *testing.T) {
	// Save original command line args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Set command line args with explicit flag
	os.Args = []string{
		"test",
		"-mqtt-tls-enabled=true",
	}

	// Reset flags and parse
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	// Check if flag was set
	if !isFlagSet("mqtt-tls-enabled") {
		t.Error("isFlagSet(mqtt-tls-enabled) = false; want true")
	}

	// Check if another flag was not set
	if isFlagSet("mqtt-tls-insecure-skip") {
		t.Error("isFlagSet(mqtt-tls-insecure-skip) = true; want false")
	}
}

// TestApplyAllRedisFlags sets ALL Redis timeout flags for full branch coverage.
func TestApplyAllRedisFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{
		"test",
		"-redis-claim-idle=1m",
		"-redis-consumer-idle-timeout=10m",
		"-redis-cleanup-interval=2m",
		"-redis-dial-timeout=3s",
		"-redis-read-timeout=4s",
		"-redis-write-timeout=5s",
		"-redis-ping-timeout=2s",
	}

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	cfg := defaultRedisConfig()
	applyRedisFlags(&cfg)

	if cfg.ClaimIdle != 1*time.Minute {
		t.Errorf("ClaimIdle = %v; want 1m", cfg.ClaimIdle)
	}
	if cfg.ConsumerIdleTimeout != 10*time.Minute {
		t.Errorf("ConsumerIdleTimeout = %v; want 10m", cfg.ConsumerIdleTimeout)
	}
	if cfg.CleanupInterval != 2*time.Minute {
		t.Errorf("CleanupInterval = %v; want 2m", cfg.CleanupInterval)
	}
	if cfg.DialTimeout != 3*time.Second {
		t.Errorf("DialTimeout = %v; want 3s", cfg.DialTimeout)
	}
	if cfg.ReadTimeout != 4*time.Second {
		t.Errorf("ReadTimeout = %v; want 4s", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 5*time.Second {
		t.Errorf("WriteTimeout = %v; want 5s", cfg.WriteTimeout)
	}
	if cfg.PingTimeout != 2*time.Second {
		t.Errorf("PingTimeout = %v; want 2s", cfg.PingTimeout)
	}
}

// TestApplyAllMQTTFlags sets ALL remaining MQTT flags for full branch coverage.
func TestApplyAllMQTTFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{
		"test",
		"-mqtt-publish-topic=custom/pub",
		"-mqtt-ack-topic=custom/ack",
		"-mqtt-connect-timeout=15s",
		"-mqtt-write-timeout=8s",
		"-mqtt-max-reconnect-interval=10s",
		"-mqtt-subscribe-timeout=12s",
		"-mqtt-disconnect-timeout=2s",
		"-mqtt-ca-cert=/path/ca.pem",
		"-mqtt-client-cert=/path/cert.pem",
		"-mqtt-client-key=/path/key.pem",
		"-mqtt-tls-insecure-skip=true",
		"-mqtt-use-cert-cn-prefix=true",
	}

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	cfg := defaultMQTTConfig()
	applyMQTTFlags(&cfg)

	assertMQTTTopics(t, &cfg)
	assertMQTTTimeouts(t, &cfg)
	assertMQTTTLS(t, &cfg)
}

func assertMQTTTopics(t *testing.T, cfg *MQTTConfig) {
	t.Helper()
	if cfg.PublishTopic != "custom/pub" {
		t.Errorf("PublishTopic = %s; want custom/pub", cfg.PublishTopic)
	}
	if cfg.AckTopic != "custom/ack" {
		t.Errorf("AckTopic = %s; want custom/ack", cfg.AckTopic)
	}
}

func assertMQTTTimeouts(t *testing.T, cfg *MQTTConfig) {
	t.Helper()
	if cfg.ConnectTimeout != 15*time.Second {
		t.Errorf("ConnectTimeout = %v; want 15s", cfg.ConnectTimeout)
	}
	if cfg.WriteTimeout != 8*time.Second {
		t.Errorf("WriteTimeout = %v; want 8s", cfg.WriteTimeout)
	}
	if cfg.MaxReconnectInterval != 10*time.Second {
		t.Errorf("MaxReconnectInterval = %v; want 10s", cfg.MaxReconnectInterval)
	}
	if cfg.SubscribeTimeout != 12*time.Second {
		t.Errorf("SubscribeTimeout = %v; want 12s", cfg.SubscribeTimeout)
	}
	if cfg.DisconnectTimeout != 2*time.Second {
		t.Errorf("DisconnectTimeout = %v; want 2s", cfg.DisconnectTimeout)
	}
}

func assertMQTTTLS(t *testing.T, cfg *MQTTConfig) {
	t.Helper()
	if cfg.CACert != "/path/ca.pem" {
		t.Errorf("CACert = %s; want /path/ca.pem", cfg.CACert)
	}
	if cfg.ClientCert != "/path/cert.pem" {
		t.Errorf("ClientCert = %s; want /path/cert.pem", cfg.ClientCert)
	}
	if cfg.ClientKey != "/path/key.pem" {
		t.Errorf("ClientKey = %s; want /path/key.pem", cfg.ClientKey)
	}
	if !cfg.InsecureSkip {
		t.Error("InsecureSkip = false; want true")
	}
	if !cfg.UseCertCNPrefix {
		t.Error("UseCertCNPrefix = false; want true")
	}
}

// TestApplyAllPipelineFlags sets ALL remaining pipeline flags.
func TestApplyAllPipelineFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{
		"test",
		"-pipeline-error-backoff=200ms",
		"-pipeline-ack-timeout=10s",
		"-pipeline-refresh-interval=5m",
	}

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	cfg := defaultPipelineConfig()
	applyPipelineFlags(&cfg)

	if cfg.ErrorBackoff != 200*time.Millisecond {
		t.Errorf("ErrorBackoff = %v; want 200ms", cfg.ErrorBackoff)
	}
	if cfg.AckTimeout != 10*time.Second {
		t.Errorf("AckTimeout = %v; want 10s", cfg.AckTimeout)
	}
	if cfg.RefreshInterval != 5*time.Minute {
		t.Errorf("RefreshInterval = %v; want 5m", cfg.RefreshInterval)
	}
}

func TestApplyCompressFlags(t *testing.T) {
	cfg := defaultCompressConfig()

	// No flags set → defaults should remain.
	applyCompressFlags(&cfg)

	if cfg.FreelistSize != 128 {
		t.Errorf("FreelistSize = %d; want 128", cfg.FreelistSize)
	}
	if cfg.MaxDecompressBytes != 256*1024*1024 {
		t.Errorf("MaxDecompressBytes = %d; want %d", cfg.MaxDecompressBytes, 256*1024*1024)
	}
	if cfg.WarmupCount != 4 {
		t.Errorf("WarmupCount = %d; want 4", cfg.WarmupCount)
	}
}

func TestApplyAllCompressFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{
		"test",
		"-compress-freelist-size=64",
		"-max-decompress-bytes=134217728",
		"-compress-warmup-count=8",
	}

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	resetFlags()
	flag.Parse()

	cfg := defaultCompressConfig()
	applyCompressFlags(&cfg)

	if cfg.FreelistSize != 64 {
		t.Errorf("FreelistSize = %d; want 64", cfg.FreelistSize)
	}
	if cfg.MaxDecompressBytes != 128*1024*1024 {
		t.Errorf("MaxDecompressBytes = %d; want %d", cfg.MaxDecompressBytes, 128*1024*1024)
	}
	if cfg.WarmupCount != 8 {
		t.Errorf("WarmupCount = %d; want 8", cfg.WarmupCount)
	}
}

// resetFlags re-initializes all flag variables for testing
func resetFlags() {
	// Redis flags
	flagRedisAddress = flag.String("redis-address", "", "Redis address")
	flagRedisStream = flag.String("redis-stream", "", "Redis stream name (empty for multi-stream mode)")
	flagRedisConsumer = flag.String("redis-consumer", "", "Redis consumer name")
	flagRedisBatchSize = flag.Int("redis-batch-size", 0, "Redis batch size")
	flagRedisBlockTimeout = flag.Duration("redis-block-timeout", 0, "Redis block timeout")
	flagRedisClaimIdle = flag.Duration("redis-claim-idle", 0, "Redis claim idle time")
	flagRedisConsumerIdle = flag.Duration("redis-consumer-idle-timeout", 0, "Redis consumer idle timeout")
	flagRedisCleanupInterval = flag.Duration("redis-cleanup-interval", 0, "Redis cleanup interval")
	flagRedisDialTimeout = flag.Duration("redis-dial-timeout", 0, "Redis dial timeout")
	flagRedisReadTimeout = flag.Duration("redis-read-timeout", 0, "Redis read timeout")
	flagRedisWriteTimeout = flag.Duration("redis-write-timeout", 0, "Redis write timeout")
	flagRedisPingTimeout = flag.Duration("redis-ping-timeout", 0, "Redis ping timeout")

	// MQTT flags
	flagMQTTBroker = flag.String("mqtt-broker", "", "MQTT broker URL")
	flagMQTTClientID = flag.String("mqtt-client-id", "", "MQTT client ID")
	flagMQTTPublishTopic = flag.String("mqtt-publish-topic", "", "MQTT publish topic")
	flagMQTTAckTopic = flag.String("mqtt-ack-topic", "", "MQTT ACK topic")
	flagMQTTQoS = flag.Int("mqtt-qos", -1, "MQTT QoS (0, 1, or 2)")
	flagMQTTConnectTimeout = flag.Duration("mqtt-connect-timeout", 0, "MQTT connect timeout")
	flagMQTTWriteTimeout = flag.Duration("mqtt-write-timeout", 0, "MQTT write timeout")
	flagMQTTPoolSize = flag.Int("mqtt-pool-size", 0, "MQTT connection pool size")
	flagMQTTMaxReconnect = flag.Duration("mqtt-max-reconnect-interval", 0, "MQTT max reconnect interval")
	flagMQTTSubscribeTimeout = flag.Duration("mqtt-subscribe-timeout", 0, "MQTT subscribe timeout")
	flagMQTTDisconnectTimeout = flag.Duration("mqtt-disconnect-timeout", 0, "MQTT disconnect timeout")
	flagMQTTTLSEnabled = flag.Bool("mqtt-tls-enabled", false, "Enable MQTT TLS")
	flagMQTTCACert = flag.String("mqtt-ca-cert", "", "MQTT CA certificate path")
	flagMQTTClientCert = flag.String("mqtt-client-cert", "", "MQTT client certificate path")
	flagMQTTClientKey = flag.String("mqtt-client-key", "", "MQTT client key path")
	flagMQTTTLSInsecureSkip = flag.Bool("mqtt-tls-insecure-skip", false, "Skip MQTT TLS verification")
	flagMQTTUseCertCNPrefix = flag.Bool("mqtt-use-cert-cn-prefix", false, "Prefix topics with client cert CN")

	// Pipeline flags
	flagPipelineBufferCapacity = flag.Int("pipeline-buffer-capacity", 0, "Pipeline buffer capacity")
	flagPipelineShutdownTimeout = flag.Duration("pipeline-shutdown-timeout", 0, "Pipeline shutdown timeout")
	flagPipelineErrorBackoff = flag.Duration("pipeline-error-backoff", 0, "Pipeline error backoff")
	flagPipelineAckTimeout = flag.Duration("pipeline-ack-timeout", 0, "Pipeline ACK timeout")
	flagPipelinePublishWorkers = flag.Int("pipeline-publish-workers", 0, "Number of concurrent publish workers")
	flagPipelineRefreshInterval = flag.Duration("pipeline-refresh-interval", 0, "Pipeline stream refresh interval")

	// Compress flags
	flagCompressFreelistSize = flag.Int("compress-freelist-size", 0, "Decoder freelist channel capacity")
	flagCompressMaxDecompressBytes = flag.Int("max-decompress-bytes", 0, "Max decompressed payload size in bytes")
	flagCompressWarmupCount = flag.Int("compress-warmup-count", 0, "Decoders pre-created at init")
}
