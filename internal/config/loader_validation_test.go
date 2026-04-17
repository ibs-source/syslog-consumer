package config

import (
	"strings"
	"testing"
)

type redisTestCase struct {
	name      string
	wantError string
	cfg       RedisConfig
}

type mqttTestCase struct {
	name      string
	wantError string
	cfg       MQTTConfig
}

type pipelineTestCase struct {
	name      string
	wantError string
	cfg       PipelineConfig
}

func TestValidate_Success(t *testing.T) {
	cfg := defaultConfig()
	if err := Validate(cfg); err != nil {
		t.Errorf("Validate() failed for valid config: %v", err)
	}
}

func TestValidate_RedisError(t *testing.T) {
	cfg := defaultConfig()
	cfg.Redis.Address = ""

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "redis address") {
		t.Errorf("Validate() error = %v; want redis address error", err)
	}
}

func TestValidate_MQTTError(t *testing.T) {
	cfg := defaultConfig()
	cfg.MQTT.Broker = ""

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "mqtt broker") {
		t.Errorf("Validate() error = %v; want mqtt broker error", err)
	}
}

func TestValidate_PipelineError(t *testing.T) {
	cfg := defaultConfig()
	cfg.Pipeline.BufferCapacity = 0

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "buffer capacity") {
		t.Errorf("Validate() error = %v; want buffer capacity error", err)
	}
}

func TestValidateRedis(t *testing.T) {
	for _, tt := range getRedisValidationTests() {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRedis(&tt.cfg)
			checkValidationError(t, err, tt.wantError)
		})
	}
}

func getRedisValidationTests() []redisTestCase {
	valid := defaultRedisConfig()

	emptyAddress := valid
	emptyAddress.Address = ""

	emptyConsumer := valid
	emptyConsumer.Consumer = ""

	zeroBatch := valid
	zeroBatch.BatchSize = 0

	negativeBatch := valid
	negativeBatch.BatchSize = -1

	zeroScanCount := valid
	zeroScanCount.DiscoveryScanCount = 0

	return []redisTestCase{
		{name: "valid config", cfg: valid, wantError: ""},
		{name: "empty address", cfg: emptyAddress, wantError: "redis address cannot be empty"},
		{name: "empty consumer", cfg: emptyConsumer, wantError: "redis consumer name cannot be empty"},
		{name: "zero batch size", cfg: zeroBatch, wantError: "redis batch size must be positive"},
		{name: "negative batch size", cfg: negativeBatch, wantError: "redis batch size must be positive"},
		{name: "zero discovery scan count", cfg: zeroScanCount, wantError: "redis discovery scan count must be positive"},
	}
}

func TestValidateMQTT(t *testing.T) {
	for _, tt := range getMQTTValidationTests() {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMQTT(&tt.cfg)
			checkValidationError(t, err, tt.wantError)
		})
	}
}

func getMQTTValidationTests() []mqttTestCase {
	valid := defaultMQTTConfig()
	valid.Broker = "tcp://localhost:1883"
	valid.ClientID = "test-client"
	valid.PublishTopic = "test/pub"
	valid.AckTopic = "test/ack"
	valid.PoolSize = 5

	emptyBroker := valid
	emptyBroker.Broker = ""

	emptyClientID := valid
	emptyClientID.ClientID = ""

	zeroPool := valid
	zeroPool.PoolSize = 0

	emptyPublish := valid
	emptyPublish.PublishTopic = ""

	emptyAck := valid
	emptyAck.AckTopic = ""

	return []mqttTestCase{
		{name: "valid config", cfg: valid, wantError: ""},
		{name: "empty broker", cfg: emptyBroker, wantError: "mqtt broker cannot be empty"},
		{name: "empty client ID", cfg: emptyClientID, wantError: "mqtt client ID cannot be empty"},
		{name: "zero pool size", cfg: zeroPool, wantError: "mqtt pool size must be positive"},
		{name: "empty publish topic", cfg: emptyPublish, wantError: "mqtt publish topic cannot be empty"},
		{name: "empty ack topic", cfg: emptyAck, wantError: "mqtt ack topic cannot be empty"},
	}
}

func TestValidatePipeline(t *testing.T) {
	for _, tt := range getPipelineValidationTests() {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePipeline(&tt.cfg)
			checkValidationError(t, err, tt.wantError)
		})
	}
}

func getPipelineValidationTests() []pipelineTestCase {
	valid := defaultPipelineConfig()
	valid.BufferCapacity = 1000
	valid.PublishWorkers = 10

	zeroBuffer := valid
	zeroBuffer.BufferCapacity = 0

	zeroWorkers := valid
	zeroWorkers.PublishWorkers = 0

	negativeBuffer := valid
	negativeBuffer.BufferCapacity = -1

	negativeWorkers := valid
	negativeWorkers.PublishWorkers = -1

	zeroAckBatch := valid
	zeroAckBatch.AckBatchSize = 0

	zeroHealthPing := valid
	zeroHealthPing.HealthPingTimeout = 0

	return []pipelineTestCase{
		{name: "valid config", cfg: valid, wantError: ""},
		{name: "zero buffer capacity", cfg: zeroBuffer, wantError: "pipeline buffer capacity must be positive"},
		{name: "zero publish workers", cfg: zeroWorkers, wantError: "pipeline publish workers must be positive"},
		{name: "negative buffer capacity", cfg: negativeBuffer, wantError: "pipeline buffer capacity must be positive"},
		{name: "negative publish workers", cfg: negativeWorkers, wantError: "pipeline publish workers must be positive"},
		{name: "zero ack batch size", cfg: zeroAckBatch, wantError: "pipeline ack batch size must be positive"},
		{name: "zero health ping timeout", cfg: zeroHealthPing, wantError: "pipeline health ping timeout must be positive"},
	}
}

type compressTestCase struct {
	name      string
	wantError string
	cfg       CompressConfig
}

func TestValidateCompress(t *testing.T) {
	valid := defaultCompressConfig()

	zeroFreelist := valid
	zeroFreelist.FreelistSize = 0

	zeroMaxDecompress := valid
	zeroMaxDecompress.MaxDecompressBytes = 0

	warmupOverFreelist := valid
	warmupOverFreelist.WarmupCount = valid.FreelistSize + 1

	negativeWarmup := valid
	negativeWarmup.WarmupCount = -1

	for _, tt := range []compressTestCase{
		{name: "valid config", cfg: valid, wantError: ""},
		{name: "zero freelist size", cfg: zeroFreelist, wantError: "compress freelist size must be positive"},
		{name: "zero max decompress", cfg: zeroMaxDecompress, wantError: "compress max decompress bytes must be positive"},
		{name: "warmup exceeds freelist", cfg: warmupOverFreelist,
			wantError: "compress warmup count must be between 0 and freelist size"},
		{name: "negative warmup", cfg: negativeWarmup,
			wantError: "compress warmup count must be between 0 and freelist size"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCompress(&tt.cfg)
			checkValidationError(t, err, tt.wantError)
		})
	}
}

func TestValidate_CompressError(t *testing.T) {
	cfg := defaultConfig()
	cfg.Compress.FreelistSize = 0

	err := Validate(cfg)
	if err == nil || !strings.Contains(err.Error(), "compress freelist") {
		t.Errorf("Validate() error = %v; want compress freelist error", err)
	}
}

func checkValidationError(t *testing.T, err error, wantError string) {
	t.Helper()
	if wantError == "" {
		if err != nil {
			t.Errorf("validation error = %v; want nil", err)
		}
		return
	}
	if err == nil {
		t.Errorf("validation error = nil; want %s", wantError)
		return
	}
	if err.Error() != wantError {
		t.Errorf("validation error = %s; want %s", err.Error(), wantError)
	}
}
