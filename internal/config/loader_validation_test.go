package config

import (
	"testing"
)

func TestValidate_Success(t *testing.T) {
	cfg := &Config{
		Redis: RedisConfig{
			Address:   "localhost:6379",
			Stream:    "test-stream",
			Consumer:  "consumer-1",
			BatchSize: 10,
		},
		MQTT: MQTTConfig{
			Broker:       "tcp://localhost:1883",
			ClientID:     "test-client",
			PublishTopic: "test/publish",
			AckTopic:     "test/ack",
			PoolSize:     1,
		},
		Pipeline: PipelineConfig{
			BufferCapacity: 100,
			PublishWorkers: 1,
		},
	}

	if err := Validate(cfg); err != nil {
		t.Errorf("Validate() failed for valid config: %v", err)
	}
}

func TestValidateRedis(t *testing.T) {
	tests := getRedisValidationTests()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRedis(&tt.cfg)
			checkValidationError(t, err, tt.wantError)
		})
	}
}

func getRedisValidationTests() []redisTestCase {
	return []redisTestCase{
		{
			name: "valid config",
			cfg: RedisConfig{
				Address:   "localhost:6379",
				Consumer:  "consumer-1",
				BatchSize: 10,
			},
			wantError: "",
		},
		{
			name: "empty address",
			cfg: RedisConfig{
				Address:   "",
				Consumer:  "consumer-1",
				BatchSize: 10,
			},
			wantError: "redis address cannot be empty",
		},
		{
			name: "empty consumer",
			cfg: RedisConfig{
				Address:   "localhost:6379",
				Consumer:  "",
				BatchSize: 10,
			},
			wantError: "redis consumer name cannot be empty",
		},
		{
			name: "zero batch size",
			cfg: RedisConfig{
				Address:   "localhost:6379",
				Consumer:  "consumer-1",
				BatchSize: 0,
			},
			wantError: "redis batch size must be positive",
		},
		{
			name: "negative batch size",
			cfg: RedisConfig{
				Address:   "localhost:6379",
				Consumer:  "consumer-1",
				BatchSize: -1,
			},
			wantError: "redis batch size must be positive",
		},
	}
}

type redisTestCase struct {
	name      string
	cfg       RedisConfig
	wantError string
}

type mqttTestCase struct {
	name      string
	cfg       MQTTConfig
	wantError string
}

type pipelineTestCase struct {
	name      string
	cfg       PipelineConfig
	wantError string
}

func checkValidationError(t *testing.T, err error, wantError string) {
	t.Helper()
	if wantError == "" {
		if err != nil {
			t.Errorf("validation error = %v; want nil", err)
		}
	} else {
		if err == nil {
			t.Errorf("validation error = nil; want %s", wantError)
		} else if err.Error() != wantError {
			t.Errorf("validation error = %s; want %s", err.Error(), wantError)
		}
	}
}

func TestValidateMQTT(t *testing.T) {
	tests := getMQTTValidationTests()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMQTT(&tt.cfg)
			checkValidationError(t, err, tt.wantError)
		})
	}
}

func getMQTTValidationTests() []mqttTestCase {
	tests := []mqttTestCase{
		getMQTTValidConfig(),
		getMQTTEmptyBrokerTest(),
		getMQTTEmptyClientIDTest(),
	}
	tests = append(tests, getMQTTAdditionalTests()...)
	return tests
}

func getMQTTValidConfig() mqttTestCase {
	return mqttTestCase{
		name: "valid config",
		cfg: MQTTConfig{
			Broker:       "tcp://localhost:1883",
			ClientID:     "test-client",
			PublishTopic: "test/pub",
			AckTopic:     "test/ack",
			PoolSize:     5,
		},
		wantError: "",
	}
}

func getMQTTEmptyBrokerTest() mqttTestCase {
	return mqttTestCase{
		name: "empty broker",
		cfg: MQTTConfig{
			Broker:       "",
			ClientID:     "test-client",
			PublishTopic: "test/pub",
			AckTopic:     "test/ack",
			PoolSize:     5,
		},
		wantError: "mqtt broker cannot be empty",
	}
}

func getMQTTEmptyClientIDTest() mqttTestCase {
	return mqttTestCase{
		name: "empty client ID",
		cfg: MQTTConfig{
			Broker:       "tcp://localhost:1883",
			ClientID:     "",
			PublishTopic: "test/pub",
			AckTopic:     "test/ack",
			PoolSize:     5,
		},
		wantError: "mqtt client ID cannot be empty",
	}
}

func getMQTTAdditionalTests() []mqttTestCase {
	return []mqttTestCase{
		{
			name: "zero pool size",
			cfg: MQTTConfig{
				Broker:       "tcp://localhost:1883",
				ClientID:     "test-client",
				PublishTopic: "test/pub",
				AckTopic:     "test/ack",
				PoolSize:     0,
			},
			wantError: "mqtt pool size must be positive",
		},
		{
			name: "empty publish topic",
			cfg: MQTTConfig{
				Broker:       "tcp://localhost:1883",
				ClientID:     "test-client",
				PublishTopic: "",
				AckTopic:     "test/ack",
				PoolSize:     5,
			},
			wantError: "mqtt publish topic cannot be empty",
		},
		{
			name: "empty ack topic",
			cfg: MQTTConfig{
				Broker:       "tcp://localhost:1883",
				ClientID:     "test-client",
				PublishTopic: "test/pub",
				AckTopic:     "",
				PoolSize:     5,
			},
			wantError: "mqtt ack topic cannot be empty",
		},
	}
}

func TestValidatePipeline(t *testing.T) {
	tests := getPipelineValidationTests()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePipeline(&tt.cfg)
			checkValidationError(t, err, tt.wantError)
		})
	}
}

func getPipelineValidationTests() []pipelineTestCase {
	return []pipelineTestCase{
		{
			name: "valid config",
			cfg: PipelineConfig{
				BufferCapacity: 1000,
				PublishWorkers: 10,
			},
			wantError: "",
		},
		{
			name: "zero buffer capacity",
			cfg: PipelineConfig{
				BufferCapacity: 0,
				PublishWorkers: 10,
			},
			wantError: "pipeline buffer capacity must be positive",
		},
		{
			name: "zero publish workers",
			cfg: PipelineConfig{
				BufferCapacity: 1000,
				PublishWorkers: 0,
			},
			wantError: "pipeline publish workers must be positive",
		},
		{
			name: "negative buffer capacity",
			cfg: PipelineConfig{
				BufferCapacity: -1,
				PublishWorkers: 10,
			},
			wantError: "pipeline buffer capacity must be positive",
		},
		{
			name: "negative publish workers",
			cfg: PipelineConfig{
				BufferCapacity: 1000,
				PublishWorkers: -1,
			},
			wantError: "pipeline publish workers must be positive",
		},
	}
}
