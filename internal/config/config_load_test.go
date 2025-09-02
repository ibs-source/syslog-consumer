package config

import (
	"os"
	"testing"
	"time"
)

func TestLoaders_OverrideWithEnv(t *testing.T) {
	ac := setAppEnv(t)
	assertAppConfig(t, ac)

	rc := setRedisEnv(t)
	assertRedisBasics(t, rc)
	assertRedisDurationsAndInts(t, rc)
	assertRedisPool(t, rc)
	assertRedisClaimConfig(t, rc)
	assertRedisIntervals(t, rc)
	assertRedisBatchBlock(t, rc)
	assertRedisDrainClaimOpts(t, rc)
	assertRedisConsumerCleanup(t, rc)

	mq := setMQTTEnv(t)
	assertMQTTBasics(t, mq)
	assertMQTTDurations(t, mq)
	assertMQTTFlags(t, mq)
	assertTLSConfig(t, mq)
	assertTopicsConfig(t, mq)
	assertPublisherInflight(t, mq)

	rs := setResourceEnv(t)
	assertResourceThresholds(t, rs)
	assertResourceIntervals(t, rs)
	assertResourceNumbers(t, rs)

	mc := setMetricsEnv(t)
	assertMetricsConfig(t, mc)

	hc := setHealthEnv(t)
	assertHealthConfig(t, hc)

	pc := setPipelineEnv(t)
	assertPipelineBasics(t, pc)
	assertPipelineFlags(t, pc)
	assertPipelineBackpressure(t, pc)

	cb := setCircuitBreakerEnv(t)
	assertCircuitBreakerConfig(t, cb)

	// Sanity check full Load() uses Validate and env/flags
	_ = os.Unsetenv("MQTT_TLS_ENABLED")
	_ = os.Unsetenv("MQTT_CA_CERT")
	_ = os.Unsetenv("MQTT_CLIENT_CERT")
	_ = os.Unsetenv("MQTT_CLIENT_KEY")
	// Ensure Load() doesn't panic with current env. Errors are acceptable here.
	_, _ = Load()
}

// ---- Environment setup helpers (reduce funlen/lll in test) ----

func setAppEnv(t *testing.T) AppConfig {
	t.Helper()
	t.Setenv("APP_NAME", "app-x")
	t.Setenv("APP_ENV", "staging")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("APP_SHUTDOWN_TIMEOUT", "5s")
	return loadAppConfig()
}

func setRedisEnv(t *testing.T) RedisConfig {
	t.Helper()
	t.Setenv("REDIS_ADDRESSES", "r1:6379,r2:6379")
	t.Setenv("REDIS_PASSWORD", "pw")
	t.Setenv("REDIS_DB", "2")
	t.Setenv("REDIS_STREAM", "s")
	t.Setenv("REDIS_CONSUMER_GROUP", "g")
	t.Setenv("REDIS_MAX_RETRIES", "7")
	t.Setenv("REDIS_RETRY_INTERVAL", "2s")
	t.Setenv("REDIS_CONNECT_TIMEOUT", "3s")
	t.Setenv("REDIS_READ_TIMEOUT", "4s")
	t.Setenv("REDIS_WRITE_TIMEOUT", "5s")
	t.Setenv("REDIS_POOL_SIZE", "11")
	t.Setenv("REDIS_MIN_IDLE_CONNS", "3")
	t.Setenv("REDIS_MAX_CONN_AGE", "6s")
	t.Setenv("REDIS_IDLE_TIMEOUT", "7s")
	t.Setenv("REDIS_CLAIM_MIN_IDLE", "8s")
	t.Setenv("REDIS_CLAIM_BATCH_SIZE", "123")
	t.Setenv("REDIS_PENDING_CHECK_INTERVAL", "9s")
	t.Setenv("REDIS_CLAIM_INTERVAL", "10s")
	t.Setenv("REDIS_BATCH_SIZE", "55")
	t.Setenv("REDIS_BLOCK_TIME", "11s")
	t.Setenv("REDIS_AGGRESSIVE_CLAIM", "false")
	t.Setenv("REDIS_CLAIM_CYCLE_DELAY", "12s")
	t.Setenv("REDIS_DRAIN_ENABLED", "false")
	t.Setenv("REDIS_DRAIN_INTERVAL", "13s")
	t.Setenv("REDIS_DRAIN_BATCH_SIZE", "77")
	t.Setenv("REDIS_CONSUMER_CLEANUP_ENABLED", "false")
	t.Setenv("REDIS_CONSUMER_IDLE_TIMEOUT", "14s")
	t.Setenv("REDIS_CONSUMER_CLEANUP_INTERVAL", "15s")
	return loadRedisConfig()
}

func setMQTTEnv(t *testing.T) MQTTConfig {
	t.Helper()
	t.Setenv("MQTT_BROKERS", "tcp://b1:1883,tcp://b2:1883")
	t.Setenv("MQTT_CLIENT_ID", "cid-x")
	t.Setenv("MQTT_QOS", "1")
	t.Setenv("MQTT_KEEP_ALIVE", "9s")
	t.Setenv("MQTT_CONNECT_TIMEOUT", "8s")
	t.Setenv("MQTT_MAX_RECONNECT_DELAY", "7s")
	t.Setenv("MQTT_CLEAN_SESSION", "false")
	t.Setenv("MQTT_ORDER_MATTERS", "false")
	t.Setenv("MQTT_CA_CERT", "/tmp/ca")
	t.Setenv("MQTT_CLIENT_CERT", "/tmp/cert")
	t.Setenv("MQTT_CLIENT_KEY", "/tmp/key")
	t.Setenv("MQTT_TLS_INSECURE", "true")
	t.Setenv("MQTT_TLS_SERVER_NAME", "srv")
	t.Setenv("MQTT_TLS_MIN_VERSION", "TLS1.3")
	t.Setenv("MQTT_TLS_CIPHER_SUITES", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
	t.Setenv("MQTT_TLS_PREFER_SERVER_CIPHERS", "true")
	t.Setenv("MQTT_PUBLISH_TOPIC", "out")
	t.Setenv("MQTT_SUBSCRIBE_TOPIC", "in")
	t.Setenv("MQTT_USE_USER_PREFIX", "false")
	t.Setenv("MQTT_CUSTOM_PREFIX", "custom")
	t.Setenv("MQTT_PUBLISHER_POOL_SIZE", "5")
	t.Setenv("MQTT_MAX_INFLIGHT", "50")
	t.Setenv("MQTT_WRITE_TIMEOUT", "6s")
	return loadMQTTConfig()
}

func setResourceEnv(t *testing.T) ResourceConfig {
	t.Helper()
	t.Setenv("RESOURCE_CPU_HIGH", "90")
	t.Setenv("RESOURCE_CPU_LOW", "10")
	t.Setenv("RESOURCE_MEM_HIGH", "85")
	t.Setenv("RESOURCE_MEM_LOW", "15")
	t.Setenv("RESOURCE_CHECK_INTERVAL", "2s")
	t.Setenv("RESOURCE_SCALE_UP_COOLDOWN", "3s")
	t.Setenv("RESOURCE_SCALE_DOWN_COOLDOWN", "4s")
	t.Setenv("RESOURCE_MIN_WORKERS", "2")
	t.Setenv("RESOURCE_MAX_WORKERS", "4")
	t.Setenv("RESOURCE_WORKER_STEP", "3")
	t.Setenv("RESOURCE_PREDICTIVE_SCALING", "false")
	t.Setenv("RESOURCE_HISTORY_WINDOW", "33")
	t.Setenv("RESOURCE_PREDICTION_HORIZON", "5s")
	return loadResourceConfig()
}

func setMetricsEnv(t *testing.T) MetricsConfig {
	t.Helper()
	t.Setenv("METRICS_ENABLED", "true")
	t.Setenv("METRICS_PORT", "9100")
	t.Setenv("METRICS_COLLECT_INTERVAL", "4s")
	t.Setenv("METRICS_NAMESPACE", "ns")
	t.Setenv("METRICS_SUBSYSTEM", "sub")
	t.Setenv("METRICS_HISTOGRAM_BUCKETS", "0.1,0.2,0.3")
	return loadMetricsConfig()
}

func setHealthEnv(t *testing.T) HealthConfig {
	t.Helper()
	t.Setenv("HEALTH_ENABLED", "true")
	t.Setenv("HEALTH_PORT", "9099")
	t.Setenv("HEALTH_READ_TIMEOUT", "3s")
	t.Setenv("HEALTH_WRITE_TIMEOUT", "4s")
	t.Setenv("HEALTH_CHECK_INTERVAL", "5s")
	t.Setenv("HEALTH_REDIS_TIMEOUT", "6s")
	t.Setenv("HEALTH_MQTT_TIMEOUT", "7s")
	return loadHealthConfig()
}

func setPipelineEnv(t *testing.T) PipelineConfig {
	t.Helper()
	t.Setenv("PIPELINE_BUFFER_SIZE", "64")
	t.Setenv("PIPELINE_BATCH_SIZE", "8")
	t.Setenv("PIPELINE_BATCH_TIMEOUT", "1s")
	t.Setenv("PIPELINE_PROCESSING_TIMEOUT", "2s")
	t.Setenv("PIPELINE_ZERO_COPY", "false")
	t.Setenv("PIPELINE_PREALLOCATE", "false")
	t.Setenv("PIPELINE_NUMA_AWARE", "true")
	t.Setenv("PIPELINE_CPU_AFFINITY", "0,1,2")
	t.Setenv("PIPELINE_BACKPRESSURE_THRESHOLD", "0.7")
	t.Setenv("PIPELINE_DROP_POLICY", "newest")
	t.Setenv("PIPELINE_FLUSH_INTERVAL", "3s")
	return loadPipelineConfig()
}

func setCircuitBreakerEnv(t *testing.T) CircuitBreakerConfig {
	t.Helper()
	t.Setenv("CIRCUIT_BREAKER_ENABLED", "true")
	t.Setenv("CIRCUIT_BREAKER_ERROR_THRESHOLD", "25")
	t.Setenv("CIRCUIT_BREAKER_SUCCESS_THRESHOLD", "3")
	t.Setenv("CIRCUIT_BREAKER_TIMEOUT", "2s")
	t.Setenv("CIRCUIT_BREAKER_MAX_CONCURRENT", "9")
	t.Setenv("CIRCUIT_BREAKER_REQUEST_VOLUME", "5")
	return loadCircuitBreakerConfig()
}

// ---- Assertions (helpers reduce line length and complexity) ----

func assertAppConfig(t *testing.T, ac AppConfig) {
	t.Helper()
	if ac.Name != "app-x" {
		t.Fatalf("app name: %v", ac.Name)
	}
	if ac.Environment != "staging" {
		t.Fatalf("env: %v", ac.Environment)
	}
	if ac.LogLevel != "debug" {
		t.Fatalf("log level: %v", ac.LogLevel)
	}
	if ac.LogFormat != "text" {
		t.Fatalf("log format: %v", ac.LogFormat)
	}
	if ac.ShutdownTimeout != 5*time.Second {
		t.Fatalf("shutdown timeout: %v", ac.ShutdownTimeout)
	}
}

func assertRedisBasics(t *testing.T, rc RedisConfig) {
	t.Helper()
	if len(rc.Addresses) != 2 {
		t.Fatalf("addresses: %v", rc.Addresses)
	}
	if rc.Password != "pw" {
		t.Fatalf("password: %v", rc.Password)
	}
	if rc.DB != 2 {
		t.Fatalf("db: %v", rc.DB)
	}
	if rc.StreamName != "s" {
		t.Fatalf("stream: %v", rc.StreamName)
	}
	if rc.ConsumerGroup != "g" {
		t.Fatalf("group: %v", rc.ConsumerGroup)
	}
}

func assertRedisDurationsAndInts(t *testing.T, rc RedisConfig) {
	t.Helper()
	if rc.MaxRetries != 7 {
		t.Fatalf("max retries: %v", rc.MaxRetries)
	}
	if rc.RetryInterval != 2*time.Second {
		t.Fatalf("retry interval: %v", rc.RetryInterval)
	}
	if rc.ConnectTimeout != 3*time.Second {
		t.Fatalf("connect timeout: %v", rc.ConnectTimeout)
	}
	if rc.ReadTimeout != 4*time.Second {
		t.Fatalf("read timeout: %v", rc.ReadTimeout)
	}
	if rc.WriteTimeout != 5*time.Second {
		t.Fatalf("write timeout: %v", rc.WriteTimeout)
	}
}

func assertRedisPool(t *testing.T, rc RedisConfig) {
	t.Helper()
	if rc.PoolSize != 11 {
		t.Fatalf("pool size: %v", rc.PoolSize)
	}
	if rc.MinIdleConns != 3 {
		t.Fatalf("min idle conns: %v", rc.MinIdleConns)
	}
	if rc.ConnMaxLifetime != 6*time.Second {
		t.Fatalf("conn max lifetime: %v", rc.ConnMaxLifetime)
	}
	if rc.ConnMaxIdleTime != 7*time.Second {
		t.Fatalf("conn max idle: %v", rc.ConnMaxIdleTime)
	}
}

func assertRedisClaimConfig(t *testing.T, rc RedisConfig) {
	t.Helper()
	if rc.ClaimMinIdleTime != 8*time.Second {
		t.Fatalf("claim min idle: %v", rc.ClaimMinIdleTime)
	}
	if rc.ClaimBatchSize != 123 {
		t.Fatalf("claim batch size: %v", rc.ClaimBatchSize)
	}
}

func assertRedisIntervals(t *testing.T, rc RedisConfig) {
	t.Helper()
	if rc.PendingCheckInterval != 9*time.Second {
		t.Fatalf("pending check interval: %v", rc.PendingCheckInterval)
	}
	if rc.ClaimInterval != 10*time.Second {
		t.Fatalf("claim interval: %v", rc.ClaimInterval)
	}
}

func assertRedisBatchBlock(t *testing.T, rc RedisConfig) {
	t.Helper()
	if rc.BatchSize != 55 {
		t.Fatalf("batch size: %v", rc.BatchSize)
	}
	if rc.BlockTime != 11*time.Second {
		t.Fatalf("block time: %v", rc.BlockTime)
	}
}

func assertRedisDrainClaimOpts(t *testing.T, rc RedisConfig) {
	t.Helper()
	if rc.AggressiveClaim != false {
		t.Fatalf("aggressive claim: %v", rc.AggressiveClaim)
	}
	if rc.DrainEnabled != false {
		t.Fatalf("drain enabled: %v", rc.DrainEnabled)
	}
	if rc.DrainInterval != 13*time.Second {
		t.Fatalf("drain interval: %v", rc.DrainInterval)
	}
	if rc.DrainBatchSize != 77 {
		t.Fatalf("drain batch size: %v", rc.DrainBatchSize)
	}
}

func assertRedisConsumerCleanup(t *testing.T, rc RedisConfig) {
	t.Helper()
	if rc.ConsumerCleanupEnabled != false {
		t.Fatalf("consumer cleanup enabled: %v", rc.ConsumerCleanupEnabled)
	}
	if rc.ConsumerIdleTimeout != 14*time.Second {
		t.Fatalf("consumer idle timeout: %v", rc.ConsumerIdleTimeout)
	}
	if rc.ConsumerCleanupInterval != 15*time.Second {
		t.Fatalf("consumer cleanup interval: %v", rc.ConsumerCleanupInterval)
	}
}

func assertMQTTBasics(t *testing.T, mq MQTTConfig) {
	t.Helper()
	if len(mq.Brokers) != 2 {
		t.Fatalf("brokers: %v", mq.Brokers)
	}
	if mq.ClientID != "cid-x" {
		t.Fatalf("client id: %v", mq.ClientID)
	}
	if mq.QoS != 1 {
		t.Fatalf("qos: %v", mq.QoS)
	}
}

func assertMQTTDurations(t *testing.T, mq MQTTConfig) {
	t.Helper()
	if mq.KeepAlive != 9*time.Second {
		t.Fatalf("keep alive: %v", mq.KeepAlive)
	}
	if mq.ConnectTimeout != 8*time.Second {
		t.Fatalf("connect timeout: %v", mq.ConnectTimeout)
	}
	if mq.MaxReconnectDelay != 7*time.Second {
		t.Fatalf("max reconnect: %v", mq.MaxReconnectDelay)
	}
}

func assertMQTTFlags(t *testing.T, mq MQTTConfig) {
	t.Helper()
	if mq.CleanSession != false {
		t.Fatalf("clean session: %v", mq.CleanSession)
	}
	if mq.OrderMatters != false {
		t.Fatalf("order matters: %v", mq.OrderMatters)
	}
}

func assertTLSConfig(t *testing.T, mq MQTTConfig) {
	t.Helper()
	if !mq.TLS.InsecureSkipVerify {
		t.Fatalf("insecure skip verify: %v", mq.TLS.InsecureSkipVerify)
	}
	if mq.TLS.ServerName != "srv" {
		t.Fatalf("server name: %v", mq.TLS.ServerName)
	}
	if mq.TLS.MinVersion != "TLS1.3" {
		t.Fatalf("min version: %v", mq.TLS.MinVersion)
	}
	if !mq.TLS.PreferServerCiphers {
		t.Fatalf("prefer server ciphers: %v", mq.TLS.PreferServerCiphers)
	}
}

func assertTopicsConfig(t *testing.T, mq MQTTConfig) {
	t.Helper()
	if mq.Topics.PublishTopic != "out" {
		t.Fatalf("publish topic: %v", mq.Topics.PublishTopic)
	}
	if mq.Topics.SubscribeTopic != "in" {
		t.Fatalf("subscribe topic: %v", mq.Topics.SubscribeTopic)
	}
	if mq.Topics.UseUserPrefix != false {
		t.Fatalf("use user prefix: %v", mq.Topics.UseUserPrefix)
	}
	if mq.Topics.CustomPrefix != "custom" {
		t.Fatalf("custom prefix: %v", mq.Topics.CustomPrefix)
	}
}

func assertPublisherInflight(t *testing.T, mq MQTTConfig) {
	t.Helper()
	if mq.PublisherPoolSize != 5 {
		t.Fatalf("publisher pool size: %v", mq.PublisherPoolSize)
	}
	if mq.MaxInflight != 50 {
		t.Fatalf("max inflight: %v", mq.MaxInflight)
	}
	if mq.WriteTimeout != 6*time.Second {
		t.Fatalf("write timeout: %v", mq.WriteTimeout)
	}
}

func assertResourceThresholds(t *testing.T, rs ResourceConfig) {
	t.Helper()
	if rs.CPUThresholdHigh != 90 {
		t.Fatalf("cpu high: %v", rs.CPUThresholdHigh)
	}
	if rs.CPUThresholdLow != 10 {
		t.Fatalf("cpu low: %v", rs.CPUThresholdLow)
	}
	if rs.MemoryThresholdHigh != 85 {
		t.Fatalf("mem high: %v", rs.MemoryThresholdHigh)
	}
	if rs.MemoryThresholdLow != 15 {
		t.Fatalf("mem low: %v", rs.MemoryThresholdLow)
	}
}

func assertResourceIntervals(t *testing.T, rs ResourceConfig) {
	t.Helper()
	if rs.CheckInterval != 2*time.Second {
		t.Fatalf("check interval: %v", rs.CheckInterval)
	}
	if rs.ScaleUpCooldown != 3*time.Second {
		t.Fatalf("scale up cooldown: %v", rs.ScaleUpCooldown)
	}
	if rs.ScaleDownCooldown != 4*time.Second {
		t.Fatalf("scale down cooldown: %v", rs.ScaleDownCooldown)
	}
}

func assertResourceNumbers(t *testing.T, rs ResourceConfig) {
	t.Helper()
	if rs.MinWorkers != 2 {
		t.Fatalf("min workers: %v", rs.MinWorkers)
	}
	if rs.MaxWorkers != 4 {
		t.Fatalf("max workers: %v", rs.MaxWorkers)
	}
	if rs.WorkerStepSize != 3 {
		t.Fatalf("worker step: %v", rs.WorkerStepSize)
	}
	if rs.EnablePredictiveScaling != false {
		t.Fatalf("predictive scaling: %v", rs.EnablePredictiveScaling)
	}
	if rs.HistoryWindowSize != 33 {
		t.Fatalf("history window: %v", rs.HistoryWindowSize)
	}
	if rs.PredictionHorizon != 5*time.Second {
		t.Fatalf("prediction horizon: %v", rs.PredictionHorizon)
	}
}

func assertMetricsConfig(t *testing.T, mc MetricsConfig) {
	t.Helper()
	if !mc.Enabled {
		t.Fatalf("metrics enabled: %v", mc.Enabled)
	}
	if mc.PrometheusPort != 9100 {
		t.Fatalf("prom port: %v", mc.PrometheusPort)
	}
	if mc.CollectInterval != 4*time.Second {
		t.Fatalf("collect interval: %v", mc.CollectInterval)
	}
	if mc.Namespace != "ns" {
		t.Fatalf("namespace: %v", mc.Namespace)
	}
	if mc.Subsystem != "sub" {
		t.Fatalf("subsystem: %v", mc.Subsystem)
	}
	if len(mc.HistogramBuckets) != 3 {
		t.Fatalf("histogram buckets: %v", mc.HistogramBuckets)
	}
}

func assertHealthConfig(t *testing.T, hc HealthConfig) {
	t.Helper()
	if !hc.Enabled {
		t.Fatalf("health enabled: %v", hc.Enabled)
	}
	if hc.Port != 9099 {
		t.Fatalf("port: %v", hc.Port)
	}
	if hc.ReadTimeout != 3*time.Second {
		t.Fatalf("read timeout: %v", hc.ReadTimeout)
	}
	if hc.WriteTimeout != 4*time.Second {
		t.Fatalf("write timeout: %v", hc.WriteTimeout)
	}
	if hc.CheckInterval != 5*time.Second {
		t.Fatalf("check interval: %v", hc.CheckInterval)
	}
	if hc.RedisTimeout != 6*time.Second {
		t.Fatalf("redis timeout: %v", hc.RedisTimeout)
	}
	if hc.MQTTTimeout != 7*time.Second {
		t.Fatalf("mqtt timeout: %v", hc.MQTTTimeout)
	}
}

func assertPipelineBasics(t *testing.T, pc PipelineConfig) {
	t.Helper()
	if pc.BufferSize != 64 {
		t.Fatalf("buffer size: %v", pc.BufferSize)
	}
	if pc.BatchSize != 8 {
		t.Fatalf("batch size: %v", pc.BatchSize)
	}
	if pc.BatchTimeout != 1*time.Second {
		t.Fatalf("batch timeout: %v", pc.BatchTimeout)
	}
	if pc.ProcessingTimeout != 2*time.Second {
		t.Fatalf("processing timeout: %v", pc.ProcessingTimeout)
	}
}

func assertPipelineFlags(t *testing.T, pc PipelineConfig) {
	t.Helper()
	if pc.UseZeroCopy != false {
		t.Fatalf("use zero copy: %v", pc.UseZeroCopy)
	}
	if pc.PreallocateBuffers != false {
		t.Fatalf("preallocate: %v", pc.PreallocateBuffers)
	}
	if pc.NumaAware != true {
		t.Fatalf("numa aware: %v", pc.NumaAware)
	}
	if len(pc.CPUAffinity) != 3 {
		t.Fatalf("cpu affinity len: %v", pc.CPUAffinity)
	}
}

func assertPipelineBackpressure(t *testing.T, pc PipelineConfig) {
	t.Helper()
	if pc.BackpressureThreshold != 0.7 {
		t.Fatalf("backpressure threshold: %v", pc.BackpressureThreshold)
	}
	if pc.DropPolicy != "newest" {
		t.Fatalf("drop policy: %v", pc.DropPolicy)
	}
	if pc.FlushInterval != 3*time.Second {
		t.Fatalf("flush interval: %v", pc.FlushInterval)
	}
}

func assertCircuitBreakerConfig(t *testing.T, cb CircuitBreakerConfig) {
	t.Helper()
	if !cb.Enabled {
		t.Fatalf("cb enabled: %v", cb.Enabled)
	}
	if cb.ErrorThreshold != 25 {
		t.Fatalf("error threshold: %v", cb.ErrorThreshold)
	}
	if cb.SuccessThreshold != 3 {
		t.Fatalf("success threshold: %v", cb.SuccessThreshold)
	}
	if cb.Timeout != 2*time.Second {
		t.Fatalf("timeout: %v", cb.Timeout)
	}
	if cb.MaxConcurrentCalls != 9 {
		t.Fatalf("max concurrent: %v", cb.MaxConcurrentCalls)
	}
	if cb.RequestVolumeThreshold != 5 {
		t.Fatalf("req volume: %v", cb.RequestVolumeThreshold)
	}
}
