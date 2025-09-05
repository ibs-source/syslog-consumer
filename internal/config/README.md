# internal/config

Configuration module for the Syslog Consumer. It loads, merges, and validates settings from defaults, environment variables, and CLI flags, then exposes a single strongly-typed `Config` used across the application.

Scope

- Define the configuration schema (App, Redis, MQTT, Pipeline, Retry, DLQ, Circuit Breaker, Metrics, Health, Resource).
- Register and parse CLI flags.
- Load defaults, apply environment variables, then override with flags (strict precedence).
- Normalize/derive values (e.g., buffer size power-of-two, generated MQTT ClientID).
- Validate resulting configuration and return errors if invalid.

Precedence & Load Flow

1. RegisterFlags()
2. GetDefaults()
3. LoadFromEnvironment(cfg) // env overrides defaults
4. ApplyFlags(cfg) // flags override env
5. cfg.Validate()

Key Behaviors & Derivations

- Pipeline buffer size is rounded up to the next power of two.
- MQTT ClientID defaults to `syslog-consumer-<hostname>-<pid>`.
- MQTT TLS is auto-enabled when broker URL includes port 8883 (when provided via `--mqtt-broker`).
- MQTT TLS ServerName inferred from broker URL if not explicitly set.
- Redis “NOGROUP” conditions are handled gracefully by clients (group auto-create when missing).

Configuration (ENV, Flags, Description)
Precedence: defaults → environment variables → CLI flags.

App & Logging
| ENV | Flag | Default | Description |
|---|---|---|---|
| APP_NAME | --app-name | syslog-consumer | Application name. |
| APP_ENV | --app-env | production | Deployment environment label. |
| LOG_LEVEL | --log-level | info | Log level: trace, debug, info, warn, error, fatal, panic. |
| LOG_FORMAT | --log-format | json | Log format: json or text. |
| APP_SHUTDOWN_TIMEOUT | --app-shutdown-timeout | 30s | Graceful shutdown timeout. |
| APP_PENDING_OPS_GRACE | --app-pending-ops-grace-ms | 500ms | Wait after stop for pending ops. |

Redis
| ENV | Flag | Default | Description |
|---|---|---|---|
| REDIS_ADDRESSES | --redis-addr | localhost:6379 | Comma-separated Redis addresses. |
| REDIS_PASSWORD | --redis-password | | Password for Redis. |
| REDIS_DB | --redis-db | 0 | Redis DB index. |
| REDIS_STREAM | --redis-stream | syslog-stream | Redis stream name. |
| REDIS_CONSUMER_GROUP | --redis-group | syslog-group | Consumer group name. |
| REDIS_MAX_RETRIES | --redis-client-retries | 5 | Max transient retries in client ops. |
| REDIS_RETRY_INTERVAL | --redis-client-retry-interval | 1s | Wait between client retries. |
| REDIS_CONNECT_TIMEOUT | | 5s | Dial/connect timeout. |
| REDIS_READ_TIMEOUT | | 3s | Read timeout. |
| REDIS_WRITE_TIMEOUT | | 3s | Write timeout. |
| REDIS_POOL_SIZE | | CPU\*10 | Connection pool size. |
| REDIS_MIN_IDLE_CONNS | | CPU | Minimum idle connections. |
| REDIS_MAX_CONN_AGE | | 30m | Max connection lifetime. |
| REDIS_IDLE_TIMEOUT | | 5m | Idle timeout per connection. |
| REDIS_CLAIM_MIN_IDLE | | 1m | Min idle to consider for claim. |
| REDIS_CLAIM_BATCH_SIZE | | 1000 | Messages per claim op. |
| REDIS_PENDING_CHECK_INTERVAL | | 30s | Interval for pending checks. |
| REDIS_CLAIM_INTERVAL | | 30s | Interval for claim loop. |
| REDIS_BATCH_SIZE | --redis-batch-size | 100 | XREADGROUP batch count. |
| REDIS_BLOCK_TIME | --redis-batch-timeout | 5s | XREADGROUP block time. |
| REDIS_AGGRESSIVE_CLAIM | --redis-aggressive-claim | true | Aggressive claim cycles until exhausted. |
| REDIS_CLAIM_CYCLE_DELAY | --redis-claim-cycle-delay | 1s | Delay between aggressive cycles. |
| REDIS_DRAIN_ENABLED | --redis-drain-enabled | true | Enable draining unassigned messages. |
| REDIS_DRAIN_INTERVAL | --redis-drain-interval | 1m | Drain loop interval. |
| REDIS_DRAIN_BATCH_SIZE | --redis-drain-batch-size | 1000 | Messages per drain read. |
| REDIS_CONSUMER_CLEANUP_ENABLED | --redis-consumer-cleanup-enabled | true | Auto-cleanup idle consumers. |
| REDIS_CONSUMER_IDLE_TIMEOUT | --redis-consumer-idle-timeout | 5m | Idle threshold to remove consumer. |
| REDIS_CONSUMER_CLEANUP_INTERVAL | --redis-consumer-cleanup-interval | 1m | Cleanup loop interval. |

MQTT
| ENV | Flag | Default | Description |
|---|---|---|---|
| MQTT_BROKERS | --mqtt-broker | tcp://localhost:1883 | Broker URL(s). ssl://host:8883 enables TLS. |
| MQTT_CLIENT_ID | --mqtt-client-id | generated | Client identifier. |
| MQTT_QOS | --mqtt-qos | 2 | QoS level (0,1,2). |
| MQTT_KEEP_ALIVE | --mqtt-keep-alive | 30s | Keepalive interval. |
| MQTT_CONNECT_TIMEOUT | --mqtt-connect-timeout | 10s | Connect timeout. |
| MQTT_MAX_RECONNECT_DELAY | --mqtt-max-reconnect-delay | 2m | Max auto-reconnect backoff. |
| MQTT_CLEAN_SESSION | --mqtt-clean-session | true | Clean session toggle. |
| MQTT_ORDER_MATTERS | --mqtt-order-matters | true | Enforce in-order message handling. |
| MQTT_PUBLISH_TOPIC | --mqtt-publish-topic | syslog | Publish topic (base). |
| MQTT_SUBSCRIBE_TOPIC | --mqtt-subscribe-topic | syslog/acknowledgement | Ack/feedback topic. |
| MQTT_USE_USER_PREFIX | --mqtt-use-user-prefix | true | Prefix topics with cert CN (if TLS). |
| MQTT_CUSTOM_PREFIX | --mqtt-custom-prefix | | Custom topic prefix if user prefix disabled. |
| MQTT_PUBLISHER_POOL_SIZE | --mqtt-publishers | 1 | Concurrent publishers (tuning). |
| MQTT_MAX_INFLIGHT | --mqtt-max-inflight | 1000 | Max inflight messages. |
| MQTT_MESSAGE_CHANNEL_DEPTH | --mqtt-message-channel-depth | 0 | Internal client channel depth. |
| MQTT_WRITE_TIMEOUT | --mqtt-write-timeout | 5s | Publish/subscribe wait timeout. |
| MQTT_TLS_ENABLED | | true | Enable TLS/mTLS. |
| MQTT_CA_CERT | --mqtt-ca-cert | | CA certificate path. |
| MQTT_CLIENT_CERT | --mqtt-client-cert | | Client certificate path. |
| MQTT_CLIENT_KEY | --mqtt-client-key | | Client key path. |
| MQTT_TLS_INSECURE | --mqtt-tls-insecure | false | Skip TLS verify (testing only). |
| MQTT_TLS_SERVER_NAME | --mqtt-tls-server-name | inferred | Override expected server name. |
| MQTT_TLS_MIN_VERSION | --mqtt-tls-min-version | TLS1.2 | Minimum TLS version. |
| MQTT_TLS_CIPHER_SUITES | --mqtt-tls-cipher-suites | | Allowed cipher suites (CSV). |
| MQTT_TLS_PREFER_SERVER_CIPHERS | --mqtt-tls-prefer-server-ciphers | false | Prefer server ciphers. |

Pipeline
| ENV | Flag | Default | Description |
|---|---|---|---|
| PIPELINE_BUFFER_SIZE | --pipeline-buffer-size | 1048576 | Ring buffer capacity (rounded to power of 2). |
| PIPELINE_BATCH_SIZE | --pipeline-batch-size | 1000 | Batch size for processing. |
| PIPELINE_BATCH_TIMEOUT | --pipeline-batch-timeout | 100ms | Max wait to flush partial batch. |
| PIPELINE_PROCESSING_TIMEOUT | --pipeline-processing-timeout | 5s | Per-batch processing timeout. |
| PIPELINE_NUMA_AWARE | --pipeline-numa-aware | false | NUMA awareness hint. |
| PIPELINE_CPU_AFFINITY | --pipeline-cpu-affinity | | CPU list for process affinity (Linux best-effort). |
| PIPELINE_BACKPRESSURE_THRESHOLD | --pipeline-backpressure-threshold | 0.8 | Buffer usage threshold (0.0-1.0). |
| PIPELINE_DROP_POLICY | --pipeline-drop-policy | oldest | Drop policy: oldest|newest|none. |
| PIPELINE_FLUSH_INTERVAL | --pipeline-flush-interval-ms | 100ms | Batch flush interval. |
| PIPELINE_BACKPRESSURE_POLL_INTERVAL | --pipeline-backpressure-poll-interval | 1s | Backpressure telemetry tick. |
| PIPELINE_IDLE_POLL_SLEEP | --pipeline-idle-poll-sleep-ms | 1ms | Idle sleep when no messages. |

Retry & DLQ
| ENV | Flag | Default | Description |
|---|---|---|---|
| PIPELINE_RETRY_ENABLED | --pipeline-retry-enabled | true | Enable retry logic. |
| PIPELINE_RETRY_MAX_ATTEMPTS | --pipeline-retry-max-attempts | | Max retry attempts. |
| PIPELINE_RETRY_INITIAL_BACKOFF | --pipeline-retry-initial-backoff | | First backoff (seconds). |
| PIPELINE_RETRY_MAX_BACKOFF | --pipeline-retry-max-backoff | | Max backoff (seconds). |
| PIPELINE_RETRY_MULTIPLIER | --pipeline-retry-multiplier | | Exponential multiplier. |
| | --pipeline-dlq-enabled | | Enable DLQ publish on failure. |
| | --pipeline-dlq-topic | | DLQ MQTT topic. |

Circuit Breaker
| ENV | Flag | Default | Description |
|---|---|---|---|
| CIRCUIT_BREAKER_ENABLED | --cb-enabled | true | Enable circuit breaker on publish. |
| CIRCUIT_BREAKER_ERROR_THRESHOLD | --cb-error-threshold | 50.0 | Open threshold (error rate %). |
| CIRCUIT_BREAKER_SUCCESS_THRESHOLD | --cb-success-threshold | 5 | Close-after successes in half-open. |
| CIRCUIT_BREAKER_TIMEOUT | --cb-timeout | 30s | Execution timeout. |
| CIRCUIT_BREAKER_MAX_CONCURRENT | --cb-max-concurrent | 100 | Concurrency limit. |
| CIRCUIT_BREAKER_REQUEST_VOLUME | --cb-request-volume | 20 | Minimum sample size/window. |

Health & Metrics
| ENV | Flag | Default | Description |
|---|---|---|---|
| HEALTH_ENABLED | --health-enabled | true | Enable health server. |
| HEALTH_PORT | --health-port | 8080 | Health server port. |
| HEALTH_READ_TIMEOUT | --health-read-timeout | 5s | Read timeout. |
| HEALTH_WRITE_TIMEOUT | --health-write-timeout | 5s | Write timeout. |
| HEALTH_CHECK_INTERVAL | --health-check-interval | 10s | Interval for internal checks. |
| HEALTH_REDIS_TIMEOUT | --health-redis-timeout | 2s | Redis ping timeout. |
| HEALTH_MQTT_TIMEOUT | --health-mqtt-timeout | 2s | MQTT check timeout. |
| METRICS_ENABLED | | true | Enable internal metrics. |
| METRICS_PORT | --metrics-port | 9090 | Port (if exporting via external handler). |
| METRICS_COLLECT_INTERVAL | --metrics-collect-interval | 10s | Collection interval. |
| METRICS_NAMESPACE | --metrics-namespace | syslog_consumer | Metrics namespace. |
| METRICS_SUBSYSTEM | --metrics-subsystem | | Metrics subsystem label. |
| METRICS_HISTOGRAM_BUCKETS | --metrics-histogram-buckets | 0.001,…,10 | Buckets (CSV of floats). |

Resource Management
| ENV | Flag | Default | Description |
|---|---|---|---|
| RESOURCE_CPU_HIGH | --resource-upper-cpu | 80.0 | High CPU threshold. |
| RESOURCE_CPU_LOW | --resource-lower-cpu | 30.0 | Low CPU threshold. |
| RESOURCE_MEM_HIGH | --resource-upper-mem | 80.0 | High memory threshold. |
| RESOURCE_MEM_LOW | --resource-lower-mem | 30.0 | Low memory threshold. |
| RESOURCE_CHECK_INTERVAL | | 5s | Resource polling interval. |
| RESOURCE_SCALE_UP_COOLDOWN | | 30s | Cooldown before scaling up. |
| RESOURCE_SCALE_DOWN_COOLDOWN | | 2m | Cooldown before scaling down. |
| RESOURCE_MIN_WORKERS | | 1 | Minimum worker count. |
| RESOURCE_MAX_WORKERS | | CPU\*4 | Maximum worker count. |
| RESOURCE_WORKER_STEP | | 2 | Step size for scaling. |
| RESOURCE_PREDICTIVE_SCALING | | true | Enable predictive scaling. |
| RESOURCE_HISTORY_WINDOW | | 100 | History window size. |
| RESOURCE_PREDICTION_HORIZON | | 30s | Prediction horizon. |

Validation

- Ensures values are within safe/expected ranges (e.g., QoS ∈ {0,1,2}).
- Verifies mandatory values for production (e.g., TLS files when MQTT TLS is enabled).
- Normalizes durations and sizes to avoid overflow or invalid states.

Usage (from cmd/consumer)

```go
cfg, err := config.Load()
if err != nil {
  // handle validation error
}
```
