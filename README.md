# Syslog Consumer (Go)

Enterprise-grade technical documentation for the Go consumer. This service reads messages from Redis Streams, processes them through a lock-free pipeline, and publishes them to MQTT with reliability controls (retries, DLQ, backpressure) protected by a circuit breaker. It exposes health/readiness/liveness endpoints and logs metrics snapshots in debug mode.

Contents

- Executive Summary
- Architecture
- End-to-End Data Flow
- Key Features
- Requirements
- Build
- Quickstart
- Configuration (ENV, Flags, Description)
  Note: The architecture is always lock-free and zero-copy; there are no flags or env variables to disable these.
- Health, Readiness, Liveness
- Security
- Performance & Concurrency
- Observability & Metrics
- Operations (shutdown, CPU affinity)
- Troubleshooting
- License

Executive Summary
This service:

- Consumes messages from a Redis Stream (XREADGROUP) with consumer group management.
- Publishes messages to MQTT (configurable QoS, mTLS recommended).
- Implements retry with exponential backoff and optional DLQ.
- Applies backpressure with configurable drop policies.
- Protects publish path with a circuit breaker.
- Exposes HTTP endpoints for health/readiness/liveness.
- Logs metrics snapshots when LOG_LEVEL=debug.

Architecture
Primary components (internal/_, pkg/_):

- cmd/consumer: application bootstrap (config, logger, Redis/MQTT clients, processor, health server).
- internal/config: configuration loading (defaults → env → flags) and validation.
- internal/logger: Logrus adapter implementing a common logging interface.
- internal/redis: Redis Streams client (go-redis v9) with conversion helpers and retry.
- internal/mqtt: MQTT client (Eclipse Paho) with lock-free handler registry and secure TLS.
- internal/processor: lock-free pipeline (ringbuffer + worker pool), backpressure, retry/DLQ, Redis claim/drain.
- internal/runtime: best-effort process CPU affinity (Linux).
- internal/domain: domain types (Message, AckMessage) and buffer pool.
- internal/ports: “ports” interfaces to decouple implementations.
- pkg/ringbuffer: generic lock-free ring buffer.
- pkg/circuitbreaker: simple circuit breaker implementation.
- pkg/jsonx: JSON helpers optimized for low allocations.

End-to-End Data Flow

1. Consumer Group: create/ensure Redis stream and consumer group.
2. Consume: processor reads batches via XREADGROUP.
3. Buffering: messages enter a lock-free ring buffer.
4. Processing: worker pool concurrently processes and publishes to MQTT.
5. Publish: payload is wrapped and published to configured topic behind a circuit breaker.
6. Ack: an external service publishes an ack to the ack topic; processor XACKs and XDELs in Redis.
7. Retry/DLQ: failures are retried with backoff; after max attempts, go to DLQ if enabled.
8. Backpressure: when buffer is near capacity, apply drop policy (oldest|newest|none).

Key Features

- Lock-free pipeline with generic ring buffer and worker pool.
- Backpressure with threshold and drop policy.
- Retry with exponential backoff; optional DLQ publishing.
- Circuit breaker with configurable thresholds.
- MQTT TLS: optional user-prefix extraction from client certificate CN.
- Health HTTP server with /health, /healthz, /heathz (alias), /ready, /live.
- Metrics snapshots logged when LOG_LEVEL=debug.

Requirements

- Go 1.21+ (recommended).
- Redis reachable (standalone/sentinel/cluster via UniversalClient).
- MQTT broker (e.g., 8883 with TLS mTLS for production).
- Certificates for mTLS unless intentionally disabled for development.

Build

- go build -o bin/consumer ./cmd/consumer
- Tests: go test ./...

Quickstart
Local (TLS disabled for development only):

- export MQTT_TLS_ENABLED=false
- export MQTT_BROKERS=tcp://localhost:1883
- export REDIS_ADDRESSES=localhost:6379
- go build -o bin/consumer ./cmd/consumer
- ./bin/consumer --log-level=debug

TLS mTLS example:

- export MQTT_BROKERS=ssl://broker.example.com:8883
- export MQTT_CA_CERT=/path/ca.crt
- export MQTT_CLIENT_CERT=/path/client.crt
- export MQTT_CLIENT_KEY=/path/client.key
- (Optional) export MQTT_USE_USER_PREFIX=true
- export REDIS_ADDRESSES=redis1:6379,redis2:6379
- ./bin/consumer --mqtt-qos=1 --log-format=json

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

Health, Readiness, Liveness

- Port: HEALTH_PORT (default 8080)
- Endpoints:
  - /health, /healthz, /heathz: checks Redis (Ping), MQTT (IsConnected), and processor state.
  - /ready: OK only when processor is running.
  - /live: always OK (process liveness).
- Per-component timeouts: HEALTH_REDIS_TIMEOUT, HEALTH_MQTT_TIMEOUT.

Security

- MQTT mTLS recommended in production (CA + client cert/key). ServerName is inferred from broker URL if not set.
- InsecureSkipVerify is never enabled automatically; use only for isolated test environments.
- Optional topic user-prefix: if MQTT_USE_USER_PREFIX=true and TLS is enabled, the CN from the client certificate is used as a topic prefix.

Performance & Concurrency

- Lock-free ring buffer with capacity auto-rounded to next power of two.
- Worker pool with lock-free message queue only; the processor never uses channel fallback. Saturation results in drops accounted in metrics.
- Backpressure:
  - oldest: make room by dropping the oldest messages (recommended for real-time streams).
  - newest: reject new inserts and count backpressure drops.
  - none: no drops; risk of timeouts or full queues.
- Zero-copy path for already-encoded JSON payloads when possible.

Observability & Metrics

- With LOG_LEVEL=debug, metrics snapshots are logged periodically (throughput, publish rate, error rate, active workers, queue depth, buffer utilization, etc.).
- Internal counters for received/published/acked/dropped, Redis/MQTT errors, processing/publish/ack times (ns aggregate).
- For Prometheus export, add an external HTTP exporter/handler that reads internal metrics (not provided here).

Operations
Graceful Shutdown:

- Signals: SIGINT/SIGTERM → cancel context → stop processor → shutdown health server → MQTT disconnect (WriteTimeout, capped by ShutdownTimeout) → close Redis → wait for goroutines.
  CPU Affinity (Linux):
- PIPELINE_CPU_AFFINITY applies best-effort process-level affinity (warns on failure; no-op on non-Linux).

Troubleshooting

- Cannot connect to MQTT:
  - Verify broker URL and port. Port 8883 implies TLS.
  - Check CA/Client cert/key and ServerName.
  - Increase MQTT_CONNECT_TIMEOUT or enable debug logs.
- Messages not acknowledged:
  - The MQTT-side service must publish acknowledgments like {"id":"...", "ack":true|false} to MQTT_SUBSCRIBE_TOPIC.
  - Ensure the Redis XREADGROUP ID is passed as id.
- High backpressure:
  - Increase PIPELINE_BUFFER_SIZE (power of two) and/or Batch/Flush, adjust drop policy, increase MaxWorkers.
- Redis NOGROUP:
  - The client auto-creates the group if missing; ensure permissions and stream name are correct.

License

- See LICENSE at repo root.

Additional Documentation

Note on package naming: Some Go files import internal/ports with the alias "core". The canonical package is internal/ports; documentation refers to these as "ports" interfaces.

- Each main Go folder contains a dedicated README.md describing purpose, API, flows, configuration, and operational notes.
