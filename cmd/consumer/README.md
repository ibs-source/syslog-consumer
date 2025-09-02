# cmd/consumer

Entry point that boots the Syslog Consumer application. It wires configuration, logging, Redis, MQTT, stream processor, and the HTTP health server. It also manages lifecycle, signals, graceful shutdown, and optional metrics snapshots in debug mode.

Scope

- Load configuration (defaults → env → flags)
- Initialize logger (Logrus adapter)
- Initialize Redis and MQTT clients and wait until both are ready
- Build circuit breaker for MQTT publish path
- Start the stream processor (Redis → ring buffer → workers → MQTT)
- Start HTTP health server (if enabled)
- Handle SIGINT/SIGTERM for graceful shutdown
- Log periodic metrics snapshots when LOG_LEVEL=debug

Lifecycle & Control Flow

1. Load config and initialize logger.
2. Apply CPU affinity if configured (best-effort; Linux-only).
3. Create Redis client and wait until Ping succeeds (with retry).
4. Create MQTT client and connect (with retry).
5. Initialize circuit breaker for publish path.
6. Start StreamProcessor and, if enabled, the health server.
7. On SIGINT/SIGTERM: cancel context → stop processor → shutdown health server → MQTT disconnect (respecting WriteTimeout, capped by ShutdownTimeout) → close Redis → wait for goroutines to finish.

Health Endpoints

- /health, /healthz, /heathz: checks Redis ping, MQTT connection, and processor state.
- /ready: OK only if the processor state is “running”.
- /live: always OK (process liveness).
- Port controlled by HEALTH_PORT; timeouts by HEALTH_READ_TIMEOUT, HEALTH_WRITE_TIMEOUT.

Metrics Snapshots (debug mode)

- When LOG_LEVEL=debug, logs periodic snapshots with throughput, publish rate, error rate, active workers, queue depth, buffer utilization, and error counters.

Configuration (ENV, Flags, Description)
Precedence: defaults → environment variables → CLI flags. The binary consumes the full configuration surface. Key settings most relevant to the entrypoint and lifecycle are summarized here; see the repo root README for the exhaustive list.

App & Logging
| ENV | Flag | Default | Description |
|---|---|---|---|
| APP_NAME | --app-name | syslog-consumer | Application name (informational). |
| APP_ENV | --app-env | production | Deployment environment label. |
| LOG_LEVEL | --log-level | info | Log level: trace, debug, info, warn, error, fatal, panic. |
| LOG_FORMAT | --log-format | json | Log format: json or text. |
| APP_SHUTDOWN_TIMEOUT | --app-shutdown-timeout | 30s | Global graceful shutdown deadline. |
| APP_PENDING_OPS_GRACE | --app-pending-ops-grace-ms | 500ms | Wait after unsubscribe/stop to allow pending ops to settle. |

Health Server
| ENV | Flag | Default | Description |
|---|---|---|---|
| HEALTH_ENABLED | --health-enabled | true | Enable the HTTP health server. |
| HEALTH_PORT | --health-port | 8080 | Port for health/readiness/liveness endpoints. |
| HEALTH_READ_TIMEOUT | --health-read-timeout | 5s | HTTP server read timeout. |
| HEALTH_WRITE_TIMEOUT | --health-write-timeout | 5s | HTTP server write timeout. |
| HEALTH_CHECK_INTERVAL | --health-check-interval | 10s | Internal health check interval (used in background loops). |
| HEALTH_REDIS_TIMEOUT | --health-redis-timeout | 2s | Timeout for Redis health ping. |
| HEALTH_MQTT_TIMEOUT | --health-mqtt-timeout | 2s | Timeout for MQTT checks. |

MQTT (shutdown interaction)
| ENV | Flag | Default | Description |
|---|---|---|---|
| MQTT_WRITE_TIMEOUT | --mqtt-write-timeout | 5s | Used for publish/subscribe waits; also used to bound MQTT disconnect wait, capped by APP_SHUTDOWN_TIMEOUT. |

CPU Affinity (Linux best-effort)
| ENV | Flag | Default | Description |
|---|---|---|---|
| PIPELINE_CPU_AFFINITY | --pipeline-cpu-affinity | | Comma-separated CPU IDs for process affinity; best-effort on Linux only. |

Run

- go build -o bin/consumer ./cmd/consumer
- ./bin/consumer --log-level=debug

Signals

- SIGINT / SIGTERM initiate graceful shutdown.

Notes

- The main will keep retrying until Redis and MQTT are both ready, unless the process is terminated.
- Circuit breaker protects MQTT publish path using thresholds and timeouts defined in configuration.
