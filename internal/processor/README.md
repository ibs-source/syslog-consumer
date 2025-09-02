# internal/processor

Lock-free, zero-copy stream processing pipeline. Pulls batches from Redis Streams, buffers them in a generic ring buffer, and processes them concurrently via a worker pool to publish onto MQTT. Supports backpressure, retry with exponential backoff, DLQ publishing, and advanced Redis claim/drain routines.

Scope

- Orchestrate end-to-end processing from Redis → buffer → workers → MQTT.
- Manage lifecycle and state machine: idle → running → paused → stopping → stopped.
- Implement backpressure with configurable drop policy.
- Implement retry with exponential backoff and optional DLQ.
- Periodically claim stale messages and drain unassigned Redis messages.
- Export telemetry via metrics counters and gauges.

State Machine

- idle: initial state before Start.
- running: main loops active (consume/process/claim/backpressure).
- paused: loops temporarily idle until Resume.
- stopping: graceful stop in progress.
- stopped: processor fully stopped.

Main Loops

- consumeMessages: reads from Redis (XREADGROUP) and enqueues into the ring buffer.
- processMessages: dequeues batches and submits tasks to the worker pool (lock-free fast path).
- claimStaleMessages: claims messages from idle/abandoned consumers (single or aggressive mode).
- drainUnassignedMessages: (optional) drains messages that weren’t delivered to the group.
- monitorBackpressure: tracks buffer utilization and accounts for drop events (newest policy).

Concurrency Model

- Ring buffer: lock-free, capacity rounded to next power of two, try-put/try-get batch APIs.
- Worker pool: lock-free task queue with channel fallback; optional batch submission for throughput.
- Zero-copy JSON path for already-encoded payloads where possible.

Acknowledgment Flow

1. External service publishes an ack JSON to the configured MQTT ack topic: {"id": "<redis-id>", "ack": true|false}
2. Processor’s MQTT subscription handler:
   - If ack=true → XACK + XDEL the message from Redis.
   - If ack=false → negative acknowledgment (no delete); message may be retried depending on configuration.

Retry & DLQ

- Retry: exponential backoff with initial/max backoff and multiplier; stop conditions by attempt count.
- DLQ: if enabled, messages that exhaust retries are published to a DLQ topic with error context.

Backpressure

- Threshold-based: compares buffer usage against a configured fraction of capacity.
- Drop policy:
  - oldest: proactively free space by dropping the oldest messages to admit new ones.
  - newest: reject new inserts; account drops in metrics.
  - none: do not drop; may lead to timeouts or queue saturation.

Metrics (high level)

- Counters: MessagesReceived, MessagesPublished, MessagesAcked, MessagesDropped, ProcessingErrors, RedisErrors, MQTTErrors.
- Gauges: BufferUtilization (percent), ActiveWorkers, QueueDepth.
- Aggregates: ProcessingTimeNs, PublishLatencyNs, AckLatencyNs.

Configuration (ENV, Flags, Description)
Precedence: defaults → environment variables → CLI flags.

Pipeline
| ENV | Flag | Default | Description |
|---|---|---|---|
| PIPELINE_BUFFER_SIZE | --pipeline-buffer-size | 1048576 | Ring buffer capacity (rounded to power of 2). |
| PIPELINE_BATCH_SIZE | --pipeline-batch-size | 1000 | Batch size for processing. |
| PIPELINE_BATCH_TIMEOUT | --pipeline-batch-timeout | 100ms | Max wait to flush partial batch. |
| PIPELINE_PROCESSING_TIMEOUT | --pipeline-processing-timeout | 5s | Per-batch processing timeout. |
| PIPELINE_ZERO_COPY | --pipeline-zero-copy | true | Prefer zero-copy JSON path where possible. |
| PIPELINE_PREALLOCATE | --pipeline-preallocate | true | Pre-allocate internal buffers. |
| PIPELINE_NUMA_AWARE | --pipeline-numa-aware | false | NUMA awareness hint. |
| PIPELINE_CPU_AFFINITY | --pipeline-cpu-affinity | | Process CPU affinity (Linux best-effort). |
| PIPELINE_BACKPRESSURE_THRESHOLD | --pipeline-backpressure-threshold | 0.8 | Buffer usage threshold (0.0-1.0). |
| PIPELINE_DROP_POLICY | --pipeline-drop-policy | oldest | Drop policy: oldest|newest|none. |
| PIPELINE_FLUSH_INTERVAL | --pipeline-flush-interval-ms | 100ms | Periodic flush interval for partial batches. |
| PIPELINE_BACKPRESSURE_POLL_INTERVAL | --pipeline-backpressure-poll-interval | 1s | Backpressure/telemetry tick interval. |
| PIPELINE_IDLE_POLL_SLEEP | --pipeline-idle-poll-sleep-ms | 1ms | Sleep duration when idle. |

Retry
| ENV | Flag | Default | Description |
|---|---|---|---|
| PIPELINE_RETRY_ENABLED | --pipeline-retry-enabled | true | Enable retry logic on publish failure. |
| PIPELINE_RETRY_MAX_ATTEMPTS | --pipeline-retry-max-attempts | | Max attempts before giving up. |
| PIPELINE_RETRY_INITIAL_BACKOFF | --pipeline-retry-initial-backoff | | Initial backoff (seconds). |
| PIPELINE_RETRY_MAX_BACKOFF | --pipeline-retry-max-backoff | | Max backoff (seconds). |
| PIPELINE_RETRY_MULTIPLIER | --pipeline-retry-multiplier | | Exponential multiplier between attempts. |

DLQ
| ENV | Flag | Default | Description |
|---|---|---|---|
| | --pipeline-dlq-enabled | | Enable DLQ publish when retries exhausted. |
| | --pipeline-dlq-topic | | MQTT topic used for DLQ messages. |

Redis Claim/Drain (interaction)
| ENV | Flag | Default | Description |
|---|---|---|---|
| REDIS_AGGRESSIVE_CLAIM | --redis-aggressive-claim | true | Repeatedly claim until no more messages. |
| REDIS_CLAIM_MIN_IDLE | | 1m | Minimum idle time before claiming a message. |
| REDIS_CLAIM_BATCH_SIZE | | 1000 | Number of messages to claim per cycle. |
| REDIS_CLAIM_CYCLE_DELAY | --redis-claim-cycle-delay | 1s | Delay between aggressive claim cycles. |
| REDIS_DRAIN_ENABLED | --redis-drain-enabled | true | Enable draining unassigned messages. |
| REDIS_DRAIN_INTERVAL | --redis-drain-interval | 1m | Interval for drain loop. |
| REDIS_DRAIN_BATCH_SIZE | --redis-drain-batch-size | 1000 | Messages to read during drain. |
| REDIS_CONSUMER_CLEANUP_ENABLED | --redis-consumer-cleanup-enabled | true | Periodically remove idle consumers. |
| REDIS_CONSUMER_IDLE_TIMEOUT | --redis-consumer-idle-timeout | 5m | Idle threshold for removal. |
| REDIS_CONSUMER_CLEANUP_INTERVAL | --redis-consumer-cleanup-interval | 1m | Cleanup loop interval. |

Operational Notes

- The processor subscribes to the MQTT ack topic at start and unsubscribes during stop.
- On stop, it waits `APP_PENDING_OPS_GRACE` after unsubscribing to allow pending acknowledgments and final operations to settle.
- Backpressure “newest” policy accounts drops when insertion fails; “oldest” proactively frees space.
- The worker pool exports ActiveWorkers and QueueDepth via metrics for visibility.

Example

```go
sp := processor.NewStreamProcessor(cfg, redisClient, mqttClient, logger, metrics, cb, nil)
if err := sp.Start(ctx); err != nil {
  logger.Error("failed to start stream processor", ports.Field{Key:"error", Value: err})
}
// ... later:
_ = sp.Stop(ctx)
```
