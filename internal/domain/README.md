# internal/domain

Core domain types shared across the processing pipeline. These types are implementation-agnostic and used by ports/adapters to exchange data and metrics with minimal allocations.

Scope

- Message and acknowledgment structures used by the processor and adapters.
- Metrics counters/gauges and snapshot utilities.
- Buffer pool for temporary allocations.

Data Types

Message

```go
type Message struct {
  ID        string
  Timestamp time.Time
  Data      []byte
  Attempts  int32
}
```

- Data is a JSON payload as bytes to preserve zero‑copy paths when possible.
- Attempts increments on retry.

AckMessage

```go
type AckMessage struct {
  ID  string `json:"id"`
  Ack bool   `json:"ack"`
}
```

- Expected JSON published by the MQTT-side service to acknowledge processing of the Redis message with the corresponding ID.
- Ack=true → ack & delete from Redis stream; Ack=false → negative ack (may be retried).

Metrics

```go
type Metrics struct {
  MessagesReceived  atomic.Uint64
  MessagesPublished atomic.Uint64
  MessagesAcked     atomic.Uint64
  MessagesDropped   atomic.Uint64
  ProcessingTimeNs  atomic.Uint64
  PublishLatencyNs  atomic.Uint64
  AckLatencyNs      atomic.Uint64
  ActiveWorkers     atomic.Int32
  QueueDepth        atomic.Int32
  MemoryUsedBytes   atomic.Uint64
  CPUPercent        atomic.Uint64
  RedisErrors       atomic.Uint64
  MQTTErrors        atomic.Uint64
  ProcessingErrors  atomic.Uint64
  BackpressureDropped atomic.Uint64
  BufferUtilization   atomic.Uint64
  StartTime         time.Time
}
```

Convenience Methods

- Rates:
  - GetThroughputRate() float64
  - GetPublishRate() float64
  - GetErrorRate() float64
- Latencies (averages over totals):
  - GetAverageProcessingTime() float64 // ns
  - GetAveragePublishLatency() float64 // ns
- Snapshot:
  - Snapshot() MetricsSnapshot with point‑in‑time counters, rates and aggregates.

Buffer Pool

```go
var BufferPool = sync.Pool{
  New: func() any { return new(bytes.Buffer) },
}
```

- Reusable buffer to reduce allocations in hot paths (e.g., payload construction).

Configuration

- This package defines data structures only; it does not expose configuration. See higher‑level packages for configuration tables (root README and internal/config).
