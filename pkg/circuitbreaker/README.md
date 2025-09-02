# pkg/circuitbreaker

Lightweight sliding‑window circuit breaker with atomic state transitions and bounded concurrency. Protects external calls (e.g., MQTT publish) from cascading failures and provides minimal runtime statistics.

Goals

- Simple, dependency‑free circuit breaker suitable for high‑throughput paths.
- Sliding window with request/error accounting and half‑open probing.
- Concurrency limit to cap outstanding operations.
- Safe integer conversions and guardrails to avoid overflows.

State Machine

- closed: all requests allowed; error rate is monitored over a recent window.
- open: requests are rejected until a timeout elapses.
- half‑open: a limited trial phase; on consecutive successes, transitions to closed; any failure re‑opens.

API (selected)

- New(name string, errorThreshold float64, successThreshold int, timeout time.Duration, maxConcurrent int, volumeThreshold int) \*CircuitBreaker
- Execute(fn func() error) error
- GetState() string // "closed" | "open" | "half-open"
- GetStats() ports.CircuitBreakerStats // requests, successes, failures, consecutive failures, state

Behavior

- Execute returns ErrOpenState if the breaker is open.
- Execute returns ErrTooManyConcurrentRequests if the concurrency limit is exceeded.
- Error threshold is a percentage (0–100); volume threshold is the minimum request count before decisions.
- Half‑open closes only after reaching successThreshold consecutive successes.

Configuration (ENV, Flags, Description)
The breaker is usually configured via the application’s config (see internal/config). For reference:

| ENV                               | Flag                   | Default | Description                                                        |
| --------------------------------- | ---------------------- | ------- | ------------------------------------------------------------------ |
| CIRCUIT_BREAKER_ENABLED           | --cb-enabled           | true    | Enable circuit breaker on protected paths.                         |
| CIRCUIT_BREAKER_ERROR_THRESHOLD   | --cb-error-threshold   | 50.0    | Error rate (%) to transition open (when volume threshold reached). |
| CIRCUIT_BREAKER_SUCCESS_THRESHOLD | --cb-success-threshold | 5       | Consecutive successes to close from half‑open.                     |
| CIRCUIT_BREAKER_TIMEOUT           | --cb-timeout           | 30s     | Time to remain open before attempting half‑open.                   |
| CIRCUIT_BREAKER_MAX_CONCURRENT    | --cb-max-concurrent    | 100     | Max concurrent Execute() operations.                               |
| CIRCUIT_BREAKER_REQUEST_VOLUME    | --cb-request-volume    | 20      | Minimum request count to evaluate error rate.                      |

Example

```go
cb := circuitbreaker.New(
  "mqtt-publish",
  50.0,          // error threshold %
  5,             // success threshold to close
  30*time.Second,// open timeout
  100,           // max concurrent
  20,            // request volume threshold
)

err := cb.Execute(func() error {
  // invoke protected operation, e.g., publish to MQTT
  return mqttClient.Publish(ctx, topic, qos, false, payload)
})
if err != nil {
  // handle open state, concurrency limit, or operation error
}
```

Notes

- Statistics are approximate over a sliding window and intended for telemetry/health, not billing‑grade accounting.
- Keep thresholds conservative in high‑variance environments to avoid flapping.
