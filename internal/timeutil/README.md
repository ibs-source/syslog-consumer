# internal/timeutil

Small utilities around time handling focused on correctness and linter-friendly duration construction.

Scope

- Provide helpers to construct `time.Duration` from integer counts without multiplying two durations (avoids durationcheck warnings).
- Keep conversions safe for negative inputs and overflow.

API

- FromMillis(ms int64) time.Duration
  - Converts a non-negative millisecond count into a `time.Duration`.
  - Negative inputs return 0.
  - Implementation multiplies integer nanoseconds, not duration-by-duration.

Example

```go
d := timeutil.FromMillis(250) // 250ms
time.Sleep(d)
```

Notes

- Prefer these helpers in code paths where durations are computed from integers (config/env/flags) to keep linters happy and avoid accidental double-duration multiplication.
