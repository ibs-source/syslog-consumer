# Contributing to Syslog Consumer

## Development Prerequisites

- **Go 1.25+**
- **Redis 6+** (for integration tests, or use miniredis)
- Make

## Getting Started

```bash
git clone https://github.com/ibs-source/syslog-consumer.git
cd syslog-consumer
make test
```

## Running Tests

```bash
make test        # All tests
make test-race   # With race detector
make test-cover  # Coverage report
make vet         # Static analysis
make lint        # golangci-lint
```

## Zero-Allocation Hot Path

The publish hot path (`buildPayload`, `severityName`) must produce **0 allocations per operation**.
This is enforced by benchmarks with `-benchmem`.

Before submitting a change, run:
```bash
go test -bench=. -benchmem ./internal/hotpath/
```

Every hot-path benchmark line must show `0 B/op` and `0 allocs/op`.

## Code Style

- Follow standard Go conventions (`gofmt`, `goimports`)
- Run `golangci-lint run ./...` (see `.golangci.yml`)
- Comments in English, no abbreviations
- Every exported function and type must have a godoc comment
- Zero unnecessary `nolint` directives — fix the code when possible

## Architecture

```
cmd/consumer/main.go       — Application entry point, signal handling, lifecycle
internal/config/            — Environment-based configuration with validation
internal/hotpath/           — Pipeline orchestrator (fetch, publish, claim, cleanup, refresh)
internal/redis/             — Redis Streams client with multi-stream support
internal/mqtt/              — MQTT client, connection pool, ACK parsing
internal/compress/          — Zstd compression utilities
internal/message/           — Message types (RedisMessage, AckMessage)
internal/health/            — HTTP health check server
internal/log/               — Structured logger
```

## Design Principles

1. **Stateless design.** All state in Redis — no local caching.
2. **Self-contained messages.** Each message carries all metadata for processing.
3. **Fail-fast configuration.** Validate all settings at startup.
4. **Horizontal scalability.** Add instances without coordination.
5. **Lock-free hot path.** Go channels and atomic operations — no mutexes.

## PGO

After significant hot-path changes, regenerate the PGO profile:
```bash
make pgo
make build-pgo
```

## License

By contributing, you agree that your contributions will be licensed under the same license as this project.
