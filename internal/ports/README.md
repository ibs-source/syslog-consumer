# internal/ports

Stable “ports” interfaces that decouple the core application from concrete implementations (Redis, MQTT, logging, metrics, circuit breaker). Adapters reside under internal/_ and pkg/_ and implement these interfaces.

Goals

- Provide a narrow, testable surface across layers.
- Allow swapping implementations without touching business logic.
- Enable deterministic unit tests with fakes/mocks.

Key Interfaces (overview)

- RedisClient: Redis Streams operations for consumer groups, pending/claim/drain, and health.
- MQTTClient: Connect, liveness, publish/subscribe/unsubscribe, and certificate-based user prefix.
- Logger: Leveled, structured logging with fields and contextual loggers.
- CircuitBreaker: Execute with protection and inspect state/stats.
- Metrics: Exposed via domain package (counters/gauges) but referenced by processor.
- RetryPolicy/BackoffStrategy: Types used to describe retry behavior (strategy implemented within processor).

Selected Signatures

RedisClient

- CreateConsumerGroup(ctx, stream, group, startID) error
- ReadMessages(ctx, group, consumer, stream string, count int64, block time.Duration) ([]\*domain.Message, error)
- AckMessages(ctx, stream, group string, ids ...string) error
- DeleteMessages(ctx, stream string, ids ...string) error
- ClaimPendingMessages(ctx, stream, group, consumer string, minIdle time.Duration, count int64) ([]\*domain.Message, error)
- GetPendingMessages(ctx, stream, group, start, end string, count int64) ([]PendingMessage, error)
- GetConsumers(ctx, stream, group string) ([]ConsumerInfo, error)
- RemoveConsumer(ctx, stream, group, consumer string) error
- ReadStreamMessages(ctx, stream, start string, count int64) ([]\*domain.Message, error)
- GetStreamInfo(ctx, stream string) (\*StreamInfo, error)
- GetConsumerGroupInfo(ctx, stream, group string) (\*ConsumerGroupInfo, error)
- GetConsumerName() string
- Ping(ctx context.Context) error
- Close() error

MQTTClient

- Connect(ctx context.Context) error
- Disconnect(timeout time.Duration)
- IsConnected() bool
- Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte) error
- Subscribe(ctx context.Context, topic string, qos byte, handler MessageHandler) error
- Unsubscribe(ctx context.Context, topics ...string) error
- GetUserPrefix() string

Logger

- Trace/Debug/Info/Warn/Error/Fatal(msg string, fields ...Field)
- WithFields(fields ...Field) Logger

CircuitBreaker

- Execute(fn func() error) error
- GetState() string
- GetStats() CircuitBreakerStats

Data Types (selected)

- Field{ Key string, Value any } for structured logging fields.
- PendingMessage, ConsumerInfo, StreamInfo, ConsumerGroupInfo.
- CircuitBreakerStats{ Requests, TotalSuccess, TotalFailure, ConsecutiveFailures, State }.

Configuration

- This package defines interfaces only; it has no configuration of its own.
- See concrete adapters for configuration tables (internal/redis, internal/mqtt, etc.).

Note on import aliases

- Some Go files import internal/ports using the alias "core". The canonical package name is internal/ports; documentation and comments refer to these as "ports" interfaces.
