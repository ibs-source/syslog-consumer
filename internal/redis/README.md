# internal/redis

Redis Streams client for the Syslog Consumer built on go-redis v9. Implements the `ports.RedisClient` interface with conversion helpers, transient error retries, and consumer group utilities (create, claim, drain, cleanup).

Scope

- Manage Redis connection(s) via UniversalClient (standalone/sentinel/cluster).
- Consumer group lifecycle: create if missing, read with XREADGROUP, ack/delete, claim stale, drain unassigned.
- Conversion helpers to build payload bytes with minimal allocations.
- Transient error handling with retry/backoff bounded by config.

Key Operations (ports.RedisClient)

- CreateConsumerGroup(ctx, stream, group, startID)
- ReadMessages(ctx, group, consumer, stream, count, block)
- AckMessages(ctx, stream, group, ids...)
- DeleteMessages(ctx, stream, ids...)
- ClaimPendingMessages(ctx, stream, group, consumer, minIdle, count)
- GetPendingMessages(ctx, stream, group, start, end, count)
- GetConsumers(ctx, stream, group)
- RemoveConsumer(ctx, stream, group, consumer)
- ReadStreamMessages(ctx, stream, start, count)
- GetStreamInfo(ctx, stream)
- GetConsumerGroupInfo(ctx, stream, group)
- GetConsumerName() string
- Ping(ctx), Close()

Behavior & Error Handling

- NOGROUP: auto-create group where appropriate and treat as non-fatal in certain flows.
- redis.Nil: treated as “no data” (non-error) in read flows.
- Transient errors (e.g., LOADING, timeouts, connection reset): retried up to `REDIS_MAX_RETRIES` with `REDIS_RETRY_INTERVAL`.
- Ack+Delete batching: XACK then XDEL via pipeline; redis.Nil and NOGROUP treated as benign.

Payload Conversion

- If field `payload` exists and is already JSON (string/[]byte starting with `{` or `[`), forward as-is (zero-copy path).
- Otherwise, JSON-encode the value or, if absent, encode the entire field map.

Configuration (ENV, Flags, Description)
Precedence: defaults → environment variables → CLI flags.

Core
| ENV | Flag | Default | Description |
|---|---|---|---|
| REDIS_ADDRESSES | --redis-addr | localhost:6379 | Comma-separated Redis addresses. |
| REDIS_PASSWORD | --redis-password | | Redis password. |
| REDIS_DB | --redis-db | 0 | Redis DB index. |
| REDIS_STREAM | --redis-stream | syslog-stream | Stream name. |
| REDIS_CONSUMER_GROUP | --redis-group | syslog-group | Consumer group name. |
| REDIS_MAX_RETRIES | --redis-client-retries | 5 | Max transient retries. |
| REDIS_RETRY_INTERVAL | --redis-client-retry-interval | 1s | Delay between retries. |
| REDIS_CONNECT_TIMEOUT | | 5s | Dial/connect timeout. |
| REDIS_READ_TIMEOUT | | 3s | Read timeout. |
| REDIS_WRITE_TIMEOUT | | 3s | Write timeout. |
| REDIS_POOL_SIZE | | CPU\*10 | Connection pool size. |
| REDIS_MIN_IDLE_CONNS | | CPU | Min idle connections. |
| REDIS_MAX_CONN_AGE | | 30m | Max connection lifetime. |
| REDIS_IDLE_TIMEOUT | | 5m | Connection idle timeout. |
| REDIS_POOL_TIMEOUT | | | Pool wait timeout. |
| REDIS_CONN_MAX_IDLE_TIME | | | Max idle time per connection. |

Consumption & Batching
| ENV | Flag | Default | Description |
|---|---|---|---|
| REDIS_BATCH_SIZE | --redis-batch-size | 100 | XREADGROUP count. |
| REDIS_BLOCK_TIME | --redis-batch-timeout | 5s | XREADGROUP block time. |

Claim & Drain
| ENV | Flag | Default | Description |
|---|---|---|---|
| REDIS_AGGRESSIVE_CLAIM | --redis-aggressive-claim | true | Repeatedly claim until no more messages each cycle. |
| REDIS_CLAIM_MIN_IDLE | | 1m | Minimum idle time before claiming a message. |
| REDIS_CLAIM_BATCH_SIZE | | 1000 | Number of messages to claim per cycle. |
| REDIS_CLAIM_CYCLE_DELAY | --redis-claim-cycle-delay | 1s | Delay between aggressive claim cycles. |
| REDIS_DRAIN_ENABLED | --redis-drain-enabled | true | Enable draining unassigned messages. |
| REDIS_DRAIN_INTERVAL | --redis-drain-interval | 1m | Interval for drain loop. |
| REDIS_DRAIN_BATCH_SIZE | --redis-drain-batch-size | 1000 | Messages to read during drain. |

Consumer Cleanup
| ENV | Flag | Default | Description |
|---|---|---|---|
| REDIS_CONSUMER_CLEANUP_ENABLED | --redis-consumer-cleanup-enabled | true | Periodically remove idle consumers. |
| REDIS_CONSUMER_IDLE_TIMEOUT | --redis-consumer-idle-timeout | 5m | Idle threshold for removal. |
| REDIS_CONSUMER_CLEANUP_INTERVAL | --redis-consumer-cleanup-interval | 1m | Cleanup loop interval. |

Example

```go
cli, _ := redis.NewClient(cfg, logger)
ctx := context.Background()

// Read via consumer group
msgs, _ := cli.ReadMessages(ctx, cfg.Redis.ConsumerGroup, cli.GetConsumerName(),
  cfg.Redis.StreamName, cfg.Redis.BatchSize, cfg.Redis.BlockTime)

// Ack & delete after successful processing
_ = cli.AckMessages(ctx, cfg.Redis.StreamName, cfg.Redis.ConsumerGroup, ids...)
_ = cli.DeleteMessages(ctx, cfg.Redis.StreamName, ids...)
```
