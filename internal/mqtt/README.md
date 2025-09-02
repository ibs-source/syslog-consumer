# internal/mqtt

MQTT client implementation for the Syslog Consumer, built on Eclipse Paho. It provides:

- A lock-free subscription handler registry (copy-on-write map with atomic pointer).
- Secure TLS/mTLS configuration with CA and client certificates.
- Optional user-prefix extraction from the client certificate’s CN to prefix topics.
- Robust connection lifecycle with bounded waits, retries, and reconnection settings.

Scope

- Create and manage a single MQTT client instance.
- Connect/Disconnect with bounded waits respecting context and configured timeouts.
- Publish/Subscribe/Unsubscribe with per-call timeouts.
- Maintain connection state and auto re-subscribe on reconnect.
- Build full topics using optional user prefix and/or custom prefix rules.

Design Highlights

- Lock-free handler registry: copy-on-write immutable map behind an atomic pointer to avoid mutexes on hot paths.
- Bounded waiting: publish/subscribe/unsubscribe calls use `WriteTimeout`, shortened by context deadlines if present.
- TLS/mTLS: strict verification (InsecureSkipVerify is not set by default); ServerName inferred from broker URL if not provided.
- User Prefix: if TLS is enabled and a client certificate is provided, the CN is extracted and used as topic prefix when `UseUserPrefix` is true.

Public Behavior Summary

- Connect(ctx): attempts connection within min(ctx deadline, MQTT_CONNECT_TIMEOUT).
- IsConnected(): double-checked connection status (client + atomic flag).
- Publish(ctx, topic, qos, retained, payload): topic is expanded with prefixing rules; waits up to `MQTT_WRITE_TIMEOUT` or ctx deadline.
- Subscribe(ctx, topic, qos, handler): registers the handler lock-free and subscribes with bounded wait.
- Unsubscribe(ctx, topics...): removes handlers lock-free and unsubscribes with bounded wait.
- GetUserPrefix(): returns the extracted user prefix from client certificate CN (if any).

Configuration (ENV, Flags, Description)
Precedence: defaults → environment variables → CLI flags.

MQTT Core
| ENV | Flag | Default | Description |
|---|---|---|---|
| MQTT_BROKERS | --mqtt-broker | tcp://localhost:1883 | Broker URL(s). ssl://host:8883 enables TLS. |
| MQTT_CLIENT_ID | --mqtt-client-id | generated | Client identifier. |
| MQTT_QOS | --mqtt-qos | 2 | QoS level (0,1,2). |
| MQTT_KEEP_ALIVE | --mqtt-keep-alive | 30s | Keepalive interval. |
| MQTT_CONNECT_TIMEOUT | --mqtt-connect-timeout | 10s | Connect timeout (upper bound for Connect). |
| MQTT_MAX_RECONNECT_DELAY | --mqtt-max-reconnect-delay | 2m | Max auto-reconnect backoff. |
| MQTT_CLEAN_SESSION | --mqtt-clean-session | true | Clean session toggle. |
| MQTT_ORDER_MATTERS | --mqtt-order-matters | true | Enforce in-order message handling. |
| MQTT_WRITE_TIMEOUT | --mqtt-write-timeout | 5s | Publish/subscribe/unsubscribe wait timeout. |
| MQTT_MESSAGE_CHANNEL_DEPTH | --mqtt-message-channel-depth | 0 | Internal client channel depth. |
| MQTT_MAX_INFLIGHT | --mqtt-max-inflight | 1000 | Max inflight messages. |

Topics & Prefixing
| ENV | Flag | Default | Description |
|---|---|---|---|
| MQTT_PUBLISH_TOPIC | --mqtt-publish-topic | syslog | Publish topic (base). |
| MQTT_SUBSCRIBE_TOPIC | --mqtt-subscribe-topic | syslog/acknowledgement | Ack/feedback topic. |
| MQTT_USE_USER_PREFIX | --mqtt-use-user-prefix | true | Prefix topics with cert CN (if TLS). |
| MQTT_CUSTOM_PREFIX | --mqtt-custom-prefix | | Custom prefix if user prefix disabled. |

TLS/mTLS
| ENV | Flag | Default | Description |
|---|---|---|---|
| MQTT_TLS_ENABLED | | true | Enable TLS/mTLS. |
| MQTT_CA_CERT | --mqtt-ca-cert | | CA certificate path. |
| MQTT_CLIENT_CERT | --mqtt-client-cert | | Client certificate path. |
| MQTT_CLIENT_KEY | --mqtt-client-key | | Client key path. |
| MQTT_TLS_INSECURE | --mqtt-tls-insecure | false | Skip TLS verify (testing only). |
| MQTT_TLS_SERVER_NAME | --mqtt-tls-server-name | inferred | Override expected server name. |
| MQTT_TLS_MIN_VERSION | --mqtt-tls-min-version | TLS1.2 | Minimum TLS version. |
| MQTT_TLS_CIPHER_SUITES | --mqtt-tls-cipher-suites | | Allowed cipher suites (CSV). |
| MQTT_TLS_PREFER_SERVER_CIPHERS | --mqtt-tls-prefer-server-ciphers | false | Prefer server ciphers. |

Operational Notes

- On reconnect, the client re-subscribes to previously registered topics.
- When using user prefixing, ensure the CN format is suitable for topic naming.
- Disconnection waits are bounded by `MQTT_WRITE_TIMEOUT` but capped at the application shutdown deadline.

Example

```go
cli, _ := mqtt.NewClient(cfg, logger)
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := cli.Connect(ctx); err != nil {
  logger.Error("connect failed", ports.Field{Key:"error", Value: err})
  return
}

_ = cli.Subscribe(ctx, cfg.MQTT.Topics.SubscribeTopic, cfg.MQTT.QoS,
  func(topic string, payload []byte) { /* handle ack */ })

payload := []byte(`{"message":{"payload":{}}}`)
_ = cli.Publish(ctx, cfg.MQTT.Topics.PublishTopic, cfg.MQTT.QoS, false, payload)
```
