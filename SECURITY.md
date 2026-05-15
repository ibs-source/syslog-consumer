# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| latest  | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in syslog-consumer, please report it responsibly:

1. **Do not** open a public GitHub issue.
2. Use [GitHub's private vulnerability reporting](https://github.com/ibs-source/syslog-consumer/security/advisories/new) for this repository, or email <security@ibs.srl>.
3. Include: description, steps to reproduce, and impact assessment.
4. We will acknowledge receipt within 48 hours and provide a fix timeline.

## Scope

syslog-consumer is a message pipeline processing syslog data. Security concerns include:

- Denial of service via crafted Redis stream entries or MQTT payloads.
- TLS certificate mishandling or insecure defaults.
- Credential exposure through logs or environment leaks.
- Injection via malformed message fields propagated to downstream systems.

## Known Limits

The pipeline enforces the following bounds to prevent resource exhaustion:

| Parameter | Limit | Configuration |
|-----------|-------|---------------|
| Message queue depth | 500 (default) | `PIPELINE_MESSAGE_QUEUE_CAPACITY` |
| ACK buffer depth | 10000 (default) | `PIPELINE_BUFFER_CAPACITY` |
| Redis batch size | 20000 (default) | `REDIS_BATCH_SIZE` |
| MQTT pool size | 25 (default) | `MQTT_POOL_SIZE` |
| Memory limit | 2 GiB (default) | `GOMEMLIMIT` |

## Production Hardening

- **TLS/mTLS** for all MQTT connections (`MQTT_TLS_ENABLED=true`)
- **Non-root** container user in Dockerfile
- **Credentials** — the Dockerfile ships a sandbox `CERTIFICATE_DEPLOYER_KEY` for the isolated test cluster; in production this MUST be overridden at runtime via Docker secrets or an external KMS/vault. Never reuse the baked-in value outside the test cluster.
- **Certificate lifecycle** managed by `wrapper`/`manager` scripts with automatic renewal
- **GC tuning (runtime, set by the binary if unset)**: `GOGC=200`, `GOMEMLIMIT=2GiB`
- **GC tuning (build-time, baked into the binary)**: `GOEXPERIMENT=greenteagc`
