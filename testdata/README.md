# testdata

TLS materials for local testing only. These files are intended for unit/integration tests (e.g., MQTT TLS client auth scenarios) and must never be used in production environments.

Contents

- `authority.pem` — Test Certificate Authority (CA) certificate.
- `certificate.pem` — Client certificate signed by the test CA.
- `key.pem` — Client private key matching `certificate.pem`.

Intended Use

- Local TLS-enabled MQTT connections during tests.
- Validating client-certificate (mTLS) flows end-to-end without relying on external PKI.

Security Notice

- Private keys are for testing only and are not secure.
- Do not deploy these files to production systems or CI environments that are not isolated.
- Rotate/remove these artifacts when publishing public releases.

Configuration (reference)

- `MQTT_CA_CERT` → path to `authority.pem`
- `MQTT_CLIENT_CERT` → path to `certificate.pem`
- `MQTT_CLIENT_KEY` → path to `key.pem`
- `MQTT_TLS_ENABLED=true` and broker URL `ssl://host:8883` to enable TLS in tests.

Example (local)

```bash
export MQTT_TLS_ENABLED=true
export MQTT_BROKERS=ssl://localhost:8883
export MQTT_CA_CERT=./testdata/authority.pem
export MQTT_CLIENT_CERT=./testdata/certificate.pem
export MQTT_CLIENT_KEY=./testdata/key.pem
```
