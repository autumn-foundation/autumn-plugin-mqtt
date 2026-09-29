# Planning — autumn-plugin-mqtt

## Goal

An MQTT v5 client plugin for Autumn: publish from handlers, subscribe with
topic filters, route messages into background work, expose broker health and
metrics.

## Why rumqttc

The de-facto Rust MQTT client. v5 `AsyncClient` + `EventLoop` split maps
cleanly onto the plugin shape: the client handle lives in app state, the
event loop runs in a supervised Tokio task. TLS through rustls (no OpenSSL
dependency). No broker in the sandbox, so integration tests are out; the
pure parts (URL parsing, topic validation, matcher, config layering) carry
the coverage.

## Decisions

- Messages route into per-filter handler callbacks, not Autumn jobs
  directly. Handlers run as Tokio tasks and the docs tell users to enqueue a
  `#[job]` from the handler for durable work. This avoids inventing a
  job-registration runtime API.
- Health indicator is `HealthOnly`, never readiness. The plugin boots even
  when the broker is down; the supervisor reconnects in the background.
- rumqttc does not re-subscribe after reconnect, so the supervisor records
  every filter and re-subscribes after each error.
- `set_keep_alive` asserts at least 5 seconds in rumqttc 0.25, so config
  validation requires `5..=3600`.
- Secrets (passwords, payloads, cert content) never reach logs or errors.

## Open items

- No integration test against a real broker (no broker available in the
  sandbox). A CI job with mosquitto would close this.
- QoS 2 delivery semantics are rumqttc's; the plugin does not add dedup.
- Shared subscriptions (`$share/...`) are passed through to the broker, not
  specially validated.
