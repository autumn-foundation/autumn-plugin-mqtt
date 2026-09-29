# CLAUDE.md — autumn-plugin-mqtt

## What this is

An MQTT v5 client plugin for Autumn, built on rumqttc 0.25. Publishes from
handlers, routes subscribed messages into per-filter handler tasks, and
exposes a health indicator plus Prometheus metrics.

## Layout

- `src/lib.rs` — crate docs, public re-exports.
- `src/plugin.rs` — `MqttPlugin` builder and the `Plugin` impl.
- `src/client.rs` — `MqttClient`, `MqttMessage`, the topic matcher, the
  supervisor event loop.
- `src/config.rs` — layered `[mqtt]` config, broker URL parsing, topic
  validation.
- `src/error.rs` — `MqttError` with HTTP status mapping.
- `src/health.rs` — health-only `mqtt` indicator.
- `src/metrics.rs` — `mqtt_*` metrics source.
- `src/<module>/tests.rs` — one test module per source module.
- `examples/subscribe.rs` — runnable subscriber example.

## Rules

- Rust edition 2024, `rust-version = "1.88"`, Apache-2.0.
- Production code: no `unwrap`/`expect`/`panic`/`todo!`/`unimplemented!`,
  no `unsafe`. Tests may use `unwrap`/`expect`.
- Secrets never reach logs or error text (no passwords, payloads, cert
  content).
- Autumn APIs are grounded through `~/workspace/skills/autumn-mcp/bin/mcp.py`
  — never from training memory.
- House style follows `autumn-foundation/autumn-plugin-rocksdb`.

## Gates

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR=~/workspace/autumn-arena/target
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
TMPDIR=~/workspace/.tmp-cargo cargo test --locked --all-targets --all-features
```

Never run concurrent cargo builds and never clean the shared target dir.

## Design notes

- rumqttc's `EventLoop` reconnects on its own inside `poll()`; the plugin's
  supervisor only adds the exponential backoff wait between errors and
  re-subscribes every registered filter, because rumqttc does not
  re-subscribe.
- Connection health is best-effort: `Connected` is set when an event-loop
  poll succeeds, `Reconnecting` when it errors.
- Message handlers run as fire-and-forget Tokio tasks; durable work belongs
  in an Autumn `#[job]` enqueued from the handler.
- The `mqtt` health indicator is `IndicatorGroup::HealthOnly` — it never
  gates readiness.
