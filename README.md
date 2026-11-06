# autumn-plugin-mqtt

An MQTT v5 client for [Autumn](https://autumn-web.app), built on
[rumqttc](https://github.com/bytebeamio/rumqtt). Publish telemetry from a
handler, subscribe with topic filters, route messages into background work,
and watch the broker on `/health` and Prometheus metrics.

## Quickstart

Add the plugin:

```rust,no_run
use autumn_plugin_mqtt::{MqttClient, MqttPlugin, Qos};
use autumn_web::prelude::*;

#[get("/telemetry/{device}")]
async fn telemetry(client: MqttClient, Path(device): Path<String>) -> AutumnResult<String> {
    client
        .publish(format!("devices/{device}/command"), Qos::AtLeastOnce, b"ping".to_vec())
        .await
        .map_err(|err| err.into_autumn())?;
    Ok("sent".to_owned())
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(MqttPlugin::new())
        .routes(routes![telemetry])
        .run()
        .await;
}
```

Configure `[mqtt]` in `autumn.toml`:

```toml
[mqtt]
broker_url = "mqtt://broker.local"
client_id = "line-1"
```

Or override with the environment: `AUTUMN_MQTT__BROKER_URL`,
`AUTUMN_MQTT__CLIENT_ID`, and so on for every key. See
`src/config.rs` for the full list.

## Receiving messages

Register a handler per topic filter. Each incoming message spawns one
background task per matching handler:

```rust
use autumn_plugin_mqtt::{MqttPlugin, Qos};

fn plugin() -> MqttPlugin {
    MqttPlugin::new().on_message("sensors/#", Qos::AtLeastOnce, |message| async move {
        let payload = message.payload_str().unwrap_or("<binary>");
        println!("{}: {payload}", message.topic);
    })
}
```

A handler can enqueue an Autumn `#[job]` from inside to run durable
background work. The handlers themselves are fire-and-forget tasks: they
must not panic, and they should do their durable work through the job
queue.

Dynamic subscriptions are also possible at runtime:

```rust
use autumn_plugin_mqtt::{MqttClient, MqttError, Qos};

async fn watch_status(client: &MqttClient) -> Result<(), MqttError> {
    client.subscribe("devices/+/status", Qos::AtLeastOnce).await?;
    Ok(())
}
```

## TLS

Use an `mqtts://` (or `tls://`, `ssl://`) broker URL and point at PEM files:

```toml
[mqtt]
broker_url = "mqtts://broker.local"
client_id = "line-1"
tls_ca_cert = "/etc/mqtt/ca.pem"
# Optional mutual TLS:
tls_client_cert = "/etc/mqtt/client.pem"
tls_client_key = "/etc/mqtt/client-key.pem"
```

A last will uses flat keys under `[mqtt]` — `will_topic`, `will_payload`,
`will_qos`, `will_retain`.

## Health and metrics

- Health indicator `mqtt` (health-only): down before startup and while the
  supervisor is reconnecting, up once the event loop has polled.
- Prometheus metrics: `mqtt_messages_published_total`,
  `mqtt_messages_received_total`, `mqtt_publish_errors_total`,
  `mqtt_connection_errors_total`, `mqtt_connected`, each labeled with
  `client_id` and broker `host`.

## Known issues

- The plugin boots even when the broker is down; the supervisor reconnects
  in the background with exponential backoff (`reconnect_initial_ms` /
  `reconnect_max_ms`).
- rumqttc drives reconnects inside its event loop; the plugin adds the
  backoff wait between errors and re-subscribes every registered filter,
  because rumqttc does not re-subscribe for you.
- `publish` queues while offline and only fails when the request channel is
  full.
- The connection health state is best-effort, based on event-loop polls.
- Dynamic `subscribe` registrations live in memory and are restored after a
  reconnect, not after a process restart.
- Secrets (passwords, payloads, certificate content) never appear in logs or
  error text.

## License

Apache-2.0. See [LICENSE](LICENSE).
