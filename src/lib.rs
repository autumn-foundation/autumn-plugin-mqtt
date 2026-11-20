//! An MQTT v5 client for Autumn.
//!
//! Add [`MqttPlugin`] to the app. Then use the [`MqttClient`] extractor in a handler,
//! or [`MqttClient::from_state`] in a job or a task.
//!
//! ```rust,no_run
//! use autumn_plugin_mqtt::{MqttClient, MqttPlugin, Qos};
//! use autumn_web::prelude::*;
//!
//! #[get("/telemetry/{device}")]
//! async fn telemetry(client: MqttClient, Path(device): Path<String>) -> AutumnResult<String> {
//!     let topic = format!("devices/{device}/command");
//!     client
//!         .publish(topic, Qos::AtLeastOnce, b"ping".to_vec())
//!         .await
//!         .map_err(|err| err.into_autumn())?;
//!     Ok("sent".to_owned())
//! }
//!
//! # async fn run() {
//! autumn_web::app()
//!     .plugin(
//!         MqttPlugin::new().on_message("sensors/#", Qos::AtLeastOnce, |message| async move {
//!             println!("{}: {} bytes", message.topic, message.payload.len());
//!         }),
//!     )
//!     .routes(routes![telemetry])
//!     .run()
//!     .await;
//! # }
//! ```
//!
//! The plugin reads `[mqtt]` in `autumn.toml`. See [`config`] for the keys.
//!
//! # What the plugin gives
//!
//! - [`MqttClient`]: publish and subscribe over MQTT v5, with topic filters and `QoS` selection.
//! - Message routing: `on_message` registers a handler per topic filter. Each incoming message
//!   spawns one background task per matching handler. A handler can enqueue an Autumn `#[job]`
//!   from inside to run durable background work.
//! - TLS: CA certificate plus optional client certificate and key, from PEM files.
//! - A last will, an exponential reconnect backoff, and re-subscribe on reconnect.
//! - A health indicator (`mqtt`, health-only) and Prometheus metrics (`mqtt_*`).
//!
//! # Limits
//!
//! - The plugin boots even when the broker is down. The supervisor reconnects in the background
//!   and the health indicator reports down until the first poll succeeds.
//! - `publish` queues while offline and fails only when the request channel is full.
//! - rumqttc drives the reconnect inside `poll()`. The plugin adds the backoff wait between
//!   errors and re-subscribes every registered filter, because rumqttc does not re-subscribe.
//! - Logs and error text never hold secrets: no passwords, payloads, or certificate content.

mod client;
pub mod config;
mod error;
mod health;
mod metrics;
mod plugin;

pub use client::{ConnectionState, MessageHandler, MqttClient, MqttMessage, topic_matches};
pub use config::{ConfigError, DEFAULT_SECTION, MqttConfig, Qos};
pub use error::{ErrorKind, MqttError};
pub use plugin::{MQTT_SUBSCRIBER, MqttPlugin, PLUGIN_NAME};

/// Compiles the Rust snippets of the README as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
