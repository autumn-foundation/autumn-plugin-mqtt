//! Call counters for the MQTT client. The plugin module adds them to Autumn.
//!
//! # Contract
//!
//! - `mqtt_messages_published_total` counts the publishes that reached the rumqttc channel.
//! - `mqtt_messages_received_total` counts the publishes the broker delivered.
//! - `mqtt_publish_errors_total` counts the failed publish and subscribe calls.
//! - `mqtt_connection_errors_total` counts the event-loop errors. Each one starts a reconnect.
//! - `mqtt_connected` is 1 when the last poll succeeded, else 0.
//! - No metric name starts with `autumn_`. Each counter name ends with `_total`.
//! - Every sample carries the `client_id` label. The broker host is a label too; it is not a secret.

use std::sync::atomic::{AtomicU64, Ordering};

use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample};

/// Counters for one MQTT client.
#[derive(Debug, Default)]
pub(crate) struct ClientMetrics {
    published: AtomicU64,
    received: AtomicU64,
    publish_errors: AtomicU64,
    connection_errors: AtomicU64,
    connected: AtomicU64,
    client_id: std::sync::OnceLock<String>,
    broker_host: std::sync::OnceLock<String>,
}

impl ClientMetrics {
    /// Records the client id and broker host for the metric labels. Runs once.
    pub(crate) fn identify(&self, client_id: &str, broker_host: &str) {
        let _ = self.client_id.set(client_id.to_owned());
        let _ = self.broker_host.set(broker_host.to_owned());
    }

    /// Marks the client connected or not.
    pub(crate) fn set_connected(&self, connected: bool) {
        self.connected
            .store(u64::from(connected), Ordering::Relaxed);
    }

    /// Counts one published message.
    pub(crate) fn note_publish(&self) {
        self.published.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one received message.
    pub(crate) fn note_received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one failed publish or subscribe call.
    pub(crate) fn note_publish_error(&self) {
        self.publish_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one event-loop connection error.
    pub(crate) fn note_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Builds the Prometheus families.
    ///
    /// Prometheus samples are `f64`; the precision loss on huge counters is inherent to the
    /// exposition format, not to the counting.
    #[allow(clippy::cast_precision_loss)]
    pub(crate) fn families(&self) -> Vec<MetricFamily> {
        let labels = || {
            let client_id = self.client_id.get().cloned().unwrap_or_default();
            let broker_host = self.broker_host.get().cloned().unwrap_or_default();
            vec![
                ("client_id".to_owned(), client_id),
                ("broker_host".to_owned(), broker_host),
            ]
        };
        let counter = |name: &str, help: &str, value: u64| MetricFamily {
            name: name.to_owned(),
            help: help.to_owned(),
            kind: MetricKind::Counter,
            samples: vec![MetricSample {
                labels: labels(),
                value: value as f64,
            }],
        };
        vec![
            counter(
                "mqtt_messages_published_total",
                "MQTT messages queued for publish.",
                self.published.load(Ordering::Relaxed),
            ),
            counter(
                "mqtt_messages_received_total",
                "MQTT messages received from the broker.",
                self.received.load(Ordering::Relaxed),
            ),
            counter(
                "mqtt_publish_errors_total",
                "Failed MQTT publish and subscribe calls.",
                self.publish_errors.load(Ordering::Relaxed),
            ),
            counter(
                "mqtt_connection_errors_total",
                "MQTT event-loop errors; each one starts a reconnect.",
                self.connection_errors.load(Ordering::Relaxed),
            ),
            MetricFamily {
                name: "mqtt_connected".to_owned(),
                help: "1 when the MQTT client holds a broker connection, else 0.".to_owned(),
                kind: MetricKind::Gauge,
                samples: vec![MetricSample {
                    labels: labels(),
                    value: self.connected.load(Ordering::Relaxed) as f64,
                }],
            },
        ]
    }
}
