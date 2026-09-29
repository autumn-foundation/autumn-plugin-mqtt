//! The health indicator.
//!
//! # Contract
//!
//! - The check is down before the plugin starts. It is up when the client holds a broker
//!   connection, down otherwise.
//! - The indicator is `HealthOnly`: a lost broker does not block deploys. The app serves
//!   requests while the supervisor reconnects in the background.
//! - The details name the client id and the broker host. They never hold a secret.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use autumn_web::actuator::{HealthCheckOutput, HealthIndicator, HealthStatus, IndicatorGroup};
use autumn_web::reexports::serde_json;

use crate::plugin::Shared;

/// The future type of a health check.
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Reports the MQTT connection state to `/actuator/health`.
pub(crate) struct MqttHealthIndicator {
    shared: Arc<Shared>,
}

impl MqttHealthIndicator {
    pub(crate) const fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }
}

impl HealthIndicator for MqttHealthIndicator {
    fn check(&self) -> BoxFuture<'_, HealthCheckOutput> {
        let shared = Arc::clone(&self.shared);
        Box::pin(async move {
            let Some(client) = shared.client.get() else {
                return HealthCheckOutput {
                    status: HealthStatus::Down,
                    details: HashMap::from([(
                        "error".to_owned(),
                        serde_json::json!("the MQTT client has not started"),
                    )]),
                };
            };
            let mut details = HashMap::from([
                (
                    "client_id".to_owned(),
                    serde_json::json!(client.client_id()),
                ),
                (
                    "broker_host".to_owned(),
                    serde_json::json!(client.broker_host()),
                ),
            ]);
            if client.is_connected() {
                HealthCheckOutput {
                    status: HealthStatus::Up,
                    details,
                }
            } else {
                details.insert(
                    "error".to_owned(),
                    serde_json::json!("the MQTT client is not connected to the broker"),
                );
                HealthCheckOutput {
                    status: HealthStatus::Down,
                    details,
                }
            }
        })
    }

    fn group(&self) -> IndicatorGroup {
        IndicatorGroup::HealthOnly
    }

    fn timeout_ms(&self) -> u64 {
        1_000
    }
}

#[cfg(test)]
mod tests;
