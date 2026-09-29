//! Tests for the health indicator.
//!
//! Tests may `expect` and `panic`: they fail the test, never production code.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use autumn_web::actuator::{HealthIndicator, HealthStatus, IndicatorGroup};
use autumn_web::reexports::serde_json;

use super::MqttHealthIndicator;
use crate::client::MqttClient;
use crate::config::test_config;
use crate::plugin::Shared;

#[tokio::test]
async fn check_is_down_before_start() {
    let indicator = MqttHealthIndicator::new(Arc::new(Shared::default()));
    let output = indicator.check().await;
    assert_eq!(output.status, HealthStatus::Down);
}

#[tokio::test]
async fn check_is_down_when_disconnected() {
    let (client, _eventloop) = MqttClient::open(&test_config(), Vec::new()).expect("opens");
    let shared = Arc::new(Shared::default());
    let _ = shared.client.set(client);
    let indicator = MqttHealthIndicator::new(shared);
    let output = indicator.check().await;
    assert_eq!(output.status, HealthStatus::Down);
    assert_eq!(
        output.details.get("client_id").expect("names the client"),
        &serde_json::json!("test-client")
    );
}

#[test]
fn indicator_is_health_only() {
    let indicator = MqttHealthIndicator::new(Arc::new(Shared::default()));
    assert_eq!(indicator.group(), IndicatorGroup::HealthOnly);
    assert_eq!(indicator.timeout_ms(), 1_000);
}
