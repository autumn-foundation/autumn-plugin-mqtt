//! Tests for the plugin builder.
//!
//! Tests may `expect` and `panic`: they fail the test, never production code.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use autumn_web::plugin::Plugin as _;

use super::{MqttPlugin, PLUGIN_NAME, prepare};
use crate::config::{Qos, test_config};
use crate::error::MqttError;

#[test]
fn plugin_name_is_stable() {
    assert_eq!(PLUGIN_NAME, "autumn-plugin-mqtt");
    let plugin = MqttPlugin::new();
    assert_eq!(plugin.name(), "autumn-plugin-mqtt");
}

#[test]
fn prepare_accepts_an_explicit_config() {
    let plugin = MqttPlugin::new()
        .config(test_config())
        .subscribe("sensors/#", Qos::AtLeastOnce)
        .on_message("alerts/+", Qos::AtMostOnce, |_message| async {});
    let (config, subscriptions) =
        prepare(&plugin.source, plugin.changes, plugin.subscriptions).expect("prepares");
    assert_eq!(config.client_id, "test-client");
    assert_eq!(subscriptions.len(), 2);
    assert!(subscriptions[0].handler.is_none());
    assert!(subscriptions[1].handler.is_some());
    assert_eq!(subscriptions[1].qos, Qos::AtMostOnce);
}

#[test]
fn prepare_applies_configure_changes() {
    let plugin = MqttPlugin::new()
        .config(test_config())
        .configure(|c| c.client_id = "changed".to_owned());
    let (config, _) =
        prepare(&plugin.source, plugin.changes, plugin.subscriptions).expect("prepares");
    assert_eq!(config.client_id, "changed");
}

#[test]
fn prepare_rejects_a_bad_filter() {
    let plugin = MqttPlugin::new()
        .config(test_config())
        .subscribe("bad/#/filter", Qos::AtMostOnce);
    let err = prepare(&plugin.source, plugin.changes, plugin.subscriptions)
        .expect_err("bad filter fails");
    assert!(
        matches!(err, MqttError::InvalidFilter { .. }),
        "an InvalidFilter, not {err:?}"
    );
}

#[test]
fn prepare_rejects_a_bad_config() {
    let mut config = test_config();
    config.keep_alive_secs = 4;
    let plugin = MqttPlugin::new().config(config);
    let err = prepare(&plugin.source, plugin.changes, plugin.subscriptions)
        .expect_err("bad keep-alive fails");
    assert!(err.to_string().contains("keep_alive_secs"), "{err}");
}

#[test]
fn debug_names_the_section() {
    let plugin = MqttPlugin::new().config_section("telemetry");
    let debug = format!("{plugin:?}");
    assert!(debug.contains("telemetry"), "{debug}");
    let plugin = MqttPlugin::new().configure(|c| c.client_id = "x".to_owned());
    let debug = format!("{plugin:?}");
    assert!(debug.contains("changes"), "{debug}");
}
