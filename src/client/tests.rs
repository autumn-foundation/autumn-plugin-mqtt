//! Tests for the MQTT client.
//!
//! Tests may `expect` and `panic`: they fail the test, never production code.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use proptest::prelude::*;

use super::{ConnectionState, MqttClient, MqttMessage, topic_matches};
use crate::config::{Qos, test_config};
use crate::error::MqttError;

#[test]
fn matcher_handles_exact_and_wildcards() {
    let matches = [
        ("sensors/temp", "sensors/temp"),
        ("sensors/+", "sensors/temp"),
        ("sensors/#", "sensors/temp"),
        ("sensors/#", "sensors"),
        ("sensors/#", "sensors/a/b/c"),
        ("#", "anything/at/all"),
        ("+/+", "a/b"),
        ("$SYS/broker/uptime", "$SYS/broker/uptime"),
    ];
    for (filter, topic) in matches {
        assert!(topic_matches(filter, topic), "{filter} matches {topic}");
    }
    let misses = [
        ("sensors/temp", "sensors/humidity"),
        ("sensors/+", "sensors/a/b"),
        ("sensors/+", "sensors"),
        ("a/b", "a/b/c"),
        ("a/#", "b/c"),
        ("+/temp", "sensors"),
        // `$` topics never match a leading wildcard, per MQTT-4.7.2-1.
        ("+/uptime", "$SYS/uptime"),
        ("#", "$SYS/uptime"),
    ];
    for (filter, topic) in misses {
        assert!(!topic_matches(filter, topic), "{filter} misses {topic}");
    }
}

proptest! {
    /// A `#` filter matches every topic.
    #[test]
    fn hash_matches_everything(topic in "[a-z]{1,12}(/[a-z]{1,12}){0,4}") {
        prop_assert!(topic_matches("#", &topic));
    }

    /// A literal filter matches its own topic.
    #[test]
    fn literal_matches_itself(topic in "[a-z0-9]{1,12}(/[a-z0-9]{1,12}){0,4}") {
        prop_assert!(topic_matches(&topic, &topic));
    }

    /// `a/#` matches `a` plus anything below it, and nothing beside it.
    #[test]
    fn hash_tail_matches_subtree(rest in "(/[a-z]{1,8}){0,4}") {
        let topic = format!("a{rest}");
        prop_assert!(topic_matches("a/#", &topic));
        prop_assert!(!topic_matches("b/#", &topic));
    }

    /// `+` matches exactly one level: it never crosses a `/`.
    #[test]
    fn plus_matches_one_level(
        left in "[a-z]{1,6}",
        mid in "[a-z]{1,6}",
        right in "[a-z]{1,6}",
    ) {
        let topic = format!("{left}/{mid}/{right}");
        prop_assert!(topic_matches("+/+/+", &topic));
        prop_assert!(!topic_matches("+/+", &topic));
        prop_assert!(!topic_matches("+/+/+/+", &topic));
    }
}

#[test]
fn payload_str_reads_utf8_only() {
    let text = MqttMessage {
        topic: "t".to_owned(),
        payload: b"hello".to_vec(),
        qos: Qos::AtMostOnce,
        retain: false,
        packet_id: 0,
    };
    assert_eq!(text.payload_str(), Some("hello"));
    let binary = MqttMessage {
        payload: vec![0xff, 0xfe],
        ..text
    };
    assert_eq!(binary.payload_str(), None);
}

#[test]
fn qos_round_trips_through_rumqttc() {
    for qos in [Qos::AtMostOnce, Qos::AtLeastOnce, Qos::ExactlyOnce] {
        assert_eq!(Qos::from_rumqttc(qos.to_rumqttc()), qos);
    }
}

/// Opens a client for tests. The event loop is dropped: no broker is needed.
fn open_client() -> MqttClient {
    match MqttClient::open(&test_config(), Vec::new()) {
        Ok((client, _eventloop)) => client,
        Err(err) => panic!("opens: {err}"),
    }
}

#[tokio::test]
async fn open_builds_a_disconnected_client() {
    let client = open_client();
    assert_eq!(client.client_id(), "test-client");
    assert_eq!(client.broker_host(), "localhost");
    assert_eq!(client.connection_state(), ConnectionState::Disconnected);
    assert!(!client.is_connected());
}

#[tokio::test]
async fn open_rejects_tls_without_ca() {
    let mut config = test_config();
    config.broker_url = "mqtts://localhost:8883".to_owned();
    let Err(err) = MqttClient::open(&config, Vec::new()) else {
        panic!("no CA fails");
    };
    assert!(
        matches!(err, MqttError::Tls { .. }),
        "a TLS error, not {err:?}"
    );
}

#[tokio::test]
async fn open_rejects_missing_ca_file() {
    let mut config = test_config();
    config.broker_url = "mqtts://localhost:8883".to_owned();
    config.tls_ca_cert = Some("/nonexistent/ca.pem".into());
    let Err(err) = MqttClient::open(&config, Vec::new()) else {
        panic!("missing CA fails");
    };
    assert!(
        matches!(err, MqttError::Tls { .. }),
        "a TLS error, not {err:?}"
    );
}

#[tokio::test]
async fn publish_rejects_a_bad_topic() {
    let client = open_client();
    let err = client
        .publish("sensors/#", Qos::AtMostOnce, b"x".to_vec())
        .await
        .expect_err("a wildcard topic fails");
    assert!(
        matches!(err, MqttError::InvalidTopic { .. }),
        "an InvalidTopic, not {err:?}"
    );
}

#[tokio::test]
async fn subscribe_rejects_a_bad_filter() {
    let client = open_client();
    let err = client
        .subscribe("sensors/#/x", Qos::AtMostOnce)
        .await
        .expect_err("a bad filter fails");
    assert!(
        matches!(err, MqttError::InvalidFilter { .. }),
        "an InvalidFilter, not {err:?}"
    );
}

#[tokio::test]
async fn extractor_rejects_with_503_when_the_plugin_is_not_installed() {
    use axum::extract::FromRequestParts as _;

    let state = autumn_web::AppState::detached();
    let (mut parts, ()) = http::Request::new(()).into_parts();
    let Err(err) = MqttClient::from_request_parts(&mut parts, &state).await else {
        panic!("no client is installed, so the extractor must reject");
    };
    assert_eq!(err.status(), http::StatusCode::SERVICE_UNAVAILABLE);
}
