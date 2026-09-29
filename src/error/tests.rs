//! Tests for the public error type.
//!
//! Tests may `expect` and `panic`: they fail the test, never production code.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use axum::response::IntoResponse;
use http::StatusCode;

use super::{ErrorKind, MqttError};
use crate::config::ConfigError;

#[test]
fn kinds_map() {
    assert_eq!(
        MqttError::from(ConfigError("bad".to_owned())).kind(),
        ErrorKind::Config
    );
    assert_eq!(MqttError::NotInstalled.kind(), ErrorKind::Connection);
    assert_eq!(MqttError::NotConnected.kind(), ErrorKind::Connection);
    assert_eq!(
        MqttError::InvalidBrokerUrl {
            reason: "x".to_owned()
        }
        .kind(),
        ErrorKind::Config
    );
    assert_eq!(
        MqttError::Tls {
            reason: "x".to_owned()
        }
        .kind(),
        ErrorKind::Config
    );
    assert_eq!(
        MqttError::Broker {
            operation: "publish",
            detail: "x".to_owned()
        }
        .kind(),
        ErrorKind::Broker
    );
    assert_eq!(
        MqttError::InvalidTopic {
            topic: "t".to_owned(),
            reason: "r".to_owned()
        }
        .kind(),
        ErrorKind::Validation
    );
    assert_eq!(
        MqttError::InvalidFilter {
            filter: "f".to_owned(),
            reason: "r".to_owned()
        }
        .kind(),
        ErrorKind::Validation
    );
}

#[test]
fn status_codes_map() {
    assert_eq!(
        MqttError::from(ConfigError("bad".to_owned())).status_code(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        MqttError::NotInstalled.status_code(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        MqttError::NotConnected.status_code(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        MqttError::Broker {
            operation: "publish",
            detail: "x".to_owned()
        }
        .status_code(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        MqttError::InvalidFilter {
            filter: "f".to_owned(),
            reason: "r".to_owned()
        }
        .status_code(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn into_autumn_carries_the_status() {
    let cases = [
        (
            MqttError::InvalidTopic {
                topic: "a/#".to_owned(),
                reason: "must not hold `+` or `#`".to_owned(),
            },
            StatusCode::BAD_REQUEST,
        ),
        (MqttError::NotConnected, StatusCode::SERVICE_UNAVAILABLE),
        (
            MqttError::from(ConfigError("bad".to_owned())),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ];
    for (error, status) in cases {
        let response = error.into_autumn().into_response();
        assert_eq!(response.status(), status);
    }
}

#[test]
fn error_text_hides_secrets() {
    let error = MqttError::Broker {
        operation: "publish",
        detail: "connection reset".to_owned(),
    };
    let text = error.to_string();
    assert!(text.contains("publish"), "{text}");
    assert!(!text.contains("s3cret"), "{text}");
}
