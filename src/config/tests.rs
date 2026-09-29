//! Tests for the `[mqtt]` configuration.
//!
//! Tests may `expect` and `panic`: they fail the test, never production code.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::HashMap;

use super::{TestEnv, parse_broker_url, topic_filter_error, topic_name_error};

/// Builds an env with `vars`.
fn env(vars: &[(&str, &str)]) -> TestEnv {
    TestEnv {
        vars: vars
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>(),
    }
}

/// Resolves with `vars` and no files.
fn resolve(vars: &[(&str, &str)]) -> Result<super::MqttConfig, super::ConfigError> {
    super::MqttConfig::resolve_with_env("mqtt", &env(vars))
}

#[test]
fn broker_url_parses_plain() {
    let parts = parse_broker_url("mqtt://broker.local").expect("parses");
    assert_eq!(parts.host, "broker.local");
    assert_eq!(parts.port, 1883);
    assert!(!parts.tls);

    let parts = parse_broker_url("tcp://10.0.0.5:1884").expect("parses");
    assert_eq!(parts.host, "10.0.0.5");
    assert_eq!(parts.port, 1884);
    assert!(!parts.tls);
}

#[test]
fn broker_url_parses_tls_schemes() {
    for url in [
        "mqtts://broker.local",
        "tls://broker.local",
        "ssl://broker.local",
    ] {
        let parts = parse_broker_url(url).expect("parses");
        assert_eq!(parts.host, "broker.local");
        assert_eq!(parts.port, 8883);
        assert!(parts.tls, "{url} selects TLS");
    }
    let parts = parse_broker_url("mqtts://broker.local:8884").expect("parses");
    assert_eq!(parts.port, 8884);
    assert!(parts.tls);
}

#[test]
fn broker_url_parses_ipv6() {
    let parts = parse_broker_url("mqtt://[::1]:1883").expect("parses");
    assert_eq!(parts.host, "::1");
    assert_eq!(parts.port, 1883);
}

#[test]
fn broker_url_rejects_bad_input() {
    for url in [
        "broker.local",
        "http://broker.local",
        "mqtt://",
        "mqtt://:1883",
        "mqtt://broker.local:0",
        "mqtt://broker.local:notaport",
        "mqtt://broker.local:",
        "mqtt://[::1",
    ] {
        assert!(parse_broker_url(url).is_err(), "{url} is rejected");
    }
}

#[test]
fn topic_name_rules() {
    assert!(topic_name_error("sensors/temp").is_none());
    assert!(topic_name_error("a").is_none());
    assert!(topic_name_error("").is_some());
    assert!(topic_name_error("sensors/+").is_some());
    assert!(topic_name_error("sensors/#").is_some());
    assert!(topic_name_error("a\0b").is_some());
}

#[test]
fn topic_filter_rules() {
    for filter in ["#", "sensors/#", "sensors/+/temp", "+", "a/b/c"] {
        assert!(topic_filter_error(filter).is_none(), "{filter} is valid");
    }
    for filter in [
        "",
        "sensors/#/x",
        "a#",
        "a/#/b",
        "sport/tennis#",
        "sport/++",
        "a\0",
    ] {
        assert!(
            topic_filter_error(filter).is_some(),
            "{filter} is not valid"
        );
    }
}

#[test]
fn resolve_needs_broker_url_and_client_id() {
    let err = resolve(&[]).expect_err("missing fields fail");
    assert!(err.0.contains("broker_url"), "names broker_url: {err}");
}

#[test]
fn resolve_uses_defaults() {
    let config = resolve(&[
        ("AUTUMN_MQTT__BROKER_URL", "mqtt://broker.local"),
        ("AUTUMN_MQTT__CLIENT_ID", "line-1"),
    ])
    .expect("resolves");
    assert_eq!(config.keep_alive_secs, 30);
    assert!(config.clean_start);
    assert_eq!(config.connect_timeout_secs, 10);
    assert_eq!(config.request_channel_capacity, 64);
    assert_eq!(config.username, None);
    assert_eq!(config.password, None);
    assert!(config.health_check);
    assert_eq!(config.reconnect_initial_ms, 1_000);
    assert_eq!(config.reconnect_max_ms, 30_000);
}

#[test]
fn resolve_reads_env_overrides() {
    let config = resolve(&[
        ("AUTUMN_MQTT__BROKER_URL", "mqtt://broker.local"),
        ("AUTUMN_MQTT__CLIENT_ID", "line-1"),
        ("AUTUMN_MQTT__KEEP_ALIVE_SECS", "60"),
        ("AUTUMN_MQTT__CLEAN_START", "false"),
        ("AUTUMN_MQTT__USERNAME", "mark"),
        ("AUTUMN_MQTT__PASSWORD", "s3cret"),
        ("AUTUMN_MQTT__WILL_TOPIC", "line-1/status"),
        ("AUTUMN_MQTT__WILL_PAYLOAD", "offline"),
        ("AUTUMN_MQTT__WILL_QOS", "at-least-once"),
        ("AUTUMN_MQTT__WILL_RETAIN", "true"),
    ])
    .expect("resolves");
    assert_eq!(config.keep_alive_secs, 60);
    assert!(!config.clean_start);
    assert_eq!(config.username.as_deref(), Some("mark"));
    assert_eq!(config.password.as_deref(), Some("s3cret"));
    assert_eq!(config.will_topic.as_deref(), Some("line-1/status"));
    assert_eq!(config.will_payload.as_deref(), Some("offline"));
    assert_eq!(config.will_qos, super::Qos::AtLeastOnce);
    assert!(config.will_retain);
}

#[test]
fn resolve_rejects_bad_values() {
    let base = [
        ("AUTUMN_MQTT__BROKER_URL", "mqtt://broker.local"),
        ("AUTUMN_MQTT__CLIENT_ID", "line-1"),
    ];
    let cases: &[(&str, &str, &str)] = &[
        ("AUTUMN_MQTT__KEEP_ALIVE_SECS", "4", "keep_alive_secs"),
        ("AUTUMN_MQTT__KEEP_ALIVE_SECS", "3601", "keep_alive_secs"),
        ("AUTUMN_MQTT__CLIENT_ID", "", "client_id"),
        ("AUTUMN_MQTT__PASSWORD", "s3cret", "password"),
        ("AUTUMN_MQTT__WILL_PAYLOAD", "offline", "will_payload"),
        (
            "AUTUMN_MQTT__RECONNECT_INITIAL_MS",
            "50",
            "reconnect_initial_ms",
        ),
        ("AUTUMN_MQTT__RECONNECT_MAX_MS", "50", "reconnect_max_ms"),
        ("AUTUMN_MQTT__WILL_QOS", "maybe", "WILL_QOS"),
    ];
    for (key, value, hint) in cases {
        let mut vars = base.to_vec();
        vars.push((key, value));
        let err = resolve(&vars).expect_err(format!("{key}={value} fails").as_str());
        assert!(err.0.contains(hint), "names {hint}: {err}");
    }
}

#[test]
fn resolve_rejects_tls_without_ca() {
    let err = resolve(&[
        ("AUTUMN_MQTT__BROKER_URL", "mqtts://broker.local"),
        ("AUTUMN_MQTT__CLIENT_ID", "line-1"),
    ])
    .expect_err("TLS without CA fails");
    assert!(err.0.contains("tls_ca_cert"), "{err}");
}

#[test]
fn resolve_rejects_half_client_auth() {
    let err = resolve(&[
        ("AUTUMN_MQTT__BROKER_URL", "mqtts://broker.local"),
        ("AUTUMN_MQTT__CLIENT_ID", "line-1"),
        ("AUTUMN_MQTT__TLS_CA_CERT", "/ca.pem"),
        ("AUTUMN_MQTT__TLS_CLIENT_CERT", "/client.pem"),
    ])
    .expect_err("cert without key fails");
    assert!(err.0.contains("tls_client_cert"), "{err}");
}

#[test]
fn resolve_rejects_tls_fields_on_plain_url() {
    let err = resolve(&[
        ("AUTUMN_MQTT__BROKER_URL", "mqtt://broker.local"),
        ("AUTUMN_MQTT__CLIENT_ID", "line-1"),
        ("AUTUMN_MQTT__TLS_CA_CERT", "/ca.pem"),
    ])
    .expect_err("TLS fields on a plain URL fail");
    assert!(err.0.contains("broker_url"), "{err}");
}

#[test]
fn resolve_rejects_unknown_keys() {
    let dir = std::env::temp_dir().join("mqtt-config-test-unknown");
    std::fs::create_dir_all(&dir).expect("makes dir");
    std::fs::write(
        dir.join("autumn.toml"),
        "[mqtt]\nbroker_url = \"mqtt://x\"\nclient_id = \"c\"\nbogus = 1\n",
    )
    .expect("writes toml");
    let env = TestEnv {
        vars: HashMap::from([(
            "AUTUMN_MANIFEST_DIR".to_owned(),
            dir.to_string_lossy().into_owned(),
        )]),
    };
    let err = super::MqttConfig::resolve_with_env("mqtt", &env).expect_err("unknown key fails");
    assert!(err.0.contains("bogus"), "{err}");
    std::fs::remove_dir_all(&dir).expect("cleans up");
}

#[test]
fn resolve_reads_autumn_toml() {
    let dir = std::env::temp_dir().join("mqtt-config-test-file");
    std::fs::create_dir_all(&dir).expect("makes dir");
    std::fs::write(
        dir.join("autumn.toml"),
        "[mqtt]\nbroker_url = \"mqtt://file-broker\"\nclient_id = \"file-client\"\nkeep_alive_secs = 45\n",
    )
    .expect("writes toml");
    let env = TestEnv {
        vars: HashMap::from([
            (
                "AUTUMN_MANIFEST_DIR".to_owned(),
                dir.to_string_lossy().into_owned(),
            ),
            ("AUTUMN_MQTT__KEEP_ALIVE_SECS".to_owned(), "60".to_owned()),
        ]),
    };
    let config = super::MqttConfig::resolve_with_env("mqtt", &env).expect("resolves");
    assert_eq!(config.broker_url, "mqtt://file-broker");
    assert_eq!(config.client_id, "file-client");
    assert_eq!(config.keep_alive_secs, 60, "the env wins over the file");
    std::fs::remove_dir_all(&dir).expect("cleans up");
}

#[test]
fn secret_never_reaches_error_text() {
    let err = resolve(&[
        ("AUTUMN_MQTT__BROKER_URL", "mqtt://broker.local"),
        ("AUTUMN_MQTT__CLIENT_ID", "line-1"),
        ("AUTUMN_MQTT__PASSWORD", "s3cret"),
    ])
    .expect_err("password without username fails");
    assert!(
        !err.0.contains("s3cret"),
        "the error hides the secret: {err}"
    );
}
