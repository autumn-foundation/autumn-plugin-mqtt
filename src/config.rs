//! The `[mqtt]` section of `autumn.toml`.
//!
//! # Contract
//!
//! - Layering, later wins: defaults, `[mqtt]` in `autumn.toml`, `[profile.<name>.mqtt]`
//!   inline, `[mqtt]` in `autumn-<profile>.toml`, then `AUTUMN_MQTT__<KEY>` variables.
//!   `AUTUMN_MQTT__PASSWORD` sets `password`. `AUTUMN_MQTT__BROKER_URL` sets `broker_url`.
//! - The merged table must deserialize into [`MqttConfig`] and pass [`MqttConfig::validate`].
//!   Unknown keys are errors. A missing `broker_url` or `client_id` is an error.
//! - [`MqttConfig::resolve`] reads the OS environment (with dotenv). Tests use
//!   [`MqttConfig::resolve_with_env`] with a fake environment.
//! - Secrets never appear in error text. Paths appear; values do not.

#[cfg(test)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use autumn_web::config::Env;
use serde::{Deserialize, Serialize};

/// A configuration error. The text names the section and the key. It never holds a secret.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

/// The default section name.
pub const DEFAULT_SECTION: &str = "mqtt";

/// The default keep-alive, in seconds.
const DEFAULT_KEEP_ALIVE_SECS: u64 = 30;
/// The default TCP/TLS connect timeout, in seconds.
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
/// The default rumqttc request channel capacity.
const DEFAULT_REQUEST_CHANNEL_CAPACITY: usize = 64;
/// The default first reconnect wait, in milliseconds.
const DEFAULT_RECONNECT_INITIAL_MS: u64 = 1_000;
/// The default reconnect wait cap, in milliseconds.
const DEFAULT_RECONNECT_MAX_MS: u64 = 30_000;

/// The MQTT quality of service. This is the plugin's own copy: a rumqttc update does not change it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Qos {
    /// Deliver at most once. No acknowledgement.
    #[default]
    AtMostOnce,
    /// Deliver at least once. The broker acknowledges each message.
    AtLeastOnce,
    /// Deliver exactly once. Four-way handshake per message.
    ExactlyOnce,
}

impl Qos {
    /// Converts to the rumqttc quality of service.
    pub(crate) const fn to_rumqttc(self) -> rumqttc::v5::mqttbytes::QoS {
        match self {
            Self::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            Self::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            Self::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
        }
    }

    /// Converts from the rumqttc quality of service.
    pub(crate) const fn from_rumqttc(qos: rumqttc::v5::mqttbytes::QoS) -> Self {
        match qos {
            rumqttc::v5::mqttbytes::QoS::AtMostOnce => Self::AtMostOnce,
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce => Self::AtLeastOnce,
            rumqttc::v5::mqttbytes::QoS::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

/// The parsed broker address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrokerParts {
    /// The broker host name or IP address.
    pub(crate) host: String,
    /// The broker port.
    pub(crate) port: u16,
    /// `true` for `mqtts://`, `tls://` and `ssl://` URLs.
    pub(crate) tls: bool,
}

/// The `[mqtt]` configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    /// The broker URL: `mqtt://host:1883` or `mqtts://host:8883`. Required.
    pub broker_url: String,
    /// The MQTT client id. Required. Must not be empty.
    pub client_id: String,
    /// The keep-alive interval. The client pings the broker when idle this long.
    #[serde(default = "default_keep_alive_secs")]
    pub keep_alive_secs: u64,
    /// Starts a clean session. `false` resumes the previous session on reconnect.
    #[serde(default = "default_true")]
    pub clean_start: bool,
    /// The TCP/TLS handshake timeout.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// The rumqttc request channel capacity. Bounds queued publishes while offline.
    #[serde(default = "default_request_channel_capacity")]
    pub request_channel_capacity: usize,
    /// The broker username. Needs `password` to have an effect on most brokers.
    #[serde(default)]
    pub username: Option<String>,
    /// The broker password. Prefer `AUTUMN_MQTT__PASSWORD` over the file.
    #[serde(default)]
    pub password: Option<String>,
    /// The PEM file with the CA certificate. Required for TLS.
    #[serde(default)]
    pub tls_ca_cert: Option<PathBuf>,
    /// The PEM file with the client certificate. Needs `tls_client_key`.
    #[serde(default)]
    pub tls_client_cert: Option<PathBuf>,
    /// The PEM file with the client private key. Needs `tls_client_cert`.
    #[serde(default)]
    pub tls_client_key: Option<PathBuf>,
    /// The last-will topic. The broker publishes the will when the client drops.
    #[serde(default)]
    pub will_topic: Option<String>,
    /// The last-will payload. Empty when unset.
    #[serde(default)]
    pub will_payload: Option<String>,
    /// The last-will quality of service.
    #[serde(default)]
    pub will_qos: Qos,
    /// Retains the last-will message on the broker.
    #[serde(default)]
    pub will_retain: bool,
    /// The first wait before the supervisor retries after a connection error.
    #[serde(default = "default_reconnect_initial_ms")]
    pub reconnect_initial_ms: u64,
    /// The cap for the exponential reconnect wait.
    #[serde(default = "default_reconnect_max_ms")]
    pub reconnect_max_ms: u64,
    /// Registers the `mqtt` health indicator. Disable it when the broker is optional.
    #[serde(default = "default_true")]
    pub health_check: bool,
}

const fn default_keep_alive_secs() -> u64 {
    DEFAULT_KEEP_ALIVE_SECS
}

const fn default_true() -> bool {
    true
}

const fn default_connect_timeout_secs() -> u64 {
    DEFAULT_CONNECT_TIMEOUT_SECS
}

const fn default_request_channel_capacity() -> usize {
    DEFAULT_REQUEST_CHANNEL_CAPACITY
}

const fn default_reconnect_initial_ms() -> u64 {
    DEFAULT_RECONNECT_INITIAL_MS
}

const fn default_reconnect_max_ms() -> u64 {
    DEFAULT_RECONNECT_MAX_MS
}

/// The type of a configuration leaf, for environment values.
#[derive(Clone, Copy)]
enum Kind {
    Text,
    Unsigned,
    Bool,
}

/// Each leaf key and its type.
const LEAVES: &[(&str, Kind)] = &[
    ("broker_url", Kind::Text),
    ("client_id", Kind::Text),
    ("keep_alive_secs", Kind::Unsigned),
    ("clean_start", Kind::Bool),
    ("connect_timeout_secs", Kind::Unsigned),
    ("request_channel_capacity", Kind::Unsigned),
    ("username", Kind::Text),
    ("password", Kind::Text),
    ("tls_ca_cert", Kind::Text),
    ("tls_client_cert", Kind::Text),
    ("tls_client_key", Kind::Text),
    ("will_topic", Kind::Text),
    ("will_payload", Kind::Text),
    ("will_qos", Kind::Text),
    ("will_retain", Kind::Bool),
    ("reconnect_initial_ms", Kind::Unsigned),
    ("reconnect_max_ms", Kind::Unsigned),
    ("health_check", Kind::Bool),
];

/// The longest client id: 256 bytes.
const MAX_CLIENT_ID_BYTES: usize = 256;
/// The longest keep-alive: one hour.
const MAX_KEEP_ALIVE_SECS: u64 = 3600;
/// The longest connect timeout: five minutes.
const MAX_CONNECT_TIMEOUT_SECS: u64 = 300;
/// The largest request channel: 65536 queued requests.
const MAX_REQUEST_CHANNEL_CAPACITY: usize = 65_536;
/// The longest topic or filter: 65535 bytes, the MQTT limit.
const MAX_TOPIC_BYTES: usize = 65_535;
/// The smallest reconnect wait: 100 ms.
const MIN_RECONNECT_MS: u64 = 100;
/// The largest reconnect wait: ten minutes.
const MAX_RECONNECT_MS: u64 = 600_000;

impl MqttConfig {
    /// Reads `[section]` from the app files and the environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if a file is not valid TOML or a value is not valid.
    pub fn resolve(section: &str) -> Result<Self, ConfigError> {
        autumn_web::dotenv::os_env_with_dotenv().map_or_else(
            |_| Self::resolve_with_env(section, &autumn_web::config::OsEnv),
            |env| Self::resolve_with_env(section, &env),
        )
    }

    /// Reads `[section]` with `env` as the environment.
    ///
    /// # Errors
    ///
    /// See [`resolve`](Self::resolve).
    pub fn resolve_with_env(section: &str, env: &dyn Env) -> Result<Self, ConfigError> {
        let (selected, profile) = active_profile(env);
        let mut merged = toml::Table::new();
        if let Some(base) = read_toml(&config_file("autumn.toml", env))? {
            merge_section(&mut merged, base.get(section), section)?;
            for name in inline_profile_names(&profile) {
                let inline = base
                    .get("profile")
                    .and_then(|p| p.get(name))
                    .and_then(|p| p.get(section));
                merge_section(&mut merged, inline, section)?;
            }
        }
        for name in autumn_web::config::profile_override_file_lookup_names(&profile, &selected) {
            if let Some(file) = read_toml(&config_file(&format!("autumn-{name}.toml"), env))? {
                merge_section(&mut merged, file.get(section), section)?;
                break;
            }
        }
        apply_env(&mut merged, section, env)?;
        let config: Self = toml::Value::Table(merged)
            .try_into()
            .map_err(|err| ConfigError(format!("[{section}]: {err}")))?;
        config.validate_section(section)?;
        Ok(config)
    }

    /// Checks each value.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] that names the first key that is not valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_section(DEFAULT_SECTION)
    }

    /// Checks each value. The errors name keys in `section`.
    pub(crate) fn validate_section(&self, section: &str) -> Result<(), ConfigError> {
        let fail = |key: &str, rule: &str| Err(ConfigError(format!("{section}.{key} {rule}")));
        let parts = parse_broker_url(&self.broker_url)
            .map_err(|reason| ConfigError(format!("{section}.broker_url {reason}")))?;
        if parts.tls && self.tls_ca_cert.is_none() {
            return fail(
                "tls_ca_cert",
                "is required for a `mqtts://`, `tls://` or `ssl://` broker_url",
            );
        }
        if !parts.tls
            && (self.tls_ca_cert.is_some()
                || self.tls_client_cert.is_some()
                || self.tls_client_key.is_some())
        {
            return fail(
                "broker_url",
                "needs a `mqtts://`, `tls://` or `ssl://` scheme when TLS fields are set",
            );
        }
        if self.client_id.is_empty() || self.client_id.len() > MAX_CLIENT_ID_BYTES {
            return fail("client_id", "must be 1 to 256 bytes");
        }
        if self.client_id.trim() != self.client_id {
            return fail("client_id", "must not start or end with white space");
        }
        if !(5..=MAX_KEEP_ALIVE_SECS).contains(&self.keep_alive_secs) {
            return fail("keep_alive_secs", "must be from 5 to 3600");
        }
        if !(1..=MAX_CONNECT_TIMEOUT_SECS).contains(&self.connect_timeout_secs) {
            return fail("connect_timeout_secs", "must be from 1 to 300");
        }
        if !(1..=MAX_REQUEST_CHANNEL_CAPACITY).contains(&self.request_channel_capacity) {
            return fail("request_channel_capacity", "must be from 1 to 65536");
        }
        if self
            .username
            .as_deref()
            .is_some_and(|username| username.trim().is_empty())
        {
            return fail("username", "must not be empty when set");
        }
        if let Some(password) = &self.password {
            if password.is_empty() {
                return fail("password", "must not be empty when set");
            }
            if self.username.is_none() {
                return fail(
                    "password",
                    "needs `username`: MQTT sends no password without one",
                );
            }
        }
        for (key, path) in [
            ("tls_ca_cert", &self.tls_ca_cert),
            ("tls_client_cert", &self.tls_client_cert),
            ("tls_client_key", &self.tls_client_key),
        ] {
            if path
                .as_ref()
                .is_some_and(|path| path.as_os_str().is_empty())
            {
                return fail(key, "must not be empty when set");
            }
        }
        if self.tls_client_cert.is_some() != self.tls_client_key.is_some() {
            return fail(
                "tls_client_cert",
                "needs `tls_client_key`: set both or neither",
            );
        }
        if let Some(topic) = &self.will_topic {
            if let Some(reason) = topic_name_error(topic) {
                return fail("will_topic", reason);
            }
        } else if self.will_payload.is_some() {
            return fail("will_payload", "needs `will_topic`");
        }
        if !(MIN_RECONNECT_MS..=MAX_RECONNECT_MS).contains(&self.reconnect_initial_ms) {
            return fail("reconnect_initial_ms", "must be from 100 to 600000");
        }
        if self.reconnect_max_ms < self.reconnect_initial_ms
            || self.reconnect_max_ms > MAX_RECONNECT_MS
        {
            return fail(
                "reconnect_max_ms",
                "must be at least `reconnect_initial_ms` and at most 600000",
            );
        }
        Ok(())
    }

    /// The keep-alive interval.
    #[must_use]
    pub const fn keep_alive(&self) -> Duration {
        Duration::from_secs(self.keep_alive_secs)
    }

    /// The connect timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    /// The parsed broker address. The config passed validation, so this parses.
    pub(crate) fn broker_parts(&self) -> BrokerParts {
        parse_broker_url(&self.broker_url).unwrap_or(BrokerParts {
            host: String::new(),
            port: 1883,
            tls: false,
        })
    }
}

/// Parses `scheme://host[:port]`. The scheme selects TLS and the default port.
pub(crate) fn parse_broker_url(url: &str) -> Result<BrokerParts, String> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| "must look like `mqtt://host:1883` or `mqtts://host:8883`".to_owned())?;
    let (tls, default_port) = match scheme.to_ascii_lowercase().as_str() {
        "mqtt" | "tcp" => (false, 1883_u16),
        "mqtts" | "tls" | "ssl" => (true, 8883_u16),
        _ => {
            return Err(format!(
                "scheme `{scheme}` is not supported: use `mqtt://`, `mqtts://`, `tcp://` or `tls://`"
            ));
        }
    };
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() {
        return Err("must have a host".to_owned());
    }
    let (host, port) = if let Some(bracketed) = rest.strip_prefix('[') {
        let (host, rest) = bracketed
            .split_once(']')
            .ok_or_else(|| "the IPv6 host misses `]`".to_owned())?;
        let port = rest
            .strip_prefix(':')
            .map_or(Ok(default_port), parse_port)
            .map_err(|reason| format!("the port {reason}"))?;
        (host.to_owned(), port)
    } else if let Some((host, maybe_port)) = rest.rsplit_once(':') {
        if maybe_port.is_empty() {
            return Err("the port must not be empty".to_owned());
        }
        if maybe_port.chars().all(|c| c.is_ascii_digit()) {
            let port = parse_port(maybe_port).map_err(|reason| format!("the port {reason}"))?;
            (host.to_owned(), port)
        } else {
            return Err(format!(
                "the port `{maybe_port}` is not a number from 1 to 65535"
            ));
        }
    } else {
        (rest.to_owned(), default_port)
    };
    if host.is_empty() {
        return Err("must have a host".to_owned());
    }
    Ok(BrokerParts { host, port, tls })
}

/// Parses a port number.
fn parse_port(text: &str) -> Result<u16, String> {
    let port: u16 = text
        .parse()
        .map_err(|_| format!("`{text}` is not a number from 1 to 65535"))?;
    if port == 0 {
        return Err("must not be 0".to_owned());
    }
    Ok(port)
}

/// Gives the rule a topic name breaks, if any.
pub(crate) fn topic_name_error(topic: &str) -> Option<&'static str> {
    if topic.is_empty() || topic.len() > MAX_TOPIC_BYTES {
        return Some("must be 1 to 65535 bytes");
    }
    if topic.contains(['+', '#']) {
        return Some("must not hold `+` or `#`: those are filter wildcards");
    }
    if topic.contains('\0') {
        return Some("must not hold NUL");
    }
    None
}

/// Gives the rule a topic filter breaks, if any.
pub(crate) fn topic_filter_error(filter: &str) -> Option<String> {
    if filter.is_empty() || filter.len() > MAX_TOPIC_BYTES {
        return Some("must be 1 to 65535 bytes".to_owned());
    }
    if filter.contains('\0') {
        return Some("must not hold NUL".to_owned());
    }
    let segments: Vec<&str> = filter.split('/').collect();
    for (index, segment) in segments.iter().enumerate() {
        let last = index + 1 == segments.len();
        if segment.contains('#') && (*segment != "#" || !last) {
            return Some(
                "`#` must fill a whole segment and sit last, as in `sensors/#`".to_owned(),
            );
        }
        if segment.contains('+') && *segment != "+" {
            return Some("`+` must fill a whole segment, as in `sensors/+/temp`".to_owned());
        }
    }
    None
}

/// Gives the selected profile text and the normalized profile, as Autumn does.
fn active_profile(env: &dyn Env) -> (String, String) {
    let selected = ["AUTUMN_ENV", "AUTUMN_PROFILE"]
        .iter()
        .filter_map(|key| env.var(key).ok())
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
        .unwrap_or_else(|| {
            let release = env.var("AUTUMN_IS_DEBUG").is_ok_and(|v| v == "0");
            if release { "prod" } else { "dev" }.to_owned()
        });
    let profile =
        autumn_web::config::normalize_profile_name(&selected).unwrap_or_else(|| "dev".to_owned());
    (selected, profile)
}

/// The inline profile names to read, in order. The canonical name is last.
fn inline_profile_names(profile: &str) -> Vec<&str> {
    match profile {
        "prod" => vec!["production", "prod"],
        "dev" => vec!["development", "dev"],
        _ => vec![profile],
    }
}

/// Finds a config file in `AUTUMN_MANIFEST_DIR`, or else in the working directory.
fn config_file(name: &str, env: &dyn Env) -> PathBuf {
    env.var("AUTUMN_MANIFEST_DIR")
        .ok()
        .map(|dir| Path::new(&dir).join(name))
        .filter(|path| path.exists())
        .unwrap_or_else(|| PathBuf::from(name))
}

fn read_toml(path: &Path) -> Result<Option<toml::Table>, ConfigError> {
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .parse::<toml::Table>()
            .map(Some)
            .map_err(|err| ConfigError(format!("{}: {err}", path.display()))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(ConfigError(format!("{}: {err}", path.display()))),
    }
}

fn merge_section(
    into: &mut toml::Table,
    layer: Option<&toml::Value>,
    section: &str,
) -> Result<(), ConfigError> {
    match layer {
        None => Ok(()),
        Some(toml::Value::Table(table)) => {
            deep_merge(into, table);
            Ok(())
        }
        Some(_) => Err(ConfigError(format!("[{section}] must be a table"))),
    }
}

fn deep_merge(into: &mut toml::Table, layer: &toml::Table) {
    for (key, value) in layer {
        match (into.get_mut(key), value) {
            (Some(toml::Value::Table(old)), toml::Value::Table(new)) => deep_merge(old, new),
            _ => {
                into.insert(key.clone(), value.clone());
            }
        }
    }
}

fn apply_env(into: &mut toml::Table, section: &str, env: &dyn Env) -> Result<(), ConfigError> {
    let name: String = section
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    for (key, kind) in LEAVES {
        let name = format!("AUTUMN_{name}__{}", key.to_ascii_uppercase());
        let Ok(raw) = env.var(&name) else {
            continue;
        };
        let bad = || ConfigError(format!("{name}: can not read {raw:?}"));
        let value = match kind {
            Kind::Text if *key == "will_qos" => {
                // `will_qos` accepts the same snake_case names as the file.
                let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
                if !matches!(
                    normalized.as_str(),
                    "at_most_once" | "at_least_once" | "exactly_once"
                ) {
                    return Err(bad());
                }
                toml::Value::String(normalized)
            }
            Kind::Text => toml::Value::String(raw.clone()),
            Kind::Unsigned => {
                let value: i64 = raw.trim().parse().map_err(|_| bad())?;
                if value < 0 {
                    return Err(bad());
                }
                toml::Value::Integer(value)
            }
            Kind::Bool => match raw.trim() {
                "true" | "1" => toml::Value::Boolean(true),
                "false" | "0" => toml::Value::Boolean(false),
                _ => return Err(bad()),
            },
        };
        into.insert((*key).to_owned(), value);
    }
    Ok(())
}

/// A fake environment for tests.
#[cfg(test)]
pub(crate) struct TestEnv {
    pub(crate) vars: HashMap<String, String>,
}

#[cfg(test)]
impl Env for TestEnv {
    fn var(&self, key: &str) -> Result<String, std::env::VarError> {
        self.vars
            .get(key)
            .cloned()
            .ok_or(std::env::VarError::NotPresent)
    }
}

/// A valid config for tests. The broker never gets a connection in unit tests.
#[cfg(test)]
pub(crate) fn test_config() -> MqttConfig {
    MqttConfig {
        broker_url: "mqtt://localhost:1883".to_owned(),
        client_id: "test-client".to_owned(),
        keep_alive_secs: 30,
        clean_start: true,
        connect_timeout_secs: 10,
        request_channel_capacity: 64,
        username: None,
        password: None,
        tls_ca_cert: None,
        tls_client_cert: None,
        tls_client_key: None,
        will_topic: None,
        will_payload: None,
        will_qos: Qos::AtMostOnce,
        will_retain: false,
        reconnect_initial_ms: 1_000,
        reconnect_max_ms: 30_000,
        health_check: true,
    }
}

#[cfg(test)]
mod tests;
