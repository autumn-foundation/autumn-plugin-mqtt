//! The public error type.
//!
//! # Contract
//!
//! - An MQTT error keeps its kind. [`ErrorKind`] is the plugin's own copy: a rumqttc update
//!   does not change it.
//! - The error text names the kind and the failing key or topic. It never holds a secret:
//!   no password, no username, no payload, no certificate content.
//! - A validation failure gives HTTP 400. A missing plugin or a lost broker connection gives
//!   HTTP 503. A broker refusal gives HTTP 503. All other errors give HTTP 500.
//! - The `Debug` output is the error text.

use autumn_web::AutumnError;
use http::StatusCode;

use crate::config::ConfigError;

/// An error from the plugin.
///
/// The `Debug` output is the error text. It never holds a secret.
#[derive(Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MqttError {
    /// The configuration is not valid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// The plugin is not installed. The extractor found no client in the app state.
    #[error("the MQTT plugin is not installed: add `MqttPlugin` to the app")]
    NotInstalled,
    /// The client lost the broker connection. The supervisor reconnects in the background.
    #[error("the MQTT client is not connected to the broker")]
    NotConnected,
    /// The broker URL does not parse.
    #[error("the broker URL is not valid: {reason}")]
    #[non_exhaustive]
    InvalidBrokerUrl {
        /// Why the URL does not parse.
        reason: String,
    },
    /// The TLS setup failed. The text names the failing step, not the file content.
    #[error("the TLS setup failed: {reason}")]
    #[non_exhaustive]
    Tls {
        /// Why the TLS setup failed.
        reason: String,
    },
    /// The broker refused an operation, or the client channel broke.
    /// The text names the operation. The detail never holds a secret.
    #[error("the MQTT {operation} failed: {detail}")]
    #[non_exhaustive]
    Broker {
        /// The operation: `publish`, `subscribe`, `unsubscribe` or `disconnect`.
        operation: &'static str,
        /// The broker or client message.
        detail: String,
    },
    /// A publish topic is not a valid MQTT topic name.
    #[error("the topic `{topic}` is not valid: {reason}")]
    #[non_exhaustive]
    InvalidTopic {
        /// The topic.
        topic: String,
        /// Why the topic is not valid.
        reason: String,
    },
    /// A subscribe filter is not a valid MQTT topic filter.
    #[error("the topic filter `{filter}` is not valid: {reason}")]
    #[non_exhaustive]
    InvalidFilter {
        /// The filter.
        filter: String,
        /// Why the filter is not valid.
        reason: String,
    },
}

/// The kind of an [`MqttError`]. A rumqttc update does not change it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The configuration is not valid.
    Config,
    /// The plugin is missing or the broker connection is down.
    Connection,
    /// The broker refused an operation.
    Broker,
    /// A topic or filter is not valid.
    Validation,
}

/// The `Debug` output is the error text. It never holds a secret.
impl std::fmt::Debug for MqttError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl MqttError {
    /// Gives the kind of the error.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::Config(_) | Self::InvalidBrokerUrl { .. } | Self::Tls { .. } => ErrorKind::Config,
            Self::NotInstalled | Self::NotConnected => ErrorKind::Connection,
            Self::Broker { .. } => ErrorKind::Broker,
            Self::InvalidTopic { .. } | Self::InvalidFilter { .. } => ErrorKind::Validation,
        }
    }

    /// Gives the HTTP status code for the error.
    #[must_use]
    pub const fn status_code(&self) -> StatusCode {
        match self.kind() {
            ErrorKind::Config => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Connection | ErrorKind::Broker => StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Validation => StatusCode::BAD_REQUEST,
        }
    }

    /// Converts the error into an Autumn error with the matching status code.
    #[must_use]
    pub fn into_autumn(self) -> AutumnError {
        let message = self.to_string();
        match self.status_code() {
            StatusCode::BAD_REQUEST => AutumnError::bad_request_msg(message),
            StatusCode::SERVICE_UNAVAILABLE => AutumnError::service_unavailable_msg(message),
            _ => AutumnError::internal_server_error_msg(message),
        }
    }
}

#[cfg(test)]
mod tests;
