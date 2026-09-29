//! [`MqttPlugin`]: installs an MQTT client in an Autumn app.
//!
//! # Contract
//!
//! - `build` reads the configuration. A bad configuration stops the boot in the startup hook.
//! - The startup hook builds the client, puts it in the app state, and spawns the supervisor.
//!   The app boots even when the broker is down; the supervisor reconnects in the background.
//! - The shutdown hook signals the supervisor and disconnects the client.
//! - The health indicator and the metrics source read the same client handle.
//! - Autumn ignores a second plugin with the same name.

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use autumn_web::actuator::{MetricFamily, MetricsSource};
use autumn_web::app::AppBuilder;
use autumn_web::plugin::Plugin;
use autumn_web::{AppState, AutumnError};
use tokio::sync::Notify;

use crate::client::{MessageHandler, MqttClient, MqttMessage, Subscription};
use crate::config::{DEFAULT_SECTION, MqttConfig, Qos, topic_filter_error};
use crate::error::MqttError;
use crate::health::MqttHealthIndicator;

/// The plugin name in Autumn diagnostics.
pub const PLUGIN_NAME: &str = "autumn-plugin-mqtt";

/// State that the plugin hooks share.
#[derive(Default)]
pub(crate) struct Shared {
    pub(crate) client: OnceLock<MqttClient>,
}

/// The metrics source: the client counters.
struct Source(Arc<Shared>);

impl MetricsSource for Source {
    fn collect(&self) -> Vec<MetricFamily> {
        self.0
            .client
            .get()
            .map_or_else(Vec::new, |client| client.metrics().families())
    }
}

enum ConfigSource {
    Section(String),
    Explicit(Box<MqttConfig>),
}

type Change = Box<dyn FnOnce(&mut MqttConfig) + Send>;

/// A subscription registered on the builder, before validation.
struct PendingSubscription {
    filter: String,
    qos: Qos,
    handler: Option<MessageHandler>,
}

/// Installs an [`MqttClient`] in an Autumn app.
///
/// ```rust,no_run
/// use autumn_plugin_mqtt::{MqttPlugin, Qos};
///
/// # async fn run() {
/// autumn_web::app()
///     .plugin(
///         MqttPlugin::new().on_message("sensors/#", Qos::AtLeastOnce, |message| async move {
///             println!("{}: {} bytes", message.topic, message.payload.len());
///         }),
///     )
///     .run()
///     .await;
/// # }
/// ```
#[must_use]
pub struct MqttPlugin {
    source: ConfigSource,
    changes: Vec<Change>,
    subscriptions: Vec<PendingSubscription>,
}

impl Default for MqttPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl MqttPlugin {
    /// Makes a plugin that reads `[mqtt]`.
    pub fn new() -> Self {
        Self {
            source: ConfigSource::Section(DEFAULT_SECTION.to_owned()),
            changes: Vec::new(),
            subscriptions: Vec::new(),
        }
    }

    /// Reads `[section]` instead of `[mqtt]`.
    ///
    /// An app can have one MQTT plugin only. Autumn ignores a second plugin with the same name.
    pub fn config_section(mut self, section: impl Into<String>) -> Self {
        self.source = ConfigSource::Section(section.into());
        self
    }

    /// Uses `config` and reads no files or variables.
    pub fn config(mut self, config: MqttConfig) -> Self {
        self.source = ConfigSource::Explicit(Box::new(config));
        self
    }

    /// Changes the configuration after the plugin reads it.
    pub fn configure(mut self, change: impl FnOnce(&mut MqttConfig) + Send + 'static) -> Self {
        self.changes.push(Box::new(change));
        self
    }

    /// Subscribes to a topic filter at startup.
    ///
    /// A bad filter stops the boot in the startup hook.
    pub fn subscribe(mut self, filter: impl Into<String>, qos: Qos) -> Self {
        self.subscriptions.push(PendingSubscription {
            filter: filter.into(),
            qos,
            handler: None,
        });
        self
    }

    /// Subscribes to a topic filter at startup and routes matching messages to `handler`.
    ///
    /// Each message spawns one background task per matching handler. A handler can enqueue
    /// an Autumn `#[job]` from inside to run durable background work.
    /// A bad filter stops the boot in the startup hook.
    pub fn on_message<F, Fut>(mut self, filter: impl Into<String>, qos: Qos, handler: F) -> Self
    where
        F: Fn(MqttMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler: MessageHandler = Arc::new(move |message| {
            Box::pin(handler(message)) as Pin<Box<dyn Future<Output = ()> + Send + 'static>>
        });
        self.subscriptions.push(PendingSubscription {
            filter: filter.into(),
            qos,
            handler: Some(handler),
        });
        self
    }
}

/// Validates the builder state into a config and subscriptions.
fn prepare(
    source: &ConfigSource,
    changes: Vec<Change>,
    subscriptions: Vec<PendingSubscription>,
) -> Result<(MqttConfig, Vec<Subscription>), MqttError> {
    let mut config = match source {
        ConfigSource::Section(section) => MqttConfig::resolve(section)?,
        ConfigSource::Explicit(config) => (**config).clone(),
    };
    for change in changes {
        change(&mut config);
    }
    match source {
        ConfigSource::Section(section) => config.validate_section(section)?,
        ConfigSource::Explicit(_) => config.validate()?,
    }
    let mut out = Vec::with_capacity(subscriptions.len());
    for subscription in subscriptions {
        if let Some(reason) = topic_filter_error(&subscription.filter) {
            return Err(MqttError::InvalidFilter {
                filter: subscription.filter,
                reason,
            });
        }
        out.push(Subscription {
            filter: subscription.filter,
            qos: subscription.qos,
            handler: subscription.handler,
        });
    }
    Ok((config, out))
}

impl Plugin for MqttPlugin {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed(PLUGIN_NAME)
    }

    fn build(self, app: AppBuilder) -> AppBuilder {
        let Self {
            source,
            changes,
            subscriptions,
        } = self;
        let mut app = app;
        if let ConfigSource::Section(section) = &source {
            app = app.config_section(section.clone());
        }
        let prepared = Arc::new(prepare(&source, changes, subscriptions));
        let shared = Arc::new(Shared::default());
        let shutdown = Arc::new(Notify::new());
        app = app.metrics_source("mqtt", Arc::new(Source(Arc::clone(&shared))));
        let health_check = prepared
            .as_ref()
            .as_ref()
            .map_or(true, |(config, _)| config.health_check);
        if health_check {
            app = app.health_indicator(
                "mqtt",
                Arc::new(MqttHealthIndicator::new(Arc::clone(&shared))),
            );
        }
        let on_start = Arc::clone(&shared);
        let stop = Arc::clone(&shutdown);
        app = app.on_startup(move |state: AppState| {
            let shared = Arc::clone(&on_start);
            let prepared = Arc::clone(&prepared);
            let stop = Arc::clone(&stop);
            async move {
                let (config, subscriptions) =
                    prepared.as_ref().clone().map_err(|err| boot_error(&err))?;
                let (client, eventloop) =
                    MqttClient::open(&config, subscriptions).map_err(|err| boot_error(&err))?;
                client
                    .metrics()
                    .identify(client.client_id(), client.broker_host());
                state.insert_extension(client.clone());
                let _ = shared.client.set(client.clone());
                tokio::spawn(client.run(eventloop, stop));
                tracing::info!("the MQTT plugin is ready");
                Ok(())
            }
        });
        let on_stop = Arc::clone(&shared);
        let stop = Arc::clone(&shutdown);
        app.on_shutdown(move || {
            let shared = Arc::clone(&on_stop);
            let stop = Arc::clone(&stop);
            async move {
                stop.notify_waiters();
                if let Some(client) = shared.client.get() {
                    client.disconnect().await;
                }
            }
        })
    }
}

/// A boot error. The text names the plugin and the failure. It never holds a secret.
fn boot_error(err: &dyn std::fmt::Display) -> AutumnError {
    AutumnError::internal_server_error_msg(format!("{PLUGIN_NAME}: {err}"))
}

impl std::fmt::Debug for MqttPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let source = match &self.source {
            ConfigSource::Section(section) => section.as_str(),
            ConfigSource::Explicit(_) => "(explicit)",
        };
        f.debug_struct("MqttPlugin")
            .field("config", &source)
            .field("changes", &self.changes.len())
            .field("subscriptions", &self.subscriptions.len())
            .finish()
    }
}

#[cfg(test)]
mod tests;
