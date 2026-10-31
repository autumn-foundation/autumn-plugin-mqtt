//! The MQTT client: publish, subscribe, and route messages to handlers.
//!
//! # Contract
//!
//! - [`MqttClient::open`] builds the client from a validated [`MqttConfig`](crate::config::MqttConfig).
//!   It reads the TLS files. It does not connect: the supervisor connects on the first poll.
//!   A bad configuration stops the boot in the startup hook.
//! - The supervisor ([`MqttClient::run`]) polls the rumqttc event loop forever. rumqttc
//!   reconnects on network loss inside `poll()`; the supervisor adds the backoff wait from
//!   the configuration and re-subscribes every registered filter after an error, because
//!   rumqttc does not re-subscribe on reconnect.
//! - Each incoming message spawns one background task per matching handler. A handler never
//!   blocks the event loop. A panicking handler ends its own task; the loop keeps running.
//! - `publish` queues the message in the rumqttc channel. It succeeds while offline; the
//!   broker receives the message after the reconnect. It fails only when the channel is full.
//! - The connection state is best-effort: any `poll()` success means connected, any error
//!   means disconnected. The health check reads it.
//! - Logs and error text never hold secrets: no passwords, payloads, or certificate content.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use autumn_web::{AppState, AutumnError};
use rumqttc::v5::mqttbytes::v5::{LastWill, Packet};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions};
use rumqttc::{NetworkOptions, TlsConfiguration, Transport};
use tokio::sync::{Notify, RwLock};

use crate::config::{BrokerParts, MqttConfig, Qos, topic_filter_error, topic_name_error};
use crate::error::MqttError;
use crate::metrics::ClientMetrics;

/// A message handler. The plugin spawns each call as a background task.
///
/// Register one with [`MqttPlugin::on_message`](crate::plugin::MqttPlugin::on_message).
/// A handler can enqueue an Autumn `#[job]` from inside to run durable background work.
pub type MessageHandler = Arc<
    dyn Fn(MqttMessage) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

/// A message from the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttMessage {
    /// The topic the broker published to.
    pub topic: String,
    /// The message payload bytes.
    pub payload: Vec<u8>,
    /// The quality of service of the delivery.
    pub qos: Qos,
    /// `true` when the broker sent a retained message.
    pub retain: bool,
    /// The MQTT packet id. Zero for `QoS` 0.
    pub packet_id: u16,
}

impl MqttMessage {
    /// Reads the payload as UTF-8. Returns `None` for binary payloads.
    #[must_use]
    pub fn payload_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.payload).ok()
    }
}

/// The connection state of the client. Best-effort: read from [`MqttClient::connection_state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// The client has no live broker connection.
    Disconnected,
    /// The last event-loop poll succeeded.
    Connected,
}

/// A registered subscription.
#[derive(Clone)]
pub(crate) struct Subscription {
    pub(crate) filter: String,
    pub(crate) qos: Qos,
    pub(crate) handler: Option<MessageHandler>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("filter", &self.filter)
            .field("qos", &self.qos)
            .field("has_handler", &self.handler.is_some())
            .finish()
    }
}

/// Matches an MQTT topic filter against a topic name.
///
/// `+` matches one level, `#` matches the rest. A filter that starts with a wildcard never
/// matches a `$`-prefixed topic, per MQTT-4.7.2-1. The filter must be valid; see
/// [`topic_filter_error`](crate::config::topic_filter_error).
#[must_use]
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') {
        let first = filter.split('/').next();
        if matches!(first, Some("+" | "#")) {
            return false;
        }
    }
    let mut filters = filter.split('/');
    let mut topics = topic.split('/');
    loop {
        match (filters.next(), topics.next()) {
            (Some("#"), _) | (None, None) => return true,
            (Some("+"), Some(_)) => {}
            (Some(expected), Some(actual)) => {
                if expected != actual {
                    return false;
                }
            }
            _ => return false,
        }
    }
}

/// The MQTT client. Clone it freely: clones share the connection.
///
/// Get it in a handler with the extractor, or in a job or task with
/// [`MqttClient::from_state`].
#[derive(Clone)]
pub struct MqttClient {
    inner: Arc<Inner>,
}

struct Inner {
    client: AsyncClient,
    client_id: String,
    broker: BrokerParts,
    subscriptions: RwLock<Vec<Subscription>>,
    state: AtomicU8,
    metrics: Arc<ClientMetrics>,
    reconnect_initial: Duration,
    reconnect_max: Duration,
}

impl std::fmt::Debug for MqttClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttClient")
            .field("client_id", &self.inner.client_id)
            .field("broker_host", &self.inner.broker.host)
            .field("connected", &self.is_connected())
            .finish()
    }
}

impl MqttClient {
    /// Builds the client from a validated config. Reads the TLS files.
    ///
    /// Does not connect: the supervisor connects on the first poll, so the app boots even
    /// when the broker is down. The supervisor then reconnects in the background.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError`] when a TLS file can not be read.
    pub(crate) fn open(
        config: &MqttConfig,
        subscriptions: Vec<Subscription>,
    ) -> Result<(Self, EventLoop), MqttError> {
        let broker = config.broker_parts();
        let mut options =
            MqttOptions::new(config.client_id.clone(), broker.host.clone(), broker.port);
        options.set_keep_alive(config.keep_alive());
        options.set_clean_start(config.clean_start);
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            options.set_credentials(username.clone(), password.clone());
        }
        if let Some(topic) = &config.will_topic {
            let will = LastWill::new(
                topic.clone(),
                config.will_payload.clone().unwrap_or_default(),
                config.will_qos.to_rumqttc(),
                config.will_retain,
                None,
            );
            options.set_last_will(will);
        }
        if broker.tls {
            options.set_transport(tls_config(config)?);
        }
        let mut network = NetworkOptions::new();
        network.set_connection_timeout(config.connect_timeout_secs);
        options.set_network_options(network);
        let (client, eventloop) = AsyncClient::new(options, config.request_channel_capacity);
        let inner = Inner {
            client,
            client_id: config.client_id.clone(),
            broker,
            subscriptions: RwLock::new(subscriptions),
            state: AtomicU8::new(ConnectionState::Disconnected as u8),
            metrics: Arc::new(ClientMetrics::default()),
            reconnect_initial: Duration::from_millis(config.reconnect_initial_ms),
            reconnect_max: Duration::from_millis(config.reconnect_max_ms),
        };
        Ok((
            Self {
                inner: Arc::new(inner),
            },
            eventloop,
        ))
    }

    /// Publishes a message. Queues it when offline; the broker gets it after the reconnect.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError`] for a bad topic or a full request channel.
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        qos: Qos,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), MqttError> {
        self.publish_inner(topic.into(), qos, payload.into(), false)
            .await
    }

    /// Publishes a retained message. The broker keeps it for future subscribers.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError`] for a bad topic or a full request channel.
    pub async fn publish_retained(
        &self,
        topic: impl Into<String>,
        qos: Qos,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), MqttError> {
        self.publish_inner(topic.into(), qos, payload.into(), true)
            .await
    }

    async fn publish_inner(
        &self,
        topic: String,
        qos: Qos,
        payload: Vec<u8>,
        retain: bool,
    ) -> Result<(), MqttError> {
        if let Some(reason) = topic_name_error(&topic) {
            return Err(MqttError::InvalidTopic {
                topic,
                reason: reason.to_owned(),
            });
        }
        match self
            .inner
            .client
            .publish(topic, qos.to_rumqttc(), retain, payload)
            .await
        {
            Ok(()) => {
                self.inner.metrics.note_publish();
                Ok(())
            }
            Err(err) => {
                self.inner.metrics.note_publish_error();
                Err(MqttError::Broker {
                    operation: "publish",
                    detail: err.to_string(),
                })
            }
        }
    }

    /// Subscribes to a topic filter. Registers the filter for re-subscribe on reconnect.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError`] for a bad filter or a full request channel.
    pub async fn subscribe(&self, filter: impl Into<String>, qos: Qos) -> Result<(), MqttError> {
        self.subscribe_inner(filter.into(), qos, None).await
    }

    /// Subscribes to a topic filter and routes matching messages to `handler`.
    ///
    /// Each message spawns one background task per matching handler.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError`] for a bad filter or a full request channel.
    pub async fn subscribe_with_handler<F, Fut>(
        &self,
        filter: impl Into<String>,
        qos: Qos,
        handler: F,
    ) -> Result<(), MqttError>
    where
        F: Fn(MqttMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler: MessageHandler = Arc::new(move |message| {
            Box::pin(handler(message)) as Pin<Box<dyn Future<Output = ()> + Send + 'static>>
        });
        self.subscribe_inner(filter.into(), qos, Some(handler))
            .await
    }

    async fn subscribe_inner(
        &self,
        filter: String,
        qos: Qos,
        handler: Option<MessageHandler>,
    ) -> Result<(), MqttError> {
        if let Some(reason) = topic_filter_error(&filter) {
            return Err(MqttError::InvalidFilter { filter, reason });
        }
        {
            let mut subscriptions = self.inner.subscriptions.write().await;
            if let Some(existing) = subscriptions.iter_mut().find(|s| s.filter == filter) {
                existing.qos = qos;
                if handler.is_some() {
                    existing.handler.clone_from(&handler);
                }
            } else {
                subscriptions.push(Subscription {
                    filter: filter.clone(),
                    qos,
                    handler,
                });
            }
        }
        self.inner
            .client
            .subscribe(filter.clone(), qos.to_rumqttc())
            .await
            .map_err(|err| {
                self.inner.metrics.note_publish_error();
                MqttError::Broker {
                    operation: "subscribe",
                    detail: err.to_string(),
                }
            })
    }

    /// Unsubscribes from a topic filter. Drops its handler registration.
    ///
    /// # Errors
    ///
    /// Returns [`MqttError`] for a bad filter or a full request channel.
    pub async fn unsubscribe(&self, filter: impl Into<String>) -> Result<(), MqttError> {
        let filter = filter.into();
        if let Some(reason) = topic_filter_error(&filter) {
            return Err(MqttError::InvalidFilter { filter, reason });
        }
        {
            let mut subscriptions = self.inner.subscriptions.write().await;
            subscriptions.retain(|s| s.filter != filter);
        }
        self.inner
            .client
            .unsubscribe(filter)
            .await
            .map_err(|err| MqttError::Broker {
                operation: "unsubscribe",
                detail: err.to_string(),
            })
    }

    /// Disconnects from the broker. The supervisor loop stops after this.
    pub async fn disconnect(&self) {
        if let Err(err) = self.inner.client.disconnect().await {
            tracing::debug!(error = %err, "the MQTT disconnect request failed");
        }
        self.mark_disconnected();
    }

    /// Reads the client from the app state, for example in a job or a task.
    #[must_use]
    pub fn from_state(state: &AppState) -> Option<Self> {
        state.extension::<Self>().map(|client| (*client).clone())
    }

    /// Gives the connection state. Best-effort: any poll success means connected.
    #[must_use]
    pub fn connection_state(&self) -> ConnectionState {
        if self.inner.state.load(Ordering::Relaxed) == ConnectionState::Connected as u8 {
            ConnectionState::Connected
        } else {
            ConnectionState::Disconnected
        }
    }

    /// Returns `true` when the last event-loop poll succeeded.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connection_state() == ConnectionState::Connected
    }

    /// Gives the configured client id.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.inner.client_id
    }

    /// Gives the broker host. Never a secret.
    #[must_use]
    pub fn broker_host(&self) -> &str {
        &self.inner.broker.host
    }

    /// Gives the client metrics for the metrics source.
    #[must_use]
    pub(crate) fn metrics(&self) -> &Arc<ClientMetrics> {
        &self.inner.metrics
    }

    /// Polls the event loop forever. Spawn this once per client.
    ///
    /// Stops when `shutdown` fires or the request channel closes.
    // `tokio::select!` expands to nested branches. Clippy 1.88 counts them
    // in the cognitive complexity score.
    #[allow(clippy::cognitive_complexity)]
    pub(crate) async fn run(self, mut eventloop: EventLoop, shutdown: Arc<Notify>) {
        self.resubscribe().await;
        let mut backoff = Backoff::new(self.inner.reconnect_initial, self.inner.reconnect_max);
        loop {
            tokio::select! {
                event = eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(Packet::Publish(publish))) => {
                            self.mark_connected();
                            backoff.reset();
                            self.dispatch(publish).await;
                        }
                        Ok(_) => {
                            self.mark_connected();
                            backoff.reset();
                        }
                        Err(err) => {
                            if matches!(err, rumqttc::v5::ConnectionError::RequestsDone) {
                                tracing::info!("the MQTT event loop stops: no client left");
                                break;
                            }
                            self.mark_disconnected();
                            self.inner.metrics.note_connection_error();
                            tracing::warn!(error = %err, "the MQTT connection broke; the client reconnects");
                            backoff.wait().await;
                            self.resubscribe().await;
                        }
                    }
                }
                () = shutdown.notified() => {
                    tracing::info!("the MQTT event loop stops");
                    break;
                }
            }
        }
    }

    /// Sends every registered subscription. rumqttc does not re-subscribe on reconnect.
    async fn resubscribe(&self) {
        let subscriptions = self.inner.subscriptions.read().await;
        for subscription in subscriptions.iter() {
            if let Err(err) = self
                .inner
                .client
                .subscribe(subscription.filter.clone(), subscription.qos.to_rumqttc())
                .await
            {
                tracing::warn!(
                    filter = %subscription.filter,
                    error = %err,
                    "the MQTT re-subscribe failed; the next error retries it"
                );
            }
        }
    }

    /// Routes one publish to every matching handler, each in its own task.
    async fn dispatch(&self, publish: rumqttc::v5::mqttbytes::v5::Publish) {
        self.inner.metrics.note_received();
        let topic = String::from_utf8_lossy(&publish.topic).into_owned();
        let message = MqttMessage {
            topic,
            payload: publish.payload.to_vec(),
            qos: Qos::from_rumqttc(publish.qos),
            retain: publish.retain,
            packet_id: publish.pkid,
        };
        let subscriptions = self.inner.subscriptions.read().await;
        for subscription in subscriptions
            .iter()
            .filter(|s| topic_matches(&s.filter, &message.topic))
        {
            if let Some(handler) = &subscription.handler {
                let handler = Arc::clone(handler);
                let message = message.clone();
                tokio::spawn(async move { handler(message).await });
            }
        }
    }

    fn mark_connected(&self) {
        let was = self
            .inner
            .state
            .swap(ConnectionState::Connected as u8, Ordering::Relaxed);
        self.inner.metrics.set_connected(true);
        if was == ConnectionState::Disconnected as u8 {
            tracing::info!(client_id = %self.inner.client_id, "the MQTT client connected");
        }
    }

    fn mark_disconnected(&self) {
        self.inner
            .state
            .store(ConnectionState::Disconnected as u8, Ordering::Relaxed);
        self.inner.metrics.set_connected(false);
    }
}

impl axum::extract::FromRequestParts<AppState> for MqttClient {
    type Rejection = AutumnError;

    // The body has nothing to await, so it returns a ready future.
    fn from_request_parts(
        _parts: &mut http::request::Parts,
        state: &AppState,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(
            Self::from_state(state).ok_or_else(|| MqttError::NotInstalled.into_autumn()),
        )
    }
}

/// Builds the TLS transport from the validated config.
fn tls_config(config: &MqttConfig) -> Result<Transport, MqttError> {
    let ca_path = config.tls_ca_cert.as_ref().ok_or_else(|| MqttError::Tls {
        reason: "tls_ca_cert is required for TLS".to_owned(),
    })?;
    let ca = read_tls_file(ca_path, "tls_ca_cert")?;
    let client_auth = match (&config.tls_client_cert, &config.tls_client_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert = read_tls_file(cert_path, "tls_client_cert")?;
            let key = read_tls_file(key_path, "tls_client_key")?;
            Some((cert, key))
        }
        _ => None,
    };
    Ok(Transport::Tls(TlsConfiguration::Simple {
        ca,
        alpn: None,
        client_auth,
    }))
}

/// Reads a TLS PEM file. The error names the key and the path, never the content.
fn read_tls_file(path: &std::path::Path, key: &str) -> Result<Vec<u8>, MqttError> {
    std::fs::read(path).map_err(|err| MqttError::Tls {
        reason: format!("{key} `{}` can not be read: {err}", path.display()),
    })
}

/// The exponential reconnect backoff.
struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    const fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            current: initial,
        }
    }

    const fn reset(&mut self) {
        self.current = self.initial;
    }

    async fn wait(&mut self) {
        tokio::time::sleep(self.current).await;
        self.current = self.current.saturating_mul(2).min(self.max);
    }
}

#[cfg(test)]
mod tests;
