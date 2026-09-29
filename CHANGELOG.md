# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.1.0] - 2026-09-29

### Added

- `MqttPlugin`: registers the MQTT client, health indicator, and metrics with
  an Autumn app.
- `MqttClient`: publish (plain and retained), subscribe, unsubscribe over
  MQTT v5 via rumqttc. Usable as an axum extractor or via
  `MqttClient::from_state`.
- `on_message` / `subscribe` with topic filters, MQTT `+`/`#` matching, and
  the `$`-topic rule from MQTT-4.7.2-1.
- Layered `[mqtt]` config: base `autumn.toml`, inline profile, profile
  override file, `AUTUMN_MQTT__*` env overrides, with validation.
- TLS via `mqtts://` / `tls://` / `ssl://` broker URLs: CA certificate plus
  optional client certificate and key, from PEM files.
- Optional last will, exponential reconnect backoff, re-subscribe on
  reconnect.
- Health-only `mqtt` indicator and `mqtt_*` Prometheus metrics labeled with
  `client_id` and broker host.
- Secret-safe logs and error text: no passwords, payloads, or certificate
  content.
