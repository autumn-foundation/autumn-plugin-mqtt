//! Subscribe to a topic and print each message.
//!
//! Run a local broker first, for example `mosquitto -p 1883`, then:
//!
//! ```sh
//! cargo run --example subscribe -- sensors/#
//! mosquitto_pub -t sensors/temp -m "21.5"
//! ```
//!
//! Point at another broker with the environment:
//!
//! ```sh
//! AUTUMN_MQTT__BROKER_URL=mqtt://broker.local cargo run --example subscribe
//! ```

use std::env;

use autumn_plugin_mqtt::{MqttPlugin, Qos};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let filter = env::args().nth(1).unwrap_or_else(|| "sensors/#".to_owned());

    Box::pin(
        autumn_web::app()
            .plugin(
                MqttPlugin::new()
                    .configure(|c| {
                        c.broker_url = String::from("mqtt://localhost:1883");
                        c.client_id = String::from("autumn-example-subscriber");
                    })
                    .on_message(filter, Qos::AtLeastOnce, |message| async move {
                        let payload = message.payload_str().unwrap_or("<binary>");
                        println!("{} [{:?}]: {payload}", message.topic, message.qos);
                    }),
            )
            .run(),
    )
    .await;
}
