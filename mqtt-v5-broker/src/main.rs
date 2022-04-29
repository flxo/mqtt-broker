use std::{env, io};

use futures::future::try_join_all;
use log::{debug, info, trace};
use mqtt_v5::types::{
    properties::{AuthenticationData, AuthenticationMethod},
    AuthenticatePacket, PublishPacket, SubscribePacket,
};
use mqtt_v5_broker::{
    broker::{Broker, BrokerMessage},
    client,
    plugin::{self},
};
use tokio::{net::TcpListener, sync::mpsc::Sender, task};

/// Bind tcp address TODO: make this configurable
const TCP_LISTENER_ADDR: &str = "0.0.0.0:1883";

/// Websocket tcp address TODO: make this configurable
const WEBSOCKET_TCP_LISTENER_ADDR: &str = "0.0.0.0:8080";

async fn tcp_server_loop(broker_tx: Sender<BrokerMessage>) -> io::Result<()> {
    info!("Listening on {}", TCP_LISTENER_ADDR);
    let listener = TcpListener::bind(TCP_LISTENER_ADDR).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Client {} connected (tcp)", addr);
        client::spawn(stream, broker_tx.clone());
    }
}

async fn websocket_server_loop(broker_tx: Sender<BrokerMessage>) -> io::Result<()> {
    info!("Listening on {}", WEBSOCKET_TCP_LISTENER_ADDR);
    let listener = TcpListener::bind(WEBSOCKET_TCP_LISTENER_ADDR).await?;

    loop {
        let (socket, addr) = listener.accept().await?;
        debug!("Client {} connected (websocket)", addr);
        client::spawn_websocket(socket, broker_tx.clone()).await;
    }
}

fn init_logging() {
    if env::var("RUST_LOG").is_err() {
        env_logger::builder().filter(None, log::LevelFilter::Debug).init();
    } else {
        env_logger::init();
    }
}

#[derive(Default)]
struct TracePlugin;

impl plugin::Plugin for TracePlugin {
    fn on_connect(
        &mut self,
        _: Option<&AuthenticationMethod>,
        _: Option<&AuthenticationData>,
    ) -> plugin::AuthentificationResult {
        plugin::AuthentificationResult::Success
    }

    fn on_authenticate(&mut self, packet: &AuthenticatePacket) -> plugin::AuthentificationResult {
        trace!("Authenticate packet received: {:?}", packet);
        plugin::AuthentificationResult::Success
    }

    fn on_subscribe(&mut self, packet: &SubscribePacket) -> plugin::SubscribeResult {
        trace!("Subscribe packet received: {:?}", packet);
        plugin::SubscribeResult::Placeholder
    }

    fn on_publish_received(&mut self, packet: &PublishPacket) -> plugin::PublishReceivedResult {
        trace!("Publish packet received: {:?}", packet);
        plugin::PublishReceivedResult::Placeholder
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let broker = Broker::with_plugin(TracePlugin::default());
    let broker_tx = broker.sender();
    let broker = task::spawn(async {
        broker.run().await;
        Ok(())
    });

    let tcp_listener = task::spawn(tcp_server_loop(broker_tx.clone()));
    let websocket_listener = task::spawn(websocket_server_loop(broker_tx));

    try_join_all([broker, tcp_listener, websocket_listener]).await?;

    Ok(())
}
