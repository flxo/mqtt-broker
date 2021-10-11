use futures::{future::try_join, FutureExt};
use mqtt_v5_broker::{listen, Broker};
use tokio::{io, runtime::Runtime};

fn main() -> io::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    // Creating a Runtime does the following:
    // * Spawn a background thread running a Reactor instance.
    // * Start a ThreadPool for executing futures.
    // * Run an instance of Timer per thread pool worker thread.
    let runtime = Runtime::new()?;

    let broker = Broker::default();
    let broker_tx = broker.sender();
    runtime.spawn(broker.run());

    let tcp_server = listen("tcp://0.0.0.0:1883", broker_tx.clone());
    let websocket_server = listen("wss://0.0.0.0:8080", broker_tx);
    let servers = try_join(tcp_server, websocket_server).map(|_| Ok(()));
    runtime.block_on(servers)
}
