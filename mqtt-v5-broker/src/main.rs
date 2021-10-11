use mqtt_v5_broker::{server_loop, websocket_server_loop, Broker};
use tokio::runtime::Runtime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a Runtime does the following:
    // * Spawn a background thread running a Reactor instance.
    // * Start a ThreadPool for executing futures.
    // * Run an instance of Timer per thread pool worker thread.
    let runtime = Runtime::new()?;

    let broker = Broker::new();
    let broker_tx = broker.sender();
    runtime.spawn(broker.run());

    let server_future = server_loop(broker_tx.clone());
    let websocket_future = websocket_server_loop(broker_tx);

    runtime.spawn(websocket_future);
    runtime.block_on(server_future);

    Ok(())
}
