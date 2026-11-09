//! Subscribe to a public channel and print events.
//!
//! ```bash
//! cargo run --example basic -- 127.0.0.1 6001 app-key my-channel
//! ```

use std::time::Duration;

use serde::Deserialize;
use sockudo_client::{ConnectionState, Result, SockudoClient, Transport};

#[derive(Debug, Deserialize)]
struct Price {
    symbol: String,
    price: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let host = args.next().unwrap_or_else(|| "127.0.0.1".into());
    let port = args.next().and_then(|p| p.parse().ok()).unwrap_or(6001);
    let key = args.next().unwrap_or_else(|| "app-key".into());
    let channel_name = args.next().unwrap_or_else(|| "prices".into());

    let client = SockudoClient::builder(key)
        .ws_host(host)
        .ws_port(port)
        .force_tls(false)
        .disabled_transports([Transport::Wss])
        .connection_recovery(true)
        .reconnect_jitter(0.5)
        .build()?;

    client.on_state_change(|change| println!("state: {} -> {}", change.previous, change.current));
    client.on_error(|error| eprintln!("error: {error}"));

    let channel = client.subscribe(&channel_name);
    channel.bind("sockudo:subscription_succeeded", |_| println!("subscribed"));
    // Typed payloads deserialize straight from the frame.
    channel.bind("price", |event| match event.json::<Price>() {
        Ok(price) => println!("{} = {}", price.symbol, price.price),
        Err(_) => println!("price: {:?}", event.value()),
    });

    // Or consume everything as an async stream.
    let mut stream = channel.stream(1024);
    tokio::spawn(async move {
        while let Some(event) = stream.recv().await {
            println!("[{}] {}", event.event, event.text().unwrap_or_default());
        }
    });

    client.connect()?;
    let socket_id = tokio::time::timeout(Duration::from_secs(10), client.connected())
        .await
        .map_err(|_| sockudo_client::Error::Timeout)??;
    println!("connected as {socket_id}");

    tokio::signal::ctrl_c().await?;
    client.disconnect().await;
    assert_eq!(client.state(), ConnectionState::Disconnected);
    Ok(())
}
