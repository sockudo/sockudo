//! Join a presence channel authorized by your backend.
//!
//! The auth endpoint receives a form POST with `socket_id` and
//! `channel_name` and must return `{"auth": "...", "channel_data": "..."}`
//! signed with the app secret (any Sockudo/Pusher server SDK can do this).
//!
//! ```bash
//! cargo run --example presence -- http://localhost:3000/sockudo/auth presence-lobby
//! ```

use sockudo_client::{Endpoint, Result, SockudoClient, Transport};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let auth_url = args
        .next()
        .unwrap_or_else(|| "http://localhost:3000/sockudo/auth".into());
    let channel_name = args.next().unwrap_or_else(|| "presence-lobby".into());

    let client = SockudoClient::builder("app-key")
        .ws_host("127.0.0.1")
        .ws_port(6001)
        .force_tls(false)
        .disabled_transports([Transport::Wss])
        .channel_auth_endpoint(Endpoint::new(auth_url).headers_provider(|| {
            // Attach the user's session for your backend to authorize.
            vec![("Authorization".into(), "Bearer <session-token>".into())]
        }))
        .build()?;

    let channel = client.subscribe(&channel_name);
    let members = channel.clone();
    channel.bind("sockudo:subscription_succeeded", move |_| {
        if let Some(members) = members.members() {
            println!("{} members, me = {:?}", members.count(), members.me());
        }
    });
    channel.bind("sockudo:member_added", |event| {
        println!("joined: {}", event.value())
    });
    channel.bind("sockudo:member_removed", |event| {
        println!("left: {}", event.value())
    });
    channel.bind("sockudo:subscription_error", |event| {
        eprintln!("auth failed: {}", event.value())
    });

    client.connect()?;
    client.connected().await?;
    channel
        .update_presence(&sonic_rs::json!({"status": "online"}))
        .await?;

    tokio::signal::ctrl_c().await?;
    client.disconnect().await;
    Ok(())
}
