//! Live tests against a real Sockudo server.
//!
//! ```bash
//! cargo run -p sockudo -- --config config/config.toml   # from the repo root
//! SOCKUDO_LIVE_TESTS=1 cargo test --test live
//! ```
//!
//! Defaults match `config/config.toml` (`app-id` / `app-key` / `app-secret`
//! on `127.0.0.1:6001`); override with `SOCKUDO_HOST`, `SOCKUDO_PORT`,
//! `SOCKUDO_APP_ID`, `SOCKUDO_APP_KEY`, `SOCKUDO_APP_SECRET`. The tests sign
//! auth and REST calls themselves, exactly as an application backend would.
#![cfg(feature = "http")]
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use sockudo_client::rest::{ChannelHistoryParams, Direction, ProxyRequest};
use sockudo_client::{
    ChannelAuthData, ChannelAuthRequest, ChannelDeltaSettings, ConnectionState, DeltaOptions,
    Event, ProtocolVersion, ProxyOptions, Rewind, SockudoClient, SubscribeOptions, Transport,
    UserAuthData, UserAuthRequest, WireFormat,
};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value, json};
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(10);

/// Pusher-compatible signing, as a backend holding the app secret does it.
mod sign {
    use std::collections::BTreeMap;

    use hmac::{Hmac, KeyInit, Mac};
    use md5::{Digest as _, Md5};
    use sha2::Sha256;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn hmac_hex(secret: &str, data: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(data.as_bytes());
        hex(&mac.finalize().into_bytes())
    }

    pub fn channel_auth(
        key: &str,
        secret: &str,
        socket_id: &str,
        channel: &str,
        channel_data: Option<&str>,
    ) -> String {
        let mut to_sign = format!("{socket_id}:{channel}");
        if let Some(data) = channel_data {
            to_sign.push(':');
            to_sign.push_str(data);
        }
        format!("{key}:{}", hmac_hex(secret, &to_sign))
    }

    pub fn user_auth(key: &str, secret: &str, socket_id: &str, user_data: &str) -> String {
        format!(
            "{key}:{}",
            hmac_hex(secret, &format!("{socket_id}::user::{user_data}"))
        )
    }

    /// `sha256(channel_name || master_key)`: the Pusher encrypted-channel key.
    pub fn shared_secret(channel: &str, master_key: &[u8; 32]) -> [u8; 32] {
        use sha2::Digest;
        let mut hasher = Sha256::new();
        hasher.update(channel.as_bytes());
        hasher.update(master_key);
        hasher.finalize().into()
    }

    /// A signed query string for a REST call.
    pub fn query(
        key: &str,
        secret: &str,
        method: &str,
        path: &str,
        body: Option<&str>,
        extra: &BTreeMap<String, String>,
    ) -> String {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut params = extra.clone();
        params.insert("auth_key".into(), key.into());
        params.insert("auth_timestamp".into(), timestamp.to_string());
        params.insert("auth_version".into(), "1.0".into());
        if let Some(body) = body {
            params.insert("body_md5".into(), hex(&Md5::digest(body.as_bytes())));
        }
        let to_sign = params
            .iter()
            .map(|(k, v)| format!("{}={v}", k.to_lowercase()))
            .collect::<Vec<_>>()
            .join("&");
        let signature = hmac_hex(
            secret,
            &format!("{}\n{path}\n{to_sign}", method.to_uppercase()),
        );
        let mut encoded = form_urlencoded::Serializer::new(String::new());
        for (k, v) in &params {
            encoded.append_pair(k, v);
        }
        encoded.append_pair("auth_signature", &signature);
        encoded.finish()
    }
}

struct Live {
    host: String,
    port: u16,
    app_id: String,
    key: String,
    secret: String,
}

fn live() -> Option<Arc<Live>> {
    if std::env::var("SOCKUDO_LIVE_TESTS").ok().as_deref() != Some("1") {
        eprintln!("skipping: set SOCKUDO_LIVE_TESTS=1 to run against a live server");
        return None;
    }
    let var =
        |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_owned());
    Some(Arc::new(Live {
        host: var("SOCKUDO_HOST", "127.0.0.1"),
        port: var("SOCKUDO_PORT", "6001").parse().unwrap(),
        app_id: var("SOCKUDO_APP_ID", "app-id"),
        key: var("SOCKUDO_APP_KEY", "app-key"),
        secret: var("SOCKUDO_APP_SECRET", "app-secret"),
    }))
}

fn unique(prefix: &str) -> String {
    format!("{prefix}{}", fastrand::u64(..))
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

impl Live {
    fn sign_channel(
        &self,
        request: &ChannelAuthRequest,
        presence: Option<&Value>,
        master_key: Option<&[u8; 32]>,
    ) -> ChannelAuthData {
        let channel_data = presence.map(Value::to_string);
        let auth = sign::channel_auth(
            &self.key,
            &self.secret,
            &request.socket_id,
            &request.channel_name,
            channel_data.as_deref(),
        );
        let mut data = ChannelAuthData::new(auth);
        data.channel_data = channel_data;
        if let Some(master_key) = master_key {
            data.shared_secret = Some(b64(&sign::shared_secret(&request.channel_name, master_key)));
        }
        data
    }

    fn builder(self: &Arc<Self>) -> sockudo_client::ClientBuilder {
        let signer = Arc::clone(self);
        SockudoClient::builder(&self.key)
            .ws_host(&self.host)
            .ws_port(self.port)
            .force_tls(false)
            .disabled_transports([Transport::Wss])
            .channel_authorizer(move |request: ChannelAuthRequest| {
                let data = signer.sign_channel(&request, None, None);
                async move { Ok(data) }
            })
    }

    fn base_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    /// Publish like `POST /apps/{id}/events` from a server SDK.
    async fn publish_raw(&self, channel: &str, event: &str, data: String) {
        let path = format!("/apps/{}/events", self.app_id);
        let body =
            sonic_rs::to_string(&json!({"name": event, "channels": [channel], "data": data}))
                .unwrap();
        let query = sign::query(
            &self.key,
            &self.secret,
            "POST",
            &path,
            Some(&body),
            &BTreeMap::new(),
        );
        let response = reqwest::Client::new()
            .post(format!("{}{path}?{query}", self.base_url()))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        assert!(
            status.is_success(),
            "publish failed: {status} {}",
            response.text().await.unwrap_or_default()
        );
    }

    async fn publish(&self, channel: &str, event: &str, data: &Value) {
        self.publish_raw(channel, event, data.to_string()).await;
    }
}

async fn next(receiver: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    tokio::time::timeout(WAIT, receiver.recv())
        .await
        .expect("timed out waiting for an event")
        .unwrap()
}

fn forward(channel: &sockudo_client::Channel, event: &str) -> mpsc::UnboundedReceiver<Event> {
    let (sender, receiver) = mpsc::unbounded_channel();
    channel.bind(event, move |event| {
        let _ = sender.send(event.clone());
    });
    receiver
}

async fn subscribed(client: &SockudoClient, channel: &sockudo_client::Channel) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !channel.is_subscribed() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} never subscribed on {:?}",
            channel.name(),
            client.state()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn connect(client: &SockudoClient) -> String {
    client.connect().unwrap();
    tokio::time::timeout(WAIT, client.connected())
        .await
        .unwrap()
        .unwrap()
}

async fn publish_round_trip(protocol: ProtocolVersion, format: WireFormat) {
    let Some(live) = live() else { return };
    let client = live
        .builder()
        .protocol_version(protocol)
        .wire_format(format)
        .build()
        .unwrap();
    let name = unique("live-public-");
    let channel = client.subscribe(&name);
    let mut events = forward(&channel, "price");
    connect(&client).await;
    subscribed(&client, &channel).await;
    live.publish(&name, "price", &json!({"symbol": "BTC", "price": 101.5}))
        .await;
    let event = next(&mut events).await;
    assert_eq!(event.channel.as_deref(), Some(name.as_str()));
    assert_eq!(event.value()["price"].as_f64(), Some(101.5));
    if protocol.is_v2() {
        assert!(event.message_id.is_some(), "V2 deliveries carry message_id");
    }
    client.disconnect().await;
}

#[tokio::test]
async fn v2_json_publish_round_trip() {
    publish_round_trip(ProtocolVersion::V2, WireFormat::Json).await;
}

#[tokio::test]
async fn v1_publish_round_trip() {
    publish_round_trip(ProtocolVersion::V1, WireFormat::Json).await;
}

#[cfg(feature = "msgpack")]
#[tokio::test]
async fn messagepack_publish_round_trip() {
    publish_round_trip(ProtocolVersion::V2, WireFormat::MessagePack).await;
}

#[cfg(feature = "protobuf")]
#[tokio::test]
async fn protobuf_publish_round_trip() {
    publish_round_trip(ProtocolVersion::V2, WireFormat::Protobuf).await;
}

fn presence_client(live: &Arc<Live>, user: &str) -> SockudoClient {
    let signer = Arc::clone(live);
    let member = json!({"user_id": user, "user_info": {"name": user}});
    live.builder()
        .channel_authorizer(move |request: ChannelAuthRequest| {
            let data = signer.sign_channel(&request, Some(&member), None);
            async move { Ok(data) }
        })
        .build()
        .unwrap()
}

#[tokio::test]
async fn presence_members_join_update_and_leave() {
    let Some(live) = live() else { return };
    let name = unique("presence-live-");
    let alice = presence_client(&live, "alice");
    let alice_channel = alice.subscribe(&name);
    let mut added = forward(&alice_channel, "sockudo:member_added");
    let mut updated = forward(&alice_channel, "sockudo:presence_update");
    let mut removed = forward(&alice_channel, "sockudo:member_removed");
    connect(&alice).await;
    subscribed(&alice, &alice_channel).await;
    assert_eq!(alice_channel.members().unwrap().me().unwrap().id, "alice");

    let bob = presence_client(&live, "bob");
    let bob_channel = bob.subscribe(&name);
    connect(&bob).await;
    subscribed(&bob, &bob_channel).await;
    assert_eq!(bob_channel.members().unwrap().count(), 2);
    assert_eq!(
        next(&mut added).await.value()["user_id"].as_str(),
        Some("bob")
    );
    assert_eq!(alice_channel.members().unwrap().count(), 2);

    bob_channel
        .update_presence(&json!({"name": "bob", "status": "away"}))
        .await
        .unwrap();
    let update = next(&mut updated).await;
    assert_eq!(update.value()["user_info"]["status"].as_str(), Some("away"));
    assert_eq!(
        alice_channel.members().unwrap().get("bob").unwrap().info["status"].as_str(),
        Some("away")
    );

    bob.disconnect().await;
    assert_eq!(
        next(&mut removed).await.value()["user_id"].as_str(),
        Some("bob")
    );
    assert_eq!(alice_channel.members().unwrap().count(), 1);
    alice.disconnect().await;
}

#[tokio::test]
async fn client_events_between_private_subscribers() {
    let Some(live) = live() else { return };
    let name = unique("private-live-");
    let sender = live.builder().build().unwrap();
    let receiver = live.builder().build().unwrap();
    let send_channel = sender.subscribe(&name);
    let receive_channel = receiver.subscribe(&name);
    let mut moves = forward(&receive_channel, "client-move");
    connect(&sender).await;
    connect(&receiver).await;
    subscribed(&sender, &send_channel).await;
    subscribed(&receiver, &receive_channel).await;
    send_channel
        .trigger("client-move", &json!({"x": 3}))
        .await
        .unwrap();
    assert_eq!(next(&mut moves).await.value(), json!({"x": 3}));
}

#[cfg(feature = "encryption")]
#[tokio::test]
async fn encrypted_channels_round_trip_end_to_end() {
    use crypto_secretbox::XSalsa20Poly1305;
    use crypto_secretbox::aead::{Aead, KeyInit};

    let Some(live) = live() else { return };
    let master_key = [42u8; 32];
    let signer = Arc::clone(&live);
    let client = live
        .builder()
        .channel_authorizer(move |request: ChannelAuthRequest| {
            let data = signer.sign_channel(&request, None, Some(&master_key));
            async move { Ok(data) }
        })
        .build()
        .unwrap();
    let name = unique("private-encrypted-live-");
    let channel = client.subscribe(&name);
    let mut docs = forward(&channel, "doc");
    connect(&client).await;
    subscribed(&client, &channel).await;
    // The server relays ciphertext only; it never holds the key.
    let key = sign::shared_secret(&name, &master_key);
    let nonce = [7u8; 24];
    let ciphertext = XSalsa20Poly1305::new_from_slice(&key)
        .unwrap()
        .encrypt(
            &nonce.into(),
            br#"{"secret":"only clients can read this"}"#.as_slice(),
        )
        .unwrap();
    let payload = json!({"nonce": b64(&nonce), "ciphertext": b64(&ciphertext)});
    live.publish_raw(&name, "doc", payload.to_string()).await;
    assert_eq!(
        next(&mut docs).await.value()["secret"].as_str(),
        Some("only clients can read this")
    );
}

#[tokio::test]
async fn delta_compression_with_conflation() {
    let Some(live) = live() else { return };
    let client = live
        .builder()
        .delta_compression(DeltaOptions::default())
        .build()
        .unwrap();
    let name = unique("ticker:live");
    let channel = client
        .subscribe_with(
            &name,
            SubscribeOptions::new().delta(ChannelDeltaSettings::enabled()),
        )
        .unwrap();
    let mut ticks = forward(&channel, "tick");
    connect(&client).await;
    subscribed(&client, &channel).await;
    let padding = "x".repeat(400);
    for index in 0..12 {
        let data = json!({"item_id": "BTC", "price": 100 + index, "padding": padding.as_str()});
        live.publish(&name, "tick", &data).await;
        let tick = next(&mut ticks).await;
        assert_eq!(
            tick.value()["price"].as_i64(),
            Some(100 + index),
            "message {index}"
        );
    }
    let stats = client.delta_stats().unwrap();
    assert_eq!(stats.errors, 0, "{stats:?}");
    assert!(stats.delta_messages > 0, "server sent deltas: {stats:?}");
    assert!(stats.bandwidth_saved() > 0);
}

#[tokio::test]
async fn recovery_replays_messages_missed_while_disconnected() {
    let Some(live) = live() else { return };
    let client = live.builder().connection_recovery(true).build().unwrap();
    let name = unique("history-recovery-");
    let channel = client.subscribe(&name);
    let mut events = forward(&channel, "e");
    let (resumed_tx, mut resumed) = mpsc::unbounded_channel();
    client.bind("sockudo:resume_success", move |event| {
        let _ = resumed_tx.send(event.clone());
    });
    connect(&client).await;
    subscribed(&client, &channel).await;
    live.publish(&name, "e", &json!({"n": 1})).await;
    assert_eq!(next(&mut events).await.value()["n"].as_i64(), Some(1));
    let position = client.recovery_position(&name).expect("serial tracked");

    client.disconnect().await;
    assert_eq!(client.state(), ConnectionState::Disconnected);
    live.publish(&name, "e", &json!({"n": 2})).await;
    live.publish(&name, "e", &json!({"n": 3})).await;

    connect(&client).await;
    let summary = sockudo_client::recovery::ResumeSuccess::from_event(&next(&mut resumed).await);
    assert!(summary.failed.is_empty(), "{summary:?}");
    let replayed = vec![
        next(&mut events).await.value()["n"].as_i64().unwrap(),
        next(&mut events).await.value()["n"].as_i64().unwrap(),
    ];
    assert_eq!(replayed, vec![2, 3]);
    assert!(client.recovery_position(&name).unwrap().serial > position.serial);
}

#[tokio::test]
async fn rewind_replays_recent_history_on_subscribe() {
    let Some(live) = live() else { return };
    let name = unique("history-rewind-");
    for n in 1..=3 {
        live.publish(&name, "e", &json!({"n": n})).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let client = live.builder().build().unwrap();
    let channel = client
        .subscribe_with(&name, SubscribeOptions::new().rewind(Rewind::Count(2)))
        .unwrap();
    // Older servers replayed `sockudo:message.create` envelopes; current
    // servers replay the original event, exactly like live delivery, so
    // handlers bound by event name also see rewound history. The client
    // accepts both (via `logical_name`); this suite pins the current shape.
    let (sender, mut events) = mpsc::unbounded_channel();
    channel.bind_global(move |event| {
        if event.logical_name() == "e" {
            let _ = sender.send(event.clone());
        }
    });
    let mut complete = forward(&channel, "sockudo:rewind_complete");
    connect(&client).await;
    let first = next(&mut events).await;
    let second = next(&mut events).await;
    assert_eq!((first.logical_name(), second.logical_name()), ("e", "e"));
    assert_eq!((first.event.as_str(), second.event.as_str()), ("e", "e"));
    assert_eq!(
        (first.value()["n"].as_i64(), second.value()["n"].as_i64()),
        (Some(2), Some(3))
    );
    let done = sockudo_client::recovery::RewindComplete::from_event(&next(&mut complete).await);
    assert_eq!(done.historical_count, 2);
}

#[tokio::test]
async fn channel_history_through_a_signing_proxy() {
    let Some(live) = live() else { return };
    let name = unique("history-proxy-");
    for n in 1..=3 {
        live.publish(&name, "e", &json!({"n": n})).await;
    }
    let backend = Arc::clone(&live);
    let http = reqwest::Client::new();
    // A minimal "backend": translate the proxy body into a signed REST call.
    let proxy = move |request: ProxyRequest| {
        let backend = Arc::clone(&backend);
        let http = http.clone();
        async move {
            let channel = request.body["channel"].as_str().unwrap().to_owned();
            let mut params = BTreeMap::new();
            if let Some(object) = request.body["params"].as_object() {
                for (key, value) in object.iter() {
                    let value = value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string());
                    params.insert(key.to_owned(), value);
                }
            }
            let path = format!("/apps/{}/channels/{channel}/history", backend.app_id);
            let query = sign::query(&backend.key, &backend.secret, "GET", &path, None, &params);
            let body = http
                .get(format!("{}{path}?{query}", backend.base_url()))
                .send()
                .await
                .map_err(|error| sockudo_client::Error::HttpTransport(error.to_string()))?
                .bytes()
                .await
                .map_err(|error| sockudo_client::Error::HttpTransport(error.to_string()))?;
            sonic_rs::from_slice::<Value>(&body).map_err(Into::into)
        }
    };
    let client = live
        .builder()
        .channel_history(ProxyOptions::handler(proxy))
        .build()
        .unwrap();
    let channel = client.subscribe(&name);
    let mut page = channel
        .history(
            ChannelHistoryParams::new()
                .limit(2)
                .direction(Direction::OldestFirst),
        )
        .await
        .unwrap();
    let mut seen: Vec<i64> = page
        .events()
        .iter()
        .filter_map(|e| e.value()["n"].as_i64())
        .collect();
    while page.has_next() {
        page = page.next().await.unwrap();
        seen.extend(page.events().iter().filter_map(|e| e.value()["n"].as_i64()));
    }
    assert_eq!(seen, vec![1, 2, 3]);
}

#[tokio::test]
async fn user_sign_in_is_confirmed_by_the_server() {
    let Some(live) = live() else { return };
    let signer = Arc::clone(&live);
    let client = live
        .builder()
        .user_authenticator(move |request: UserAuthRequest| {
            let user_data = r#"{"id":"live-user-1","name":"Ada"}"#;
            let auth = sign::user_auth(&signer.key, &signer.secret, &request.socket_id, user_data);
            async move { Ok(UserAuthData::new(auth, user_data)) }
        })
        .build()
        .unwrap();
    let (signed_tx, mut signed) = mpsc::unbounded_channel();
    client.user().bind("sockudo:signin_success", move |event| {
        let _ = signed_tx.send(event.clone());
    });
    client.sign_in().unwrap();
    connect(&client).await;
    next(&mut signed).await;
    assert_eq!(client.user().user_id().as_deref(), Some("live-user-1"));
}

#[tokio::test]
async fn invalid_capability_tokens_are_refused() {
    let Some(live) = live() else { return };
    let client = live
        .builder()
        .token(format!("not-a-real-token-{}", fastrand::u32(..)))
        .max_reconnect_attempts(Some(0))
        .build()
        .unwrap();
    client.connect().unwrap();
    let result = tokio::time::timeout(WAIT, client.connected())
        .await
        .unwrap();
    assert!(
        result.is_err(),
        "an invalid capability token must not connect"
    );
}

#[tokio::test]
async fn websocket_compression_matches_the_server_mode() {
    let Some(live) = live() else { return };
    let expect = std::env::var("SOCKUDO_LIVE_EXPECT_DEFLATE").ok().as_deref() == Some("1");
    let client = live.builder().build().unwrap();
    let name = unique("live-deflate-");
    let channel = client.subscribe(&name);
    let mut events = forward(&channel, "blob");
    connect(&client).await;
    assert_eq!(client.is_compressed(), expect, "permessage-deflate state");
    subscribed(&client, &channel).await;
    let blob = "sockudo ".repeat(4096);
    live.publish(&name, "blob", &json!({ "blob": blob.as_str() }))
        .await;
    assert_eq!(
        next(&mut events).await.value()["blob"].as_str(),
        Some(blob.as_str())
    );
}

/// The object-shaped `presence_update` (as .NET and Swift send it) must be
/// accepted by the server, not only the JSON-string form this client uses.
#[tokio::test]
async fn server_accepts_object_shaped_presence_updates() {
    let Some(live) = live() else { return };
    let name = unique("presence-live-object-");
    let alice = presence_client(&live, "alice");
    let alice_channel = alice.subscribe(&name);
    let mut updated = forward(&alice_channel, "sockudo:presence_update");
    connect(&alice).await;
    subscribed(&alice, &alice_channel).await;
    let bob = presence_client(&live, "bob");
    let bob_channel = bob.subscribe(&name);
    connect(&bob).await;
    subscribed(&bob, &bob_channel).await;
    bob.send_event(
        "sockudo:presence_update",
        &json!({"channel": name.as_str(), "data": {"status": "typing"}}),
        None,
    )
    .await
    .unwrap();
    let update = next(&mut updated).await;
    assert_eq!(update.value()["user_id"].as_str(), Some("bob"));
    assert_eq!(
        update.value()["user_info"]["status"].as_str(),
        Some("typing")
    );
}

/// Delta bases are the server's JSON bytes, so the server refuses delta
/// compression on binary wire formats at both negotiation points, while JSON
/// sockets keep it.
#[cfg(feature = "protobuf")]
#[tokio::test]
async fn server_refuses_delta_on_binary_wire_formats() {
    let Some(live) = live() else { return };
    let notices = |client: &SockudoClient| {
        let (sender, receiver) = mpsc::unbounded_channel();
        client.bind("sockudo:delta_compression_enabled", move |event| {
            let _ = sender.send(event.value());
        });
        receiver
    };
    let recv = |mut receiver: mpsc::UnboundedReceiver<Value>| async move {
        tokio::time::timeout(WAIT, receiver.recv())
            .await
            .unwrap()
            .unwrap()
    };

    let binary = live
        .builder()
        .wire_format(WireFormat::Protobuf)
        .build()
        .unwrap();
    let refused = notices(&binary);
    connect(&binary).await;
    binary
        .send_event(
            "sockudo:enable_delta_compression",
            &json!({"algorithms": ["fossil"]}),
            None,
        )
        .await
        .unwrap();
    let notice = recv(refused).await;
    assert_eq!(notice["enabled"].as_bool(), Some(false));
    assert!(
        notice["reason"]
            .as_str()
            .unwrap()
            .contains("JSON wire format")
    );

    let refused = notices(&binary);
    let channel = unique("ticker:binary");
    binary
        .send_event(
            "sockudo:subscribe",
            &json!({"channel": channel.as_str(), "auth": "", "delta": true}),
            None,
        )
        .await
        .unwrap();
    let notice = recv(refused).await;
    assert_eq!(notice["enabled"].as_bool(), Some(false));
    assert_eq!(notice["channel"].as_str(), Some(channel.as_str()));

    let json_client = live.builder().build().unwrap();
    let accepted = notices(&json_client);
    connect(&json_client).await;
    json_client
        .send_event(
            "sockudo:enable_delta_compression",
            &json!({"algorithms": ["fossil"]}),
            None,
        )
        .await
        .unwrap();
    assert_eq!(recv(accepted).await["enabled"].as_bool(), Some(true));
}
