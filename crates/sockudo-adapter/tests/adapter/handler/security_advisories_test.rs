//! Wire-level regression tests for the 5.2.0 security advisories:
//!
//! - GHSA-gg5j-wcw2-v78r: public wildcard subscriptions received private/presence traffic.
//! - GHSA-r5q6-rw3h-q24r: `resume` replayed private/presence history to unsubscribed sockets.
//! - GHSA-4795-c9wj-3q58: presence subscribe without `channel_data` panicked the socket task.
//!
//! Every test drives a real WebSocket through `ConnectionHandler::handle_socket`, so frames
//! go through the same parse/route/error path a production client hits.

use crate::mocks::connection_handler_mock::MockCacheManager;
use futures_util::{SinkExt, StreamExt};
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::channel_manager::ChannelManager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::history::{HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig};
use sockudo_core::options::ServerOptions;
use sockudo_core::version_store::{MemoryVersionStore, VersionStore};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sockudo_protocol::{AppendMode, ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::WebSocket;
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Message, Stream as WsStream, WebSocketStream};
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, JsonValueTrait, Value, json};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

type TestClient = WebSocketStream<WsStream<Http1>>;

const QUIET: Duration = Duration::from_millis(300);

struct Harness {
    handler: ConnectionHandler,
    app: App,
    other_app: App,
}

fn test_app(id: &str) -> App {
    App::from_policy(
        id.to_string(),
        format!("{id}-key"),
        format!("{id}-secret"),
        true,
        AppPolicy::default(),
    )
}

async fn build_harness(options: ServerOptions) -> Harness {
    build_harness_with_history(
        options,
        Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
    )
    .await
}

async fn build_harness_with_history(
    options: ServerOptions,
    history_store: Arc<dyn HistoryStore + Send + Sync>,
) -> Harness {
    build_harness_with_stores(options, history_store, None).await
}

async fn build_harness_with_stores(
    options: ServerOptions,
    history_store: Arc<dyn HistoryStore + Send + Sync>,
    version_store: Option<Arc<dyn VersionStore + Send + Sync>>,
) -> Harness {
    let app = test_app("app");
    let other_app = test_app("other-app");
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app.clone()).await.unwrap();
    app_manager.create_app(other_app.clone()).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let builder = ConnectionHandler::builder(
        app_manager as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        options,
    )
    .local_adapter(adapter)
    .history_store(history_store);
    let handler = match version_store {
        Some(version_store) => builder.version_store(version_store).build(),
        None => builder.build(),
    };

    Harness {
        handler,
        app,
        other_app,
    }
}

fn recovery_options() -> ServerOptions {
    let mut options = ServerOptions::default();
    options.connection_recovery.enabled = true;
    options
}

struct Client {
    ws: TestClient,
    socket_id: String,
    prefix: &'static str,
}

fn prefix_for(version: ProtocolVersion) -> &'static str {
    match version {
        ProtocolVersion::V1 => "pusher",
        ProtocolVersion::V2 => "sockudo",
    }
}

async fn connect(handler: &ConnectionHandler, app: &App, version: ProtocolVersion) -> Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        WebSocket::from_tcp(stream, WsConfig::default())
    });
    let stream = TcpStream::connect(address).await.unwrap();
    let (ws, _): (TestClient, _) = WebSocketClient::<Http1>::new(WsConfig::default())
        .connect(stream, &address.to_string(), "/", None)
        .await
        .unwrap();
    let socket = server.await.unwrap();

    let handler = handler.clone();
    let app_key = app.key.clone();
    tokio::spawn(async move {
        let _ = handler
            .handle_socket(
                socket,
                app_key,
                None,
                version,
                WireFormat::Json,
                true,
                AppendMode::Full,
                None,
            )
            .await;
    });

    let mut client = Client {
        ws,
        socket_id: String::new(),
        prefix: prefix_for(version),
    };
    let established = client.recv().await.expect("connection_established");
    assert!(event_is(&established, "connection_established"));
    let data: Value = sonic_rs::from_str(established["data"].as_str().unwrap()).unwrap();
    client.socket_id = data["socket_id"].as_str().unwrap().to_string();
    client
}

fn event_name(frame: &Value) -> &str {
    frame["event"].as_str().unwrap_or_default()
}

/// Matches `pusher:x`, `sockudo:x`, `pusher_internal:x` and `sockudo_internal:x`.
fn event_is(frame: &Value, suffix: &str) -> bool {
    event_name(frame)
        .split_once(':')
        .is_some_and(|(_, name)| name == suffix)
}

fn frame_data(frame: &Value) -> Value {
    match frame.get("data") {
        Some(data) if data.is_str() => sonic_rs::from_str(data.as_str().unwrap()).unwrap(),
        Some(data) => data.clone(),
        None => Value::new_null(),
    }
}

impl Client {
    async fn send(&mut self, event: &str, data: Value) {
        let frame = json!({ "event": event, "data": data }).to_string();
        self.ws
            .send(Message::Text(bytes::Bytes::from(frame)))
            .await
            .unwrap();
    }

    async fn send_prefixed(&mut self, event: &str, data: Value) {
        let event = format!("{}:{event}", self.prefix);
        self.send(&event, data).await;
    }

    async fn recv_within(&mut self, wait: Duration) -> Option<Value> {
        loop {
            match timeout(wait, self.ws.next()).await {
                Err(_) | Ok(None) => return None,
                Ok(Some(Err(error))) => panic!("websocket receive failed: {error:?}"),
                Ok(Some(Ok(Message::Text(text)))) => {
                    return Some(sonic_rs::from_slice(text.as_ref()).unwrap());
                }
                Ok(Some(Ok(Message::Close(_)))) => panic!("server closed the connection"),
                Ok(Some(Ok(_))) => continue,
            }
        }
    }

    async fn recv(&mut self) -> Option<Value> {
        self.recv_within(Duration::from_secs(2)).await
    }

    async fn recv_until(&mut self, suffix: &str) -> Vec<Value> {
        let mut frames = Vec::new();
        loop {
            let frame = self
                .recv()
                .await
                .unwrap_or_else(|| panic!("timed out waiting for {suffix}; got {frames:?}"));
            let done = event_is(&frame, suffix);
            frames.push(frame);
            if done {
                return frames;
            }
        }
    }

    /// Collect everything that arrives until the socket stays quiet.
    async fn drain(&mut self) -> Vec<Value> {
        let mut frames = Vec::new();
        while let Some(frame) = self.recv_within(QUIET).await {
            frames.push(frame);
        }
        frames
    }

    fn auth_for(&self, app: &App, channel: &str, channel_data: Option<&str>) -> String {
        let socket_id = SocketId::from_string(&self.socket_id).unwrap();
        let mut message = PusherMessage::channel_event("pusher:subscribe", channel, json!({}));
        message.data = Some(MessageData::Json(match channel_data {
            Some(channel_data) => json!({ "channel": channel, "channel_data": channel_data }),
            None => json!({ "channel": channel }),
        }));
        ChannelManager::get_expected_signature(app, &socket_id, message)
    }

    async fn subscribe_public(&mut self, channel: &str) {
        self.send_prefixed("subscribe", json!({ "channel": channel }))
            .await;
        self.expect_subscribed(channel).await;
    }

    async fn send_private_subscribe(&mut self, app: &App, channel: &str) {
        let channel_data = channel
            .starts_with("presence-")
            .then(|| json!({ "user_id": format!("user-{}", self.socket_id) }).to_string());
        let auth = self.auth_for(app, channel, channel_data.as_deref());
        let mut data = json!({ "channel": channel, "auth": auth });
        if let Some(channel_data) = channel_data {
            data.as_object_mut()
                .unwrap()
                .insert("channel_data", json!(channel_data));
        }
        self.send_prefixed("subscribe", data).await;
    }

    async fn subscribe_private(&mut self, app: &App, channel: &str) {
        self.send_private_subscribe(app, channel).await;
        self.expect_subscribed(channel).await;
    }

    async fn expect_subscribed(&mut self, channel: &str) {
        let frame = self.recv().await.expect("subscription response");
        assert!(
            event_is(&frame, "subscription_succeeded"),
            "expected subscription_succeeded for {channel}, got {frame:?}"
        );
        assert_eq!(frame["channel"].as_str(), Some(channel));
    }

    /// Clients send resume data as a JSON string, like `pusher:subscribe` auth payloads.
    async fn send_resume(&mut self, positions: Value) {
        self.send_resume_raw(json!({ "channel_positions": positions }))
            .await;
    }

    async fn send_resume_raw(&mut self, data: Value) {
        let data = Value::from(data.to_string().as_str());
        self.send_prefixed("resume", data).await;
    }

    /// Prove the socket task is still alive by doing a ping round trip.
    async fn assert_alive(&mut self) {
        self.send_prefixed("ping", json!({})).await;
        let frames = self.recv_until("pong").await;
        assert!(event_is(frames.last().unwrap(), "pong"));
    }
}

fn live_message(channel: &str, event: &str, message_id: &str) -> PusherMessage {
    let mut message = PusherMessage::channel_event(event, channel, json!({ "event": event }));
    message.message_id = Some(message_id.to_string());
    message
}

async fn publish(harness: &Harness, app: &App, channel: &str, event: &str, message_id: &str) {
    harness
        .handler
        .broadcast_to_channel(app, channel, live_message(channel, event, message_id), None)
        .await
        .unwrap();
}

fn render(frames: &[Value]) -> String {
    frames
        .iter()
        .map(|frame| frame.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// GHSA-gg5j-wcw2-v78r — wildcard subscriptions must never see restricted channels
// ---------------------------------------------------------------------------

const RESTRICTED_CHANNELS: &[&str] = &[
    "private-acme.orders.1",
    "presence-acme.orders.1",
    "private-encrypted-acme.orders.1",
    "private-cache-acme.orders.1",
    "presence-cache-acme.orders.1",
    "private-encrypted-cache-acme.orders.1",
];

#[tokio::test]
async fn wildcard_subscriber_never_receives_private_or_presence_channels() {
    let harness = build_harness(ServerOptions::default()).await;
    let patterns = [
        ("*", "pub.orders.1"),
        ("p*", "pub.orders.1"),
        ("pri*", "prices.orders.1"),
        ("pres*", "press.orders.1"),
        ("*.orders.1", "pub.orders.1"),
    ];

    for (pattern, public_match) in patterns {
        let mut wildcard = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
        wildcard.subscribe_public(pattern).await;

        for (index, channel) in RESTRICTED_CHANNELS.iter().enumerate() {
            publish(
                &harness,
                &harness.app,
                channel,
                "secret",
                &format!("{pattern}-secret-{index}"),
            )
            .await;
        }
        publish(
            &harness,
            &harness.app,
            public_match,
            "public",
            &format!("{pattern}-public"),
        )
        .await;

        let frames = wildcard.drain().await;
        assert!(
            frames
                .iter()
                .all(|frame| frame["event"].as_str() != Some("secret")),
            "wildcard pattern {pattern:?} leaked restricted traffic:\n{}",
            render(&frames)
        );
        assert_eq!(
            frames
                .iter()
                .filter(|frame| frame["event"].as_str() == Some("public"))
                .count(),
            1,
            "wildcard pattern {pattern:?} must still receive {public_match}:\n{}",
            render(&frames)
        );
    }
}

#[tokio::test]
async fn authenticated_private_subscriber_still_receives_while_wildcard_does_not() {
    let harness = build_harness(ServerOptions::default()).await;
    let mut wildcard = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
    wildcard.subscribe_public("*").await;
    let mut member = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
    member
        .subscribe_private(&harness.app, "private-acme.orders.1")
        .await;
    let mut presence = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
    presence
        .subscribe_private(&harness.app, "presence-acme.orders.1")
        .await;
    // Presence join notifications are not part of this assertion.
    presence.drain().await;
    wildcard.drain().await;

    publish(
        &harness,
        &harness.app,
        "private-acme.orders.1",
        "private-event",
        "p-1",
    )
    .await;
    publish(
        &harness,
        &harness.app,
        "presence-acme.orders.1",
        "presence-event",
        "pr-1",
    )
    .await;

    let member_frames = member.drain().await;
    assert!(
        member_frames
            .iter()
            .any(|frame| frame["event"].as_str() == Some("private-event"))
    );
    let presence_frames = presence.drain().await;
    assert!(
        presence_frames
            .iter()
            .any(|frame| frame["event"].as_str() == Some("presence-event"))
    );
    let wildcard_frames = wildcard.drain().await;
    assert!(
        wildcard_frames.is_empty(),
        "wildcard leaked: {}",
        render(&wildcard_frames)
    );
}

#[tokio::test]
async fn wildcard_subscriber_of_another_app_receives_nothing() {
    let harness = build_harness(ServerOptions::default()).await;
    let mut foreign = connect(&harness.handler, &harness.other_app, ProtocolVersion::V2).await;
    foreign.subscribe_public("*").await;

    publish(
        &harness,
        &harness.app,
        "private-acme.orders.1",
        "secret",
        "x-1",
    )
    .await;
    publish(&harness, &harness.app, "pub.orders.1", "public", "x-2").await;

    let frames = foreign.drain().await;
    assert!(frames.is_empty(), "cross-app leak: {}", render(&frames));
}

// ---------------------------------------------------------------------------
// GHSA-r5q6-rw3h-q24r — resume must not replay channels the socket does not hold
// ---------------------------------------------------------------------------

/// Subscribe a legitimate member, publish three messages, and return the position of
/// the first one (so a resume would replay messages 2 and 3) plus the stream id.
async fn restricted_position(harness: &Harness, channel: &str) -> (Value, String) {
    let mut member = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
    member.subscribe_private(&harness.app, channel).await;
    member.drain().await;
    for index in 1..=3 {
        publish(
            harness,
            &harness.app,
            channel,
            "secret-event",
            &format!("secret-msg-{index}"),
        )
        .await;
    }
    let frames = member.drain().await;
    let first = frames
        .iter()
        .find(|frame| frame["event"].as_str() == Some("secret-event"))
        .unwrap_or_else(|| panic!("member did not receive traffic: {}", render(&frames)))
        .clone();
    let stream_id = first["stream_id"]
        .as_str()
        .expect("V2 delivery carries stream_id")
        .to_string();
    let position = json!({
        "stream_id": stream_id,
        "serial": first["serial"].as_u64().expect("V2 delivery carries serial"),
        "last_message_id": first["message_id"],
    });
    (position, stream_id)
}

fn assert_no_leak(frames: &[Value], stream_id: &str) {
    let rendered = render(frames);
    assert!(
        !rendered.contains("secret-msg") && !rendered.contains("secret-event"),
        "resume replayed restricted history to an unsubscribed socket:\n{rendered}"
    );
    assert!(
        !rendered.contains(stream_id),
        "resume leaked stream metadata for an unsubscribed channel:\n{rendered}"
    );
    for frame in frames {
        if event_is(frame, "resume_failed") {
            let data = frame_data(frame);
            for key in [
                "current_stream_id",
                "oldest_available_serial",
                "newest_available_serial",
            ] {
                assert!(
                    data.get(key).is_none_or(|value| value.is_null()),
                    "resume_failed leaked {key}: {frame:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn resume_on_unsubscribed_restricted_channel_replays_nothing() {
    for version in [ProtocolVersion::V1, ProtocolVersion::V2] {
        for channel in ["private-secret", "presence-room"] {
            let harness = build_harness(recovery_options()).await;
            let (position, stream_id) = restricted_position(&harness, channel).await;

            let mut attacker = connect(&harness.handler, &harness.app, version).await;
            // Exact position, a position without stream_id, and a wrong stream_id (which
            // used to return the current stream id in resume_failed).
            let mut without_stream = position.clone();
            without_stream.as_object_mut().unwrap().remove(&"stream_id");
            let mut wrong_stream = position.clone();
            wrong_stream
                .as_object_mut()
                .unwrap()
                .insert("stream_id", json!("guessed-stream"));

            for attempt in [position.clone(), without_stream, wrong_stream] {
                attacker.send_resume(json!({ channel: attempt })).await;
                let frames = attacker.recv_until("resume_success").await;
                let mut frames = frames;
                frames.extend(attacker.drain().await);
                assert_no_leak(&frames, &stream_id);
            }

            // Legacy channel_serials form.
            attacker
                .send_resume_raw(json!({ "channel_serials": { channel: 0 } }))
                .await;
            let mut frames = attacker.recv_until("resume_success").await;
            frames.extend(attacker.drain().await);
            assert_no_leak(&frames, &stream_id);
            attacker.assert_alive().await;
        }
    }
}

#[tokio::test]
async fn reconnect_resume_sent_before_private_auth_completes_still_recovers() {
    for channel in ["private-secret", "presence-room"] {
        let harness = build_harness(recovery_options()).await;
        let (position, _) = restricted_position(&harness, channel).await;

        // Mirrors @sockudo/client: subscribeAll() fires the private subscribe only after
        // the auth endpoint answers, while `resume` goes out immediately.
        let mut client = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
        client.send_resume(json!({ channel: position })).await;
        let early = client.recv_until("resume_success").await;
        let early_rendered = render(&early);
        assert!(
            !early_rendered.contains("secret-msg"),
            "replay must wait for the subscription: {early_rendered}"
        );

        client.send_private_subscribe(&harness.app, channel).await;
        let frames = client.recv_until("resume_success").await;
        let subscribed_at = frames
            .iter()
            .position(|frame| event_is(frame, "subscription_succeeded"))
            .unwrap_or_else(|| panic!("no subscription_succeeded: {}", render(&frames)));
        let replayed: Vec<&str> = frames
            .iter()
            .filter(|frame| frame["event"].as_str() == Some("secret-event"))
            .filter_map(|frame| frame["message_id"].as_str())
            .collect();
        assert_eq!(replayed, vec!["secret-msg-2", "secret-msg-3"]);
        let first_replay = frames
            .iter()
            .position(|frame| frame["event"].as_str() == Some("secret-event"))
            .unwrap();
        assert!(
            subscribed_at < first_replay,
            "replay preceded authorization"
        );

        let summary = frame_data(frames.last().unwrap());
        let recovered = summary["recovered"].as_array().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0]["channel"].as_str(), Some(channel));
        assert_eq!(recovered[0]["replayed"].as_u64(), Some(2));
    }
}

#[tokio::test]
async fn pending_resume_is_dropped_when_subscription_is_rejected() {
    let harness = build_harness(recovery_options()).await;
    let (position, stream_id) = restricted_position(&harness, "private-secret").await;

    let mut attacker = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
    attacker
        .send_resume(json!({ "private-secret": position }))
        .await;
    attacker.recv_until("resume_success").await;
    attacker
        .send_prefixed(
            "subscribe",
            json!({ "channel": "private-secret", "auth": "app-key:forged" }),
        )
        .await;
    let frames = attacker.drain().await;
    assert!(
        frames
            .iter()
            .any(|frame| event_is(frame, "error") || event_is(frame, "subscription_error")),
        "rejected subscribe must answer with an error frame: {}",
        render(&frames)
    );
    assert_no_leak(&frames, &stream_id);
    attacker.assert_alive().await;
}

#[tokio::test]
async fn durable_history_resume_on_another_node_requires_subscription() {
    let mut options = recovery_options();
    options.history.enabled = true;
    options.history.max_page_size = 100;
    let history: Arc<dyn HistoryStore + Send + Sync> =
        Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
    let node_a = build_harness_with_history(options.clone(), history.clone()).await;
    let node_b = build_harness_with_history(options, history).await;
    let (position, stream_id) = restricted_position(&node_a, "private-secret").await;

    // Node B has an empty hot buffer, so recovery must come from durable history.
    let mut attacker = connect(&node_b.handler, &node_b.app, ProtocolVersion::V2).await;
    attacker
        .send_resume(json!({ "private-secret": position.clone() }))
        .await;
    let mut frames = attacker.recv_until("resume_success").await;
    frames.extend(attacker.drain().await);
    assert_no_leak(&frames, &stream_id);

    let mut client = connect(&node_b.handler, &node_b.app, ProtocolVersion::V2).await;
    client
        .send_resume(json!({ "private-secret": position }))
        .await;
    client.recv_until("resume_success").await;
    client
        .send_private_subscribe(&node_b.app, "private-secret")
        .await;
    let frames = client.recv_until("resume_success").await;
    let replayed: Vec<&str> = frames
        .iter()
        .filter(|frame| frame["event"].as_str() == Some("secret-event"))
        .filter_map(|frame| frame["message_id"].as_str())
        .collect();
    assert_eq!(
        replayed,
        vec!["secret-msg-2", "secret-msg-3"],
        "{}",
        render(&frames)
    );
    let summary = frame_data(frames.last().unwrap());
    assert_eq!(
        summary["recovered"][0]["source"].as_str(),
        Some("cold"),
        "{summary:?}"
    );
}

// ---------------------------------------------------------------------------
// GHSA-4795-c9wj-3q58 — malformed subscribe frames must not kill the socket task
// ---------------------------------------------------------------------------

#[tokio::test]
async fn presence_subscribe_without_channel_data_returns_error_and_keeps_connection() {
    let harness = build_harness(ServerOptions::default()).await;
    for version in [ProtocolVersion::V1, ProtocolVersion::V2] {
        for event in ["pusher:subscribe", "sockudo:subscribe"] {
            let mut client = connect(&harness.handler, &harness.app, version).await;
            for data in [
                json!({ "channel": "presence-x" }),
                json!({ "channel": "presence-x", "auth": "app-key:sig" }),
            ] {
                client.send(event, data).await;
                let frame = client.recv().await.unwrap_or_else(|| {
                    panic!("{version:?} {event}: no error frame (socket task died?)")
                });
                assert!(
                    event_is(&frame, "error"),
                    "{version:?} {event}: expected error frame, got {frame:?}"
                );
            }
            client.assert_alive().await;
        }
    }
}

#[tokio::test]
async fn oversized_multibyte_event_filter_returns_error_and_keeps_connection() {
    let harness = build_harness(ServerOptions::default()).await;
    let mut client = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
    // Byte 40 falls inside a two-byte 'é', which used to panic while formatting the error.
    let name = format!("a{}", "é".repeat(150));
    client
        .send_prefixed(
            "subscribe",
            json!({ "channel": "room", "filter": { "events": [name] } }),
        )
        .await;
    let frame = client.recv().await.expect("error frame");
    assert!(event_is(&frame, "error"), "got {frame:?}");
    client.assert_alive().await;
}

// ---------------------------------------------------------------------------
// Resume after re-subscribing on a node that has not seen the channel yet
// ---------------------------------------------------------------------------

/// With versioned messages enabled, live delivery uses the version store's stream. A socket
/// that re-subscribes on a fresh node (empty hot buffer) and then resumes must recover from
/// durable state instead of failing as a stream reset because the subscribe seeded the hot
/// buffer with a different (history) stream generation.
#[tokio::test]
async fn versioned_resume_after_subscribe_on_fresh_node_recovers_cold() {
    let mut options = recovery_options();
    options.history.enabled = true;
    options.versioned_messages.enabled = true;
    let history_store: Arc<dyn HistoryStore + Send + Sync> =
        Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
    let version_store: Arc<dyn VersionStore + Send + Sync> = Arc::new(MemoryVersionStore::new());

    let origin = build_harness_with_stores(
        options.clone(),
        history_store.clone(),
        Some(version_store.clone()),
    )
    .await;
    let mut member = connect(&origin.handler, &origin.app, ProtocolVersion::V2).await;
    member.subscribe_public("chat").await;
    member.drain().await;
    for index in 1..=3 {
        publish(
            &origin,
            &origin.app,
            "chat",
            "chat-event",
            &format!("msg-{index}"),
        )
        .await;
    }
    let frames = member.drain().await;
    let first = frames
        .iter()
        .find(|frame| frame["message_id"].as_str() == Some("msg-1"))
        .unwrap_or_else(|| panic!("member did not receive traffic: {}", render(&frames)))
        .clone();
    let position = json!({
        "stream_id": first["stream_id"].as_str().expect("V2 delivery carries stream_id"),
        "serial": first["serial"].as_u64().expect("V2 delivery carries serial"),
        "last_message_id": "msg-1",
    });

    // Same durable stores, fresh node: nothing in its hot replay buffer.
    let fresh = build_harness_with_stores(options, history_store, Some(version_store)).await;
    let mut client = connect(&fresh.handler, &fresh.app, ProtocolVersion::V2).await;
    client.subscribe_public("chat").await;
    client.send_resume(json!({ "chat": position })).await;
    let frames = client.recv_until("resume_success").await;

    // Versioned channels deliver publishes as `message.create` carrying the original name.
    let replayed: Vec<&str> = frames
        .iter()
        .filter(|frame| frame["name"].as_str() == Some("chat-event"))
        .filter_map(|frame| frame["message_id"].as_str())
        .collect();
    assert_eq!(replayed, vec!["msg-2", "msg-3"], "{}", render(&frames));
    let summary = frame_data(frames.last().unwrap());
    assert!(
        summary["failed"].as_array().unwrap().is_empty(),
        "resume failed: {summary:?}"
    );
    let recovered = summary["recovered"].as_array().unwrap();
    assert_eq!(recovered.len(), 1, "{summary:?}");
    assert_eq!(recovered[0]["source"].as_str(), Some("cold"));
    assert_eq!(recovered[0]["replayed"].as_u64(), Some(2));
}

/// Parked positions are retained per socket until their subscription arrives, so the
/// client-supplied identifiers they carry are bounded: oversized ones are refused up front
/// and a later subscribe replays nothing for them.
#[tokio::test]
async fn resume_positions_with_oversized_ids_are_not_parked() {
    let harness = build_harness(recovery_options()).await;
    let (position, stream_id) = restricted_position(&harness, "private-secret").await;

    let mut client = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
    for key in ["stream_id", "last_message_id"] {
        let mut oversized = position.clone();
        oversized
            .as_object_mut()
            .unwrap()
            .insert(key, json!("x".repeat(4096)));
        client
            .send_resume(json!({ "private-secret": oversized }))
            .await;
        let frames = client.recv_until("resume_success").await;
        let summary = frame_data(frames.last().unwrap());
        assert!(
            summary
                .get("pending")
                .is_none_or(|pending| pending.is_null()),
            "oversized {key} must not be parked: {summary:?}"
        );
        let failed = summary["failed"].as_array().unwrap();
        assert_eq!(failed.len(), 1, "{summary:?}");
        assert_eq!(
            failed[0]["reason"].as_str(),
            Some("pending_resume_limit_exceeded")
        );
        assert_no_leak(&frames, &stream_id);
    }

    client
        .subscribe_private(&harness.app, "private-secret")
        .await;
    let frames = client.drain().await;
    assert!(
        !frames.iter().any(|frame| event_is(frame, "resume_success")
            || frame["event"].as_str() == Some("secret-event")),
        "a refused position must not replay on subscribe: {}",
        render(&frames)
    );
    client.assert_alive().await;
}

/// The position advertised in `subscription_succeeded` for a versioned channel that has no
/// messages yet must be on the stream its first message is delivered on. A client that
/// disconnects before any traffic and later resumes from that position (on a node that
/// never saw the traffic) must then recover everything published meanwhile.
#[tokio::test]
async fn versioned_empty_channel_subscribe_position_is_resumable_across_nodes() {
    let mut options = recovery_options();
    options.history.enabled = true;
    options.versioned_messages.enabled = true;
    let history_store: Arc<dyn HistoryStore + Send + Sync> =
        Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
    let version_store: Arc<dyn VersionStore + Send + Sync> = Arc::new(MemoryVersionStore::new());
    let node_a = build_harness_with_stores(
        options.clone(),
        history_store.clone(),
        Some(version_store.clone()),
    )
    .await;
    let node_b = build_harness_with_stores(options, history_store, Some(version_store)).await;

    let mut early = connect(&node_a.handler, &node_a.app, ProtocolVersion::V2).await;
    early
        .send_prefixed("subscribe", json!({ "channel": "chat" }))
        .await;
    let subscribed = early.recv().await.expect("subscription response");
    assert!(
        event_is(&subscribed, "subscription_succeeded"),
        "{subscribed:?}"
    );
    let advertised_stream = subscribed["stream_id"]
        .as_str()
        .expect("V2 subscribe advertises a stream_id")
        .to_string();
    let advertised_serial = subscribed["serial"].as_u64().expect("and a serial");
    drop(early);

    let mut member = connect(&node_b.handler, &node_b.app, ProtocolVersion::V2).await;
    member.subscribe_public("chat").await;
    member.drain().await;
    for index in 1..=2 {
        publish(
            &node_b,
            &node_b.app,
            "chat",
            "chat-event",
            &format!("msg-{index}"),
        )
        .await;
    }
    let delivered = member.drain().await;
    let delivered_stream = delivered
        .iter()
        .find_map(|frame| frame["stream_id"].as_str())
        .unwrap_or_else(|| panic!("no delivery: {}", render(&delivered)));
    assert_eq!(
        delivered_stream, advertised_stream,
        "subscribe advertised a stream that live delivery does not use"
    );

    let mut client = connect(&node_a.handler, &node_a.app, ProtocolVersion::V2).await;
    client.subscribe_public("chat").await;
    client
        .send_resume(json!({ "chat": {
            "stream_id": advertised_stream,
            "serial": advertised_serial,
        }}))
        .await;
    let frames = client.recv_until("resume_success").await;
    let replayed: Vec<&str> = frames
        .iter()
        .filter(|frame| frame["name"].as_str() == Some("chat-event"))
        .filter_map(|frame| frame["message_id"].as_str())
        .collect();
    assert_eq!(replayed, vec!["msg-1", "msg-2"], "{}", render(&frames));
    let summary = frame_data(frames.last().unwrap());
    assert!(
        summary["failed"].as_array().unwrap().is_empty(),
        "resume failed: {summary:?}"
    );
}

/// A position parked for a channel the client never subscribes to gets an explicit
/// `resume_failed` once it expires, even if the socket sends nothing else.
#[tokio::test]
async fn expired_parked_resume_is_reported_without_further_traffic() {
    let harness = build_harness(recovery_options()).await;
    let (position, stream_id) = restricted_position(&harness, "private-secret").await;

    let mut client = connect(&harness.handler, &harness.app, ProtocolVersion::V2).await;
    client
        .send_resume(json!({ "private-secret": position }))
        .await;
    let frames = client.recv_until("resume_success").await;
    let summary = frame_data(frames.last().unwrap());
    assert_eq!(
        summary["pending"].as_array().map(|pending| pending.len()),
        Some(1),
        "{summary:?}"
    );

    // Nothing has expired yet: the sweep must leave the parked position alone.
    assert_eq!(harness.handler.evict_expired_pending_resumes().await, 0);
    assert!(client.drain().await.is_empty());

    // The periodic sweep with a zero age limit stands in for the TTL elapsing (the TTL
    // boundary itself is covered by the recovery unit tests).
    assert_eq!(
        harness
            .handler
            .evict_pending_resumes_older_than(Duration::ZERO)
            .await,
        1
    );

    let frames = client.drain().await;
    let failure = frames
        .iter()
        .find(|frame| event_is(frame, "resume_failed"))
        .unwrap_or_else(|| panic!("expiry produced no resume_failed: {}", render(&frames)));
    assert_eq!(failure["channel"].as_str(), Some("private-secret"));
    assert_eq!(
        frame_data(failure)["reason"].as_str(),
        Some("pending_resume_expired")
    );
    assert_no_leak(&frames, &stream_id);
    client.assert_alive().await;
}
