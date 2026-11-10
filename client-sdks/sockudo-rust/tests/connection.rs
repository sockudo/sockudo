//! Connection lifecycle, routing, keepalive and reconnect policy against a
//! scriptable mock server.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use sockudo_client::{ConnectionState, Error, ProtocolVersion, SockudoClient};
use sonic_rs::{JsonValueTrait, json};
use support::{MockServer, Recorder, client_for, data_of, eventually};

#[tokio::test]
async fn state_callbacks_may_call_connect() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let handle = client.clone();
    let states = Recorder::default();
    let s = states.clone();
    client.on_state_change(move |change| {
        // Re-entering the client from a callback must not deadlock.
        handle.connect().unwrap();
        s.push(change.current);
    });
    client.connect().unwrap();
    let conn = server.accept().await;
    conn.establish("1.2");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), client.connected())
            .await
            .expect("connect() from a state callback deadlocked")
            .unwrap(),
        "1.2"
    );
    assert_eq!(
        states.snapshot(),
        vec![ConnectionState::Connecting, ConnectionState::Connected]
    );
    assert!(
        server
            .try_accept(Duration::from_millis(200))
            .await
            .is_none(),
        "re-entrant connect() opened a second socket"
    );
}

#[tokio::test]
async fn connects_with_the_documented_query_and_reports_state() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let states = Recorder::default();
    let s = states.clone();
    client.on_state_change(move |change| s.push(change.current));

    assert_eq!(client.state(), ConnectionState::Initialized);
    client.connect().unwrap();
    let conn = server.accept().await;
    assert!(conn.path.starts_with("/app/app-key?"));
    assert_eq!(conn.query("protocol").as_deref(), Some("2"));
    assert_eq!(conn.query("client").as_deref(), Some("rust"));
    assert_eq!(
        conn.query("version").as_deref(),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(conn.query("format").as_deref(), Some("json"));
    assert_eq!(conn.query("echo_messages").as_deref(), Some("true"));
    assert_eq!(conn.query("append_mode").as_deref(), Some("delta"));
    assert_eq!(conn.query("token"), None);

    conn.establish("123.456");
    assert_eq!(client.connected().await.unwrap(), "123.456");
    assert_eq!(client.socket_id().as_deref(), Some("123.456"));
    assert_eq!(
        states.snapshot(),
        vec![ConnectionState::Connecting, ConnectionState::Connected]
    );

    // connect() is idempotent while connected.
    client.connect().unwrap();
    assert!(
        server
            .try_accept(Duration::from_millis(200))
            .await
            .is_none()
    );

    client.disconnect().await;
    assert_eq!(client.state(), ConnectionState::Disconnected);
    assert!(client.socket_id().is_none());
}

#[tokio::test]
async fn v1_speaks_pusher_prefixes() {
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .protocol_version(ProtocolVersion::V1)
        .build()
        .unwrap();
    let channel = client.subscribe("news");
    let succeeded = Recorder::default();
    let s = succeeded.clone();
    channel.bind("pusher:subscription_succeeded", move |event| {
        s.push(event.event.clone())
    });
    client.connect().unwrap();
    let mut conn = server.accept().await;
    assert_eq!(conn.query("protocol").as_deref(), Some("7"));
    assert_eq!(conn.query("format"), None);
    conn.establish("1.1");
    let subscribe = conn.expect_event("pusher:subscribe").await;
    assert_eq!(data_of(&subscribe)["channel"].as_str(), Some("news"));
    conn.subscription_succeeded("news", &json!({}));
    eventually(|| succeeded.len() == 1).await;
    assert_eq!(
        succeeded.snapshot(),
        vec!["pusher:subscription_succeeded".to_owned()]
    );
}

#[tokio::test]
async fn routes_events_to_channel_global_and_streams() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let channel = client.subscribe("prices");
    let on_channel = Recorder::default();
    let on_global = Recorder::default();
    let c = on_channel.clone();
    channel.bind("price", move |event| c.push(event.value()));
    let g = on_global.clone();
    client.bind_global(move |event| g.push(event.event.clone()));
    let mut stream = channel.stream_event("price", 16);

    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(
        data_of(&subscribe),
        json!({"auth": "", "channel": "prices"})
    );
    conn.subscription_succeeded("prices", &json!({"attach_serial": 41}));
    eventually(|| channel.is_subscribed()).await;
    assert_eq!(channel.attach_serial(), Some(41));

    conn.send_event("price", Some("prices"), &json!({"p": 1.5}));
    conn.send_event("other", Some("unknown-channel"), &json!(1));
    eventually(|| on_channel.len() == 1).await;
    assert_eq!(on_channel.snapshot()[0], json!({"p": 1.5}));
    let streamed = tokio::time::timeout(support::WAIT, stream.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(streamed.channel.as_deref(), Some("prices"));
    eventually(|| on_global.len() == 2).await;
    // Internal events (subscription_succeeded) and consumed protocol events
    // (connection_established) never reach global bindings.
    assert_eq!(
        on_global.snapshot(),
        vec!["price".to_owned(), "other".to_owned()]
    );
}

#[tokio::test]
async fn subscription_count_summary_and_annotations_are_normalized() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let channel = client.subscribe("room");
    let seen = Recorder::default();
    let s = seen.clone();
    channel.bind_global(move |event| s.push(event.event.clone()));
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    conn.expect_event("sockudo:subscribe").await;
    conn.subscription_succeeded("room", &json!({}));
    conn.send_event(
        "sockudo_internal:subscription_count",
        Some("room"),
        &json!({"subscription_count": 7}),
    );
    conn.send_event(
        "sockudo_internal:message",
        Some("room"),
        &json!({"action": "message.summary", "serial": "m1", "summary": {}}),
    );
    conn.send_event(
        "sockudo_internal:annotation",
        Some("room"),
        &json!({"action": "annotation.create", "serial": "a1"}),
    );
    eventually(|| seen.len() == 4).await;
    assert_eq!(
        seen.snapshot(),
        vec![
            "sockudo:subscription_succeeded".to_owned(),
            "sockudo:subscription_count".to_owned(),
            "message.summary".to_owned(),
            "annotation.create".to_owned(),
        ]
    );
    assert_eq!(channel.subscription_count(), Some(7));
}

#[tokio::test]
async fn answers_server_pings_and_pings_after_inactivity() {
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .activity_timeout(Duration::from_secs(120))
        .pong_timeout(Duration::from_millis(400))
        .build()
        .unwrap();
    client.connect().unwrap();
    let mut conn = server.accept().await;
    // The server's lower activity timeout wins.
    conn.establish_with_timeout("1.1", 1);
    client.connected().await.unwrap();

    conn.send_event("sockudo:ping", None, &json!({}));
    conn.expect_event("sockudo:pong").await;

    // After ~1s of silence the client pings; answer it.
    let started = tokio::time::Instant::now();
    conn.expect_event("sockudo:ping").await;
    assert!(started.elapsed() >= Duration::from_millis(800));
    conn.send_event("sockudo:pong", None, &json!({}));

    // Next ping goes unanswered: the client reconnects.
    conn.expect_event("sockudo:ping").await;
    let reconnect = server.accept().await;
    assert_eq!(reconnect.query("protocol").as_deref(), Some("2"));
}

#[tokio::test]
async fn retry_close_codes_reconnect_and_resubscribe() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let states = Recorder::default();
    let s = states.clone();
    client.on_state_change(move |change| s.push(change.current));
    let channel = client.subscribe("feed");
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    conn.expect_event("sockudo:subscribe").await;
    conn.subscription_succeeded("feed", &json!({}));
    eventually(|| channel.is_subscribed()).await;

    conn.close(4200, "please reconnect");
    let mut conn = server.accept().await;
    eventually(|| !channel.is_subscribed()).await;
    conn.establish("2.2");
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(data_of(&subscribe)["channel"].as_str(), Some("feed"));
    assert_eq!(client.connected().await.unwrap(), "2.2");
    assert!(states.snapshot().contains(&ConnectionState::Reconnecting));
}

#[tokio::test]
async fn refused_close_codes_stop_and_report() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let errors = Recorder::default();
    let e = errors.clone();
    client.on_error(move |error| e.push(error.code()));
    client.connect().unwrap();
    let conn = server.accept().await;
    conn.close(4001, "app does not exist");
    let error = client.connected().await.unwrap_err();
    assert!(
        matches!(error, Error::Disconnected { reason: Some(ref r) } if r.contains("4001")),
        "{error}"
    );
    assert_eq!(client.state(), ConnectionState::Disconnected);
    assert!(errors.snapshot().contains(&Some(4001)));
    assert!(
        server
            .try_accept(Duration::from_millis(300))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn reconnect_budget_is_bounded() {
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .max_reconnect_attempts(Some(2))
        .build()
        .unwrap();
    let exhausted = Arc::new(AtomicUsize::new(0));
    let x = Arc::clone(&exhausted);
    client.on_error(move |error| {
        if matches!(error, Error::ReconnectExhausted { attempts: 2 }) {
            x.fetch_add(1, Ordering::SeqCst);
        }
    });
    client.connect().unwrap();
    // Initial attempt + 2 retries (0s, 1s), each dropped without a handshake.
    for _ in 0..3 {
        drop(server.accept().await);
    }
    let error = tokio::time::timeout(support::WAIT, client.connected())
        .await
        .unwrap();
    assert!(error.is_err());
    assert_eq!(exhausted.load(Ordering::SeqCst), 1);
    assert!(
        server
            .try_accept(Duration::from_millis(1500))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn unanswered_handshake_becomes_unavailable() {
    // Accepts TCP but never answers the upgrade.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    let client = SockudoClient::builder("k")
        .ws_host("127.0.0.1")
        .ws_port(port)
        .force_tls(false)
        .disabled_transports([sockudo_client::Transport::Wss])
        .unavailable_timeout(Duration::from_millis(150))
        .connect_timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    client.connect().unwrap();
    let mut watcher = client.state_watcher();
    tokio::time::timeout(Duration::from_secs(2), async {
        while *watcher.borrow_and_update() != ConnectionState::Unavailable {
            watcher.changed().await.unwrap();
        }
    })
    .await
    .expect("state never became unavailable");
}

#[tokio::test]
async fn disconnect_then_connect_resubscribes() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let channel = client.subscribe("feed");
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    conn.expect_event("sockudo:subscribe").await;
    client.disconnect().await;
    assert_eq!(conn.wait_closed().await, 1000);
    assert!(!channel.is_subscribed());

    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.2");
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(data_of(&subscribe)["channel"].as_str(), Some("feed"));
}

#[tokio::test]
async fn dropping_the_last_handle_closes_the_socket() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let clone = client.clone();
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    client.connected().await.unwrap();
    drop(client);
    assert!(
        conn.try_recv_frame(Duration::from_millis(200))
            .await
            .is_none()
    );
    drop(clone);
    let code = conn.wait_closed().await;
    assert_eq!(code, 1000);
}

#[tokio::test]
async fn panicking_callbacks_do_not_break_the_connection() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let channel = client.subscribe("c");
    channel.bind("boom", |_| panic!("handler bug"));
    let after = Recorder::default();
    let a = after.clone();
    channel.bind("after", move |event| a.push(event.event.clone()));
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    conn.expect_event("sockudo:subscribe").await;
    conn.send_event("boom", Some("c"), &json!({}));
    conn.send_event("after", Some("c"), &json!({}));
    eventually(|| after.len() == 1).await;
    assert_eq!(client.state(), ConnectionState::Connected);
}

#[tokio::test]
async fn server_errors_reach_error_listeners() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let errors = Recorder::default();
    let e = errors.clone();
    client.on_error(move |error| e.push(error.to_string()));
    client.connect().unwrap();
    let conn = server.accept().await;
    conn.establish("1.1");
    conn.send_event(
        "sockudo:error",
        None,
        &json!({"code": 4301, "message": "nope"}),
    );
    conn.send_text("this is not json");
    eventually(|| errors.len() == 2).await;
    let errors = errors.snapshot();
    assert!(errors[0].contains("4301") && errors[0].contains("nope"));
    assert!(errors[1].starts_with("decode:"));
}

#[tokio::test]
async fn deduplicates_by_message_id() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    let channel = client.subscribe("c");
    let seen = Recorder::default();
    let s = seen.clone();
    channel.bind("e", move |event| s.push(event.message_id.clone()));
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    conn.expect_event("sockudo:subscribe").await;
    for id in ["m1", "m1", "m2", "m1"] {
        conn.send_json(&json!({"event": "e", "channel": "c", "data": "{}", "message_id": id}));
    }
    conn.send_json(&json!({"event": "e", "channel": "c", "data": "{}"}));
    eventually(|| seen.len() == 3).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        seen.snapshot(),
        vec![Some("m1".to_owned()), Some("m2".to_owned()), None]
    );
}

#[tokio::test]
async fn client_events_and_unsubscribe_frames() {
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .channel_authorizer(|request: sockudo_client::ChannelAuthRequest| async move {
            Ok(sockudo_client::ChannelAuthData::new(format!(
                "app-key:{}",
                request.socket_id
            )))
        })
        .build()
        .unwrap();
    let channel = client.subscribe("private-game");
    assert!(matches!(
        channel.trigger("move", &json!({})).await,
        Err(Error::BadEventName(_))
    ));
    assert!(matches!(
        channel.trigger("client-move", &json!({})).await,
        Err(Error::NotConnected)
    ));

    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("9.9");
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(data_of(&subscribe)["auth"].as_str(), Some("app-key:9.9"));
    conn.subscription_succeeded("private-game", &json!({}));
    eventually(|| channel.is_subscribed()).await;

    channel
        .trigger("client-move", &json!({"x": 1}))
        .await
        .unwrap();
    let frame = conn.expect_event("client-move").await;
    assert_eq!(frame["channel"].as_str(), Some("private-game"));
    assert_eq!(frame["data"], json!({"x": 1}));
    channel
        .try_trigger("client-move", &json!({"x": 2}))
        .unwrap();
    assert_eq!(
        conn.expect_event("client-move").await["data"],
        json!({"x": 2})
    );

    client.unsubscribe("private-game");
    let unsubscribe = conn.expect_event("sockudo:unsubscribe").await;
    assert_eq!(
        data_of(&unsubscribe)["channel"].as_str(),
        Some("private-game")
    );
    assert!(client.channel("private-game").is_none());
}

#[tokio::test]
async fn unsubscribing_while_pending_completes_after_success() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    client.subscribe("c");
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    conn.expect_event("sockudo:subscribe").await;
    client.unsubscribe("c"); // still pending
    assert!(client.channel("c").is_some());
    conn.subscription_succeeded("c", &json!({}));
    conn.expect_event("sockudo:unsubscribe").await;
    eventually(|| client.channel("c").is_none()).await;
}

#[tokio::test]
async fn send_event_requires_a_connection() {
    let server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    assert!(matches!(
        client.send_event("x", &json!({}), None).await,
        Err(Error::NotConnected)
    ));
}

#[cfg(feature = "deflate")]
#[tokio::test]
async fn negotiates_permessage_deflate_when_the_server_accepts() {
    let mut server = MockServer::start_with_deflate().await;
    let client = client_for(&server).build().unwrap();
    let channel = client.subscribe("big");
    let payloads = Recorder::default();
    let p = payloads.clone();
    channel.bind("blob", move |event| p.push(event.value()));
    client.connect().unwrap();
    let mut conn = server.accept().await;
    assert!(
        conn.extensions
            .as_deref()
            .is_some_and(|e| e.starts_with("permessage-deflate")),
        "{:?}",
        conn.extensions
    );
    conn.establish("1.1");
    client.connected().await.unwrap();
    assert!(client.is_compressed());
    // Compressed in both directions.
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(data_of(&subscribe)["channel"].as_str(), Some("big"));
    let blob = "x".repeat(10_000);
    conn.send_event("blob", Some("big"), &json!({"blob": blob}));
    eventually(|| payloads.len() == 1).await;
    assert_eq!(
        payloads.snapshot()[0]["blob"].as_str().map(str::len),
        Some(10_000)
    );
}

#[cfg(feature = "deflate")]
#[tokio::test]
async fn deflate_offer_can_be_disabled() {
    let mut server = MockServer::start_with_deflate().await;
    let client = client_for(&server)
        .permessage_deflate(false)
        .build()
        .unwrap();
    client.connect().unwrap();
    let conn = server.accept().await;
    assert_eq!(conn.extensions, None);
    conn.establish("1.1");
    client.connected().await.unwrap();
    assert!(!client.is_compressed());
}
