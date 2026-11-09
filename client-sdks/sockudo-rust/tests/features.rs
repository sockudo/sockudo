//! Protocol V2 features end to end against the mock server.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[cfg(feature = "http")]
use axum::routing::post;
#[cfg(feature = "http")]
use axum::{Form, Json, Router};
use base64::Engine as _;
#[cfg(feature = "http")]
use sockudo_client::Endpoint;
use sockudo_client::rest::{ChannelHistoryParams, Direction, ProxyRequest, PublishAnnotation};
use sockudo_client::{
    ChannelAuthData, ChannelAuthRequest, ChannelDeltaSettings, DeltaAlgorithm, DeltaOptions, Error,
    Filter, ProxyOptions, RecoveryPosition, Rewind, SubscribeOptions, TokenAuthData, TokenRequest,
    TokenRequestReason, UserAuthData, UserAuthRequest,
};
use sonic_rs::{JsonValueTrait, json};
use support::{MockServer, Recorder, client_for, data_of, eventually};

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[cfg(feature = "http")]
#[tokio::test]
async fn http_channel_auth_endpoint_contract() {
    let requests = Recorder::default();
    let r = requests.clone();
    let router = Router::new().route(
        "/auth",
        post(
            move |headers: axum::http::HeaderMap, Form(form): Form<Vec<(String, String)>>| {
                let r = r.clone();
                async move {
                    let session = headers
                        .get("x-session")
                        .map(|value| value.to_str().unwrap().to_owned());
                    r.push((form.clone(), session));
                    let channel = form
                        .iter()
                        .find(|(k, _)| k == "channel_name")
                        .unwrap()
                        .1
                        .clone();
                    if channel == "private-denied" {
                        return (axum::http::StatusCode::FORBIDDEN, Json(json!({})));
                    }
                    (
                        axum::http::StatusCode::OK,
                        Json(json!({"auth": format!("app-key:sig-{channel}")})),
                    )
                }
            },
        ),
    );
    let base = support::serve_http(router).await;
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .channel_auth_endpoint(
            Endpoint::new(format!("{base}/auth"))
                .header("X-Session", "abc")
                .param("tenant", "t1")
                .params_provider(|| vec![("dynamic".into(), "yes".into())]),
        )
        .build()
        .unwrap();
    let channel = client.subscribe("private-orders");
    let denied = client.subscribe("private-denied");
    let denied_events = Recorder::default();
    let d = denied_events.clone();
    denied.bind("sockudo:subscription_error", move |event| {
        d.push(event.value())
    });
    let errors = Recorder::default();
    let e = errors.clone();
    client.on_error(move |error| {
        e.push(matches!(
            error,
            Error::Auth {
                status: Some(403),
                ..
            }
        ))
    });

    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("11.22");
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(
        data_of(&subscribe),
        json!({"auth": "app-key:sig-private-orders", "channel": "private-orders"})
    );
    eventually(|| denied_events.len() == 1).await;
    assert_eq!(
        denied_events.snapshot()[0]["type"].as_str(),
        Some("AuthError")
    );
    assert_eq!(denied_events.snapshot()[0]["status"].as_u64(), Some(403));
    assert!(errors.snapshot().contains(&true));
    assert!(!denied.is_pending() && !denied.is_subscribed());

    let (form, session) = requests
        .snapshot()
        .into_iter()
        .find(|(form, _)| form.iter().any(|(_, v)| v == "private-orders"))
        .unwrap();
    assert_eq!(session.as_deref(), Some("abc"));
    for expected in [
        ("tenant", "t1"),
        ("dynamic", "yes"),
        ("socket_id", "11.22"),
        ("channel_name", "private-orders"),
    ] {
        assert!(
            form.contains(&(expected.0.to_owned(), expected.1.to_owned())),
            "{form:?}"
        );
    }
    conn.subscription_succeeded("private-orders", &json!({}));
    eventually(|| channel.is_subscribed()).await;
}

fn presence_authorizer() -> impl sockudo_client::auth::ChannelAuthorizer {
    |request: ChannelAuthRequest| async move {
        Ok(
            ChannelAuthData::new(format!("app-key:{}", request.socket_id))
                .with_channel_data(r#"{"user_id":"u1","user_info":{"name":"Ada"}}"#),
        )
    }
}

#[tokio::test]
async fn presence_membership_and_updates() {
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .channel_authorizer(presence_authorizer())
        .build()
        .unwrap();
    let channel = client.subscribe("presence-lobby");
    let events = Recorder::default();
    let e = events.clone();
    channel.bind_global(move |event| e.push(event.event.clone()));
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(
        data_of(&subscribe)["channel_data"].as_str(),
        Some(r#"{"user_id":"u1","user_info":{"name":"Ada"}}"#)
    );
    conn.subscription_succeeded(
        "presence-lobby",
        &json!({"presence": {"ids": ["u1", "u2"], "hash": {"u1": {"name": "Ada"}, "u2": {"name": "Bob"}}, "count": 2}}),
    );
    eventually(|| channel.is_subscribed()).await;
    let members = channel.members().unwrap();
    assert_eq!(members.count(), 2);
    assert_eq!(members.me().unwrap().info, json!({"name": "Ada"}));

    conn.send_event(
        "sockudo_internal:member_added",
        Some("presence-lobby"),
        &json!({"user_id": "u3", "user_info": {}}),
    );
    conn.send_event(
        "sockudo_internal:member_removed",
        Some("presence-lobby"),
        &json!({"user_id": "u2"}),
    );
    conn.send_event(
        "sockudo_internal:presence_update",
        Some("presence-lobby"),
        &json!({"user_id": "u1", "user_info": {"status": "away"}}),
    );
    // Removing an unknown member emits nothing.
    conn.send_event(
        "sockudo_internal:member_removed",
        Some("presence-lobby"),
        &json!({"user_id": "nobody"}),
    );
    eventually(|| events.len() == 4).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        events.snapshot(),
        vec![
            "sockudo:subscription_succeeded",
            "sockudo:member_added",
            "sockudo:member_removed",
            "sockudo:presence_update"
        ]
    );
    let members = channel.members().unwrap();
    assert_eq!(members.count(), 2);
    assert!(members.get("u2").is_none());
    assert_eq!(members.me().unwrap().info, json!({"status": "away"}));

    channel
        .update_presence(&json!({"status": "busy"}))
        .await
        .unwrap();
    let update = conn.expect_event("sockudo:presence_update").await;
    // The server contract: data = {channel, data}, JSON-encoded on the JSON
    // wire so the server's presence handler receives `channel` intact.
    assert!(update["data"].is_str());
    assert_eq!(
        data_of(&update),
        json!({"channel": "presence-lobby", "data": {"status": "busy"}})
    );
}

#[tokio::test]
async fn presence_without_user_id_fails_closed() {
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .channel_authorizer(|_request: ChannelAuthRequest| async {
            Ok(ChannelAuthData::new("app-key:sig"))
        })
        .build()
        .unwrap();
    let channel = client.subscribe("presence-x");
    let failed = Recorder::default();
    let f = failed.clone();
    channel.bind("sockudo:subscription_error", move |_| f.push(()));
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    eventually(|| failed.len() == 1).await;
    let frames = conn.drain_json(Duration::from_millis(150)).await;
    assert!(
        frames
            .iter()
            .all(|f| f["event"].as_str() != Some("sockudo:subscribe"))
    );
}

#[cfg(feature = "encryption")]
#[tokio::test]
async fn encrypted_channels_decrypt_and_rotate_keys() {
    use crypto_secretbox::XSalsa20Poly1305;
    use crypto_secretbox::aead::{Aead, KeyInit};

    let keys = Arc::new(parking_lot::Mutex::new([1u8; 32]));
    let auths = Arc::new(AtomicUsize::new(0));
    let k = Arc::clone(&keys);
    let a = Arc::clone(&auths);
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .channel_authorizer(move |_request: ChannelAuthRequest| {
            let key = *k.lock();
            a.fetch_add(1, Ordering::SeqCst);
            async move {
                // Slow enough that the rotation burst below arrives mid-auth.
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(ChannelAuthData::new("app-key:sig").with_shared_secret(b64(&key)))
            }
        })
        .build()
        .unwrap();
    let channel = client.subscribe("private-encrypted-docs");
    let plaintexts = Recorder::default();
    let p = plaintexts.clone();
    channel.bind("doc", move |event| p.push(event.value()));
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    conn.expect_event("sockudo:subscribe").await;
    conn.subscription_succeeded("private-encrypted-docs", &json!({}));

    let encrypt = |key: [u8; 32], nonce: [u8; 24], text: &str| {
        let cipher = XSalsa20Poly1305::new_from_slice(&key).unwrap();
        let ciphertext = cipher.encrypt(&nonce.into(), text.as_bytes()).unwrap();
        json!({"ciphertext": b64(&ciphertext), "nonce": b64(&nonce)})
    };
    conn.send_event(
        "doc",
        Some("private-encrypted-docs"),
        &encrypt([1; 32], [9; 24], r#"{"v":1}"#),
    );
    eventually(|| plaintexts.len() == 1).await;
    assert_eq!(plaintexts.snapshot()[0], json!({"v": 1}));

    // Rotate: a burst under the new key. The client re-authorizes once and
    // delivers every event, in order, including those that arrived while
    // the key refresh was in flight.
    *keys.lock() = [2u8; 32];
    for (version, nonce) in [(2u8, 8u8), (3, 7), (4, 6)] {
        conn.send_event(
            "doc",
            Some("private-encrypted-docs"),
            &encrypt([2; 32], [nonce; 24], &format!(r#"{{"v":{version}}}"#)),
        );
    }
    eventually(|| plaintexts.len() == 4).await;
    assert_eq!(
        plaintexts.snapshot()[1..],
        [json!({"v": 2}), json!({"v": 3}), json!({"v": 4})]
    );
    assert_eq!(auths.load(Ordering::SeqCst), 2);

    assert!(matches!(
        channel.trigger("client-x", &json!({})).await,
        Err(Error::Unsupported(_))
    ));
}

#[tokio::test]
async fn user_sign_in_server_channel_and_watchlist() {
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .user_authenticator(|request: UserAuthRequest| async move {
            Ok(UserAuthData::new(
                format!("app-key:{}", request.socket_id),
                r#"{"id":"u7","name":"Ada"}"#,
            ))
        })
        .build()
        .unwrap();
    let user = client.user();
    let personal = Recorder::default();
    let p = personal.clone();
    user.bind("invoice-ready", move |event| p.push(event.value()));
    let online = Recorder::default();
    let o = online.clone();
    user.watchlist()
        .bind("online", move |event| o.push(event.value()));
    client.sign_in().unwrap();
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("5.5");
    let signin = conn.expect_event("sockudo:signin").await;
    assert_eq!(signin["data"]["auth"].as_str(), Some("app-key:5.5"));
    assert_eq!(
        signin["data"]["user_data"].as_str(),
        Some(r#"{"id":"u7","name":"Ada"}"#)
    );

    conn.send_event(
        "sockudo:signin_success",
        None,
        &json!({"user_data": r#"{"id":"u7","name":"Ada"}"#}),
    );
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(
        data_of(&subscribe)["channel"].as_str(),
        Some("#server-to-user-u7")
    );
    eventually(|| user.user_id().as_deref() == Some("u7")).await;
    conn.subscription_succeeded("#server-to-user-u7", &json!({}));
    conn.send_event(
        "invoice-ready",
        Some("#server-to-user-u7"),
        &json!({"id": 1}),
    );
    conn.send_event(
        "sockudo_internal:watchlist_events",
        None,
        &json!({"events": [{"name": "online", "user_ids": ["u8"]}, {"name": "offline", "user_ids": ["u9"]}]}),
    );
    eventually(|| personal.len() == 1 && online.len() == 1).await;
    assert_eq!(personal.snapshot()[0], json!({"id": 1}));
    assert_eq!(online.snapshot()[0]["user_ids"], json!(["u8"]));
    // The server channel is internal to the user facade.
    assert!(client.channel("#server-to-user-u7").is_none());

    // After a reconnect the client signs in again before resubscribing.
    conn.close(4200, "");
    let mut conn = server.accept().await;
    conn.establish("6.6");
    let frames = conn.drain_json(Duration::from_millis(200)).await;
    assert!(
        frames
            .iter()
            .any(|f| f["event"].as_str() == Some("sockudo:signin"))
    );
    assert!(
        frames
            .iter()
            .all(|f| f["event"].as_str() != Some("sockudo:subscribe"))
    );
}

#[tokio::test]
async fn recovery_tracks_positions_and_resumes() {
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .connection_recovery(true)
        .build()
        .unwrap();
    // Positions restored from storage are resumed on the first connection.
    client.set_recovery_positions([(
        "restored".to_owned(),
        RecoveryPosition {
            serial: 3,
            stream_id: Some("s0".into()),
            last_message_id: None,
        },
    )]);
    let channel = client.subscribe("feed");
    client.subscribe("restored");
    let resumed = Recorder::default();
    let r = resumed.clone();
    client.bind("sockudo:resume_success", move |event| {
        r.push(
            sockudo_client::recovery::ResumeSuccess::from_event(event)
                .recovered
                .len(),
        )
    });

    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    let frames = conn.drain_json(Duration::from_millis(200)).await;
    let names: Vec<_> = frames
        .iter()
        .map(|f| f["event"].as_str().unwrap().to_owned())
        .collect();
    let resume_at = names.iter().position(|n| n == "sockudo:resume").unwrap();
    // Every subscribe precedes resume.
    assert!(
        names[..resume_at]
            .iter()
            .filter(|n| *n == "sockudo:subscribe")
            .count()
            == 2
    );
    assert_eq!(
        frames[resume_at]["data"]["channel_positions"]["restored"],
        json!({"serial": 3, "stream_id": "s0"})
    );

    conn.subscription_succeeded("feed", &json!({}));
    eventually(|| channel.is_subscribed()).await;
    conn.send_json(&json!({"event": "e", "channel": "feed", "data": "{}", "serial": 10, "stream_id": "s1", "message_id": "m10"}));
    eventually(|| {
        client
            .recovery_position("feed")
            .is_some_and(|p| p.serial == 10)
    })
    .await;

    conn.close(4200, "");
    let mut conn = server.accept().await;
    conn.establish("1.2");
    let resume = conn.expect_event("sockudo:resume").await;
    assert_eq!(
        resume["data"]["channel_positions"]["feed"],
        json!({"serial": 10, "stream_id": "s1", "last_message_id": "m10"})
    );
    conn.send_event(
        "sockudo:resume_success",
        None,
        &json!({"recovered": [{"channel": "feed", "source": "hot", "replayed": 0, "position": {"serial": 12, "stream_id": "s1"}}], "failed": []}),
    );
    eventually(|| resumed.len() == 1).await;
    assert_eq!(client.recovery_position("feed").unwrap().serial, 12);

    // A failed resume drops the position and resubscribes the channel.
    conn.subscription_succeeded("feed", &json!({}));
    eventually(|| channel.is_subscribed()).await;
    conn.send_event(
        "sockudo:resume_failed",
        None,
        &json!({"channel": "feed", "code": "stream_reset", "reason": "continuity lost"}),
    );
    let resubscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(data_of(&resubscribe)["channel"].as_str(), Some("feed"));
    assert!(client.recovery_position("feed").is_none());
}

#[tokio::test]
async fn subscribe_frames_carry_v2_options() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    client
        .subscribe_with(
            "price:btc",
            SubscribeOptions::new()
                .filter(Filter::and([
                    Filter::eq("market", "spot"),
                    Filter::gt("spread", "0"),
                ]))
                .events(["price.updated"])
                .expression("data.price >= `100`")
                .delta(ChannelDeltaSettings::with_algorithm(DeltaAlgorithm::Fossil))
                .rewind(Rewind::Count(25))
                .annotation_subscribe(true),
        )
        .unwrap();
    assert!(
        client
            .subscribe_with("bad", SubscribeOptions::new().filter(Filter::eq("k", "")))
            .is_err()
    );
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    let subscribe = conn.expect_event("sockudo:subscribe").await;
    assert_eq!(
        subscribe["data"],
        json!({
            "auth": "",
            "channel": "price:btc",
            "filter": {
                "events": ["price.updated"],
                "tags": {"op": "and", "nodes": [
                    {"key": "market", "cmp": "eq", "val": "spot"},
                    {"key": "spread", "cmp": "gt", "val": "0"}
                ]},
                "expression": "data.price >= `100`"
            },
            "delta": {"enabled": true, "algorithm": "fossil"},
            "rewind": 25,
            "modes": ["SUBSCRIBE", "ANNOTATION_SUBSCRIBE"]
        })
    );
}

fn fossil_b64(old: &[u8], new: &[u8]) -> String {
    // The server computes `delta(new, old)`.
    b64(&fossil_delta::delta(new, old))
}

#[tokio::test]
async fn delta_compression_reconstructs_and_resyncs() {
    let mut server = MockServer::start().await;
    let stats_calls = Arc::new(AtomicUsize::new(0));
    let s = Arc::clone(&stats_calls);
    // The callback reads the client's delta statistics: it must not run
    // under the client's delta lock.
    let handle = Arc::new(std::sync::OnceLock::<sockudo_client::SockudoClient>::new());
    let h = Arc::clone(&handle);
    let client = client_for(&server)
        .delta_compression(DeltaOptions {
            on_stats: Some(Arc::new(move |_| {
                if let Some(client) = h.get() {
                    let _ = client.delta_stats();
                }
                s.fetch_add(1, Ordering::SeqCst);
            })),
            ..DeltaOptions::default()
        })
        .build()
        .unwrap();
    let _ = handle.set(client.clone());
    let channel = client.subscribe("ticker:btc");
    let prices = Recorder::default();
    let p = prices.clone();
    channel.bind("tick", move |event| {
        p.push((event.value(), event.message_id.clone()))
    });
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    let enable = conn.expect_event("sockudo:enable_delta_compression").await;
    let algorithms = enable["data"]["algorithms"].clone();
    #[cfg(feature = "xdelta3")]
    assert_eq!(algorithms, json!(["fossil", "xdelta3"]));
    #[cfg(not(feature = "xdelta3"))]
    assert_eq!(algorithms, json!(["fossil"]));
    conn.expect_event("sockudo:subscribe").await;
    conn.send_event(
        "sockudo:delta_compression_enabled",
        None,
        &json!({"enabled": true, "algorithm": "fossil"}),
    );
    conn.subscription_succeeded("ticker:btc", &json!({}));
    eventually(|| client.is_delta_compression_enabled()).await;

    let base = r#"{"event":"tick","channel":"ticker:btc","data":"{\"price\":100.10,\"seq\":1}","message_id":"m1"}"#;
    let full = r#"{"event":"tick","channel":"ticker:btc","data":"{\"price\":100.10,\"seq\":1}","message_id":"m1","__delta_seq":0}"#;
    conn.send_text(full);
    let next = r#"{"event":"tick","channel":"ticker:btc","data":"{\"price\":100.20,\"seq\":2}","message_id":"m2"}"#;
    let delta = json!({"event": "tick", "delta": fossil_b64(base.as_bytes(), next.as_bytes()), "seq": 1, "algorithm": "fossil", "base_index": 0});
    conn.send_json(&json!({"event": "sockudo:delta", "channel": "ticker:btc", "data": delta}));
    eventually(|| prices.len() == 2).await;
    let received = prices.snapshot();
    assert_eq!(received[0].0["price"].as_f64(), Some(100.10));
    assert_eq!(received[1].0["price"].as_f64(), Some(100.20));
    assert_eq!(received[1].1.as_deref(), Some("m2"));
    let stats = client.delta_stats().unwrap();
    assert_eq!((stats.full_messages, stats.delta_messages), (1, 1));
    assert!(stats_calls.load(Ordering::SeqCst) >= 2);

    #[cfg(feature = "xdelta3")]
    {
        let third = r#"{"event":"tick","channel":"ticker:btc","data":"{\"price\":100.30,\"seq\":3}","message_id":"m3"}"#;
        let mut vcdiff = Vec::new();
        oxidelta::compress::encoder::encode_all(
            &mut vcdiff,
            next.as_bytes(),
            third.as_bytes(),
            oxidelta::compress::encoder::CompressOptions {
                checksum: false,
                ..Default::default()
            },
        )
        .unwrap();
        conn.send_json(&json!({"event": "sockudo:delta", "channel": "ticker:btc", "data": {
            "event": "tick", "delta": b64(&vcdiff), "seq": 2, "algorithm": "xdelta3", "base_index": 1
        }}));
        eventually(|| prices.len() == 3).await;
        assert_eq!(prices.snapshot()[2].0["price"].as_f64(), Some(100.30));
    }

    // A delta against an unknown base triggers a resync request.
    conn.send_json(
        &json!({"event": "sockudo:delta", "channel": "ticker:btc", "data": {
            "event": "tick", "delta": "AAAA", "seq": 9, "base_index": 77
        }}),
    );
    let resync = conn.expect_event("sockudo:delta_sync_error").await;
    assert_eq!(resync["data"], json!({"channel": "ticker:btc"}));
    assert!(client.delta_stats().unwrap().errors >= 1);
}

#[tokio::test]
async fn capability_tokens_connect_refresh_and_expire() {
    let mut server = MockServer::start().await;
    let requests = Recorder::default();
    let r = requests.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&counter);
    let errors = Recorder::default();
    let e = errors.clone();
    let client = client_for(&server)
        .token_provider(move |request: TokenRequest| {
            let n = c.fetch_add(1, Ordering::SeqCst);
            r.push(request.reason);
            async move {
                Ok(TokenAuthData::new(format!("tok-{n}")).with_expires_in(Duration::from_secs(2)))
            }
        })
        .auth_refresh_leeway(Duration::from_secs(30))
        .build()
        .unwrap();
    client.on_error(move |error| e.push(error.code()));
    client.connect().unwrap();
    let mut conn = server.accept().await;
    assert_eq!(conn.query("token").as_deref(), Some("tok-0"));
    conn.establish("1.1");

    // Proactive refresh (lifetime 2s → refresh after the 1s minimum).
    let auth = conn.expect_event("sockudo:auth").await;
    assert_eq!(auth["data"]["token"].as_str(), Some("tok-1"));
    conn.send_event(
        "sockudo:auth_success",
        None,
        &json!({"token": "tok-1", "expires_in": 600}),
    );

    // Reactive refresh on 40142.
    conn.send_event(
        "sockudo:token_expired",
        None,
        &json!({"code": 40142, "reason": "expired"}),
    );
    let auth = conn.expect_event("sockudo:auth").await;
    assert_eq!(auth["data"]["token"].as_str(), Some("tok-2"));
    assert_eq!(
        requests.snapshot(),
        vec![
            TokenRequestReason::Initial,
            TokenRequestReason::Refresh,
            TokenRequestReason::Expired
        ]
    );

    // Revocation is reported and not retried in place.
    conn.send_event(
        "sockudo:token_expired",
        None,
        &json!({"code": 40160, "reason": "revoked"}),
    );
    eventually(|| errors.snapshot().contains(&Some(40160))).await;
    let frames = conn.drain_json(Duration::from_millis(300)).await;
    assert!(
        frames
            .iter()
            .all(|f| f["event"].as_str() != Some("sockudo:auth"))
    );

    // Reconnects fetch a fresh token for the URL.
    conn.close(4200, "");
    let conn = server.accept().await;
    assert_eq!(conn.query("token").as_deref(), Some("tok-3"));
    assert_eq!(
        requests.snapshot().last(),
        Some(&TokenRequestReason::Reconnect)
    );
}

#[tokio::test]
async fn static_tokens_are_reused_and_never_refreshed() {
    let mut server = MockServer::start().await;
    let client = client_for(&server).token("static-token").build().unwrap();
    let errors = Recorder::default();
    let e = errors.clone();
    client.on_error(move |error| e.push(matches!(error, Error::TokenExpired { .. })));
    client.connect().unwrap();
    let conn = server.accept().await;
    assert_eq!(conn.query("token").as_deref(), Some("static-token"));
    conn.establish("1.1");
    conn.send_event("sockudo:token_expired", None, &json!({"code": 40142}));
    eventually(|| errors.snapshot().contains(&true)).await;
    conn.close(4200, "");
    let conn = server.accept().await;
    assert_eq!(conn.query("token").as_deref(), Some("static-token"));
}

#[cfg(feature = "http")]
#[tokio::test]
async fn token_url_contract() {
    let bodies = Recorder::default();
    let b = bodies.clone();
    let router = Router::new().route(
        "/token",
        post(move |body: axum::body::Bytes| {
            let b = b.clone();
            async move {
                b.push(sonic_rs::from_slice::<sonic_rs::Value>(&body).unwrap());
                Json(json!({"token": "from-url", "expires_in": 3600}))
            }
        }),
    );
    let base = support::serve_http(router).await;
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .auth_url(format!("{base}/token"))
        .build()
        .unwrap();
    client.connect().unwrap();
    let conn = server.accept().await;
    assert_eq!(conn.query("token").as_deref(), Some("from-url"));
    assert_eq!(
        bodies.snapshot()[0],
        json!({"socket_id": null, "reason": "initial"})
    );
}

#[tokio::test]
async fn proxies_send_the_documented_bodies() {
    let calls = Recorder::default();
    let c = calls.clone();
    let handler = move |request: ProxyRequest| {
        c.push(request.body.clone());
        async move {
            Ok(match request.action.as_str() {
                "channel_history" => json!({
                    "items": [{"event": "e", "channel": "room", "data": "1", "serial": 1}],
                    "has_more": true, "next_cursor": "c2", "limit": 1
                }),
                "get_message" => json!({"channel": "room", "item": {"serial": "m1"}}),
                "publish_annotation" => json!({"annotationSerial": "a1"}),
                "create" => json!({"ack": {"message_id": "m1", "message_serial": 7}}),
                other => json!({"unexpected": other}),
            })
        }
    };
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .versioned_messages(ProxyOptions::handler(handler))
        .build()
        .unwrap();
    let channel = client.subscribe("room");
    client.connect().unwrap();
    let mut conn = server.accept().await;
    conn.establish("1.1");
    conn.expect_event("sockudo:subscribe").await;
    conn.subscription_succeeded("room", &json!({"attach_serial": 99}));
    eventually(|| channel.is_subscribed()).await;

    let page = channel
        .history(
            ChannelHistoryParams::new()
                .limit(1)
                .direction(Direction::OldestFirst)
                .until_attach(),
        )
        .await
        .unwrap();
    assert!(page.has_next());
    assert_eq!(page.events()[0].event, "e");
    let _second = page.next().await.unwrap();
    let message = channel.get_message("m1").await.unwrap();
    assert_eq!(message.item["serial"].as_str(), Some("m1"));
    let annotation = channel
        .publish_annotation(
            "m1",
            PublishAnnotation::new("reaction:distinct.v1").name("👍"),
        )
        .await
        .unwrap();
    assert_eq!(annotation.annotation_serial, "a1");
    let ack = client
        .versioned_messages()
        .create("room", "chat.message", &json!({"text": "hi"}), None)
        .await
        .unwrap();
    assert_eq!(
        (ack.message_id.as_deref(), ack.message_serial),
        (Some("m1"), Some(7))
    );

    let calls = calls.snapshot();
    assert_eq!(
        calls[0],
        json!({"channel": "room", "params": {"direction": "oldest_first", "limit": 1, "until_attach": true, "attach_serial": 99}, "action": "channel_history"})
    );
    assert_eq!(calls[1]["params"]["cursor"].as_str(), Some("c2"));
    assert_eq!(
        calls[2],
        json!({"messageSerial": "m1", "channel": "room", "action": "get_message"})
    );
    assert_eq!(
        calls[3],
        json!({"messageSerial": "m1", "annotation": {"type": "reaction:distinct.v1", "name": "👍"}, "channel": "room", "action": "publish_annotation"})
    );
    assert_eq!(
        calls[4],
        json!({"channel": "room", "params": {"event": "chat.message", "data": {"text": "hi"}}, "action": "create"})
    );

    // Presence APIs need a presence channel and the presence proxy.
    assert!(matches!(
        channel.presence_history(Default::default()).await,
        Err(Error::Unsupported(_))
    ));
}

#[cfg(feature = "http")]
#[tokio::test]
async fn http_proxy_endpoint_and_errors() {
    let router = Router::new()
        .route(
            "/history",
            post(|body: axum::body::Bytes| async move {
                let body: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
                assert_eq!(body["action"].as_str(), Some("history"));
                Json(json!({"items": [], "has_more": false}))
            }),
        )
        .route(
            "/broken",
            post(|| async { (axum::http::StatusCode::BAD_GATEWAY, "upstream down") }),
        );
    let base = support::serve_http(router).await;
    let server = MockServer::start().await;
    let client = client_for(&server)
        .channel_history(ProxyOptions::endpoint(format!("{base}/history")))
        .versioned_messages(ProxyOptions::endpoint(format!("{base}/broken")))
        .build()
        .unwrap();
    let channel = client.subscribe("room");
    let page = channel.history(ChannelHistoryParams::new()).await.unwrap();
    assert!(!page.has_next());
    assert!(matches!(page.next().await, Err(Error::NoMorePages)));
    let error = channel.get_message("m").await.unwrap_err();
    assert!(
        matches!(error, Error::Http { status: 502, ref body } if body == "upstream down"),
        "{error}"
    );
}

#[cfg(any(feature = "msgpack", feature = "protobuf"))]
mod binary {
    use super::*;
    use sockudo_client::WireFormat;
    use sockudo_protocol::messages::{MessageData, PusherMessage};
    use sockudo_protocol::wire::{self, WireFormat as ServerFormat};
    use sockudo_ws::Message;

    fn server_message(event: &str, channel: Option<&str>, data: &str) -> PusherMessage {
        let mut message: PusherMessage = sonic_rs::from_str(&format!(
            r#"{{"event":{}}}"#,
            sonic_rs::to_string(event).unwrap()
        ))
        .unwrap();
        message.channel = channel.map(str::to_owned);
        message.data = Some(MessageData::String(data.to_owned()));
        message
    }

    async fn round_trip(client_format: WireFormat, server_format: ServerFormat, query: &str) {
        let mut server = MockServer::start().await;
        let client = client_for(&server)
            .wire_format(client_format)
            .build()
            .unwrap();
        let channel = client.subscribe("bin");
        let seen = Recorder::default();
        let s = seen.clone();
        channel.bind("e", move |event| s.push(event.value()));
        client.connect().unwrap();
        let mut conn = server.accept().await;
        assert_eq!(conn.query("format").as_deref(), Some(query));
        let send = |conn: &support::MockConn, message: &PusherMessage| {
            conn.send_binary(wire::serialize_message(message, server_format).unwrap());
        };
        send(
            &conn,
            &server_message(
                "sockudo:connection_established",
                None,
                r#"{"socket_id":"7.7","activity_timeout":120}"#,
            ),
        );
        // The client's subscribe must decode with the server's own codec.
        let frame = loop {
            match conn.recv_frame().await {
                Message::Binary(bytes) => break bytes,
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("expected binary, got {other:?}"),
            }
        };
        let decoded = wire::deserialize_message(&frame, server_format).unwrap();
        assert_eq!(decoded.event.as_deref(), Some("sockudo:subscribe"));
        let Some(MessageData::Json(data)) = decoded.data else {
            panic!("subscribe data must be structured JSON");
        };
        assert_eq!(data.get("channel").and_then(|c| c.as_str()), Some("bin"));

        send(
            &conn,
            &server_message("sockudo_internal:subscription_succeeded", Some("bin"), "{}"),
        );
        let mut event = server_message("e", Some("bin"), r#"{"n":1}"#);
        event.message_id = Some("m1".into());
        event.serial = Some(5);
        send(&conn, &event);
        eventually(|| seen.len() == 1).await;
        assert_eq!(seen.snapshot()[0], json!({"n": 1}));
        assert!(channel.is_subscribed());
    }

    #[cfg(feature = "msgpack")]
    #[tokio::test]
    async fn messagepack_round_trips_with_the_server_codec() {
        round_trip(
            WireFormat::MessagePack,
            ServerFormat::MessagePack,
            "messagepack",
        )
        .await;
    }

    #[cfg(feature = "protobuf")]
    #[tokio::test]
    async fn protobuf_round_trips_with_the_server_codec() {
        round_trip(WireFormat::Protobuf, ServerFormat::Protobuf, "protobuf").await;
    }
}

#[cfg(feature = "http")]
#[tokio::test]
async fn stats_timeline_is_sent_on_connect_when_enabled() {
    let bundles = Recorder::default();
    let b = bundles.clone();
    let router = Router::new().route(
        "/timeline/v2/fetch/2",
        axum::routing::get(
            move |axum::extract::RawQuery(query): axum::extract::RawQuery| {
                let b = b.clone();
                async move {
                    b.push(query.unwrap_or_default());
                    Json(json!({}))
                }
            },
        ),
    );
    let base = support::serve_http(router).await;
    let mut server = MockServer::start().await;
    let client = client_for(&server)
        .enable_stats(true)
        .stats_host(base.trim_start_matches("http://"))
        .timeline_param("tenant", "acme")
        .build()
        .unwrap();
    client.connect().unwrap();
    let conn = server.accept().await;
    conn.establish("1.1");
    client.connected().await.unwrap();
    eventually(|| bundles.len() == 1).await;
    let engine = base64::engine::general_purpose::STANDARD;
    let query = bundles.snapshot()[0].clone();
    let params: std::collections::HashMap<String, String> =
        form_urlencoded::parse(query.as_bytes())
            .map(|(k, v)| {
                (
                    k.into_owned(),
                    String::from_utf8(engine.decode(v.as_bytes()).unwrap()).unwrap(),
                )
            })
            .collect();
    assert_eq!(params["lib"], "rust");
    assert_eq!(params["key"], "app-key");
    assert_eq!(params["tenant"], "acme");
    let timeline: Vec<sonic_rs::Value> = sonic_rs::from_str(&params["timeline"]).unwrap();
    let states: Vec<_> = timeline
        .iter()
        .filter_map(|e| e["state"].as_str())
        .collect();
    assert_eq!(states, vec!["connecting", "connected"]);
}

#[tokio::test]
async fn stats_are_off_by_default() {
    // Without enable_stats nothing is recorded or sent; connecting needs no
    // stats host at all.
    let mut server = MockServer::start().await;
    let client = client_for(&server).build().unwrap();
    client.connect().unwrap();
    let conn = server.accept().await;
    conn.establish("1.1");
    client.connected().await.unwrap();
}
