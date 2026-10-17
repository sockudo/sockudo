#![cfg(unix)]

use axum::{Router, extract::State, routing::post};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sockudo_core::token::Token;
use sockudo_ws::{Config, Http1, Message, client::WebSocketClient};
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::{Instant, sleep, timeout},
};

struct Fixture {
    folder: PathBuf,
    receiver: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.receiver.abort();
        let _ = std::fs::remove_dir_all(&self.folder);
    }
}

async fn receive(
    State(events): State<Arc<Mutex<Vec<Value>>>>,
    axum::Json(body): axum::Json<Value>,
) {
    events
        .lock()
        .await
        .extend(body["events"].as_array().unwrap().iter().cloned());
}

async fn shutdown_case(
    protocol: u8,
    grace: u64,
    lease: u64,
    batch_ms: u64,
    async_cleanup: bool,
    drain_ms: u64,
) -> (Vec<Value>, Duration, String) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hook_addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/hook", post(receive))
        .with_state(events.clone());
    let receiver = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let folder = std::env::temp_dir().join(format!("sockudo-shutdown-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&folder).unwrap();
    let fixture = Fixture { folder, receiver };
    let config = json!({
        "host": "127.0.0.1", "port": address.port(), "shutdown_grace_period": grace,
        "adapter": {"driver": "local"}, "cache": {"driver": "memory"},
        "queue": {"driver": "memory", "reliability": {"shutdown_timeout_ms": drain_ms}},
        "rate_limiter": {"enabled": false}, "metrics": {"enabled": false},
        "presence": {"v2_ungraceful_timeout_seconds": lease},
        "cleanup": {"async_enabled": async_cleanup, "worker_threads": 2},
        "webhooks": {"batching": {"enabled": true, "duration": batch_ms, "size": 100}},
        "app_manager": {"driver": "memory", "array": {"apps": [{
            "id": "shutdown-test", "key": "test-key", "secret": "test-secret", "enabled": true,
            "policy": {"webhooks": [{"url": format!("http://{hook_addr}/hook"),
                "event_types": ["member_removed"]}]}
        }]}}
    });
    let config_path = fixture.folder.join("config.json");
    std::fs::write(&config_path, config.to_string()).unwrap();
    let log_path = fixture.folder.join("server.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_sockudo"));
    command
        .arg("--config")
        .arg(&config_path)
        .current_dir(&fixture.folder)
        .env_clear()
        .env("RUST_LOG", "info")
        .kill_on_drop(true)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    let mut child = command.spawn().unwrap();
    let stream = timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(stream) = TcpStream::connect(address).await {
                break stream;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "server exited: {}",
                std::fs::read_to_string(&log_path).unwrap()
            );
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("server should start");
    let client = WebSocketClient::<Http1>::new(Config::default());
    let (mut socket, _) = client
        .connect(
            stream,
            &address.to_string(),
            &format!("/app/test-key?protocol={protocol}"),
            None,
        )
        .await
        .unwrap();
    let established = timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(bytes) = established else {
        panic!("expected connection event")
    };
    let established: Value = serde_json::from_slice(&bytes).unwrap();
    let data = if let Some(data) = established["data"].as_str() {
        serde_json::from_str(data).unwrap()
    } else {
        established["data"].clone()
    };
    let socket_id = data["socket_id"].as_str().unwrap();
    let channel_data = r#"{"user_id":"departing-user"}"#;
    let auth = Token::new("test-key".into(), "test-secret".into())
        .sign(&format!("{socket_id}:presence-shutdown:{channel_data}"));
    let prefix = if protocol == 2 { "sockudo" } else { "pusher" };
    socket.send(Message::text(json!({"event": format!("{prefix}:subscribe"), "data": {
        "channel": "presence-shutdown", "auth": format!("test-key:{auth}"), "channel_data": channel_data,
    }}).to_string())).await.unwrap();
    let subscribed = timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(bytes) = subscribed else {
        panic!("expected subscription event")
    };
    let subscribed: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        subscribed["event"]
            .as_str()
            .unwrap()
            .ends_with("subscription_succeeded"),
        "{subscribed}"
    );

    let started = Instant::now();
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    timeout(Duration::from_secs(5), async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Close(Some(frame)) => {
                    assert_eq!(frame.code, 4200);
                    break;
                }
                Message::Text(_) => {}
                message => panic!("expected shutdown close, got {message:?}"),
            }
        }
    })
    .await
    .expect("shutdown should close the socket promptly");
    drop(socket);
    let status = timeout(Duration::from_secs(35), child.wait())
        .await
        .expect("shutdown must be bounded")
        .unwrap();
    let elapsed = started.elapsed();
    let log = std::fs::read_to_string(&log_path).unwrap();
    assert!(status.success(), "server failed: {log}");
    let events = events.lock().await.clone();
    (events, elapsed, log)
}

fn assert_removal(events: &[Value]) {
    assert_eq!(
        events.len(),
        1,
        "one last-leave webhook must reach the receiver"
    );
    assert_eq!(events[0]["name"], "member_removed");
    assert_eq!(events[0]["user_id"], "departing-user");
    assert_eq!(events[0]["channel"], "presence-shutdown");
}

#[tokio::test]
async fn v2_shutdown_preserves_default_lease_and_delivers_after_grace_expires() {
    let (events, elapsed, _) = shutdown_case(2, 10, 15, 50, true, 30_000).await;
    assert_removal(&events);
    assert!(
        elapsed >= Duration::from_secs(18),
        "do not shorten the presence lease or webhook delay"
    );
}

#[tokio::test]
async fn v1_shutdown_flushes_events_produced_after_a_long_batch_interval_started() {
    let (events, _, _) = shutdown_case(7, 0, 15, 60_000, true, 10_000).await;
    assert_removal(&events);
}

#[tokio::test]
async fn synchronous_cleanup_drains_delayed_webhooks_too() {
    let (events, _, _) = shutdown_case(2, 0, 1, 60_000, false, 10_000).await;
    assert_removal(&events);
}

#[tokio::test]
async fn short_drain_budget_exits_with_an_explicit_loss_warning() {
    let (events, elapsed, log) = shutdown_case(2, 0, 15, 50, true, 100).await;
    assert!(events.is_empty());
    assert!(elapsed < Duration::from_secs(5));
    assert!(log.contains("shutdown webhook drain timed out"));
}
