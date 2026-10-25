//! A failed delivery must reach the queue as `Err` so the backend can retry or
//! dead-letter the job. Before the fix every case here returned `Ok(())`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::error::Error;
use sockudo_core::options::WebhookRetryConfig;
use sockudo_core::webhook_types::{JobData, JobPayload, Webhook};
use sockudo_webhook::sender::WebhookSender;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serves `status_line` to every request and counts requests.
async fn spawn_server(status_line: &'static str) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let counter = counter.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                counter.fetch_add(1, Ordering::SeqCst);
                let response = format!(
                    "HTTP/1.1 {status_line}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (format!("http://{addr}/hook"), hits)
}

async fn closed_port_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("http://{addr}/hook")
}

fn webhook(url: Option<&str>) -> Webhook {
    Webhook {
        url: url.map(|url| url::Url::parse(url).unwrap()),
        lambda_function: None,
        lambda: None,
        event_types: vec!["channel_occupied".to_string()],
        filter: None,
        headers: None,
        retry: None,
        request_timeout_ms: None,
    }
}

/// Fails fast: one attempt, so tests do not wait for the 5-minute budget.
fn no_retry() -> WebhookRetryConfig {
    WebhookRetryConfig {
        enabled: false,
        ..WebhookRetryConfig::default()
    }
}

async fn sender_for(webhooks: Vec<Webhook>, retry: WebhookRetryConfig) -> WebhookSender {
    let app_manager = Arc::new(MemoryAppManager::new());
    let app = App::from_policy(
        "failure_app".to_string(),
        "failure_key".to_string(),
        "failure_secret".to_string(),
        true,
        AppPolicy {
            webhooks: Some(webhooks),
            ..Default::default()
        },
    );
    app_manager.create_app(app).await.unwrap();
    WebhookSender::new(app_manager, retry, 1_000)
}

fn job() -> JobData {
    JobData {
        app_id: "failure_app".to_string(),
        app_key: "failure_key".to_string(),
        app_secret: "failure_secret".to_string(),
        payload: JobPayload {
            time_ms: 1_700_000_000_000,
            events: vec![sonic_rs::json!({
                "name": "channel_occupied",
                "channel": "failure-channel"
            })],
        },
        original_signature: "sig".to_string(),
        ..JobData::default()
    }
}

#[tokio::test]
async fn http_500_returns_err() {
    let (url, hits) = spawn_server("500 Internal Server Error").await;
    let sender = sender_for(vec![webhook(Some(&url))], no_retry()).await;

    assert!(sender.process_webhook_job(job()).await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn exhausted_in_process_retries_return_err() {
    let (url, hits) = spawn_server("503 Service Unavailable").await;
    let retry = WebhookRetryConfig {
        enabled: true,
        max_attempts: Some(2),
        initial_backoff_ms: 10,
        max_backoff_ms: 20,
        ..WebhookRetryConfig::default()
    };
    let sender = sender_for(vec![webhook(Some(&url))], retry).await;

    assert!(sender.process_webhook_job(job()).await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn connection_refused_returns_err() {
    let url = closed_port_url().await;
    let sender = sender_for(vec![webhook(Some(&url))], no_retry()).await;

    assert!(sender.process_webhook_job(job()).await.is_err());
}

#[tokio::test]
async fn permanent_4xx_returns_err_without_in_process_retry() {
    let (url, hits) = spawn_server("400 Bad Request").await;
    // Retries enabled: a non-retryable status must still stop after one attempt.
    let sender = sender_for(vec![webhook(Some(&url))], WebhookRetryConfig::default()).await;

    assert!(sender.process_webhook_job(job()).await.is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn partial_failure_fails_the_whole_job() {
    let (ok_url, ok_hits) = spawn_server("200 OK").await;
    let (failing_url, failing_hits) = spawn_server("500 Internal Server Error").await;
    let sender = sender_for(
        vec![webhook(Some(&ok_url)), webhook(Some(&failing_url))],
        no_retry(),
    )
    .await;

    // Phase 1 redrives the whole job, so the healthy endpoint is POSTed again
    // on retry. Phase 2 (per-endpoint jobs) removes that duplicate.
    let error = sender.process_webhook_job(job()).await.unwrap_err();
    assert!(
        matches!(&error, Error::Other(message) if message == "1 of 2 webhook endpoints failed"),
        "{error:?}"
    );
    assert_eq!(ok_hits.load(Ordering::SeqCst), 1);
    assert_eq!(failing_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn successful_delivery_still_returns_ok() {
    let (url, hits) = spawn_server("200 OK").await;
    let sender = sender_for(vec![webhook(Some(&url))], no_retry()).await;

    assert!(sender.process_webhook_job(job()).await.is_ok());
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn endpoint_without_url_or_lambda_is_acked() {
    // Without a URL or Lambda the endpoint is never selected: nothing to retry.
    let sender = sender_for(vec![webhook(None)], no_retry()).await;

    assert!(sender.process_webhook_job(job()).await.is_ok());
}
