use super::{batch_events, batch_idempotency_cache_key, events, idempotency_cache_key};
use crate::http_handler::test_support::{empty_event_query, test_app};
#[cfg(feature = "push")]
use crate::http_handler::test_support::{test_push_admission, test_push_queue, test_push_store};
use async_trait::async_trait;
use axum::{
    Json,
    extract::{Extension, Path, Query, RawQuery, State},
    http::{HeaderMap, StatusCode, Uri},
    response::IntoResponse,
};
use sockudo_adapter::{ConnectionHandler, ConnectionHandlerBuilder, local_adapter::LocalAdapter};
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_cache::memory_cache_manager::MemoryCacheManager;
use sockudo_core::{
    app::AppManager,
    cache::CacheManager,
    error::{Error, Result},
    options::{MemoryCacheOptions, ServerOptions},
};
use sockudo_protocol::messages::{ApiMessageData, BatchPusherApiMessage, PusherApiMessage};
use std::sync::atomic::AtomicBool;
use std::{sync::Arc, time::Duration};

struct UnavailableCache;

impl UnavailableCache {
    fn error<T>() -> Result<T> {
        Err(Error::Cache("injected idempotency outage".to_string()))
    }
}

#[async_trait]
impl CacheManager for UnavailableCache {
    async fn has(&self, _key: &str) -> Result<bool> {
        Self::error()
    }

    async fn get(&self, _key: &str) -> Result<Option<String>> {
        Self::error()
    }

    async fn set(&self, _key: &str, _value: &str, _ttl_seconds: u64) -> Result<()> {
        Self::error()
    }

    async fn remove(&self, _key: &str) -> Result<()> {
        Self::error()
    }

    async fn disconnect(&self) -> Result<()> {
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> Result<Option<Duration>> {
        Self::error()
    }
}

fn handler_with_cache(cache: Arc<dyn CacheManager + Send + Sync>) -> Arc<ConnectionHandler> {
    let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
    let adapter =
        Arc::new(LocalAdapter::new()) as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    Arc::new(
        ConnectionHandlerBuilder::new(app_manager, adapter, cache, ServerOptions::default())
            .build(),
    )
}

fn handler_with_unavailable_cache() -> Arc<ConnectionHandler> {
    handler_with_cache(Arc::new(UnavailableCache))
}

fn event(idempotency_key: &str) -> PusherApiMessage {
    PusherApiMessage {
        name: Some("distributed.correctness".to_string()),
        data: Some(ApiMessageData::String("{\"sequence\":1}".to_string())),
        channel: Some("distributed-correctness".to_string()),
        channels: None,
        socket_id: None,
        info: None,
        tags: None,
        delta: None,
        idempotency_key: Some(idempotency_key.to_string()),
        message_id: None,
        extras: None,
    }
}

#[tokio::test]
async fn single_publish_fails_closed_when_idempotency_cache_is_unavailable() {
    let result = events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(test_app()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler_with_unavailable_cache()),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(event("single-outage")),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("publish must fail closed"),
        Err(error) => error,
    };
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn concurrent_publish_timeout_returns_retryable_backpressure() {
    let cache = Arc::new(MemoryCacheManager::new(
        "idempotency-test".to_string(),
        MemoryCacheOptions::default(),
    ));
    cache
        .set(
            &idempotency_cache_key("app-1", "still-processing"),
            "__processing__",
            120,
        )
        .await
        .unwrap();
    let result = events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(test_app()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler_with_cache(cache)),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(event("still-processing")),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("concurrent retry must not publish without owning the claim"),
        Err(error) => error,
    };
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
}

#[tokio::test]
async fn batch_publish_fails_closed_when_idempotency_cache_is_unavailable() {
    let mut headers = HeaderMap::new();
    headers.insert("x-idempotency-key", "batch-outage".parse().unwrap());
    let result = batch_events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(test_app()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler_with_unavailable_cache()),
        headers,
        Uri::from_static("/apps/app-1/batch_events"),
        RawQuery(None),
        Json(BatchPusherApiMessage {
            batch: vec![event("batch-event-outage")],
        }),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("batch must fail closed"),
        Err(error) => error,
    };
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn batch_publish_rejects_empty_per_event_idempotency_key() {
    let cache = Arc::new(MemoryCacheManager::new(
        "batch-empty-key".to_string(),
        MemoryCacheOptions::default(),
    ));
    let result = batch_events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(test_app()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler_with_cache(cache)),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/batch_events"),
        RawQuery(None),
        Json(BatchPusherApiMessage {
            batch: vec![event("")],
        }),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("empty per-event key must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_publish_rejects_overlength_per_event_idempotency_key() {
    let cache = Arc::new(MemoryCacheManager::new(
        "batch-long-key".to_string(),
        MemoryCacheOptions::default(),
    ));
    let overlength_key = "x".repeat(ServerOptions::default().idempotency.max_key_length + 1);
    let result = batch_events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(test_app()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler_with_cache(cache)),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/batch_events"),
        RawQuery(None),
        Json(BatchPusherApiMessage {
            batch: vec![event(&overlength_key)],
        }),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("overlength per-event key must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_request_key_does_not_shadow_matching_event_key() {
    let cache = Arc::new(MemoryCacheManager::new(
        "batch-key-domains".to_string(),
        MemoryCacheOptions::default(),
    ));
    let mut headers = HeaderMap::new();
    headers.insert("x-idempotency-key", "shared-key".parse().unwrap());

    let response = batch_events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(test_app()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler_with_cache(cache.clone())),
        headers,
        Uri::from_static("/apps/app-1/batch_events"),
        RawQuery(None),
        Json(BatchPusherApiMessage {
            batch: vec![event("shared-key")],
        }),
    )
    .await
    .expect("batch should publish")
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        cache
            .get(&idempotency_cache_key("app-1", "shared-key"))
            .await
            .unwrap()
            .as_deref(),
        Some("1")
    );
    assert_eq!(
        cache
            .get(&batch_idempotency_cache_key("app-1", "shared-key"))
            .await
            .unwrap()
            .as_deref(),
        Some("{}")
    );
}

#[tokio::test]
async fn oversized_batch_is_rejected_before_its_idempotency_key_is_claimed() {
    let cache = Arc::new(MemoryCacheManager::new(
        "batch-validation-before-claim".to_string(),
        MemoryCacheOptions::default(),
    ));
    let mut app = test_app();
    app.policy_mut().limits.max_event_batch_size = Some(1);
    let mut headers = HeaderMap::new();
    headers.insert("x-idempotency-key", "oversized-batch".parse().unwrap());

    let result = batch_events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(app),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler_with_cache(cache.clone())),
        headers,
        Uri::from_static("/apps/app-1/batch_events"),
        RawQuery(None),
        Json(BatchPusherApiMessage {
            batch: vec![event("first"), event("second")],
        }),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("oversized batch must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        cache
            .get(&batch_idempotency_cache_key("app-1", "oversized-batch"))
            .await
            .unwrap(),
        None
    );
}

fn draining_handler(
    cache: Arc<dyn CacheManager + Send + Sync>,
    shutdown_grace_period: u64,
) -> Arc<ConnectionHandler> {
    let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
    let adapter =
        Arc::new(LocalAdapter::new()) as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    let options = ServerOptions {
        shutdown_grace_period,
        ..ServerOptions::default()
    };
    Arc::new(
        ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
            .running(Arc::new(AtomicBool::new(false)))
            .build(),
    )
}

async fn assert_draining(response: axum::response::Response, retry_after: &str) {
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers()[axum::http::header::RETRY_AFTER],
        retry_after
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    use sonic_rs::JsonValueTrait;
    let body: sonic_rs::Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(body["code"].as_str(), Some("draining"));
    assert_eq!(body["status"].as_u64(), Some(503));
}

#[tokio::test]
async fn publish_is_refused_while_draining_without_claiming_its_idempotency_key() {
    let cache = Arc::new(MemoryCacheManager::new(
        "drain-test".to_string(),
        MemoryCacheOptions::default(),
    ));
    let result = events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(test_app()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(draining_handler(cache.clone(), 10)),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(event("during-drain")),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("a draining local node must not acknowledge a publish"),
        Err(error) => error,
    };
    assert_draining(error.into_response(), "10").await;
    assert!(
        cache
            .get(&idempotency_cache_key("app-1", "during-drain"))
            .await
            .unwrap()
            .is_none(),
        "the retry must not find a claimed idempotency key"
    );
}

#[tokio::test]
async fn batch_publish_is_refused_while_draining_with_at_least_one_second_retry() {
    let cache = Arc::new(MemoryCacheManager::new(
        "drain-test".to_string(),
        MemoryCacheOptions::default(),
    ));
    let mut headers = HeaderMap::new();
    headers.insert("x-idempotency-key", "batch-during-drain".parse().unwrap());
    let result = batch_events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(test_app()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(draining_handler(cache.clone(), 0)),
        headers,
        Uri::from_static("/apps/app-1/batch_events"),
        RawQuery(None),
        Json(BatchPusherApiMessage {
            batch: vec![event("batch-event-during-drain")],
        }),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("a draining local node must not acknowledge a batch"),
        Err(error) => error,
    };
    assert_draining(error.into_response(), "1").await;
    for key in [
        batch_idempotency_cache_key("app-1", "batch-during-drain"),
        idempotency_cache_key("app-1", "batch-event-during-drain"),
    ] {
        assert!(
            cache.get(&key).await.unwrap().is_none(),
            "the retry must not find a claimed idempotency key"
        );
    }
}
