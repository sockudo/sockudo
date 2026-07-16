use super::{batch_events, events, idempotency_cache_key};
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
