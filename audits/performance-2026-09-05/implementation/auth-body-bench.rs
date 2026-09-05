//! Included identically by two server test variants; calls actual middleware.
use super::pusher_api_auth_middleware;
use axum::{Router, body::{Body, Bytes}, extract::DefaultBodyLimit, routing::post};
use sockudo_core::options::{MemoryCacheOptions, ServerOptions};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Instant;
use tower::ServiceExt;

#[tokio::test]
#[ignore = "isolated performance audit probe"]
async fn benchmark_auth_body_admission() {
    let bounded = std::env::var("EXPECT_BOUNDED_AUTH").unwrap() == "1";
    let handler = Arc::new(sockudo_adapter::ConnectionHandlerBuilder::new(
        Arc::new(sockudo_app::memory_app_manager::MemoryAppManager::new()),
        Arc::new(sockudo_adapter::local_adapter::LocalAdapter::new()),
        Arc::new(sockudo_cache::memory_cache_manager::MemoryCacheManager::new("body-probe".into(), MemoryCacheOptions::default())),
        ServerOptions::default(),
    ).build());
    let router = Router::new().route("/apps/audit/events", post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }))
        .layer(axum::middleware::from_fn_with_state(handler, pusher_api_auth_middleware))
        .layer(DefaultBodyLimit::max(65536));
    println!("AUTH_CSV,offered_bytes,limit_bytes,sample,polled_bytes,status,elapsed_us");
    for offered in [32768usize, 131072, 64 * 1024 * 1024] {
        for sample in 0..7 {
            let polled = Arc::new(AtomicUsize::new(0));
            let counter = polled.clone();
            let chunk = Bytes::from(vec![b'x'; 4096]);
            let stream = futures_util::stream::iter((0..offered / chunk.len()).map(move |_| {
                counter.fetch_add(chunk.len(), Ordering::Relaxed);
                Ok::<_, std::convert::Infallible>(chunk.clone())
            }));
            let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
            let request = axum::http::Request::builder().method("POST")
                .uri(format!("/apps/audit/events?auth_key=key&auth_timestamp={timestamp}&auth_version=1.0&auth_signature=invalid"))
                .body(Body::from_stream(stream)).unwrap();
            let start = Instant::now();
            let response = router.clone().oneshot(request).await.unwrap();
            let elapsed = start.elapsed().as_micros();
            let polled = polled.load(Ordering::Relaxed);
            if bounded && offered > 65536 {
                assert_eq!(response.status().as_u16(), 413);
                assert_eq!(polled, 65536 + 4096);
            } else {
                assert_eq!(polled, offered);
                assert!(response.status().is_client_error());
                assert_ne!(response.status().as_u16(), 413);
            }
            println!("AUTH_CSV,{offered},65536,{sample},{polled},{},{elapsed}", response.status().as_u16());
        }
    }
}
