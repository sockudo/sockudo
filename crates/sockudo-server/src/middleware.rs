use crate::http_handler::{AppError, EventQuery};
use axum::{
    RequestExt,
    body::{Body, Bytes},
    extract::State,
    http::Request as HttpRequest,
    middleware::Next,
    response::Response,
};
use http_body_util::BodyExt;
use sockudo_adapter::ConnectionHandler;
use sockudo_core::auth::AuthValidator;
use std::{collections::BTreeMap, sync::Arc};

// Helper to extract query parameters for the signature
fn get_params_for_signature(
    query_str_option: Option<&str>,
) -> Result<BTreeMap<String, String>, AppError> {
    let mut params_map = BTreeMap::new();
    if let Some(query_str) = query_str_option {
        let parsed_pairs =
            serde_urlencoded::from_str::<Vec<(String, String)>>(query_str).map_err(|e| {
                AppError::InvalidInput(format!(
                    "Failed to parse query string for signature map: {e}"
                ))
            })?;

        if parsed_pairs.is_empty() {
            return Err(AppError::InvalidInput(
                "Query string is empty or invalid".to_string(),
            ));
        }

        for (key, value) in parsed_pairs {
            if key != "auth_signature" {
                params_map.insert(key, value);
            }
        }
    }
    Ok(params_map)
}

/// Axum middleware for Pusher API authentication.
///
/// This middleware authenticates incoming requests based on the Pusher protocol,
/// checking the auth_signature, timestamp, and optionally body_md5.
/// It requires the `ConnectionHandler` state to access the `AppManager` for app details.
pub async fn pusher_api_auth_middleware(
    State(handler_state): State<Arc<ConnectionHandler>>,
    request: HttpRequest<Body>,
    next: Next,
) -> Result<Response, AppError> {
    tracing::debug!("Entering Pusher API Auth Middleware");

    let uri = request.uri().clone();
    let query_str_option = uri.query();
    let method = request.method().clone();
    let path = uri.path().to_string();

    // 1. Extract Pusher's authentication query parameters
    let auth_q_params_struct: EventQuery = if let Some(query_str) = query_str_option {
        serde_urlencoded::from_str(query_str).map_err(|e| {
            tracing::warn!(error = %e, "event query parse failed");
            AppError::InvalidInput(format!("Invalid authentication query parameters: {e}"))
        })?
    } else {
        tracing::warn!("missing authentication query parameters");
        return Err(AppError::InvalidInput(
            "Missing authentication query parameters".to_string(),
        ));
    };

    // 2. Collect all query parameters (excluding auth_signature) for the signature string.
    let all_query_params_for_sig_map = get_params_for_signature(query_str_option)?;

    // 3. Buffer the request body.
    let (parts, body_bytes) = collect_auth_body(request).await?;
    tracing::debug!(body_bytes = body_bytes.len(), "request body buffered");

    // 4. Perform the authentication using AuthValidator.
    let auth_validator = AuthValidator::new(handler_state.app_manager().clone());

    match auth_validator
        .authenticate_pusher_api_request(
            &auth_q_params_struct,
            method.as_str(),
            &path,
            &all_query_params_for_sig_map,
            Some(&body_bytes),
        )
        .await
    {
        Ok(app) => {
            tracing::debug!(path = %path, "pusher API authentication successful");
            let mut request = HttpRequest::from_parts(parts, Body::from(body_bytes.clone()));
            request.extensions_mut().insert(app);
            Ok(next.run(request).await)
        }
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "pusher API authentication failed");
            Err(e.into())
        }
    }
}

// DefaultBodyLimit configures extractors through request extensions. Auth reads
// the body directly, so explicitly apply that limit before polling any frames.
async fn collect_auth_body(
    request: HttpRequest<Body>,
) -> Result<(axum::http::request::Parts, Bytes), AppError> {
    let (parts, body) = request.with_limited_body().into_parts();
    match body.collect().await {
        Ok(collected) => Ok((parts, collected.to_bytes())),
        Err(error) => {
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
            while let Some(current) = source {
                if current.is::<http_body_util::LengthLimitError>() {
                    tracing::warn!("auth request body limit exceeded");
                    return Err(AppError::PayloadTooLarge(
                        "request body exceeds configured limit".into(),
                    ));
                }
                source = current.source();
            }
            // Transport errors can contain request data; do not log their text.
            tracing::error!("auth request body buffering failed");
            Err(AppError::InternalError(
                "failed to read request body".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_params_for_signature_empty() {
        let result = get_params_for_signature(None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_params_for_signature_with_auth() {
        let query = "auth_key=key123&auth_timestamp=1234567890&auth_signature=abc123";
        let result = get_params_for_signature(Some(query)).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("auth_key"), Some(&"key123".to_string()));
        assert_eq!(
            result.get("auth_timestamp"),
            Some(&"1234567890".to_string())
        );
        assert_eq!(result.get("auth_signature"), None);
    }

    #[test]
    fn test_get_params_for_signature_with_auth2() {
        let query = "auth_key=key1&auth_timestamp=1749377222&auth_version=1.0&body_md5=fc820aa38714282f8300c2ca039cd034&auth_signature=737d666bce65766b2447e5fd3907b8855507305afcb4a25c6f1607d3eb3a2aa7";
        let result = get_params_for_signature(Some(query)).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result.get("auth_key"), Some(&"key1".to_string()));
        assert_eq!(
            result.get("auth_timestamp"),
            Some(&"1749377222".to_string())
        );
        assert_eq!(result.get("auth_signature"), None);
    }

    #[test]
    fn test_get_params_for_signature_invalid_queries() {
        let invalid_queries = ["&", ""];

        for query in invalid_queries.iter() {
            let result = get_params_for_signature(Some(query));
            assert!(
                result.is_err(),
                "Expected error for invalid query: {query:?}"
            );
        }
    }

    #[tokio::test]
    async fn auth_body_limit_preserves_bytes_and_stops_chunked_overflow() {
        use axum::response::IntoResponse;
        use axum::{Router, extract::DefaultBodyLimit, http::StatusCode, routing::post};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tower::ServiceExt;
        let app = Router::new()
            .route(
                "/",
                post(|request: HttpRequest<Body>| async {
                    match collect_auth_body(request).await {
                        Ok((_, bytes)) => bytes.into_response(),
                        Err(error) => error.into_response(),
                    }
                }),
            )
            .layer(DefaultBodyLimit::max(8));
        for raw in [b"".as_slice(), b"  { } \n", b"12345678"] {
            let response = app
                .clone()
                .oneshot(
                    HttpRequest::post("/")
                        .body(Body::from(raw.to_vec()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .as_ref(),
                raw
            );
        }
        let response = app
            .clone()
            .oneshot(
                HttpRequest::post("/")
                    .body(Body::from("123456789"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let polled = Arc::new(AtomicUsize::new(0));
        let counter = polled.clone();
        let stream = futures_util::stream::iter((0..100).map(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok::<_, std::io::Error>(Bytes::from_static(b"1234"))
        }));
        // A dishonest Content-Length must not bypass the streaming limit.
        let response = app
            .oneshot(
                HttpRequest::post("/")
                    .header("content-length", "4")
                    .body(Body::from_stream(stream))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(polled.load(Ordering::Relaxed), 3);
    }
}

#[cfg(test)]
mod signed_body_tests {
    use super::*;
    use axum::{
        Router,
        extract::{DefaultBodyLimit, Extension},
        routing::post,
    };
    use sockudo_core::app::{App, AppManager};
    use sockudo_core::options::{MemoryCacheOptions, ServerOptions};
    use tower::ServiceExt;

    #[tokio::test]
    async fn limited_auth_preserves_signed_whitespace_utf8_and_downstream_bytes() {
        let app = App::from_policy(
            "app-1".into(),
            "key".into(),
            "secret".into(),
            true,
            Default::default(),
        );
        let apps = Arc::new(sockudo_app::memory_app_manager::MemoryAppManager::new());
        apps.create_app(app.clone()).await.unwrap();
        let handler = Arc::new(
            sockudo_adapter::ConnectionHandlerBuilder::new(
                apps,
                Arc::new(sockudo_adapter::local_adapter::LocalAdapter::new()),
                Arc::new(
                    sockudo_cache::memory_cache_manager::MemoryCacheManager::new(
                        "auth-test".into(),
                        MemoryCacheOptions::default(),
                    ),
                ),
                ServerOptions::default(),
            )
            .build(),
        );
        let body = " {\"hello\":\"世界\"}\n".as_bytes();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let params = format!(
            "auth_key=key&auth_timestamp={timestamp}&auth_version=1.0&body_md5=1c280c32c35a98ef80feedd13d39f02c"
        );
        let signature = sockudo_core::token::Token::new(app.key, app.secret)
            .sign(&format!("POST\n/apps/app-1/events\n{params}"));
        let uri = format!("/apps/app-1/events?{params}&auth_signature={signature}");
        let router = Router::new()
            .route(
                "/apps/app-1/events",
                post(|Extension(app): Extension<App>, bytes: Bytes| async move {
                    assert_eq!(app.id, "app-1");
                    bytes
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                handler,
                pusher_api_auth_middleware,
            ))
            .layer(DefaultBodyLimit::max(body.len()));
        let chunked = || {
            Body::from_stream(futures_util::stream::iter(
                body.chunks(3)
                    .map(|chunk| Ok::<_, std::convert::Infallible>(Bytes::copy_from_slice(chunk)))
                    .collect::<Vec<_>>(),
            ))
        };
        let request = HttpRequest::builder()
            .method("POST")
            .uri(&uri)
            .header("content-length", "1")
            .body(chunked())
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            body
        );
        let request = HttpRequest::builder()
            .method("POST")
            .uri(&uri)
            .body(Body::from([body, b" "].concat()))
            .unwrap();
        assert_eq!(
            router.clone().oneshot(request).await.unwrap().status(),
            axum::http::StatusCode::PAYLOAD_TOO_LARGE
        );
        let wrong_signature = uri.replace(&signature, "invalid");
        let request = HttpRequest::builder()
            .method("POST")
            .uri(wrong_signature)
            .body(chunked())
            .unwrap();
        assert!(!router.oneshot(request).await.unwrap().status().is_success());
    }
}
