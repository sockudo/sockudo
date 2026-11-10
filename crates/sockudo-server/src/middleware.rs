use crate::http_handler::{AppError, EventQuery};
use axum::{
    body::Body, extract::State, http::Request as HttpRequest, middleware::Next, response::Response,
};
use http_body_util::BodyExt;
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::auth::AuthValidator;
use sockudo_core::rate_limiter::RateLimiter;
use std::{collections::BTreeMap, sync::Arc};

/// Shared backend for the per-app HTTP API limits. Present as a request
/// extension when `[rate_limiter]` is enabled and its backend initialized, so
/// the limits are enforced through the configured rate limiter driver
/// (cluster-wide with Redis, per node with `memory`).
#[derive(Clone)]
pub struct AppApiRateLimiter(pub Arc<dyn RateLimiter + Send + Sync>);

/// Which per-app limit a request counts against, and how many units it costs.
#[derive(Debug, PartialEq, Eq)]
struct AppApiLimit {
    kind: &'static str,
    max_per_second: u32,
    cost: u32,
}

/// `max_read_requests_per_second` covers GET (and HEAD) requests and
/// `max_backend_events_per_second` covers published events, with a batch
/// costing one unit per event. Unset or zero limits are unlimited.
fn app_api_limit(app: &App, method: &str, path: &str, body: &[u8]) -> Option<AppApiLimit> {
    let limits = &app.policy.limits;
    let is_read = method.eq_ignore_ascii_case("GET") || method.eq_ignore_ascii_case("HEAD");
    let (kind, max_per_second, cost) = if is_read {
        ("read_requests", limits.max_read_requests_per_second?, 1)
    } else if method.eq_ignore_ascii_case("POST") && path.ends_with("/batch_events") {
        // Parse the batch only when the limit will actually be enforced;
        // unlimited apps skip the extra full-body parse.
        let max_backend_events = limits
            .max_backend_events_per_second
            .filter(|limit| *limit > 0)?;
        let events = sonic_rs::get(body, &["batch"])
            .ok()
            .and_then(|batch| {
                sonic_rs::from_str::<Vec<serde::de::IgnoredAny>>(batch.as_raw_str()).ok()
            })
            .map_or(1, |batch| batch.len().max(1));
        (
            "backend_events",
            max_backend_events,
            u32::try_from(events).unwrap_or(u32::MAX),
        )
    } else if method.eq_ignore_ascii_case("POST") && path.ends_with("/events") {
        ("backend_events", limits.max_backend_events_per_second?, 1)
    } else {
        return None;
    };
    (max_per_second > 0).then_some(AppApiLimit {
        kind,
        max_per_second,
        cost,
    })
}

async fn enforce_app_api_limit(
    limiter: &AppApiRateLimiter,
    handler: &ConnectionHandler,
    app: &App,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<(), AppError> {
    let Some(limit) = app_api_limit(app, method, path, body) else {
        return Ok(());
    };
    // Reject before the limiter is charged: an oversized batch will be
    // rejected by the handler anyway, and here it would consume the app's
    // event budget or one Redis zset member per event.
    if path.ends_with("/batch_events")
        && let Some(max_batch) = app.event_batch_size_limit()
        && limit.cost > max_batch
    {
        return Err(AppError::LimitExceeded(format!(
            "Batch size ({}) exceeds limit ({max_batch})",
            limit.cost
        )));
    }
    if let Some(metrics) = handler.metrics() {
        metrics.mark_rate_limit_check_with_context(&app.id, limit.kind, "http_api");
    }
    let key = format!("{}:{}", limit.kind, app.id);
    match limiter
        .0
        .increment_with_limit(&key, limit.cost, limit.max_per_second, 1)
        .await
    {
        Ok(result) if !result.allowed => {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_rate_limit_triggered_with_context(&app.id, limit.kind, "http_api");
            }
            tracing::warn!(
                app_id = %app.id,
                limit = limit.kind,
                max_per_second = limit.max_per_second,
                cost = limit.cost,
                "per-app API rate limit exceeded"
            );
            Err(AppError::TooManyRequests {
                message: format!(
                    "App {} limit of {} per second exceeded",
                    limit.kind.replace('_', " "),
                    limit.max_per_second
                ),
                retry_after_seconds: result.reset_after.max(1),
            })
        }
        Ok(_) => Ok(()),
        Err(e) => {
            // Fail open: a limiter backend outage must not take the publish
            // API down with it. (The per-IP HTTP API limiter is fail-closed.)
            tracing::warn!(app_id = %app.id, error = %e, "per-app API rate limit check failed, allowing request");
            Ok(())
        }
    }
}

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

/// Whether the query string carries the parameters a Pusher signature needs.
fn has_signature_params(query: Option<&str>) -> bool {
    query
        .and_then(|query| serde_urlencoded::from_str::<EventQuery>(query).ok())
        .is_some_and(|params| !params.auth_key.is_empty() && !params.auth_signature.is_empty())
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
    let method = request.method().clone();

    // Requests that cannot authenticate are rejected before any of their body is read.
    if !has_signature_params(uri.query()) {
        tracing::warn!("missing authentication query parameters");
        return Err(AppError::InvalidInput(
            "Missing authentication query parameters".to_string(),
        ));
    }

    // Buffer the request body for the body_md5 check, capped at the configured request limit
    // (`DefaultBodyLimit` only applies to extractors, not to this direct read).
    let limit_bytes = usize::try_from(handler_state.server_options().http_api.request_limit_in_mb)
        .unwrap_or(usize::MAX)
        .saturating_mul(1024 * 1024);
    let (parts, body) = request.into_parts();
    let body_bytes = match http_body_util::Limited::new(body, limit_bytes)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(err)
            if err
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some() =>
        {
            return Err(AppError::PayloadTooLarge(format!(
                "Request body exceeds {} MB",
                handler_state.server_options().http_api.request_limit_in_mb
            )));
        }
        Err(err) => {
            tracing::error!("auth request body buffering failed");
            // TODO: HTTP response body disclosure
            return Err(AppError::InternalError(format!(
                "Failed to read request body: {err}"
            )));
        }
    };
    tracing::debug!(body_bytes = body_bytes.len(), "request body buffered");

    let app = authenticate_pusher_request(
        &handler_state,
        parts.extensions.get::<AppApiRateLimiter>(),
        method.as_str(),
        &uri,
        Some(&body_bytes),
    )
    .await?;
    let mut request = HttpRequest::from_parts(parts, Body::from(body_bytes.clone()));
    request.extensions_mut().insert(app);
    Ok(next.run(request).await)
}

/// Validate a Pusher HTTP API signature, enforce the app's per-app API limit when a
/// limiter is configured, and return the signing app.
///
/// Shared by [`pusher_api_auth_middleware`] and routes that dispatch between auth schemes
/// themselves (for example `/stats` with Ably compatibility enabled).
pub(crate) async fn authenticate_pusher_request(
    handler_state: &Arc<ConnectionHandler>,
    limiter: Option<&AppApiRateLimiter>,
    method: &str,
    uri: &axum::http::Uri,
    body: Option<&[u8]>,
) -> Result<sockudo_core::app::App, AppError> {
    let query_str_option = uri.query();
    let path = uri.path();

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

    // 3. Perform the authentication using AuthValidator.
    let auth_validator = AuthValidator::new(handler_state.app_manager().clone());
    match auth_validator
        .authenticate_pusher_api_request(
            &auth_q_params_struct,
            method,
            path,
            &all_query_params_for_sig_map,
            body,
        )
        .await
    {
        Ok(app) => {
            tracing::debug!(path = %path, "pusher API authentication successful");
            if let Some(limiter) = limiter {
                enforce_app_api_limit(
                    limiter,
                    handler_state,
                    &app,
                    method,
                    path,
                    body.unwrap_or_default(),
                )
                .await?;
            }
            Ok(app)
        }
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "pusher API authentication failed");
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app_with_limits(backend: Option<u32>, read: Option<u32>) -> App {
        let mut app = App {
            id: "app-1".to_string(),
            ..App::default()
        };
        app.policy.limits.max_backend_events_per_second = backend;
        app.policy.limits.max_read_requests_per_second = read;
        app
    }

    #[test]
    fn app_api_limit_classifies_requests() {
        let app = app_with_limits(Some(20), Some(5));
        assert_eq!(
            app_api_limit(&app, "POST", "/apps/app-1/events", b"{}"),
            Some(AppApiLimit {
                kind: "backend_events",
                max_per_second: 20,
                cost: 1
            })
        );
        assert_eq!(
            app_api_limit(
                &app,
                "POST",
                "/apps/app-1/batch_events",
                br#"{"batch":[{"name":"a"},{"name":"b"},{"name":"c"}]}"#
            ),
            Some(AppApiLimit {
                kind: "backend_events",
                max_per_second: 20,
                cost: 3
            })
        );
        assert_eq!(
            app_api_limit(&app, "GET", "/apps/app-1/channels", b""),
            Some(AppApiLimit {
                kind: "read_requests",
                max_per_second: 5,
                cost: 1
            })
        );
        // axum serves HEAD from GET routes, so HEAD counts as a read too.
        assert_eq!(
            app_api_limit(&app, "HEAD", "/apps/app-1/channels", b""),
            Some(AppApiLimit {
                kind: "read_requests",
                max_per_second: 5,
                cost: 1
            })
        );
        // Other writes (revocations, annotations, mutations) are not counted.
        assert_eq!(
            app_api_limit(&app, "POST", "/apps/app-1/revocations", b"{}"),
            None
        );
    }

    #[test]
    fn app_api_limit_unset_or_zero_is_unlimited() {
        assert_eq!(
            app_api_limit(
                &app_with_limits(None, None),
                "POST",
                "/apps/app-1/events",
                b"{}"
            ),
            None
        );
        assert_eq!(
            app_api_limit(
                &app_with_limits(None, None),
                "POST",
                "/apps/app-1/batch_events",
                br#"{"batch":[{"name":"a"}]}"#
            ),
            None
        );
        assert_eq!(
            app_api_limit(
                &app_with_limits(Some(0), Some(0)),
                "GET",
                "/apps/app-1/channels",
                b""
            ),
            None
        );
    }

    #[tokio::test]
    async fn app_api_limit_rejects_over_limit_with_429() {
        use axum::response::IntoResponse;
        use sockudo_rate_limiter::memory_limiter::MemoryRateLimiter;

        let handler = crate::http_handler::test_support::test_presence_history_handler(100);
        let limiter = AppApiRateLimiter(Arc::new(MemoryRateLimiter::new(u32::MAX, 1)));
        let app = app_with_limits(Some(2), None);
        let path = "/apps/app-1/events";

        for _ in 0..2 {
            enforce_app_api_limit(&limiter, &handler, &app, "POST", path, b"{}")
                .await
                .expect("within limit");
        }
        let err = enforce_app_api_limit(&limiter, &handler, &app, "POST", path, b"{}")
            .await
            .expect_err("third publish in the same second is rejected");
        let response = err.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["retry-after"], "1");

        // Another app has its own budget.
        let mut other = app_with_limits(Some(2), None);
        other.id = "app-2".to_string();
        enforce_app_api_limit(
            &limiter,
            &handler,
            &other,
            "POST",
            "/apps/app-2/events",
            b"{}",
        )
        .await
        .expect("separate per-app budget");
    }

    #[tokio::test]
    async fn oversized_batch_is_rejected_before_the_limiter() {
        use axum::response::IntoResponse;
        use sockudo_rate_limiter::memory_limiter::MemoryRateLimiter;

        let handler = crate::http_handler::test_support::test_presence_history_handler(100);
        let limiter = AppApiRateLimiter(Arc::new(MemoryRateLimiter::new(u32::MAX, 1)));
        let mut app = app_with_limits(Some(20), None);
        app.policy.limits.max_event_batch_size = Some(10);
        // 11 events, one over the batch limit.
        let body =
            br#"{"batch":[{"name":"a"},{"name":"b"},{"name":"c"},{"name":"d"},{"name":"e"},{"name":"f"},{"name":"g"},{"name":"h"},{"name":"i"},{"name":"j"},{"name":"k"}]}"#;

        let err = enforce_app_api_limit(
            &limiter,
            &handler,
            &app,
            "POST",
            "/apps/app-1/batch_events",
            body,
        )
        .await
        .expect_err("oversized batch is rejected");
        assert_eq!(
            err.into_response().status(),
            axum::http::StatusCode::BAD_REQUEST
        );

        // The batch-size bound is batch-scoped: with a batch limit of zero,
        // single publishes and reads still go through.
        app.policy.limits.max_event_batch_size = Some(0);
        enforce_app_api_limit(
            &limiter,
            &handler,
            &app,
            "POST",
            "/apps/app-1/events",
            br#"{"name":"a","channel":"c"}"#,
        )
        .await
        .expect("single publish is not a batch");
        enforce_app_api_limit(&limiter, &handler, &app, "GET", "/apps/app-1/channels", b"")
            .await
            .expect("read is not a batch");
    }

    /// Minimal publish router with the real auth middleware, mirroring
    /// `configure_http_routes` and the `mcp` module's in-process test router.
    fn publish_router(handler: Arc<ConnectionHandler>, limiter: AppApiRateLimiter) -> axum::Router {
        use axum::routing::post;

        let router = axum::Router::new().route(
            "/apps/{appId}/events",
            post(crate::http_handler::events).route_layer(axum::middleware::from_fn_with_state(
                handler.clone(),
                pusher_api_auth_middleware,
            )),
        );
        #[cfg(feature = "push")]
        let router = router
            .layer(crate::http_handler::test_support::test_push_queue())
            .layer(crate::http_handler::test_support::test_push_store())
            .layer(crate::http_handler::test_support::test_push_admission());
        router.layer(axum::Extension(limiter)).with_state(handler)
    }

    fn signed_publish_request(app: &App, body: &str) -> HttpRequest<Body> {
        use sockudo_core::token::Token;

        let body_md5 = format!("{:x}", md5::compute(body));
        let timestamp = chrono::Utc::now().timestamp();
        let mut params = BTreeMap::new();
        params.insert("auth_key", app.key.clone());
        params.insert("auth_timestamp", timestamp.to_string());
        params.insert("auth_version", "1.0".to_string());
        params.insert("body_md5", body_md5);

        let mut query_for_sig = String::new();
        for (index, (key, value)) in params.iter().enumerate() {
            if index > 0 {
                query_for_sig.push('&');
            }
            query_for_sig.push_str(key);
            query_for_sig.push('=');
            query_for_sig.push_str(value);
        }
        let string_to_sign = format!("POST\n/apps/app-1/events\n{query_for_sig}");
        let signature = Token::new(app.key.clone(), app.secret.clone()).sign(&string_to_sign);

        let query = format!("{query_for_sig}&auth_signature={signature}");
        HttpRequest::builder()
            .method("POST")
            .uri(format!("/apps/app-1/events?{query}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn signed_publishes_over_per_app_limit_get_429_through_auth_middleware() {
        use axum::body::to_bytes;
        use sockudo_core::app::AppManager;
        use sockudo_rate_limiter::memory_limiter::MemoryRateLimiter;
        use sonic_rs::JsonValueTrait as _;
        use tower::ServiceExt;

        let (handler, app_manager) =
            crate::http_handler::test_support::test_realtime_handler_harness();
        // test_realtime_handler_harness registers no apps; use credentials the
        // middleware can look up, with a limit of 2 publishes per second.
        let mut app = app_with_limits(Some(2), None);
        app.key = "key".to_string();
        app.secret = "secret".to_string();
        app.enabled = true;
        app_manager.create_app(app.clone()).await.unwrap();
        let limiter = AppApiRateLimiter(Arc::new(MemoryRateLimiter::new(u32::MAX, 1)));
        let router = publish_router(handler, limiter);

        let body = r#"{"name":"tick","channel":"orders","data":"{}"}"#;

        // Two signed publishes within the limit succeed.
        for _ in 0..2 {
            let response = router
                .clone()
                .oneshot(signed_publish_request(&app, body))
                .await
                .expect("infallible response");
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
            assert!(sonic_rs::from_slice::<sonic_rs::Value>(&bytes).is_ok());
        }

        // The third signed publish in the same second is rate limited.
        let limited = router
            .clone()
            .oneshot(signed_publish_request(&app, body))
            .await
            .expect("infallible response");
        assert_eq!(limited.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(limited.headers()["retry-after"], "1");
        let bytes = to_bytes(limited.into_body(), 4096).await.unwrap();
        let json: sonic_rs::Value = sonic_rs::from_slice(&bytes).unwrap();
        assert_eq!(json["code"].as_str(), Some("rate_limited"));

        // Enforcement runs after authentication: a request signed with an
        // unknown key never reaches the limiter, so the per-app budget is
        // unchanged by rejected authentication. Unknown keys deliberately map
        // to 404 (key existence is not disclosed).
        let bad_app = App {
            key: "wrong".to_string(),
            ..app.clone()
        };
        let response = router
            .oneshot(signed_publish_request(&bad_app, body))
            .await
            .expect("infallible response");
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn signature_params_are_required_before_body_read() {
        assert!(!has_signature_params(None));
        assert!(!has_signature_params(Some("unit=minute")));
        assert!(!has_signature_params(Some("auth_key=k")));
        assert!(has_signature_params(Some("auth_key=k&auth_signature=s")));
    }

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
}
