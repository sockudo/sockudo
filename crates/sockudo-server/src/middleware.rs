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
/// extension only when `[rate_limiter]` is enabled, so the limits apply
/// cluster-wide through the configured rate limiter driver.
#[derive(Clone)]
pub struct AppApiRateLimiter(pub Arc<dyn RateLimiter + Send + Sync>);

/// Which per-app limit a request counts against, and how many units it costs.
#[derive(Debug, PartialEq, Eq)]
struct AppApiLimit {
    kind: &'static str,
    max_per_second: u32,
    cost: u32,
}

/// `max_read_requests_per_second` covers GET requests and
/// `max_backend_events_per_second` covers published events, with a batch
/// costing one unit per event. Unset or zero limits are unlimited.
fn app_api_limit(app: &App, method: &str, path: &str, body: &[u8]) -> Option<AppApiLimit> {
    let limits = &app.policy.limits;
    let (kind, max_per_second, cost) = if method.eq_ignore_ascii_case("GET") {
        ("read_requests", limits.max_read_requests_per_second?, 1)
    } else if method.eq_ignore_ascii_case("POST") && path.ends_with("/batch_events") {
        let events = sonic_rs::get(body, &["batch"])
            .ok()
            .and_then(|batch| {
                sonic_rs::from_str::<Vec<serde::de::IgnoredAny>>(batch.as_raw_str()).ok()
            })
            .map_or(1, |batch| batch.len().max(1));
        (
            "backend_events",
            limits.max_backend_events_per_second?,
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
    if let Some(metrics) = handler.metrics() {
        metrics.mark_rate_limit_check(&app.id, limit.kind);
    }
    let key = format!("{}:{}", limit.kind, app.id);
    match limiter
        .0
        .increment_with_limit(&key, limit.cost, limit.max_per_second, 1)
        .await
    {
        Ok(result) if !result.allowed => {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_rate_limit_triggered(&app.id, limit.kind);
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
                retry_after_seconds: 1,
            })
        }
        Ok(_) => Ok(()),
        Err(e) => {
            // Fail open like the per-IP limiter: a limiter backend outage must
            // not take the publish API down with it.
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
    let (parts, body) = request.into_parts();
    let body_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            tracing::error!("auth request body buffering failed");
            // TODO: HTTP response body disclosure
            return Err(AppError::InternalError(format!(
                "Failed to read request body: {err}"
            )));
        }
    };
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
            if let Some(limiter) = parts.extensions.get::<AppApiRateLimiter>() {
                enforce_app_api_limit(
                    limiter,
                    &handler_state,
                    &app,
                    method.as_str(),
                    &path,
                    &body_bytes,
                )
                .await?;
            }
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
