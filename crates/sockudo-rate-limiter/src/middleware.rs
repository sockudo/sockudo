use axum::{
    body::Body as AxumBody,
    extract::ConnectInfo,
    http::{HeaderMap, HeaderName, HeaderValue, Request as AxumRequest, StatusCode},
    response::{IntoResponse, Response as AxumResponse},
};
use futures_util::future::BoxFuture;
use hyper::Request as HyperRequest;
use sockudo_core::rate_limiter::{RateLimitResult, RateLimiter};
use sonic_rs::json;
use std::{
    fmt,
    net::SocketAddr,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};
use tower_layer::Layer;
use tower_service::Service;
use tracing::{debug, error, warn};

#[derive(Debug)]
pub enum RateLimitMiddlewareError {
    InvalidHeaderName(String),
    ExtractionFailed(String),
}

impl fmt::Display for RateLimitMiddlewareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RateLimitMiddlewareError::InvalidHeaderName(e) => {
                write!(f, "Invalid header name for key extraction: {e}")
            }
            RateLimitMiddlewareError::ExtractionFailed(e) => {
                write!(f, "Key extraction failed: {e}")
            }
        }
    }
}

impl std::error::Error for RateLimitMiddlewareError {}

// Define header names as constants
const HEADER_LIMIT: HeaderName = HeaderName::from_static("x-ratelimit-limit");
const HEADER_REMAINING: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
const HEADER_RESET: HeaderName = HeaderName::from_static("x-ratelimit-reset");
const HEADER_RETRY_AFTER: HeaderName = HeaderName::from_static("retry-after");

#[derive(Debug, Clone)]
pub struct RateLimitOptions {
    pub include_headers: bool,
    pub fail_open: bool,
    pub key_prefix: Option<String>,
}

impl Default for RateLimitOptions {
    fn default() -> Self {
        Self {
            include_headers: true,
            fail_open: true,
            key_prefix: None,
        }
    }
}

#[derive(Clone)]
pub struct RateLimitLayer<K> {
    limiter: Arc<dyn RateLimiter>,
    key_extractor: Arc<K>,
    options: RateLimitOptions,
    metrics: Option<Arc<dyn sockudo_core::metrics::MetricsInterface + Send + Sync>>,
    config_name: String,
}

impl<K> RateLimitLayer<K>
where
    K: KeyExtractor + Clone + Send + Sync + 'static,
{
    pub fn new(limiter: Arc<dyn RateLimiter>, key_extractor: K) -> Self {
        Self::with_options(limiter, key_extractor, RateLimitOptions::default())
    }

    #[allow(dead_code)]
    pub fn with_options(
        limiter: Arc<dyn RateLimiter>,
        key_extractor: K,
        options: RateLimitOptions,
    ) -> Self {
        Self {
            limiter,
            key_extractor: Arc::new(key_extractor),
            options,
            metrics: None,
            config_name: "unknown".to_string(),
        }
    }

    pub fn with_config_name(mut self, config_name: String) -> Self {
        self.config_name = config_name;
        self
    }

    pub fn with_metrics(
        mut self,
        metrics: Arc<dyn sockudo_core::metrics::MetricsInterface + Send + Sync>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

impl<S, K> Layer<S> for RateLimitLayer<K>
where
    S: Clone + Send + 'static,
    S: Service<AxumRequest<AxumBody>, Response = AxumResponse>,
    S::Future: Send + 'static,
    K: KeyExtractor + Clone + Send + Sync + 'static,
{
    type Service = RateLimitService<S, K>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: self.limiter.clone(),
            key_extractor: self.key_extractor.clone(),
            options: self.options.clone(),
            metrics: self.metrics.clone(),
            config_name: self.config_name.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RateLimitService<S, K> {
    inner: S,
    limiter: Arc<dyn RateLimiter>,
    key_extractor: Arc<K>,
    options: RateLimitOptions,
    metrics: Option<Arc<dyn sockudo_core::metrics::MetricsInterface + Send + Sync>>,
    config_name: String,
}

impl<S, K> Service<AxumRequest<AxumBody>> for RateLimitService<S, K>
where
    S: Clone + Send + 'static,
    S: Service<AxumRequest<AxumBody>, Response = AxumResponse>,
    S::Future: Send + 'static,
    S::Error: IntoResponse + Send,
    K: KeyExtractor + Send + Sync + 'static,
{
    type Response = AxumResponse;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: AxumRequest<AxumBody>) -> Self::Future {
        let limiter = self.limiter.clone();
        let key_extractor = self.key_extractor.clone();
        let options = self.options.clone();
        let metrics = self.metrics.clone();
        let config_name = self.config_name.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let key = match key_extractor.extract(&req) {
                Ok(k) => k,
                Err(e) => {
                    error!(error = %e, "failed to extract key for rate limiting");
                    return Ok(internal_server_error_response_with_message(
                        "Key extraction failed for rate limiting.",
                    ));
                }
            };

            debug!("rate limit key extracted");

            let final_key = if let Some(prefix) = &options.key_prefix {
                format!("{prefix}:{key}")
            } else {
                key
            };
            debug!("final rate limit key computed");

            let primary_limiter_type = &config_name;

            let path = req.uri().path();
            let request_context = if path.starts_with("/app/") {
                "websocket_upgrade"
            } else if path.starts_with("/apps/") {
                "http_api"
            } else if path.starts_with("/up/") || path == "/live" {
                "health_check"
            } else {
                "other"
            };

            if let Some(ref metrics) = metrics {
                metrics.mark_rate_limit_check_with_context(
                    "global",
                    primary_limiter_type,
                    request_context,
                );
            }

            let rate_limit_result = match limiter.increment(&final_key).await {
                Ok(result) => result,
                Err(e) => {
                    error!(error = %e, "rate limiter backend error");
                    if options.fail_open {
                        warn!("rate limiter failed open");
                        RateLimitResult {
                            allowed: true,
                            remaining: 0,
                            reset_after: 0,
                            limit: 0,
                        }
                    } else {
                        error!("rate limiter failed closed");
                        return Ok(internal_server_error_response_with_message(
                            "Rate limiter backend unavailable.",
                        ));
                    }
                }
            };

            if !rate_limit_result.allowed {
                debug!(config_name, outcome = "rejected", "rate limit exceeded");

                if let Some(ref metrics) = metrics {
                    metrics.mark_rate_limit_triggered_with_context(
                        "global",
                        primary_limiter_type,
                        request_context,
                    );
                }

                return Ok(rate_limit_error_response(Some(&rate_limit_result)));
            }

            debug!(outcome = "allowed", "rate limit check passed");
            let result = inner.call(req).await;

            match result {
                Ok(mut response) => {
                    if options.include_headers && rate_limit_result.limit > 0 {
                        add_rate_limit_headers(response.headers_mut(), &rate_limit_result, false);
                    }
                    Ok(response)
                }
                Err(err) => Err(err),
            }
        })
    }
}

// --- Key Extractors ---

pub trait KeyExtractor: Send + Sync {
    fn extract<B>(&self, req: &HyperRequest<B>) -> Result<String, RateLimitMiddlewareError>;
}

#[derive(Clone, Debug)]
pub struct IpKeyExtractor {
    trust_hops: usize,
    warned_unusable_forwarded_for: Arc<AtomicBool>,
}

impl IpKeyExtractor {
    pub fn new(trust_hops: usize) -> Self {
        Self {
            trust_hops,
            warned_unusable_forwarded_for: Arc::new(AtomicBool::new(false)),
        }
    }

    fn get_ip<B>(&self, req: &HyperRequest<B>) -> Option<String> {
        if self.trust_hops > 0
            && let Some(value) = req.headers().get("x-forwarded-for")
            && let Ok(forwarded_str) = value.to_str()
        {
            let ips: Vec<&str> = forwarded_str.split(',').map(str::trim).collect();
            // trust_hops is an offset from the right-hand end of the chain, where 1 selects the
            // last entry. A single proxy that appends the client address therefore needs 2.
            let client_ip_index = ips.len().saturating_sub(self.trust_hops);
            if let Some(ip_str) = ips.get(client_ip_index)
                && ip_str.parse::<std::net::IpAddr>().is_ok()
            {
                return Some(ip_str.to_string());
            }
            self.warn_unusable_forwarded_for(forwarded_str, client_ip_index);
        }

        if let Some(value) = req.headers().get("x-real-ip")
            && let Ok(real_ip_str) = value.to_str()
        {
            let real_ip = real_ip_str.trim();
            if real_ip.parse::<std::net::IpAddr>().is_ok() {
                return Some(real_ip.to_string());
            }
        }

        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip().to_string())
    }

    /// Reports a configured `trust_hops` that does not match the actual proxy chain.
    ///
    /// Falling through to `x-real-ip` or the socket peer puts every client behind the same proxy
    /// into one rate-limit bucket, which is otherwise silent. Logged once per extractor: clients
    /// can set `X-Forwarded-For` themselves, so a per-request log would be a flooding primitive.
    fn warn_unusable_forwarded_for(&self, forwarded_for: &str, selected_index: usize) {
        if self
            .warned_unusable_forwarded_for
            .swap(true, Ordering::Relaxed)
        {
            return;
        }

        warn!(
            trust_hops = self.trust_hops,
            selected_index,
            forwarded_for,
            "no usable address at the configured trust_hops offset in X-Forwarded-For; falling \
             back to x-real-ip or the socket peer, which shares one rate-limit bucket across every \
             client behind the proxy"
        );
    }
}

impl Default for IpKeyExtractor {
    fn default() -> Self {
        Self::new(0)
    }
}

impl KeyExtractor for IpKeyExtractor {
    fn extract<B>(&self, req: &HyperRequest<B>) -> Result<String, RateLimitMiddlewareError> {
        Ok(self.get_ip(req).unwrap_or_else(|| {
            warn!("could not extract ip for rate limiting, using unknown_ip fallback");
            "unknown_ip".to_string()
        }))
    }
}

// --- Helper Functions ---

fn rate_limit_error_response(result: Option<&RateLimitResult>) -> AxumResponse {
    let mut response = axum::response::Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(AxumBody::from(
            sonic_rs::to_string(&json!({
                "error": "Too Many Requests",
                "message": "Rate limit exceeded. Please try again later.",
            }))
            .expect("Failed to serialize rate limit error response"),
        ))
        .expect("Failed to build rate limit error response");

    if let Some(res) = result {
        add_rate_limit_headers(response.headers_mut(), res, true);
    }
    response
}

fn internal_server_error_response_with_message(message: &str) -> AxumResponse {
    axum::response::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(AxumBody::from(
            sonic_rs::to_string(&json!({
                "error": "Internal Server Error",
                "message": message,
            }))
            .expect("Failed to serialize internal server error response"),
        ))
        .expect("Failed to build internal server error response")
}

fn add_rate_limit_headers(
    headers: &mut HeaderMap,
    result: &RateLimitResult,
    is_rate_limited: bool,
) {
    if let Ok(value) = HeaderValue::try_from(result.limit.to_string()) {
        headers.insert(HEADER_LIMIT, value);
    } else {
        warn!(
            value = result.limit,
            "Failed to convert rate limit limit value for header X-RateLimit-Limit"
        );
    }

    if let Ok(value) = HeaderValue::try_from(result.remaining.to_string()) {
        headers.insert(HEADER_REMAINING, value);
    } else {
        warn!(
            value = result.remaining,
            "Failed to convert rate limit remaining value for header X-RateLimit-Remaining"
        );
    }

    if let Ok(value) = HeaderValue::try_from(result.reset_after.to_string()) {
        headers.insert(HEADER_RESET, value.clone());
        if is_rate_limited {
            headers.insert(HEADER_RETRY_AFTER, value);
        }
    } else {
        warn!(
            value = result.reset_after,
            "Failed to convert rate limit reset_after value for header X-RateLimit-Reset/Retry-After"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(headers: &[(&str, &str)]) -> HyperRequest<()> {
        let mut builder = HyperRequest::builder();
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(()).unwrap()
    }

    #[test]
    fn trust_hops_counts_from_the_right_so_one_proxy_needs_two() {
        let chain = [("x-forwarded-for", "203.0.113.7, 198.51.100.1")];

        assert_eq!(
            IpKeyExtractor::new(2).get_ip(&request(&chain)),
            Some("203.0.113.7".to_string()),
            "2 selects the client for a single proxy that appends its own address"
        );
        assert_eq!(
            IpKeyExtractor::new(1).get_ip(&request(&chain)),
            Some("198.51.100.1".to_string()),
            "1 selects the last entry, which is the proxy rather than the client"
        );
    }

    #[test]
    fn client_supplied_entries_cannot_reach_the_selected_offset() {
        let ip = IpKeyExtractor::new(2).get_ip(&request(&[(
            "x-forwarded-for",
            "192.0.2.9, 203.0.113.7, 198.51.100.1",
        )]));

        assert_eq!(
            ip,
            Some("203.0.113.7".to_string()),
            "a prepended entry shifts the offset away from the client, it cannot be selected"
        );
    }

    #[test]
    fn zero_trust_hops_ignores_forwarded_for() {
        let ip = IpKeyExtractor::new(0).get_ip(&request(&[
            ("x-forwarded-for", "203.0.113.7, 198.51.100.1"),
            ("x-real-ip", "192.0.2.44"),
        ]));

        assert_eq!(ip, Some("192.0.2.44".to_string()));
    }

    #[test]
    fn unusable_entry_at_the_offset_falls_back_instead_of_picking_another() {
        let ip = IpKeyExtractor::new(2).get_ip(&request(&[
            ("x-forwarded-for", "not-an-ip, 198.51.100.1"),
            ("x-real-ip", "192.0.2.44"),
        ]));

        assert_eq!(ip, Some("192.0.2.44".to_string()));
    }

    #[test]
    fn a_shorter_chain_than_the_offset_selects_the_first_entry() {
        let ip = IpKeyExtractor::new(5).get_ip(&request(&[(
            "x-forwarded-for",
            "203.0.113.7, 198.51.100.1",
        )]));

        assert_eq!(
            ip,
            Some("203.0.113.7".to_string()),
            "saturating_sub clamps the offset to the start of the chain"
        );
    }
}
