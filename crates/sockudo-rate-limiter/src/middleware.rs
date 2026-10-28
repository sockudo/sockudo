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
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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
    warned_invalid_forwarding: Arc<AtomicBool>,
}

impl IpKeyExtractor {
    pub fn new(trust_hops: usize) -> Self {
        Self {
            trust_hops,
            warned_invalid_forwarding: Arc::new(AtomicBool::new(false)),
        }
    }

    fn get_ip<B>(&self, req: &HyperRequest<B>) -> Option<String> {
        // A zero offset means direct exposure: neither forwarding header is trusted.
        if self.trust_hops > 0 {
            let forwarded = req.headers().get_all("x-forwarded-for");
            let mut values = forwarded.iter();
            if let Some(value) = values.next() {
                // Do not silently select a different chain when duplicate fields disagree.
                if values.next().is_some() {
                    self.warn_invalid_forwarding(0, "multiple_headers");
                } else if let Ok(chain) = value.to_str() {
                    let entry_count = chain.split(',').count();
                    if let Some(index) = entry_count.checked_sub(self.trust_hops) {
                        if let Some(ip) = chain
                            .split(',')
                            .nth(index)
                            .and_then(|entry| entry.trim().parse::<IpAddr>().ok())
                        {
                            return Some(ip.to_string());
                        }
                        self.warn_invalid_forwarding(entry_count, "invalid_selected_address");
                    } else {
                        self.warn_invalid_forwarding(entry_count, "short_chain");
                    }
                } else {
                    self.warn_invalid_forwarding(0, "invalid_header");
                }
                // An unusable XFF chain must not fall through to another supplied identity.
            } else if let Some(value) = req.headers().get("x-real-ip") {
                if req.headers().get_all("x-real-ip").iter().count() == 1
                    && let Ok(value) = value.to_str()
                    && let Ok(ip) = value.trim().parse::<IpAddr>()
                {
                    return Some(ip.to_string());
                }
                self.warn_invalid_forwarding(0, "invalid_real_ip");
            }
        }

        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip().to_string())
    }

    fn warn_invalid_forwarding(&self, entry_count: usize, reason: &'static str) {
        if !self.warned_invalid_forwarding.swap(true, Ordering::Relaxed) {
            warn!(
                trust_hops = self.trust_hops,
                entry_count, reason, "forwarded client ip unavailable, using socket peer"
            );
        }
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
        let mut request = HyperRequest::builder().body(()).unwrap();
        for (name, value) in headers {
            request.headers_mut().append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        request.extensions_mut().insert(ConnectInfo(
            "198.51.100.20:1234".parse::<SocketAddr>().unwrap(),
        ));
        request
    }

    // `tracing` caches callsite interest process-wide, so a sibling test that
    // emits this `warn!` from another thread can leave the callsite cached as
    // disabled and drop the event the capture test expects to see. Serialize
    // every test in this module that reaches that callsite.
    static WARN_CALLSITE: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn warn_callsite() -> std::sync::MutexGuard<'static, ()> {
        WARN_CALLSITE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn forwarded_address_selection_respects_the_configured_proxy_shape() {
        let _guard = warn_callsite();
        for (hops, chain, expected) in [
            (1, "203.0.113.7", "203.0.113.7"),
            (1, "192.0.2.99, 203.0.113.7", "203.0.113.7"),
            (2, "203.0.113.7, 198.51.100.1", "203.0.113.7"),
            (2, "192.0.2.99, 203.0.113.7, 198.51.100.1", "203.0.113.7"),
            (1, " 2001:0db8:0:0:0:0:0:1 ", "2001:db8::1"),
        ] {
            assert_eq!(
                IpKeyExtractor::new(hops)
                    .extract(&request(&[("x-forwarded-for", chain)]))
                    .unwrap(),
                expected
            );
        }
    }

    #[test]
    fn zero_trust_ignores_both_forwarding_headers() {
        assert_eq!(
            IpKeyExtractor::default()
                .extract(&request(&[
                    ("x-forwarded-for", "192.0.2.99"),
                    ("x-real-ip", "192.0.2.98"),
                ]))
                .unwrap(),
            "198.51.100.20"
        );
    }

    #[test]
    fn malformed_or_short_chains_fall_back_to_socket_not_real_ip() {
        let _guard = warn_callsite();
        for (hops, chain) in [
            (3, "192.0.2.99"),
            (2, "invalid, 203.0.113.7"),
            (1, ""),
            (1, "203.0.113.7,"),
        ] {
            assert_eq!(
                IpKeyExtractor::new(hops)
                    .extract(&request(&[
                        ("x-forwarded-for", chain),
                        ("x-real-ip", "192.0.2.98"),
                    ]))
                    .unwrap(),
                "198.51.100.20"
            );
        }
    }

    #[test]
    fn explicit_proxy_trust_allows_real_ip_only_without_forwarded_for() {
        let _guard = warn_callsite();
        assert_eq!(
            IpKeyExtractor::new(1)
                .extract(&request(&[("x-real-ip", "2001:0db8::1")]))
                .unwrap(),
            "2001:db8::1"
        );
    }

    #[test]
    fn duplicate_forwarding_fields_fall_back_to_socket() {
        let _guard = warn_callsite();
        for name in ["x-forwarded-for", "x-real-ip"] {
            assert_eq!(
                IpKeyExtractor::new(1)
                    .extract(&request(&[(name, "192.0.2.99"), (name, "203.0.113.7")]))
                    .unwrap(),
                "198.51.100.20"
            );
        }
    }

    #[test]
    fn non_text_forwarding_header_falls_back_to_socket() {
        let _guard = warn_callsite();
        let mut req = request(&[("x-real-ip", "192.0.2.98")]);
        req.headers_mut()
            .insert("x-forwarded-for", HeaderValue::from_bytes(&[0xff]).unwrap());
        assert_eq!(
            IpKeyExtractor::new(1).extract(&req).unwrap(),
            "198.51.100.20"
        );
    }

    #[test]
    fn absent_forwarding_headers_use_socket_peer() {
        assert_eq!(
            IpKeyExtractor::new(1).extract(&request(&[])).unwrap(),
            "198.51.100.20"
        );
    }

    #[test]
    fn invalid_forwarding_warning_is_shared_and_does_not_log_header_contents() {
        let _guard = warn_callsite();
        #[derive(Clone)]
        struct Writer(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = Writer(output.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let extractor = IpKeyExtractor::new(1);
            let req = request(&[("x-forwarded-for", "private-header-content")]);
            extractor.extract(&req).unwrap();
            extractor.clone().extract(&req).unwrap();
        });
        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert_eq!(output.matches("forwarded client ip unavailable").count(), 1);
        assert!(output.contains("invalid_selected_address"));
        assert!(!output.contains("private-header-content"));
    }
}
