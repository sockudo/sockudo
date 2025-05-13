// src/rate_limiter/middleware.rs
use super::{RateLimitResult, RateLimiter}; // Assuming these are defined elsewhere
use axum::{
    body::Body,
    extract::ConnectInfo, // Correct import for connection info
    extract::Request,
    http::{HeaderName, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures_util::future::BoxFuture;
use serde_json::json;
use std::{
    fmt,
    net::SocketAddr, // Use SocketAddr directly
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower_layer::Layer;
use tower_service::Service;
use tracing::{debug, error, warn}; // Use tracing for logging

// Define a local error type for configuration issues
#[derive(Debug)]
pub enum RateLimitMiddlewareError {
    InvalidHeaderName(String),
    // Potentially other configuration errors
}

impl fmt::Display for RateLimitMiddlewareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RateLimitMiddlewareError::InvalidHeaderName(e) => {
                write!(f, "Invalid header name: {}", e)
            }
        }
    }
}

impl std::error::Error for RateLimitMiddlewareError {}

// Standard headers for rate limiting following RFC draft
static HEADER_LIMIT: &str = "X-RateLimit-Limit";
static HEADER_REMAINING: &str = "X-RateLimit-Remaining";
static HEADER_RESET: &str = "X-RateLimit-Reset";
static HEADER_RETRY_AFTER: &str = "Retry-After";

/// Options for rate limiting middleware
#[derive(Debug, Clone)]
pub struct RateLimitOptions {
    /// Whether to include rate limit headers in the response on success.
    /// Headers are always included on a 429 response.
    pub include_headers: bool,
    /// Whether to allow the request if the rate limiter backend fails.
    pub fail_open: bool,
    /// A prefix to add to the rate limit key (e.g., "ip:" or "user:").
    pub key_prefix: Option<String>,
}

impl Default for RateLimitOptions {
    fn default() -> Self {
        Self {
            include_headers: true,
            fail_open: true, // Defaulting to fail-open might be safer in many cases
            key_prefix: None,
        }
    }
}

/// Layer that applies rate limiting to requests.
#[derive(Clone)]
pub struct RateLimitLayer<K> {
    limiter: Arc<dyn RateLimiter>,
    key_extractor: Arc<K>,
    options: RateLimitOptions,
}

impl<K> RateLimitLayer<K>
where
    K: KeyExtractor + Clone + Send + Sync + 'static,
{
    pub fn new(limiter: Arc<dyn RateLimiter>, key_extractor: K) -> Self {
        Self {
            limiter,
            key_extractor: Arc::new(key_extractor),
            options: RateLimitOptions::default(),
        }
    }

    pub fn with_options(
        limiter: Arc<dyn RateLimiter>,
        key_extractor: K,
        options: RateLimitOptions,
    ) -> Self {
        Self {
            limiter,
            key_extractor: Arc::new(key_extractor),
            options,
        }
    }
}

impl<S, K> Layer<S> for RateLimitLayer<K>
where
    // No constraint on S::Error here anymore!
    // S: Service<Request<Body>> + Clone + Send + 'static, // Require Clone here
    S: Clone + Send + 'static,                      // Require Clone here
    S: Service<Request<Body>, Response = Response>, // Axum typically deals with Response directly
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
        }
    }
}

/// Service that applies rate limiting logic.
#[derive(Clone)]
pub struct RateLimitService<S, K> {
    inner: S,
    limiter: Arc<dyn RateLimiter>,
    key_extractor: Arc<K>,
    options: RateLimitOptions,
}

impl<S, K> Service<Request<Body>> for RateLimitService<S, K>
where
    // S must be Clone because we clone it in `call`
    // S: Service<Request<Body>> + Clone + Send + 'static,
    S: Clone + Send + 'static,
    S: Service<Request<Body>, Response = Response>, // Ensure the inner service produces a Response
    S::Future: Send + 'static,
    // We handle errors from the *inner* service by propagating them,
    // so its Error type needs to be compatible with how Axum handles errors.
    // Typically, this means S::Error: IntoResponse or using Axum's error handling.
    // We don't need an explicit bound here if S::Response = Response, as errors
    // should be handled *within* S or via Axum's error handling layers.
    // If S *can* return an error, it should implement IntoResponse.
    S::Error: IntoResponse + Send, // <<< Added this constraint for propagation
    K: KeyExtractor + Send + Sync + 'static,
{
    // The response is always Axum's Response type, whether success, rate limit error, or propagated inner error.
    type Response = Response;
    // The error type of *this* service layer. We aim to handle our own errors (rate limit)
    // and convert them into Ok(Response), propagating inner errors.
    type Error = S::Error; // Propagate the inner service's error type
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Delegate readiness check to the inner service
        self.inner.poll_ready(cx)
        // Note: If the *limiter itself* could be "not ready", we might need to check it here too.
        // However, typical limiters (Redis, in-memory) are generally always ready.
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // Clone the necessary components for the async block
        let limiter = self.limiter.clone();
        let key_extractor = self.key_extractor.clone();
        let options = self.options.clone();
        // Clone the inner service to satisfy the 'static lifetime and allow mutable borrow
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // 1. Extract the key
            let key = key_extractor.extract_key(&req);
            debug!(key = %key, "Extracted rate limit key");

            // 2. Apply prefix if configured
            let final_key = if let Some(prefix) = &options.key_prefix {
                format!("{}:{}", prefix, key)
            } else {
                key
            };
            debug!(final_key = %final_key, "Final rate limit key");

            // 3. Check the rate limiter
            let rate_limit_result = match limiter.increment(&final_key).await {
                Ok(result) => result,
                Err(e) => {
                    // Log the underlying limiter error
                    error!("Rate limiter backend error for key '{}': {}", final_key, e);
                    if options.fail_open {
                        // Allow request, but maybe add warning headers or log prominently
                        warn!(key = %final_key, "Rate limiter failed open");
                        // We still need some result to potentially add headers if options.include_headers is true
                        // Use dummy values indicating pass-through due to error.
                        RateLimitResult {
                            allowed: true,
                            remaining: 0, // Indicate unknown state
                            reset_after: 0,
                            limit: 0,
                        }
                    } else {
                        // Block the request with a generic server error response
                        error!(key = %final_key, "Rate limiter failed closed");
                        // Return a 500 Internal Server Error response
                        return Ok(internal_server_error_response());
                    }
                }
            };

            // 4. Handle Rate Limit Exceeded
            if !rate_limit_result.allowed {
                debug!(key = %final_key, "Rate limit exceeded");
                // Return the 429 Too Many Requests response
                // Headers are added by rate_limit_error_response
                return Ok(rate_limit_error_response(Some(&rate_limit_result)));
            }

            // 5. Rate Limit Check Passed - Call Inner Service
            debug!(key = %final_key, "Rate limit check passed");
            // Propagate the result (Ok<Response> or Err<S::Error>) from the inner service
            let result = inner.call(req).await; // Result<S::Response, S::Error> which is Result<Response, S::Error>

            match result {
                Ok(mut response) => {
                    // 6. Add Headers to Successful Response (if enabled)
                    if options.include_headers && rate_limit_result.limit > 0 {
                        // Only add headers if limiter worked
                        add_rate_limit_headers(response.headers_mut(), &rate_limit_result, false);
                    }
                    Ok(response)
                }
                Err(err) => {
                    // Propagate the inner service's error. Axum's error handling
                    // should convert it using its IntoResponse implementation.
                    Err(err)
                }
            }
        })
    }
}

// --- Key Extractors ---

/// Trait for extracting a string key from a request for rate limiting.
pub trait KeyExtractor: Send + Sync {
    /// Asynchronously extracts a key from the request.
    fn extract_key(&self, req: &Request<Body>) -> String;
}

/// Extracts the key from the client's IP address.
/// It prioritizes headers like `X-Forwarded-For` and `X-Real-IP`
/// before falling back to the connection's socket address.
/// **Note:** Ensure your reverse proxy setup correctly sets/validates these headers
/// and that you trust the proxy providing them.
#[derive(Clone, Debug)]
pub struct IpKeyExtractor {
    /// How many hops to trust in X-Forwarded-For. 0 means only trust X-Real-IP or socket addr.
    trust_hops: usize,
}

impl IpKeyExtractor {
    /// Create a new extractor, trusting only the direct connection or X-Real-IP.
    pub fn new() -> Self {
        Self { trust_hops: 0 }
    }

    /// Create a new extractor, trusting `hops` number of proxies in X-Forwarded-For.
    pub fn trusting(hops: usize) -> Self {
        Self { trust_hops: hops }
    }

    fn get_ip(&self, req: &Request<Body>) -> Option<String> {
        // 1. Check X-Forwarded-For if trust_hops > 0
        if self.trust_hops > 0 {
            if let Some(value) = req.headers().get("X-Forwarded-For") {
                if let Ok(forwarded_str) = value.to_str() {
                    let mut ips = forwarded_str.split(',');
                    // If trust_hops is 1, we want the last IP before the proxy (client ip)
                    // ips vector: [client, proxy1, proxy2] -> if trust_hops=1, take last. if trust_hops=2, take second last.
                    // Get the IP at index `count - trust_hops`.
                    let ip_list: Vec<&str> = ips.map(str::trim).collect();
                    let relevant_ip_index = ip_list.len().saturating_sub(self.trust_hops);
                    if let Some(ip) = ip_list.get(relevant_ip_index) {
                        // Basic validation: Check if it parses as an IP address
                        if ip.parse::<std::net::IpAddr>().is_ok() {
                            debug!(ip = %ip, header = "X-Forwarded-For", trust_hops = self.trust_hops, "Extracted IP");
                            return Some(ip.to_string());
                        } else {
                            warn!(ip = %ip, header = "X-Forwarded-For", "Invalid IP format found");
                        }
                    }
                }
            }
        }

        // 2. Check X-Real-IP (often set by proxies like Nginx)
        if let Some(value) = req.headers().get("X-Real-IP") {
            if let Ok(real_ip_str) = value.to_str() {
                let real_ip = real_ip_str.trim();
                if real_ip.parse::<std::net::IpAddr>().is_ok() {
                    debug!(ip = %real_ip, header = "X-Real-IP", "Extracted IP");
                    return Some(real_ip.to_string());
                } else {
                    warn!(ip = %real_ip, header = "X-Real-IP", "Invalid IP format found");
                }
            }
        }

        // 3. Fallback to connection info
        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|connect_info| {
                let ip = connect_info.0.ip().to_string();
                debug!(ip = %ip, source = "ConnectInfo", "Extracted IP");
                ip
            })
    }
}

impl Default for IpKeyExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyExtractor for IpKeyExtractor {
    // Change async fn to fn
    fn extract_key(&self, req: &Request<Body>) -> String {
        self.get_ip(req).unwrap_or_else(|| {
            warn!("Could not extract IP address for rate limiting, falling back to 'unknown'");
            "unknown".to_string()
        })
    }
}

/// Extracts the key from a specified request header.
#[derive(Clone, Debug)]
pub struct HeaderKeyExtractor {
    header_name: HeaderName,
}

impl HeaderKeyExtractor {
    /// Creates a new extractor for the given header name.
    /// Returns an error if the header name is invalid.
    pub fn new(header_name: &str) -> Result<Self, RateLimitMiddlewareError> {
        HeaderName::try_from(header_name)
            .map(|name| {
                debug!(header = %name, "Creating HeaderKeyExtractor");
                Self { header_name: name }
            })
            .map_err(|e| {
                error!("Invalid header name provided: {}", header_name);
                RateLimitMiddlewareError::InvalidHeaderName(e.to_string())
            })
    }
}

impl KeyExtractor for HeaderKeyExtractor {
    fn extract_key(&self, req: &Request<Body>) -> String {
        req.headers()
            .get(&self.header_name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()) // Convert &str to String
            .unwrap_or_else(|| {
                warn!(header = %self.header_name, "Header not found or invalid UTF-8, falling back to 'unknown'");
                "unknown".to_string()
            })
    }
}

/// Extracts the key from the request's URI path.
#[derive(Clone, Debug, Default)]
pub struct PathKeyExtractor;

impl KeyExtractor for PathKeyExtractor {
    fn extract_key(&self, req: &Request<Body>) -> String {
        let path = req.uri().path().to_string();
        debug!(path = %path, "Extracted path key");
        path
    }
}

/// Extracts the key using a provided synchronous function.
#[derive(Clone)]
pub struct FnKeyExtractor<F> {
    // Note: The function itself must be Clone
    extractor: F,
}

impl<F> FnKeyExtractor<F>
where
    F: Fn(&Request<Body>) -> String + Send + Sync + Clone + 'static, // Added Clone bound
{
    /// Creates a new extractor using the given function.
    pub fn new(extractor: F) -> Self {
        Self { extractor }
    }
}
impl<F> KeyExtractor for FnKeyExtractor<F>
where
    F: Fn(&Request<Body>) -> String + Send + Sync + Clone + 'static, // Added Clone bound
{
    fn extract_key(&self, req: &Request<Body>) -> String {
        // Since the function is sync, we just call it.
        (self.extractor)(req)
    }
}

// --- Helper Functions ---

/// Creates a `Response` for the `429 Too Many Requests` error.
fn rate_limit_error_response(result: Option<&RateLimitResult>) -> Response {
    let mut response = Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        // Using expect here because serialization of this simple map should never fail.
        .body(Body::from(
            serde_json::to_string(&json!({
                "error": "Too Many Requests",
                "message": "Rate limit exceeded. Please try again later.",
            }))
            .expect("Failed to serialize rate limit error response"),
        ))
        // Using expect as builder errors are unlikely with static values
        .expect("Failed to build rate limit error response");

    // Add rate limit headers regardless of options.include_headers for 429 responses
    if let Some(res) = result {
        add_rate_limit_headers(response.headers_mut(), res, true); // Indicate it's a rate-limited response
    }

    response
}

/// Creates a `Response` for the `500 Internal Server Error`.
fn internal_server_error_response() -> Response {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_string(&json!({
                "error": "Internal Server Error",
                "message": "The server encountered an internal error and was unable to complete your request.",
            }))
                .expect("Failed to serialize internal server error response"),
        ))
        .expect("Failed to build internal server error response")
}

/// Adds standard rate limit headers to the response headers.
fn add_rate_limit_headers(
    headers: &mut axum::http::HeaderMap,
    result: &RateLimitResult,
    is_rate_limited: bool, // Pass whether the request was actually limited
) {
    // Use HeaderValue::from if possible, fallback to string conversion if needed
    if let Ok(value) = result.limit.try_into() {
        headers.insert(HeaderName::from_static(HEADER_LIMIT), value);
    }
    if let Ok(value) = result.remaining.try_into() {
        headers.insert(HeaderName::from_static(HEADER_REMAINING), value);
    }

    // Handle Reset and potential Retry-After
    // Cannot clone HeaderValue, so convert the source value twice if needed.
    if let Ok(reset_value) = result.reset_after.try_into() {
        // Insert HEADER_RESET, moving the first converted value
        headers.insert(HeaderName::from_static(HEADER_RESET), reset_value);

        // Only add Retry-After if the request was actually rate limited (429)
        if is_rate_limited {
            // Convert the original value *again* for the second header.
            // This creates a new, independent HeaderValue.
            if let Ok(retry_value) = result.reset_after.try_into() {
                headers.insert(HeaderName::from_static(HEADER_RETRY_AFTER), retry_value);
            } else {
                // This case is unlikely if the first try_into succeeded, but handle defensively
                warn!(value = result.reset_after, "Failed to convert rate limit reset value for Retry-After header (second attempt)");
            }
        }
    } else {
        warn!(
            value = result.reset_after,
            "Failed to convert rate limit reset value for X-RateLimit-Reset header"
        );
    }
}

// --- Convenience Constructors ---

/// Creates rate limit middleware using IP-based key extraction.
/// Consider using `with_ip_limiter_trusting` if behind proxies.
pub fn with_ip_limiter(
    limiter: Arc<dyn RateLimiter>,
    options: RateLimitOptions,
) -> RateLimitLayer<IpKeyExtractor> {
    RateLimitLayer::with_options(limiter, IpKeyExtractor::new(), options)
}

/// Creates rate limit middleware using IP-based key extraction, trusting `hops` proxies.
pub fn with_ip_limiter_trusting(
    limiter: Arc<dyn RateLimiter>,
    trust_hops: usize,
    options: RateLimitOptions,
) -> RateLimitLayer<IpKeyExtractor> {
    RateLimitLayer::with_options(limiter, IpKeyExtractor::trusting(trust_hops), options)
}

/// Creates rate limit middleware using header-based key extraction.
pub fn with_header_limiter(
    limiter: Arc<dyn RateLimiter>,
    header_name: &str,
    options: RateLimitOptions,
) -> Result<RateLimitLayer<HeaderKeyExtractor>, RateLimitMiddlewareError> {
    let extractor = HeaderKeyExtractor::new(header_name)?;
    Ok(RateLimitLayer::with_options(limiter, extractor, options))
}

/// Creates rate limit middleware using path-based key extraction.
pub fn with_path_limiter(
    limiter: Arc<dyn RateLimiter>,
    options: RateLimitOptions,
) -> RateLimitLayer<PathKeyExtractor> {
    RateLimitLayer::with_options(limiter, PathKeyExtractor, options)
}

/// Creates rate limit middleware using a custom key extraction function.
pub fn with_fn_limiter<F>(
    limiter: Arc<dyn RateLimiter>,
    extractor: F,
    options: RateLimitOptions,
) -> RateLimitLayer<FnKeyExtractor<F>>
where
    F: Fn(&Request<Body>) -> String + Send + Sync + Clone + 'static, // Extractor fn needs Clone
{
    RateLimitLayer::with_options(limiter, FnKeyExtractor::new(extractor), options)
}

// --- Placeholder RateLimiter trait and Result ---
// You would have your actual implementation (e.g., Redis, in-memory)
// #[async_trait::async_trait]
// pub trait RateLimiter: Send + Sync {
//     async fn increment(&self, key: &str) -> Result<RateLimitResult, Box<dyn std::error::Error + Send + Sync>>;
// }
//
// #[derive(Debug, Clone)]
// pub struct RateLimitResult {
//     pub allowed: bool,
//     pub remaining: u64,
//     pub reset_after: u64, // Seconds until reset
//     pub limit: u64,       // The limit for the window
// }
