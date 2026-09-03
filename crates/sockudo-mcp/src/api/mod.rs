//! Signed client for the Sockudo HTTP API contract over a pluggable transport.
//!
//! Tool handlers describe *what* to call as an [`ApiRequest`] (typed
//! [`Endpoint`] + query + JSON body). [`SockudoApi`] resolves credentials,
//! signs the request exactly like a server SDK would, and hands it to an
//! [`ApiTransport`]. Because the contract is the public HTTP API, tool results
//! are the documented response shapes, byte for byte.

pub mod credentials;
pub mod endpoint;
#[cfg(feature = "remote")]
pub mod remote;
pub mod signing;

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{ACCEPT, CONTENT_TYPE, HOST};
use http::{HeaderName, HeaderValue, StatusCode};
use serde::Serialize;

pub use credentials::{AppCredentials, AppSummary, CredentialSource, StaticCredentials};
pub use endpoint::Endpoint;
#[cfg(feature = "remote")]
pub use remote::RemoteTransport;

/// Default per-request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Failures raised by the API layer itself (not HTTP error statuses, which are
/// returned as [`ApiResponse`] so the agent can read the server's message).
#[derive(Debug, Clone, thiserror::Error)]
pub enum ApiError {
    /// The app id is not known to the credential source.
    #[error("app '{0}' is not known to this MCP server")]
    UnknownApp(String),
    /// The route is app-scoped but no app id was provided.
    #[error("route requires an app id")]
    MissingAppId,
    /// The transport failed before a response arrived.
    #[error("transport failure: {0}")]
    Transport(String),
    /// The request exceeded the configured timeout.
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    /// The request could not be constructed.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// Anything else.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Delivers an already-signed HTTP request to Sockudo.
#[async_trait]
pub trait ApiTransport: Send + Sync + 'static {
    /// Send one request and return the full response.
    async fn send(&self, request: http::Request<Bytes>) -> Result<http::Response<Bytes>, ApiError>;

    /// Short label for logs and the `sockudo://server/info` resource.
    fn kind(&self) -> &'static str;
}

/// A request against the Sockudo HTTP API, before signing.
#[derive(Debug, Clone)]
pub struct ApiRequest {
    /// Route.
    pub endpoint: Endpoint,
    /// Query parameters with decoded values.
    pub query: Vec<(Cow<'static, str>, String)>,
    /// Optional JSON body.
    pub body: Option<Bytes>,
    /// Extra headers (for example push capability headers).
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

impl ApiRequest {
    /// Start a request for `endpoint`.
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            query: Vec::new(),
            body: None,
            headers: Vec::new(),
        }
    }

    /// Add a query parameter.
    #[must_use]
    pub fn query(mut self, key: impl Into<Cow<'static, str>>, value: impl Into<String>) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    /// Add a query parameter when `value` is `Some`.
    #[must_use]
    pub fn query_opt<V: ToString>(
        mut self,
        key: impl Into<Cow<'static, str>>,
        value: Option<V>,
    ) -> Self {
        if let Some(value) = value {
            self.query.push((key.into(), value.to_string()));
        }
        self
    }

    /// Attach a JSON body.
    pub fn json<T: Serialize + ?Sized>(mut self, value: &T) -> Result<Self, ApiError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|error| ApiError::InvalidRequest(format!("cannot encode body: {error}")))?;
        self.body = Some(Bytes::from(bytes));
        Ok(self)
    }

    /// Attach a pre-encoded JSON body.
    #[must_use]
    pub fn raw_body(mut self, body: Bytes) -> Self {
        self.body = Some(body);
        self
    }

    /// Add a header.
    #[must_use]
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.push((name, value));
        self
    }
}

/// Response from the Sockudo HTTP API.
#[derive(Clone)]
pub struct ApiResponse {
    /// HTTP status.
    pub status: StatusCode,
    /// Raw body (JSON for every API route).
    pub body: Bytes,
    /// `Content-Type` header, if present.
    pub content_type: Option<HeaderValue>,
}

impl ApiResponse {
    /// `2xx`.
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Body as UTF-8 text (lossy).
    pub fn text(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    /// Body parsed as JSON when it is valid JSON.
    pub fn json(&self) -> Option<serde_json::Value> {
        if self.body.is_empty() {
            return None;
        }
        serde_json::from_slice(&self.body).ok()
    }
}

impl fmt::Debug for ApiResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiResponse")
            .field("status", &self.status)
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

/// Signed Sockudo API client.
pub struct SockudoApi {
    transport: Arc<dyn ApiTransport>,
    credentials: Arc<dyn CredentialSource>,
    timeout: Duration,
    host: HeaderValue,
}

impl fmt::Debug for SockudoApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SockudoApi")
            .field("transport", &self.transport.kind())
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl SockudoApi {
    /// Build a client.
    pub fn new(transport: Arc<dyn ApiTransport>, credentials: Arc<dyn CredentialSource>) -> Self {
        Self {
            transport,
            credentials,
            timeout: DEFAULT_TIMEOUT,
            host: HeaderValue::from_static("sockudo-mcp.internal"),
        }
    }

    /// Per-request timeout (default 30s).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// `Host` header sent with in-process requests. Remote transports replace
    /// it with the real authority.
    #[must_use]
    pub fn with_host(mut self, host: HeaderValue) -> Self {
        self.host = host;
        self
    }

    /// Credential source (apps, policies).
    pub fn credentials(&self) -> &Arc<dyn CredentialSource> {
        &self.credentials
    }

    /// Transport label.
    pub fn transport_kind(&self) -> &'static str {
        self.transport.kind()
    }

    /// Configured timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Sign (when required) and send a request.
    pub async fn call(&self, request: ApiRequest) -> Result<ApiResponse, ApiError> {
        let method = request.endpoint.method();
        let path = request.endpoint.path();
        let body_bytes = request.body.clone().unwrap_or_default();

        let query = if request.endpoint.requires_signature() {
            let app_id = request.endpoint.app_id().ok_or(ApiError::MissingAppId)?;
            let credentials = self
                .credentials
                .resolve(app_id)
                .await?
                .ok_or_else(|| ApiError::UnknownApp(app_id.to_string()))?;
            signing::signed_query(
                &credentials,
                &method,
                &path,
                &request.query,
                request.body.as_deref(),
                unix_timestamp(),
            )?
        } else {
            serde_urlencoded::to_string(&request.query).map_err(|error| {
                ApiError::InvalidRequest(format!("cannot encode query: {error}"))
            })?
        };

        let uri = if query.is_empty() {
            path
        } else {
            format!("{path}?{query}")
        };

        let mut builder = http::Request::builder()
            .method(method)
            .uri(uri)
            .header(HOST, self.host.clone())
            .header(ACCEPT, HeaderValue::from_static("application/json"));
        if request.body.is_some() {
            builder = builder.header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let http_request = builder
            .body(body_bytes)
            .map_err(|error| ApiError::InvalidRequest(error.to_string()))?;

        let response = tokio::time::timeout(self.timeout, self.transport.send(http_request))
            .await
            .map_err(|_| ApiError::Timeout(self.timeout))??;

        let (parts, body) = response.into_parts();
        Ok(ApiResponse {
            status: parts.status,
            content_type: parts.headers.get(CONTENT_TYPE).cloned(),
            body,
        })
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod test_support {
    //! In-memory transport that records requests and replays canned responses.

    use std::sync::Mutex;

    use super::*;

    #[derive(Debug, Default)]
    pub struct RecordingTransport {
        pub requests: Mutex<Vec<http::Request<Bytes>>>,
        pub responses: Mutex<Vec<(StatusCode, &'static str)>>,
    }

    impl RecordingTransport {
        pub fn with_response(status: StatusCode, body: &'static str) -> Arc<Self> {
            let transport = Self::default();
            transport.responses.lock().unwrap().push((status, body));
            Arc::new(transport)
        }

        pub fn last_request(&self) -> http::Request<Bytes> {
            let requests = self.requests.lock().unwrap();
            let last = requests.last().expect("a request was recorded");
            let mut clone = http::Request::builder()
                .method(last.method().clone())
                .uri(last.uri().clone())
                .body(last.body().clone())
                .unwrap();
            *clone.headers_mut() = last.headers().clone();
            clone
        }
    }

    #[async_trait]
    impl ApiTransport for RecordingTransport {
        async fn send(
            &self,
            request: http::Request<Bytes>,
        ) -> Result<http::Response<Bytes>, ApiError> {
            self.requests.lock().unwrap().push(request);
            let (status, body) = {
                let mut responses = self.responses.lock().unwrap();
                if responses.len() > 1 {
                    responses.remove(0)
                } else {
                    responses
                        .first()
                        .copied()
                        .unwrap_or((StatusCode::OK, r#"{"ok":true}"#))
                }
            };
            let mut response = http::Response::new(Bytes::from_static(body.as_bytes()));
            *response.status_mut() = status;
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            Ok(response)
        }

        fn kind(&self) -> &'static str {
            "recording"
        }
    }

    pub fn test_api(transport: Arc<RecordingTransport>) -> SockudoApi {
        let credentials =
            StaticCredentials::new(vec![AppCredentials::new("app-1", "key1", "secret1")]);
        SockudoApi::new(transport, Arc::new(credentials))
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn signs_app_scoped_requests() {
        let transport = RecordingTransport::with_response(StatusCode::OK, r#"{"channels":{}}"#);
        let api = test_api(transport.clone());
        let response = api
            .call(
                ApiRequest::new(Endpoint::Channels {
                    app_id: "app-1".into(),
                })
                .query("filter_by_prefix", "presence-"),
            )
            .await
            .unwrap();
        assert!(response.is_success());
        let request = transport.last_request();
        let query = request.uri().query().unwrap();
        assert!(query.contains("auth_signature="));
        assert!(query.contains("auth_key=key1"));
        assert!(query.contains("filter_by_prefix=presence-"));
        assert_eq!(request.uri().path(), "/apps/app-1/channels");
        assert_eq!(request.headers().get(HOST).unwrap(), "sockudo-mcp.internal");
    }

    #[tokio::test]
    async fn unsigned_routes_have_no_auth_parameters() {
        let transport = RecordingTransport::with_response(StatusCode::OK, "{}");
        let api = test_api(transport.clone());
        api.call(ApiRequest::new(Endpoint::Up)).await.unwrap();
        let request = transport.last_request();
        assert_eq!(request.uri().path(), "/up");
        assert!(request.uri().query().is_none());
    }

    #[tokio::test]
    async fn unknown_app_is_rejected_before_sending() {
        let transport = RecordingTransport::with_response(StatusCode::OK, "{}");
        let api = test_api(transport.clone());
        let error = api
            .call(ApiRequest::new(Endpoint::Channels {
                app_id: "nope".into(),
            }))
            .await
            .unwrap_err();
        assert!(matches!(error, ApiError::UnknownApp(app) if app == "nope"));
        assert!(transport.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn post_bodies_are_md5_signed_and_json_typed() {
        let transport = RecordingTransport::with_response(StatusCode::OK, r#"{"ok":true}"#);
        let api = test_api(transport.clone());
        let request = ApiRequest::new(Endpoint::Events {
            app_id: "app-1".into(),
        })
        .json(&serde_json::json!({"name": "e", "channel": "c", "data": "{}"}))
        .unwrap();
        api.call(request).await.unwrap();
        let sent = transport.last_request();
        assert_eq!(sent.method(), http::Method::POST);
        assert!(sent.uri().query().unwrap().contains("body_md5="));
        assert_eq!(
            sent.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert!(!sent.body().is_empty());
    }
}
