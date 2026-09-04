//! Streamable HTTP transport: bearer authentication in front of rmcp's tower
//! service, mounted on an axum router.
//!
//! The auth middleware resolves the bearer token to a [`Principal`] and stores
//! it in the request extensions. rmcp copies `http::request::Parts` (including
//! those extensions) into every request context, so the handler recovers the
//! principal per request without any shared mutable state.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::auth::{AuthError, Authenticator, Principal};
use crate::handler::{McpCore, SockudoMcp};

/// HTTP transport tunables.
#[derive(Debug, Clone)]
pub struct HttpTransportConfig {
    /// Allowed `Host` authorities (DNS-rebinding protection). Empty disables
    /// the check; the default only allows loopback.
    pub allowed_hosts: Vec<String>,
    /// Allowed browser origins. Empty disables Origin validation.
    pub allowed_origins: Vec<String>,
    /// Maximum POST body size.
    pub max_body_bytes: usize,
    /// SSE keep-alive interval.
    pub sse_keep_alive: Option<Duration>,
    /// Idle session lifetime for legacy (session-based) protocol versions.
    pub session_keep_alive: Option<Duration>,
    /// Prefer plain JSON responses when no streaming is needed.
    pub json_response: bool,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            allowed_hosts: Vec::new(),
            allowed_origins: Vec::new(),
            max_body_bytes: 1024 * 1024,
            sse_keep_alive: Some(Duration::from_secs(15)),
            session_keep_alive: Some(Duration::from_secs(30 * 60)),
            json_response: true,
        }
    }
}

/// Alias for the rmcp service type used here.
pub type McpHttpService = StreamableHttpService<SockudoMcp, LocalSessionManager>;

/// Build the rmcp tower service (without authentication).
pub fn service(
    core: Arc<McpCore>,
    config: &HttpTransportConfig,
    ct: CancellationToken,
) -> McpHttpService {
    let mut session_manager = LocalSessionManager::default();
    session_manager.session_config.keep_alive = config.session_keep_alive;

    let mut http_config = StreamableHttpServerConfig::default()
        .with_sse_keep_alive(config.sse_keep_alive)
        .with_json_response(config.json_response)
        .with_max_request_body_bytes(config.max_body_bytes)
        .with_cancellation_token(ct)
        .with_allowed_origins(config.allowed_origins.iter().cloned());
    http_config = if config.allowed_hosts.is_empty() {
        http_config.disable_allowed_hosts()
    } else {
        http_config.with_allowed_hosts(config.allowed_hosts.iter().cloned())
    };

    StreamableHttpService::new(
        move || Ok(SockudoMcp::new(Arc::clone(&core))),
        Arc::new(session_manager),
        http_config,
    )
}

/// Axum router exposing MCP at `path` behind bearer authentication.
pub fn router(
    path: &str,
    core: Arc<McpCore>,
    authenticator: Arc<dyn Authenticator>,
    config: &HttpTransportConfig,
    ct: CancellationToken,
) -> Router {
    let mcp = service(core, config, ct);
    Router::new()
        .nest_service(path, mcp)
        .layer(middleware::from_fn_with_state(authenticator, bearer_auth))
}

fn bearer_credential(request: &Request) -> Option<&str> {
    let value = request.headers().get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
}

async fn bearer_auth(
    State(authenticator): State<Arc<dyn Authenticator>>,
    mut request: Request,
    next: Next,
) -> Response {
    match authenticator.authenticate(bearer_credential(&request)) {
        Ok(principal) => {
            request.extensions_mut().insert::<Principal>(principal);
            next.run(request).await
        }
        Err(error) => {
            warn!(error = %error, "mcp http authentication failed");
            unauthorized(&error)
        }
    }
}

fn unauthorized(error: &AuthError) -> Response {
    let body = serde_json::json!({
        "error": "unauthorized",
        "message": error.to_string(),
    });
    let mut response = (StatusCode::UNAUTHORIZED, Body::from(body.to_string())).into_response();
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static(r#"Bearer realm="sockudo-mcp""#),
    );
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}
