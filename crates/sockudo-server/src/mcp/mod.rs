//! Embedded Model Context Protocol server.
//!
//! The MCP tools speak the public Sockudo HTTP API contract. Instead of
//! re-implementing every handler, the in-process transport signs requests with
//! the app credentials from the `AppManager` and drives the server's own axum
//! router through `tower::ServiceExt::oneshot`. Every validation, idempotency
//! claim, metric, feature gate, and role restriction that applies to external
//! callers therefore applies to agents too, with no network hop.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::ConnectInfo;
use serde_json::{Value, json};
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::{App, AppManager};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{McpConfig, McpTokenConfig};
use sockudo_mcp::api::{ApiError, ApiTransport, AppCredentials, AppSummary, CredentialSource};
use sockudo_mcp::auth::{
    AppAccess, Authenticator, Principal, Scope, ScopeSet, StaticAuthenticator, TokenAuthenticator,
};
use sockudo_mcp::transport::http::HttpTransportConfig;
use sockudo_mcp::{McpCore, McpCoreConfig, ServerIntrospection, SockudoApi, ToolError};
use sockudo_rate_limiter::memory_limiter::MemoryRateLimiter;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tracing::{info, warn};

use crate::bootstrap::SockudoServer;

/// Upper bound on API response bodies buffered for a tool result.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Built MCP surface, ready to mount or serve.
pub(crate) struct McpRuntime {
    /// Router serving MCP at `config.mcp.path` behind bearer auth.
    pub(crate) router: Router,
    /// `(host, port)` when MCP gets its own listener.
    pub(crate) dedicated: Option<(String, u16)>,
    /// Cancels sessions and SSE streams on shutdown.
    #[allow(dead_code)]
    pub(crate) cancellation: CancellationToken,
}

/// Drives the server's own API router without a socket.
struct InProcessTransport {
    router: Router,
}

#[async_trait]
impl ApiTransport for InProcessTransport {
    async fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> std::result::Result<http::Response<Bytes>, ApiError> {
        let (parts, body) = request.into_parts();
        let mut request = http::Request::from_parts(parts, Body::from(body));
        // Rate-limit and logging layers key on the peer address; loopback keeps
        // in-process calls in one bucket and out of any client's bucket.
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .map_err(|never| match never {})?;
        let (parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, MAX_RESPONSE_BYTES)
            .await
            .map_err(|error| ApiError::Transport(format!("cannot read response body: {error}")))?;
        Ok(http::Response::from_parts(parts, bytes))
    }

    fn kind(&self) -> &'static str {
        "in-process"
    }
}

/// Resolves credentials and app metadata from the live `AppManager`.
struct AppManagerCredentials {
    app_manager: Arc<dyn AppManager + Send + Sync>,
}

fn summarize(app: &App) -> AppSummary {
    let mut policy = serde_json::to_value(&app.policy).unwrap_or(Value::Null);
    // Webhook headers can carry bearer tokens for downstream systems.
    if let Some(webhooks) = policy.get_mut("webhooks").and_then(Value::as_array_mut) {
        for webhook in webhooks {
            if let Some(object) = webhook.as_object_mut()
                && object.contains_key("headers")
            {
                object.insert(
                    "headers".to_string(),
                    Value::String("<redacted>".to_string()),
                );
            }
        }
    }
    AppSummary {
        id: app.id.clone(),
        key: app.key.clone(),
        enabled: app.enabled,
        policy: Some(policy),
    }
}

#[async_trait]
impl CredentialSource for AppManagerCredentials {
    async fn resolve(&self, app_id: &str) -> std::result::Result<Option<AppCredentials>, ApiError> {
        let app = self
            .app_manager
            .find_by_id(app_id)
            .await
            .map_err(|error| ApiError::Internal(format!("app lookup failed: {error}")))?;
        Ok(app.map(|app| AppCredentials::new(app.id, app.key, app.secret)))
    }

    async fn list_apps(&self) -> std::result::Result<Vec<AppSummary>, ApiError> {
        let apps = self
            .app_manager
            .get_apps()
            .await
            .map_err(|error| ApiError::Internal(format!("app listing failed: {error}")))?;
        let mut summaries: Vec<AppSummary> = apps.iter().map(summarize).collect();
        summaries.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(summaries)
    }

    async fn describe_app(
        &self,
        app_id: &str,
    ) -> std::result::Result<Option<AppSummary>, ApiError> {
        let app = self
            .app_manager
            .find_by_id(app_id)
            .await
            .map_err(|error| ApiError::Internal(format!("app lookup failed: {error}")))?;
        Ok(app.as_ref().map(summarize))
    }
}

/// Non-secret facts about this node for `sockudo_server_info`.
struct NodeIntrospection {
    handler: Arc<ConnectionHandler>,
}

#[async_trait]
impl ServerIntrospection for NodeIntrospection {
    fn describe(&self) -> Value {
        let options = self.handler.server_options();
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "mode": "embedded",
            "server_role": serde_json::to_value(options.server_role).unwrap_or(Value::Null),
            "adapter_driver": format!("{:?}", options.adapter.driver).to_lowercase(),
            "features": {
                "v2": cfg!(feature = "v2"),
                "delta": cfg!(feature = "delta"),
                "tag_filtering": cfg!(feature = "tag-filtering"),
                "recovery": cfg!(feature = "recovery"),
                "versioned_messages": cfg!(feature = "versioned-messages"),
                "ai_transport": cfg!(feature = "ai-transport"),
                "ably_compat": cfg!(feature = "ably-compat"),
                "push": cfg!(feature = "push"),
            },
            "runtime": {
                "history_enabled": options.history.enabled,
                "versioned_messages_enabled": options.versioned_messages.enabled,
                "annotations_enabled": options.annotations.enabled,
                "presence_history_enabled": options.presence_history.enabled,
                "ai_transport_enabled": options.ai_transport.enabled,
                "connection_recovery_enabled": options.connection_recovery.enabled,
                "idempotency_enabled": options.idempotency.enabled,
                "metrics_enabled": options.metrics.enabled,
                "usage_endpoints_enabled": options.http_api.usage_enabled,
                "accepting_connections": self.handler.is_accepting(),
            },
        })
    }

    async fn metrics_text(&self) -> std::result::Result<Option<String>, ToolError> {
        match self.handler.metrics() {
            Some(metrics) => Ok(Some(metrics.get_metrics_as_plaintext().await)),
            None => Ok(None),
        }
    }
}

fn scopes_from_config(scopes: &[String]) -> ScopeSet {
    ScopeSet::from_scopes(scopes.iter().filter_map(|scope| Scope::parse(scope)))
}

fn principal_from_token(token: &McpTokenConfig) -> Principal {
    Principal::new(
        token.name.trim().to_string(),
        scopes_from_config(&token.scopes),
        AppAccess::from_list(token.apps.iter().cloned()),
    )
}

fn build_authenticator(config: &McpConfig) -> Arc<dyn Authenticator> {
    if config.tokens.is_empty() {
        warn!(
            scopes = ?config.anonymous_scopes,
            "mcp.allow_anonymous is set: every caller gets the anonymous principal (development only)"
        );
        return Arc::new(StaticAuthenticator(Principal::new(
            "anonymous",
            scopes_from_config(&config.anonymous_scopes),
            AppAccess::All,
        )));
    }
    if config.allow_anonymous {
        warn!("mcp.allow_anonymous is ignored because [[mcp.tokens]] are configured");
    }
    Arc::new(TokenAuthenticator::new(
        config
            .tokens
            .iter()
            .map(|token| (token.token.clone(), principal_from_token(token))),
    ))
}

impl SockudoServer {
    /// Build the MCP surface from the finished API router. Returns `None`
    /// when `[mcp]` is disabled.
    pub(crate) fn build_mcp(&self, api_router: &Router) -> Result<Option<McpRuntime>> {
        let config = &self.config.mcp;
        if !config.enabled {
            return Ok(None);
        }
        if config.tokens.is_empty() && !config.allow_anonymous {
            return Err(Error::Configuration(
                "mcp requires [[mcp.tokens]] or mcp.allow_anonymous".to_string(),
            ));
        }

        let transport = InProcessTransport {
            router: api_router.clone(),
        };
        let credentials = AppManagerCredentials {
            app_manager: self.handler.app_manager().clone(),
        };
        let api = SockudoApi::new(Arc::new(transport), Arc::new(credentials))
            .with_timeout(Duration::from_millis(config.request_timeout_ms));

        let mut core = McpCore::new(
            api,
            McpCoreConfig {
                disabled_tools: config
                    .disabled_tools
                    .iter()
                    .map(|tool| tool.trim().to_string())
                    .filter(|tool| !tool.is_empty())
                    .collect(),
                instructions: config.instructions.clone(),
                ..McpCoreConfig::default()
            },
        )
        .with_introspection(Arc::new(NodeIntrospection {
            handler: Arc::clone(&self.handler),
        }));
        if config.rate_limit_per_minute > 0 {
            core = core.with_rate_limiter(Arc::new(MemoryRateLimiter::new(
                config.rate_limit_per_minute,
                60,
            )));
        }
        if let Some(metrics) = self.handler.metrics() {
            core = core.with_metrics(Arc::clone(metrics));
        }

        let http = HttpTransportConfig {
            allowed_hosts: config.allowed_hosts.clone(),
            allowed_origins: config.allowed_origins.clone(),
            max_body_bytes: config.max_body_bytes,
            session_keep_alive: Some(Duration::from_secs(config.session_ttl_seconds)),
            ..HttpTransportConfig::default()
        };
        let cancellation = CancellationToken::new();
        let router = sockudo_mcp::transport::http::router(
            config.path.trim(),
            Arc::new(core),
            build_authenticator(config),
            &http,
            cancellation.clone(),
        );
        info!(
            path = %config.path,
            tokens = config.tokens.len(),
            rate_limit_per_minute = config.rate_limit_per_minute,
            dedicated_port = ?config.port,
            "mcp surface built"
        );
        Ok(Some(McpRuntime {
            router,
            dedicated: config.port.map(|port| {
                (
                    config
                        .host
                        .clone()
                        .unwrap_or_else(|| self.config.host.clone()),
                    port,
                )
            }),
            cancellation,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_handler::test_support::{test_app, test_realtime_handler_harness};
    use crate::http_handler::{channels, events};
    use crate::middleware::pusher_api_auth_middleware;
    use axum::routing::{get, post};
    use serde_json::Map;
    use sockudo_app::memory_app_manager::MemoryAppManager;

    /// Minimal API router with the real auth middleware, mirroring how
    /// `configure_http_routes` wires the publish and channel routes.
    fn api_router(handler: Arc<ConnectionHandler>) -> Router {
        let auth = |handler: &Arc<ConnectionHandler>| {
            axum::middleware::from_fn_with_state(handler.clone(), pusher_api_auth_middleware)
        };
        let router = Router::new()
            .route(
                "/apps/{appId}/events",
                post(events).route_layer(auth(&handler)),
            )
            .route(
                "/apps/{appId}/channels",
                get(channels).route_layer(auth(&handler)),
            );
        #[cfg(feature = "push")]
        let router = router
            .layer(crate::http_handler::test_support::test_push_queue())
            .layer(crate::http_handler::test_support::test_push_store())
            .layer(crate::http_handler::test_support::test_push_admission());
        router.with_state(handler)
    }

    async fn core_for_test() -> (Arc<McpCore>, Arc<MemoryAppManager>) {
        let (handler, app_manager) = test_realtime_handler_harness();
        app_manager.create_app(test_app()).await.unwrap();
        let router = api_router(handler.clone());
        let api = SockudoApi::new(
            Arc::new(InProcessTransport { router }),
            Arc::new(AppManagerCredentials {
                app_manager: app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
            }),
        );
        let core = McpCore::new(api, McpCoreConfig::default())
            .with_introspection(Arc::new(NodeIntrospection { handler }));
        (Arc::new(core), app_manager)
    }

    fn args(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[tokio::test]
    async fn in_process_transport_signs_and_publishes_through_real_middleware() {
        let (core, _apps) = core_for_test().await;
        let principal = Principal::local(ScopeSet::READ_WRITE);
        let app_id = test_app().id;

        let result = core
            .execute_tool(
                &principal,
                "sockudo_trigger_event",
                Some(&args(json!({
                    "app_id": app_id,
                    "name": "order.created",
                    "channel": "orders",
                    "data": {"id": "ord_1"}
                }))),
            )
            .await
            .unwrap();
        assert_ne!(result.is_error, Some(true), "{:?}", result.content);
        let body = result.structured_content.expect("json object body");
        assert_eq!(body["ok"], true);

        let listed = core
            .execute_tool(
                &principal,
                "sockudo_list_channels",
                Some(&args(json!({ "app_id": app_id }))),
            )
            .await
            .unwrap();
        assert_ne!(listed.is_error, Some(true), "{:?}", listed.content);
        assert!(listed.structured_content.unwrap().get("channels").is_some());
    }

    #[tokio::test]
    async fn unknown_app_and_scope_violations_are_reported() {
        let (core, _apps) = core_for_test().await;
        let reader = Principal::local(ScopeSet::READ);
        let forbidden = core
            .execute_tool(
                &reader,
                "sockudo_trigger_event",
                Some(&args(
                    json!({"app_id": "app-1", "name": "e", "channel": "c"}),
                )),
            )
            .await;
        assert!(
            forbidden.is_err(),
            "write tool must be refused for read scope"
        );

        let missing = core
            .execute_tool(
                &Principal::local(ScopeSet::ALL),
                "sockudo_list_channels",
                Some(&args(json!({"app_id": "nope"}))),
            )
            .await
            .unwrap();
        assert_eq!(missing.is_error, Some(true));
    }

    #[tokio::test]
    async fn list_apps_redacts_secrets() {
        let (core, _apps) = core_for_test().await;
        let principal = Principal::local(ScopeSet::READ);
        let result = core
            .execute_tool(&principal, "sockudo_list_apps", None)
            .await
            .unwrap();
        let text = result.content[0].as_text().unwrap().text.clone();
        assert!(!text.contains(&test_app().secret));
        assert!(
            result.structured_content.unwrap()["count"]
                .as_u64()
                .unwrap()
                >= 1
        );
    }

    #[tokio::test]
    async fn server_info_reports_embedded_mode() {
        let (core, _apps) = core_for_test().await;
        let principal = Principal::local(ScopeSet::READ);
        let result = core
            .execute_tool(&principal, "sockudo_server_info", None)
            .await
            .unwrap();
        let body = result.structured_content.unwrap();
        assert_eq!(body["sockudo"]["mode"], "embedded");
        assert_eq!(body["mcp_server"]["transport"], "in-process");
    }

    #[test]
    fn token_principals_map_scopes_and_apps() {
        let principal = principal_from_token(&McpTokenConfig {
            name: " ops ".to_string(),
            token: "0123456789abcdef".to_string(),
            scopes: vec!["write".to_string()],
            apps: vec!["app-1".to_string()],
        });
        assert_eq!(principal.name.as_ref(), "ops");
        assert!(principal.scopes.allows(Scope::Read));
        assert!(!principal.scopes.allows(Scope::Admin));
        assert!(principal.require_app("app-2").is_err());
    }
}
