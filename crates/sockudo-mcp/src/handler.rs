//! [`rmcp::ServerHandler`] implementation: authorization, rate limiting,
//! dispatch to tools/resources/prompts, metrics, and audit logging.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, CompleteRequestParams,
    CompleteResult, CompletionInfo, Extensions, GetPromptRequestParams, GetPromptResponse,
    Implementation, InitializeResult, JsonObject, ListPromptsResult, ListResourceTemplatesResult,
    ListResourcesResult, ListToolsResult, PaginatedRequestParams, ProtocolVersion,
    ReadResourceRequestParams, ReadResourceResponse, Reference, ServerCapabilities, ServerInfo,
};
use rmcp::service::{NotificationContext, RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};
use serde_json::{Value, json};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::rate_limiter::RateLimiter;
use tracing::{debug, info, warn};

use crate::api::{ApiRequest, Endpoint, SockudoApi};
use crate::auth::{Principal, Scope};
use crate::error::{ERROR_CODE_UNAUTHORIZED, ToolError, auth_error, rate_limited, success_result};
use crate::tools::{Args, Catalog, ToolContext, ToolKind};
use crate::{SERVER_NAME, SERVER_VERSION, prompts, resources};

/// Server-side facts the MCP layer cannot learn from the HTTP API alone.
#[async_trait]
pub trait ServerIntrospection: Send + Sync + 'static {
    /// Non-secret description of the deployment (version, features, role).
    fn describe(&self) -> Value;

    /// Prometheus text exposition, or `None` when metrics are disabled.
    async fn metrics_text(&self) -> Result<Option<String>, ToolError>;
}

/// Tunables for [`McpCore`].
#[derive(Debug, Clone)]
pub struct McpCoreConfig {
    /// Name reported to clients.
    pub server_name: String,
    /// Title reported to clients.
    pub server_title: String,
    /// Extra instructions appended to the built-in guidance.
    pub instructions: Option<String>,
    /// Tools to hide entirely.
    pub disabled_tools: HashSet<String>,
}

impl Default for McpCoreConfig {
    fn default() -> Self {
        Self {
            server_name: SERVER_NAME.to_string(),
            server_title: "Sockudo realtime server".to_string(),
            instructions: None,
            disabled_tools: HashSet::new(),
        }
    }
}

/// Cache hint for principal-dependent listings (tools, resources).
const LIST_TTL_MS: u64 = 300_000;
/// Cache hint for static catalogs (prompts, resource templates).
const STATIC_TTL_MS: u64 = 3_600_000;

const BASE_INSTRUCTIONS: &str = "Sockudo is a Pusher-compatible realtime WebSocket server with \
Protocol V2 extensions (durable history, versioned messages, presence history, annotations, push). \
Start with sockudo_list_apps to discover app ids. Read tools are safe; write tools publish or \
mutate; admin tools are destructive and require confirm=true plus a reason. Responses are the \
documented HTTP API JSON shapes. Never ask for or print app secrets; signature tools return only \
derived auth strings. Read the sockudo://docs/* resources for channel and operations guidance.";

/// Shared, transport-independent server state.
pub struct McpCore {
    api: SockudoApi,
    catalog: Catalog,
    config: McpCoreConfig,
    introspection: Option<Arc<dyn ServerIntrospection>>,
    rate_limiter: Option<Arc<dyn RateLimiter + Send + Sync>>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    instructions: String,
}

impl fmt::Debug for McpCore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpCore")
            .field("api", &self.api)
            .field("tools", &self.catalog.len())
            .field("rate_limited", &self.rate_limiter.is_some())
            .field("metrics", &self.metrics.is_some())
            .finish_non_exhaustive()
    }
}

impl McpCore {
    /// Build the core around a signed API client.
    pub fn new(api: SockudoApi, config: McpCoreConfig) -> Self {
        let catalog = Catalog::new(&config.disabled_tools);
        let instructions = match &config.instructions {
            Some(extra) if !extra.trim().is_empty() => format!("{BASE_INSTRUCTIONS}\n\n{extra}"),
            _ => BASE_INSTRUCTIONS.to_string(),
        };
        Self {
            api,
            catalog,
            config,
            introspection: None,
            rate_limiter: None,
            metrics: None,
            instructions,
        }
    }

    /// Attach deployment introspection (metrics text, feature flags).
    #[must_use]
    pub fn with_introspection(mut self, introspection: Arc<dyn ServerIntrospection>) -> Self {
        self.introspection = Some(introspection);
        self
    }

    /// Attach a per-principal rate limiter keyed by principal name.
    #[must_use]
    pub fn with_rate_limiter(mut self, limiter: Arc<dyn RateLimiter + Send + Sync>) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    /// Attach the server metrics sink.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsInterface + Send + Sync>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Signed API client.
    pub fn api(&self) -> &SockudoApi {
        &self.api
    }

    /// Tool catalog.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Deployment introspection, when available.
    pub fn introspection(&self) -> Option<&Arc<dyn ServerIntrospection>> {
        self.introspection.as_ref()
    }

    /// Configuration.
    pub fn config(&self) -> &McpCoreConfig {
        &self.config
    }

    /// Description used by `sockudo_server_info` and `sockudo://server/info`.
    pub fn describe(&self, principal: &Principal) -> Value {
        let apps = match &principal.apps {
            crate::auth::AppAccess::All => Value::String("*".into()),
            crate::auth::AppAccess::Only(list) => json!(list),
        };
        json!({
            "mcp_server": {
                "name": self.config.server_name,
                "version": SERVER_VERSION,
                "protocol_versions": ProtocolVersion::KNOWN_VERSIONS
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                "transport": self.api.transport_kind(),
                "tools": self.catalog.len(),
                "request_timeout_ms": self.api.timeout().as_millis() as u64,
                "rate_limited": self.rate_limiter.is_some(),
            },
            "principal": {
                "name": principal.name,
                "scopes": principal.scopes.iter().map(Scope::as_str).collect::<Vec<_>>(),
                "apps": apps,
            },
            "sockudo": self.introspection.as_ref().map(|i| i.describe()).unwrap_or(Value::Null),
        })
    }

    async fn enforce_rate_limit(&self, principal: &Principal) -> Result<(), ErrorData> {
        let Some(limiter) = &self.rate_limiter else {
            return Ok(());
        };
        let key = format!("mcp:{}", principal.name);
        match limiter.increment(&key).await {
            Ok(result) if result.allowed => Ok(()),
            Ok(result) => {
                self.mark_request("rate_limited");
                warn!(principal = %principal.name, "mcp request budget exceeded");
                Err(rate_limited(result.reset_after.max(1)))
            }
            Err(error) => {
                // Fail closed: a broken limiter must not turn into unlimited access.
                warn!(error = %error, "mcp rate limiter unavailable; rejecting request");
                Err(ErrorData::internal_error(
                    "rate limiter unavailable; retry later",
                    None,
                ))
            }
        }
    }

    fn mark_request(&self, outcome: &str) {
        if let Some(metrics) = &self.metrics {
            metrics.mark_mcp_request(outcome);
        }
    }

    /// Execute a tool on behalf of `principal`.
    pub async fn execute_tool(
        &self,
        principal: &Principal,
        name: &str,
        arguments: Option<&JsonObject>,
    ) -> Result<CallToolResult, ErrorData> {
        let Some(spec) = self.catalog.get(name) else {
            self.mark_tool(name, "unknown_tool", None);
            return Err(ErrorData::invalid_params(
                format!("unknown tool '{name}'"),
                None,
            ));
        };
        if let Err(error) = principal.require(spec.scope) {
            self.mark_tool(name, "forbidden_scope", None);
            audit(principal, name, arguments, "forbidden_scope", 0.0);
            return Err(auth_error(&error));
        }
        self.enforce_rate_limit(principal).await?;

        let empty = JsonObject::new();
        let args = Args::new(arguments.unwrap_or(&empty));
        let started = Instant::now();
        let outcome: Result<CallToolResult, ToolError> = match spec.kind {
            ToolKind::Api(builder) => {
                async {
                    let request = builder(&args)?;
                    if let Some(app_id) = request.endpoint.app_id() {
                        principal.require_app(app_id)?;
                    }
                    let response = self.api.call(request).await?;
                    if response.is_success() {
                        Ok(success_result(&response))
                    } else {
                        Err(ToolError::Upstream(response))
                    }
                }
                .await
            }
            ToolKind::Custom(handler) => {
                handler(
                    ToolContext {
                        core: self,
                        principal,
                    },
                    args,
                )
                .await
            }
        };
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        let label = match &outcome {
            Ok(_) => "ok",
            Err(error) => error.outcome(),
        };
        self.mark_tool(name, label, Some(latency_ms));
        audit(principal, name, arguments, label, latency_ms);
        match outcome {
            Ok(result) => Ok(result),
            Err(error) => error.into_call_result(),
        }
    }

    fn mark_tool(&self, tool: &str, outcome: &str, latency_ms: Option<f64>) {
        if let Some(metrics) = &self.metrics {
            metrics.mark_mcp_tool_call(tool, outcome);
            if let Some(latency_ms) = latency_ms {
                metrics.track_mcp_tool_latency(tool, latency_ms);
            }
        }
    }

    /// Argument completion for prompts and resource templates.
    async fn complete(
        &self,
        principal: &Principal,
        argument: &str,
        prefix: &str,
        context_app_id: Option<&str>,
    ) -> Result<Vec<String>, ErrorData> {
        const MAX: usize = CompletionInfo::MAX_VALUES;
        match argument {
            "app_id" => {
                let apps = self
                    .api
                    .credentials()
                    .list_apps()
                    .await
                    .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
                Ok(apps
                    .into_iter()
                    .filter(|app| principal.apps.allows(&app.id) && app.id.starts_with(prefix))
                    .map(|app| app.id)
                    .take(MAX)
                    .collect())
            }
            "channel" => {
                let Some(app_id) = context_app_id else {
                    return Ok(Vec::new());
                };
                if principal.require_app(app_id).is_err() {
                    return Ok(Vec::new());
                }
                let response = self
                    .api
                    .call(
                        ApiRequest::new(Endpoint::Channels {
                            app_id: app_id.to_string(),
                        })
                        .query("filter_by_prefix", prefix),
                    )
                    .await
                    .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
                let Some(Value::Object(body)) = response.json() else {
                    return Ok(Vec::new());
                };
                let mut names: Vec<String> = body
                    .get("channels")
                    .and_then(Value::as_object)
                    .map(|channels| channels.keys().cloned().collect())
                    .unwrap_or_default();
                names.sort_unstable();
                names.truncate(MAX);
                Ok(names)
            }
            _ => Ok(Vec::new()),
        }
    }
}

fn audit(
    principal: &Principal,
    tool: &str,
    arguments: Option<&JsonObject>,
    outcome: &str,
    latency_ms: f64,
) {
    let app_id = arguments
        .and_then(|args| args.get("app_id"))
        .and_then(Value::as_str)
        .unwrap_or("-");
    let channel = arguments
        .and_then(|args| args.get("channel"))
        .and_then(Value::as_str)
        .unwrap_or("-");
    info!(
        target: "sockudo_mcp::audit",
        principal = %principal.name,
        tool,
        app_id,
        channel,
        outcome,
        latency_ms = format_args!("{latency_ms:.2}"),
        "mcp tool call"
    );
}

/// One MCP server instance (rmcp creates one per session or per stateless
/// request; construction is an `Arc` clone).
#[derive(Clone)]
pub struct SockudoMcp {
    core: Arc<McpCore>,
    principal: Option<Principal>,
}

impl fmt::Debug for SockudoMcp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SockudoMcp")
            .field("core", &self.core)
            .field(
                "static_principal",
                &self.principal.as_ref().map(|p| p.name.clone()),
            )
            .finish()
    }
}

impl SockudoMcp {
    /// Handler whose principal comes from the HTTP layer (bearer auth
    /// middleware inserts a [`Principal`] into the request extensions).
    pub fn new(core: Arc<McpCore>) -> Self {
        Self {
            core,
            principal: None,
        }
    }

    /// Handler with a fixed principal, for stdio and trusted local transports.
    pub fn with_principal(core: Arc<McpCore>, principal: Principal) -> Self {
        Self {
            core,
            principal: Some(principal),
        }
    }

    /// Shared state.
    pub fn core(&self) -> &Arc<McpCore> {
        &self.core
    }

    fn principal(&self, extensions: &Extensions) -> Result<Principal, ErrorData> {
        if let Some(parts) = extensions.get::<http::request::Parts>()
            && let Some(principal) = parts.extensions.get::<Principal>()
        {
            return Ok(principal.clone());
        }
        if let Some(principal) = &self.principal {
            return Ok(principal.clone());
        }
        self.core.mark_request("unauthorized");
        Err(ErrorData::new(
            ERROR_CODE_UNAUTHORIZED,
            "no authenticated principal for this request",
            None,
        ))
    }

    fn read_principal(&self, extensions: &Extensions) -> Result<Principal, ErrorData> {
        let principal = self.principal(extensions)?;
        principal
            .require(Scope::Read)
            .map_err(|error| auth_error(&error))?;
        Ok(principal)
    }
}

impl ServerHandler for SockudoMcp {
    fn get_info(&self) -> ServerInfo {
        let capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .enable_prompts()
            .enable_completions()
            .build();
        let mut info = InitializeResult::new(capabilities);
        info.server_info =
            Implementation::new(self.core.config.server_name.clone(), SERVER_VERSION)
                .with_title(self.core.config.server_title.clone());
        info.instructions = Some(self.core.instructions.clone());
        info
    }

    async fn on_initialized(&self, context: NotificationContext<RoleServer>) {
        let client = context
            .peer
            .peer_info()
            .map(|info| format!("{} {}", info.client_info.name, info.client_info.version))
            .unwrap_or_else(|| "unknown".to_string());
        debug!(client = %client, "mcp client initialized");
        self.core.mark_request("initialized");
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let principal = self.principal(&context.extensions)?;
        self.core.mark_request("tools_list");
        let tools = self.core.catalog.list_for(principal.scopes);
        // Protocol 2026-07-28 requires cache hints on list results; rmcp strips
        // them for older peers. Tool lists depend on the caller's scopes.
        Ok(ListToolsResult::with_all_items(tools.as_ref().clone())
            .with_ttl_ms(LIST_TTL_MS)
            .with_cache_scope(CacheScope::Private))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let principal = self.principal(&context.extensions)?;
        self.core.mark_request("tools_call");
        let ct = context.ct.clone();
        let execute = self
            .core
            .execute_tool(&principal, &request.name, request.arguments.as_ref());
        tokio::select! {
            result = execute => result.map(CallToolResponse::from),
            _ = ct.cancelled() => {
                self.core.mark_tool(&request.name, "cancelled", None);
                Err(ErrorData::internal_error("request cancelled", None))
            }
        }
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let principal = self.read_principal(&context.extensions)?;
        self.core.mark_request("resources_list");
        Ok(
            ListResourcesResult::with_all_items(resources::list(&principal))
                .with_ttl_ms(LIST_TTL_MS)
                .with_cache_scope(CacheScope::Private),
        )
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        self.read_principal(&context.extensions)?;
        self.core.mark_request("resources_templates_list");
        Ok(
            ListResourceTemplatesResult::with_all_items(resources::templates())
                .with_ttl_ms(STATIC_TTL_MS)
                .with_cache_scope(CacheScope::Public),
        )
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let principal = self.read_principal(&context.extensions)?;
        self.core.mark_request("resources_read");
        self.core.enforce_rate_limit(&principal).await?;
        let started = Instant::now();
        let result = resources::read(&self.core, &principal, &request.uri).await;
        info!(
            target: "sockudo_mcp::audit",
            principal = %principal.name,
            uri = %request.uri,
            outcome = if result.is_ok() { "ok" } else { "error" },
            latency_ms = format_args!("{:.2}", started.elapsed().as_secs_f64() * 1000.0),
            "mcp resource read"
        );
        result.map(ReadResourceResponse::from)
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        self.read_principal(&context.extensions)?;
        self.core.mark_request("prompts_list");
        Ok(ListPromptsResult::with_all_items(prompts::list())
            .with_ttl_ms(STATIC_TTL_MS)
            .with_cache_scope(CacheScope::Public))
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        self.read_principal(&context.extensions)?;
        self.core.mark_request("prompts_get");
        prompts::get(&request.name, request.arguments.as_ref()).map(GetPromptResponse::from)
    }

    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        let principal = self.read_principal(&context.extensions)?;
        self.core.mark_request("completion_complete");
        let context_app_id = request
            .context
            .as_ref()
            .and_then(|ctx| ctx.get_argument("app_id"))
            .map(String::as_str);
        let known = match &request.r#ref {
            Reference::Prompt(prompt) => prompts::list().iter().any(|p| p.name == prompt.name),
            Reference::Resource(resource) => resources::templates()
                .iter()
                .any(|template| template.uri_template == resource.uri),
            _ => false,
        };
        if !known {
            return Ok(CompleteResult::default());
        }
        let values = self
            .core
            .complete(
                &principal,
                &request.argument.name,
                &request.argument.value,
                context_app_id,
            )
            .await?;
        let total = values.len() as u32;
        let mut completion =
            CompletionInfo::new(values).map_err(|error| ErrorData::internal_error(error, None))?;
        completion.total = Some(total);
        completion.has_more = Some(false);
        Ok(CompleteResult::new(completion))
    }
}
