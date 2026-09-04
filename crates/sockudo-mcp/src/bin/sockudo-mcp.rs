//! Standalone MCP server that talks to a remote Sockudo deployment.
//!
//! ```text
//! sockudo-mcp --url https://rt.example.com --app app-1:key:secret            # stdio
//! sockudo-mcp --transport http --listen 127.0.0.1:6100 --token ops/read+write=...  # HTTP
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use serde_json::{Value, json};
use sockudo_mcp::api::RemoteTransport;
use sockudo_mcp::auth::{AppAccess, Principal, Scope, ScopeSet, StaticAuthenticator};
use sockudo_mcp::transport::http::HttpTransportConfig;
use sockudo_mcp::{
    AppCredentials, Authenticator, McpCore, McpCoreConfig, ServerIntrospection, SockudoApi,
    StaticCredentials, TokenAuthenticator, ToolError,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Transport {
    /// Newline-delimited JSON-RPC over stdin/stdout (Claude Desktop, IDEs).
    Stdio,
    /// Streamable HTTP with bearer-token authentication.
    Http,
}

#[derive(Debug, Parser)]
#[command(
    name = "sockudo-mcp",
    version,
    about = "Model Context Protocol server for a remote Sockudo deployment"
)]
struct Cli {
    /// Base URL of the Sockudo HTTP API (may include a path prefix).
    #[arg(long, env = "SOCKUDO_URL", default_value = "http://127.0.0.1:6001")]
    url: Url,

    /// App credentials as `app_id:app_key:app_secret` (repeatable, or
    /// comma-separated in SOCKUDO_MCP_APPS).
    #[arg(long = "app", env = "SOCKUDO_MCP_APPS", value_delimiter = ',')]
    apps: Vec<String>,

    /// Single-app convenience: app id (with --app-key and --app-secret).
    #[arg(long, env = "SOCKUDO_APP_ID")]
    app_id: Option<String>,
    /// Single-app convenience: app key.
    #[arg(long, env = "SOCKUDO_APP_KEY")]
    app_key: Option<String>,
    /// Single-app convenience: app secret.
    #[arg(long, env = "SOCKUDO_APP_SECRET", hide_env_values = true)]
    app_secret: Option<String>,

    /// MCP transport.
    #[arg(long, value_enum, default_value_t = Transport::Stdio, env = "SOCKUDO_MCP_TRANSPORT")]
    transport: Transport,

    /// Listen address for --transport http.
    #[arg(long, default_value = "127.0.0.1:6100", env = "SOCKUDO_MCP_LISTEN")]
    listen: SocketAddr,

    /// URL path for --transport http.
    #[arg(long, default_value = "/mcp", env = "SOCKUDO_MCP_PATH")]
    path: String,

    /// Bearer tokens for --transport http: `token`, `name=token`, or
    /// `name/scope+scope=token` (repeatable or comma-separated in
    /// SOCKUDO_MCP_TOKENS). Tokens without scopes use --scopes.
    #[arg(
        long = "token",
        env = "SOCKUDO_MCP_TOKENS",
        value_delimiter = ',',
        hide_env_values = true
    )]
    tokens: Vec<String>,

    /// Scopes for stdio and for tokens without explicit scopes
    /// (comma-separated: read, write, admin).
    #[arg(long, default_value = "read,write", env = "SOCKUDO_MCP_SCOPES")]
    scopes: String,

    /// Serve HTTP without authentication (development only).
    #[arg(long, env = "SOCKUDO_MCP_ALLOW_ANONYMOUS")]
    allow_anonymous: bool,

    /// Prometheus metrics URL (enables sockudo_server_metrics).
    #[arg(long, env = "SOCKUDO_METRICS_URL")]
    metrics_url: Option<Url>,

    /// Per-request timeout in milliseconds.
    #[arg(long, default_value_t = 30_000, env = "SOCKUDO_MCP_TIMEOUT_MS")]
    timeout_ms: u64,

    /// Allowed Host authorities for HTTP (DNS-rebinding protection). Empty
    /// allows any host.
    #[arg(long, value_delimiter = ',', env = "SOCKUDO_MCP_ALLOWED_HOSTS")]
    allowed_hosts: Vec<String>,

    /// Allowed browser origins for HTTP. Empty disables Origin checks.
    #[arg(long, value_delimiter = ',', env = "SOCKUDO_MCP_ALLOWED_ORIGINS")]
    allowed_origins: Vec<String>,

    /// Tools to hide (repeatable or comma-separated).
    #[arg(
        long = "disable-tool",
        value_delimiter = ',',
        env = "SOCKUDO_MCP_DISABLED_TOOLS"
    )]
    disabled_tools: Vec<String>,

    /// Extra instructions appended to the server's MCP instructions.
    #[arg(long, env = "SOCKUDO_MCP_INSTRUCTIONS")]
    instructions: Option<String>,
}

struct RemoteIntrospection {
    base_url: Url,
    metrics_url: Option<Url>,
    client: reqwest::Client,
}

#[async_trait]
impl ServerIntrospection for RemoteIntrospection {
    fn describe(&self) -> Value {
        json!({
            "mode": "remote",
            "base_url": self.base_url.as_str(),
            "metrics_available": self.metrics_url.is_some(),
        })
    }

    async fn metrics_text(&self) -> Result<Option<String>, ToolError> {
        let Some(url) = &self.metrics_url else {
            return Ok(None);
        };
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|error| ToolError::Internal(format!("metrics fetch failed: {error}")))?;
        if !response.status().is_success() {
            return Err(ToolError::Internal(format!(
                "metrics endpoint returned HTTP {}",
                response.status()
            )));
        }
        response
            .text()
            .await
            .map(Some)
            .map_err(|error| ToolError::Internal(format!("metrics read failed: {error}")))
    }
}

fn parse_scopes(raw: &str) -> Result<ScopeSet, String> {
    let mut set = ScopeSet::EMPTY;
    for part in raw
        .split([',', '+'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let scope = Scope::parse(part).ok_or_else(|| format!("unknown scope '{part}'"))?;
        set = set.with(scope);
    }
    if set == ScopeSet::EMPTY {
        return Err("at least one scope is required".to_string());
    }
    Ok(set)
}

/// `token`, `name=token`, or `name/scope+scope=token`.
fn parse_token(
    spec: &str,
    index: usize,
    default_scopes: ScopeSet,
) -> Result<(String, Principal), String> {
    let spec = spec.trim();
    let (label, token) = match spec.split_once('=') {
        Some((label, token)) => (Some(label), token),
        None => (None, spec),
    };
    if token.len() < 16 {
        return Err(format!(
            "token #{} is shorter than 16 characters",
            index + 1
        ));
    }
    let (name, scopes) = match label {
        Some(label) => match label.split_once('/') {
            Some((name, scopes)) => (name.to_string(), parse_scopes(scopes)?),
            None => (label.to_string(), default_scopes),
        },
        None => (format!("token-{}", index + 1), default_scopes),
    };
    Ok((
        token.to_string(),
        Principal::new(name, scopes, AppAccess::All),
    ))
}

fn credentials(cli: &Cli) -> Result<StaticCredentials, String> {
    let mut apps = Vec::new();
    for spec in &cli.apps {
        if spec.trim().is_empty() {
            continue;
        }
        apps.push(AppCredentials::parse_spec(spec).map_err(|error| error.to_string())?);
    }
    match (&cli.app_id, &cli.app_key, &cli.app_secret) {
        (Some(id), Some(key), Some(secret)) => apps.push(AppCredentials::new(id, key, secret)),
        (None, None, None) => {}
        _ => {
            return Err("--app-id, --app-key, and --app-secret must be given together".to_string());
        }
    }
    if apps.is_empty() {
        return Err(
            "no app credentials: pass --app app_id:app_key:app_secret or SOCKUDO_MCP_APPS"
                .to_string(),
        );
    }
    Ok(StaticCredentials::new(apps))
}

#[tokio::main]
async fn main() {
    // stdout is the protocol channel in stdio mode; logs always go to stderr.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    if let Err(error) = run(Cli::parse()).await {
        error!(error = %error, "sockudo-mcp exited with an error");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let scopes = parse_scopes(&cli.scopes)?;
    let credentials = credentials(&cli)?;
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_millis(cli.timeout_ms))
        .user_agent(concat!("sockudo-mcp/", env!("CARGO_PKG_VERSION")))
        .build()?;

    let transport = RemoteTransport::new(cli.url.clone(), http_client.clone());
    let api = SockudoApi::new(Arc::new(transport), Arc::new(credentials))
        .with_timeout(Duration::from_millis(cli.timeout_ms));
    let config = McpCoreConfig {
        disabled_tools: cli
            .disabled_tools
            .iter()
            .map(|tool| tool.trim().to_string())
            .filter(|tool| !tool.is_empty())
            .collect(),
        instructions: cli.instructions.clone(),
        ..McpCoreConfig::default()
    };
    let core = Arc::new(McpCore::new(api, config).with_introspection(Arc::new(
        RemoteIntrospection {
            base_url: cli.url.clone(),
            metrics_url: cli.metrics_url.clone(),
            client: http_client,
        },
    )));
    info!(
        url = %cli.url,
        tools = core.catalog().len(),
        scopes = %scopes,
        transport = ?cli.transport,
        "sockudo-mcp starting"
    );

    match cli.transport {
        Transport::Stdio => {
            sockudo_mcp::transport::stdio::serve(core, Principal::local(scopes)).await
        }
        Transport::Http => serve_http(cli, core, scopes).await,
    }
}

async fn serve_http(
    cli: Cli,
    core: Arc<McpCore>,
    default_scopes: ScopeSet,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let authenticator: Arc<dyn Authenticator> = if cli.tokens.iter().any(|t| !t.trim().is_empty()) {
        let mut entries = Vec::new();
        for (index, spec) in cli
            .tokens
            .iter()
            .filter(|t| !t.trim().is_empty())
            .enumerate()
        {
            entries.push(parse_token(spec, index, default_scopes)?);
        }
        Arc::new(TokenAuthenticator::new(entries))
    } else if cli.allow_anonymous {
        warn!("serving MCP over HTTP without authentication (--allow-anonymous)");
        Arc::new(StaticAuthenticator(Principal::local(default_scopes)))
    } else {
        return Err(
            "HTTP transport requires --token (or --allow-anonymous for development)".into(),
        );
    };

    let mut path = cli.path.trim().to_string();
    if !path.starts_with('/') {
        path.insert(0, '/');
    }
    let config = HttpTransportConfig {
        allowed_hosts: cli.allowed_hosts.clone(),
        allowed_origins: cli.allowed_origins.clone(),
        ..HttpTransportConfig::default()
    };
    let ct = CancellationToken::new();
    let router =
        sockudo_mcp::transport::http::router(&path, core, authenticator, &config, ct.clone());
    let _ = LocalSessionManager::default;

    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    info!(listen = %cli.listen, path = %path, "mcp http transport listening");
    let shutdown = async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown signal received");
        ct.cancel();
    };
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_specs() {
        let (token, principal) =
            parse_token("ops/read+admin=0123456789abcdef", 0, ScopeSet::READ).unwrap();
        assert_eq!(token, "0123456789abcdef");
        assert_eq!(principal.name.as_ref(), "ops");
        assert!(principal.scopes.allows(Scope::Admin));
        let (_, plain) = parse_token("0123456789abcdef", 2, ScopeSet::READ_WRITE).unwrap();
        assert_eq!(plain.name.as_ref(), "token-3");
        assert!(plain.scopes.allows(Scope::Write));
        assert!(parse_token("short", 0, ScopeSet::READ).is_err());
        assert!(parse_scopes("read,bogus").is_err());
    }
}
