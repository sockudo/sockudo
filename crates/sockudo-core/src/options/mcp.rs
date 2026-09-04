//! `[mcp]` configuration: the embedded Model Context Protocol server.

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Scope names accepted in `[[mcp.tokens]].scopes` and `mcp.anonymous_scopes`.
pub const MCP_SCOPES: &[&str] = &["read", "write", "admin"];

/// Minimum accepted bearer token length.
pub const MCP_MIN_TOKEN_LEN: usize = 16;

/// Route prefixes the MCP path must not shadow.
const RESERVED_PREFIXES: &[&str] = &[
    "/apps",
    "/app",
    "/up",
    "/live",
    "/usage",
    "/stats",
    "/operator",
    "/accept-traffic",
    "/metrics",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct McpConfig {
    /// Serve MCP. Requires the `mcp` Cargo feature.
    pub enabled: bool,
    /// URL path for the Streamable HTTP endpoint.
    pub path: String,
    /// Bind host for a dedicated MCP listener; defaults to the server host.
    pub host: Option<String>,
    /// Dedicated MCP port. When unset, MCP shares the main HTTP listener.
    pub port: Option<u16>,
    /// Allowed `Host` authorities (DNS-rebinding protection). Empty accepts any
    /// host, which is appropriate behind a trusted reverse proxy.
    pub allowed_hosts: Vec<String>,
    /// Allowed browser origins. Empty disables `Origin` validation.
    pub allowed_origins: Vec<String>,
    /// Accept unauthenticated requests with `anonymous_scopes`. Development only.
    pub allow_anonymous: bool,
    /// Scopes granted to anonymous callers when `allow_anonymous` is set.
    pub anonymous_scopes: Vec<String>,
    /// Bearer tokens and their principals.
    pub tokens: Vec<McpTokenConfig>,
    /// Per-tool upstream timeout.
    pub request_timeout_ms: u64,
    /// Maximum MCP POST body size.
    pub max_body_bytes: usize,
    /// Idle session lifetime for session-based protocol versions.
    pub session_ttl_seconds: u64,
    /// Per-token tool/resource call budget per minute. `0` disables.
    pub rate_limit_per_minute: u32,
    /// Tool names to hide entirely.
    pub disabled_tools: Vec<String>,
    /// Extra instructions appended to the MCP server instructions.
    pub instructions: Option<String>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/mcp".to_string(),
            host: None,
            port: None,
            allowed_hosts: Vec::new(),
            allowed_origins: Vec::new(),
            allow_anonymous: false,
            anonymous_scopes: vec!["read".to_string()],
            tokens: Vec::new(),
            request_timeout_ms: 30_000,
            max_body_bytes: 1024 * 1024,
            session_ttl_seconds: 1800,
            rate_limit_per_minute: 600,
            disabled_tools: Vec::new(),
            instructions: None,
        }
    }
}

/// One bearer token principal.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct McpTokenConfig {
    /// Audit-log name; must be unique.
    pub name: String,
    /// Bearer token value (use `${ENV}` interpolation rather than literals).
    pub token: String,
    /// Granted scopes: `read`, `write`, `admin` (higher levels imply lower).
    pub scopes: Vec<String>,
    /// App ids this token may touch; `["*"]` for all.
    pub apps: Vec<String>,
}

impl Default for McpTokenConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            token: String::new(),
            scopes: vec!["read".to_string()],
            apps: vec!["*".to_string()],
        }
    }
}

impl fmt::Debug for McpTokenConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpTokenConfig")
            .field("name", &self.name)
            .field("token", &"<redacted>")
            .field("scopes", &self.scopes)
            .field("apps", &self.apps)
            .finish()
    }
}

fn validate_scopes(scopes: &[String], context: &str) -> Result<(), String> {
    if scopes.is_empty() {
        return Err(format!("{context} must list at least one scope"));
    }
    for scope in scopes {
        if !MCP_SCOPES.contains(&scope.trim().to_ascii_lowercase().as_str()) {
            return Err(format!(
                "{context} contains unknown scope '{scope}'; expected one of read, write, admin"
            ));
        }
    }
    Ok(())
}

impl McpConfig {
    pub(super) fn validate(
        &self,
        main_port: u16,
        metrics_enabled: bool,
        metrics_port: u16,
    ) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        let path = self.path.trim();
        if !path.starts_with('/') || path.len() < 2 || path.contains(char::is_whitespace) {
            return Err("mcp.path must be an absolute URL path such as /mcp".to_string());
        }
        if self.port.is_none()
            && RESERVED_PREFIXES
                .iter()
                .any(|prefix| path == *prefix || path.starts_with(&format!("{prefix}/")))
        {
            return Err(format!(
                "mcp.path '{path}' would shadow a Sockudo API route; choose another path or set mcp.port"
            ));
        }
        if let Some(port) = self.port {
            if port == 0 {
                return Err("mcp.port must be greater than 0".to_string());
            }
            if port == main_port {
                return Err(
                    "mcp.port must differ from the main HTTP port; omit it to share the listener"
                        .to_string(),
                );
            }
            if metrics_enabled && port == metrics_port {
                return Err("mcp.port must differ from metrics.port".to_string());
            }
        }
        if self.tokens.is_empty() && !self.allow_anonymous {
            return Err(
                "mcp.enabled requires at least one [[mcp.tokens]] entry or mcp.allow_anonymous = true"
                    .to_string(),
            );
        }
        if self.allow_anonymous {
            validate_scopes(&self.anonymous_scopes, "mcp.anonymous_scopes")?;
        }
        let mut names = HashSet::new();
        for (index, token) in self.tokens.iter().enumerate() {
            let context = format!("mcp.tokens[{index}]");
            if token.name.trim().is_empty() {
                return Err(format!("{context}.name must not be empty"));
            }
            if !names.insert(token.name.trim().to_string()) {
                return Err(format!("{context}.name '{}' is duplicated", token.name));
            }
            if token.token.len() < MCP_MIN_TOKEN_LEN {
                return Err(format!(
                    "{context}.token must be at least {MCP_MIN_TOKEN_LEN} characters"
                ));
            }
            validate_scopes(&token.scopes, &format!("{context}.scopes"))?;
            if token.apps.is_empty() {
                return Err(format!("{context}.apps must list app ids or \"*\""));
            }
        }
        if self.request_timeout_ms == 0 {
            return Err("mcp.request_timeout_ms must be greater than 0".to_string());
        }
        if self.max_body_bytes < 1024 {
            return Err("mcp.max_body_bytes must be at least 1024".to_string());
        }
        if self.session_ttl_seconds == 0 {
            return Err("mcp.session_ttl_seconds must be greater than 0".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(name: &str) -> McpTokenConfig {
        McpTokenConfig {
            name: name.to_string(),
            token: "0123456789abcdef0123".to_string(),
            ..McpTokenConfig::default()
        }
    }

    #[test]
    fn disabled_config_is_always_valid() {
        assert!(McpConfig::default().validate(6001, true, 9601).is_ok());
    }

    #[test]
    fn enabled_requires_tokens_or_anonymous() {
        let mut config = McpConfig {
            enabled: true,
            ..McpConfig::default()
        };
        assert!(config.validate(6001, true, 9601).is_err());
        config.allow_anonymous = true;
        assert!(config.validate(6001, true, 9601).is_ok());
        config.allow_anonymous = false;
        config.tokens.push(token("ops"));
        assert!(config.validate(6001, true, 9601).is_ok());
    }

    #[test]
    fn rejects_shadowing_paths_short_tokens_and_port_conflicts() {
        let mut config = McpConfig {
            enabled: true,
            tokens: vec![token("ops")],
            ..McpConfig::default()
        };
        config.path = "/apps/x".to_string();
        assert!(config.validate(6001, true, 9601).is_err());
        config.path = "/mcp".to_string();
        config.tokens[0].token = "short".to_string();
        assert!(config.validate(6001, true, 9601).is_err());
        config.tokens[0].token = "0123456789abcdef0123".to_string();
        config.tokens.push(token("ops"));
        assert!(
            config.validate(6001, true, 9601).is_err(),
            "duplicate names"
        );
        config.tokens.pop();
        config.port = Some(6001);
        assert!(config.validate(6001, true, 9601).is_err());
        config.port = Some(9601);
        assert!(config.validate(6001, true, 9601).is_err());
        config.port = Some(6100);
        assert!(config.validate(6001, true, 9601).is_ok());
        config.tokens[0].scopes = vec!["root".to_string()];
        assert!(config.validate(6001, true, 9601).is_err());
    }

    #[test]
    fn debug_redacts_tokens() {
        let text = format!("{:?}", token("ops"));
        assert!(!text.contains("0123456789abcdef0123"));
    }
}
