//! Model Context Protocol (MCP) server for Sockudo.
//!
//! The protocol layer (JSON-RPC framing, lifecycle, sessions, Streamable HTTP
//! and stdio transports) comes from the official [`rmcp`] SDK. This crate adds
//! the Sockudo-specific layer on top of it:
//!
//! - [`api`]: a signed client for the Sockudo HTTP API contract over a
//!   pluggable [`ApiTransport`]. The `sockudo` binary drives its own axum
//!   router in-process (no network hop, every existing validation, idempotency,
//!   metrics, and feature gate reused); the `sockudo-mcp` binary talks to a
//!   remote deployment with the `remote` feature.
//! - [`auth`]: bearer-token principals with `read` / `write` / `admin` scopes
//!   and per-app allow-lists.
//! - [`tools`], [`resources`], [`prompts`]: the MCP surface an agent sees.
//! - [`handler`]: the [`rmcp::ServerHandler`] implementation wiring it all
//!   together, plus rate limiting, metrics, and audit logging.

#![forbid(unsafe_code)]

pub mod api;
pub mod auth;
pub mod error;
pub mod handler;
pub mod prompts;
pub mod resources;
pub mod tools;
pub mod transport;

pub use api::{
    ApiError, ApiRequest, ApiResponse, ApiTransport, AppCredentials, AppSummary, CredentialSource,
    Endpoint, SockudoApi, StaticCredentials,
};
pub use auth::{AppAccess, Authenticator, Principal, Scope, ScopeSet, TokenAuthenticator};
pub use error::ToolError;
pub use handler::{McpCore, McpCoreConfig, ServerIntrospection, SockudoMcp};

/// Crate version reported in `initialize` responses.
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
/// MCP server name reported in `initialize` responses.
pub const SERVER_NAME: &str = "sockudo";
