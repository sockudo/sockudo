//! stdio transport for local MCP hosts (Claude Desktop, Claude Code, IDEs).

use std::sync::Arc;

use rmcp::ServiceExt;

use crate::auth::Principal;
use crate::handler::{McpCore, SockudoMcp};

/// Serve MCP over stdin/stdout with a fixed principal until the host closes
/// the stream. Logging must go to stderr; stdout is the protocol channel.
pub async fn serve(
    core: Arc<McpCore>,
    principal: Principal,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let running = SockudoMcp::with_principal(core, principal)
        .serve(rmcp::transport::stdio())
        .await?;
    running.waiting().await?;
    Ok(())
}
