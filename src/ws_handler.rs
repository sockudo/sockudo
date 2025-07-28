#![allow(unused_variables)]
#![allow(dead_code)]

use crate::adapter::ConnectionHandler;

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use fastwebsockets::upgrade;
use serde::Deserialize;
use std::sync::Arc;
use tracing::log::error;

#[derive(Debug, Deserialize)]
pub struct ConnectionQuery {
    protocol: Option<u8>,
    client: Option<String>,
    version: Option<String>,
}

// WebSocket upgrade handler
pub async fn handle_ws_upgrade(
    Path(app_key): Path<String>,
    Query(_params): Query<ConnectionQuery>,
    ws: upgrade::IncomingUpgrade,
    State(handler): State<Arc<ConnectionHandler>>,
) -> impl IntoResponse {
    let (response, fut) = match ws.upgrade() {
        Ok((response, fut)) => (response, fut),
        Err(e) => {
            error!("WebSocket upgrade failed: {}", e);
            // Track WebSocket upgrade failure
            if let Some(ref metrics) = handler.metrics {
                let metrics_locked = metrics.lock().await;
                metrics_locked.mark_connection_error(&app_key, "websocket_upgrade_failed");
            }
            // Return HTTP error response
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "WebSocket upgrade failed",
            )
                .into_response();
        }
    };

    tokio::task::spawn(async move {
        if let Err(e) = handler.handle_socket(fut, app_key.clone()).await {
            error!("Error handling socket: {e}");
            // Track socket handling errors
            if let Some(ref metrics) = handler.metrics {
                let metrics_locked = metrics.lock().await;
                metrics_locked.mark_connection_error(&app_key, "socket_handling_failed");
            }
        }
    });
    response.into_response()
}
