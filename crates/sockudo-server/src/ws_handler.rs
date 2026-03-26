#![allow(unused_variables)]
#![allow(dead_code)]

use sockudo_adapter::ConnectionHandler;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use serde::Deserialize;
use sockudo_protocol::ProtocolVersion;
use sockudo_ws::axum_integration::WebSocketUpgrade;
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
    Query(params): Query<ConnectionQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    State(handler): State<Arc<ConnectionHandler>>,
) -> impl IntoResponse {
    // Extract Origin header if present
    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    // Parse protocol version from query params (?protocol=2 for Sockudo-native)
    let protocol_version = ProtocolVersion::from_query_param(params.protocol);

    let server_options = handler.server_options();
    let ws_cfg = server_options.websocket.to_sockudo_ws_config(
        server_options.websocket_max_payload_kb,
        server_options.activity_timeout,
    );

    ws.config(ws_cfg)
        .on_upgrade(move |socket| async move {
            if let Err(e) = handler.handle_socket(socket, app_key.clone(), origin, protocol_version).await {
                error!("Error handling socket: {e}");
                if let Some(metrics) = handler.metrics() {
                    match &e {
                        sockudo_core::error::Error::ApplicationNotFound
                        | sockudo_core::error::Error::ApplicationDisabled
                        | sockudo_core::error::Error::OriginNotAllowed
                        | sockudo_core::error::Error::Auth(_)
                        | sockudo_core::error::Error::InvalidMessageFormat(_)
                        | sockudo_core::error::Error::InvalidEventName(_) => {}
                        _ => {
                            metrics.mark_connection_error(&app_key, "socket_handling_failed");
                        }
                    }
                }
            }
        })
        .into_response()
}
