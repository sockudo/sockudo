#![allow(unused_variables)]
#![allow(dead_code)]

use sockudo_adapter::ConnectionHandler;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use serde::Deserialize;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::WebSocketUpgrade;
use std::sync::Arc;
use tracing::log::error;

#[derive(Debug, Deserialize)]
pub struct ConnectionQuery {
    protocol: Option<u8>,
    client: Option<String>,
    version: Option<String>,
    format: Option<String>,
    /// V2 only. Set to false to disable echo (publisher won't receive own messages).
    /// Default: true.
    echo_messages: Option<bool>,
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

    let server_options = handler.server_options();
    // Parse protocol version from query params (?protocol=2 for Sockudo-native)
    let protocol_version = ProtocolVersion::from_query_param(params.protocol);
    let wire_format = if protocol_version == ProtocolVersion::V2 {
        match WireFormat::parse_query_param(params.format.as_deref()) {
            Ok(format) => format,
            Err(_) => {
                return axum::http::StatusCode::BAD_REQUEST
                    .into_response();
            }
        }
    } else {
        WireFormat::Json
    };
    let echo_messages = if server_options.echo_control.enabled {
        params
            .echo_messages
            .unwrap_or(server_options.echo_control.default_echo_messages)
    } else {
        true
    };
    let ws_cfg = server_options.websocket.to_sockudo_ws_config(
        server_options.websocket_max_payload_kb,
        server_options.activity_timeout,
    );

    ws.config(ws_cfg)
        .on_upgrade(move |socket| async move {
            if let Err(e) = handler
                .handle_socket(
                    socket,
                    app_key.clone(),
                    origin,
                    protocol_version,
                    wire_format,
                    echo_messages,
                )
                .await
            {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_rejects_unknown_wire_format() {
        let format = WireFormat::parse_query_param(Some("unknown"));
        assert!(format.is_err());
    }

    #[test]
    fn v1_ignores_wire_format_query() {
        let protocol_version = ProtocolVersion::from_query_param(Some(7));
        let wire_format = if protocol_version == ProtocolVersion::V2 {
            WireFormat::parse_query_param(Some("protobuf")).unwrap()
        } else {
            WireFormat::Json
        };

        assert_eq!(wire_format, WireFormat::Json);
    }
}
