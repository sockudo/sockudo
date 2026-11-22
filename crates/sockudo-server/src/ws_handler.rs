#![allow(unused_variables)]
#![allow(dead_code)]

use sockudo_adapter::ConnectionHandler;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use serde::Deserialize;
use sockudo_protocol::{AppendMode, ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::WebSocketUpgrade;
use std::sync::Arc;
use tracing::{Instrument, debug, error, field, info_span};

#[derive(Debug, Deserialize)]
pub struct ConnectionQuery {
    protocol: Option<u8>,
    client: Option<String>,
    version: Option<String>,
    format: Option<String>,
    /// V2 only. Set to false to disable echo (publisher won't receive own messages).
    /// Default: true.
    echo_messages: Option<bool>,
    /// V2 AI Transport append rollup window preference.
    append_rollup_window: Option<u64>,
    /// V2 mutable message append delivery mode: delta (default) or full.
    append_mode: Option<String>,
    /// V2 capability-token authentication.
    token: Option<String>,
}

fn websocket_config_for_protocol(
    server_options: &sockudo_core::options::ServerOptions,
    protocol_version: ProtocolVersion,
) -> sockudo_ws::Config {
    server_options
        .websocket
        .to_sockudo_ws_config_with_native_heartbeat(
            server_options.websocket_max_payload_kb,
            server_options.activity_timeout,
            protocol_version == ProtocolVersion::V2,
        )
}

/// A peer or transport termination that needs no operator action: the socket is
/// already gone, the peer closed/reset it, or this node is shutting down.
/// Scoped to the upgrade callback so the rest of the server keeps treating
/// `ConnectionClosed` as a real error where it has another meaning.
fn is_expected_websocket_termination(error: &sockudo_core::error::Error) -> bool {
    use sockudo_core::error::Error;

    match error {
        Error::ConnectionClosed(_) => true,
        Error::WebSocket(error) => match error {
            sockudo_ws::Error::ConnectionClosed
            | sockudo_ws::Error::ConnectionReset
            | sockudo_ws::Error::Closed(_)
            | sockudo_ws::Error::HeartbeatTimeout
            | sockudo_ws::Error::IdleTimeout => true,
            sockudo_ws::Error::Io(error) => matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::TimedOut
            ),
            _ => false,
        },
        _ => false,
    }
}

// WebSocket upgrade handler
pub async fn handle_ws_upgrade(
    Path(app_key): Path<String>,
    Query(params): Query<ConnectionQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    State(handler): State<Arc<ConnectionHandler>>,
) -> impl IntoResponse {
    // Reject new connections once draining so a terminating pod stops taking work
    // and its sockudo_connected gauge can decay to zero instead of spiking.
    if !handler.is_accepting() {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if handler.is_memory_pressure_shedding() {
        handler.mark_memory_pressure_rejection();
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            "MEMORY_PRESSURE",
        )
            .into_response();
    }

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
                return axum::http::StatusCode::BAD_REQUEST.into_response();
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
    if protocol_version == ProtocolVersion::V2
        && let Some(window_ms) = params.append_rollup_window
        && !server_options.ai_transport.rollup.allows_window(window_ms)
    {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }
    let append_mode = if protocol_version == ProtocolVersion::V2 {
        match AppendMode::parse_query_param(params.append_mode.as_deref()) {
            Ok(mode) => mode,
            Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
        }
    } else {
        AppendMode::Full
    };
    let ws_cfg = websocket_config_for_protocol(server_options, protocol_version);

    let connection_span = info_span!(
        target: "sockudo_telemetry",
        "websocket.connection",
        otel.kind = "server",
        otel.name = "websocket connection",
        network.protocol.name = "websocket",
        sockudo.protocol.version = protocol_version as u8,
        app_id = field::Empty,
        socket_id = field::Empty,
        otel.status_code = field::Empty,
    );

    ws.config(ws_cfg)
        .on_upgrade(move |socket| {
            async move {
                if let Err(e) = handler
                    .handle_socket(
                        socket,
                        app_key.clone(),
                        origin,
                        protocol_version,
                        wire_format,
                        echo_messages,
                        append_mode,
                        params.token,
                    )
                    .await
                {
                    if is_expected_websocket_termination(&e) {
                        debug!(error = %e, "websocket transport closed");
                    } else {
                        error!(error = %e, "socket handling failed");
                        tracing::Span::current().record("otel.status_code", "ERROR");
                    }
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
            }
            .instrument(connection_span)
        })
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_websocket_terminations_are_classified() {
        use sockudo_core::error::Error;

        assert!(is_expected_websocket_termination(&Error::ConnectionClosed(
            "gone".into()
        )));
        assert!(is_expected_websocket_termination(&Error::WebSocket(
            sockudo_ws::Error::ConnectionReset
        )));
        assert!(is_expected_websocket_termination(&Error::WebSocket(
            sockudo_ws::Error::HeartbeatTimeout
        )));
        assert!(is_expected_websocket_termination(&Error::WebSocket(
            sockudo_ws::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset",
            ))
        )));
        assert!(!is_expected_websocket_termination(&Error::WebSocket(
            sockudo_ws::Error::Protocol("bad")
        )));
        assert!(!is_expected_websocket_termination(&Error::WebSocket(
            sockudo_ws::Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad tls record",
            ))
        )));
        assert!(!is_expected_websocket_termination(&Error::Internal(
            "boom".into()
        )));
    }

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

    #[test]
    fn websocket_heartbeat_ownership_follows_protocol_version() {
        let server_options = sockudo_core::options::ServerOptions::default();

        let v2 = websocket_config_for_protocol(&server_options, ProtocolVersion::V2);
        assert!(v2.auto_ping);
        assert_eq!(v2.pong_timeout_close_code, 4201);

        let v1 = websocket_config_for_protocol(&server_options, ProtocolVersion::V1);
        assert!(!v1.auto_ping);
        assert_eq!(v1.idle_timeout, 0);
    }

    #[test]
    fn append_rollup_window_accepts_locked_values() {
        let rollup = sockudo_core::options::AiTransportRollupConfig::default();
        for value in [0, 20, 40, 100, 500] {
            assert!(rollup.allows_window(value));
        }
        for value in [1, 19, 60, 501] {
            assert!(!rollup.allows_window(value));
        }
    }

    #[test]
    fn append_mode_query_accepts_delta_and_full() {
        assert_eq!(
            AppendMode::parse_query_param(None).unwrap(),
            AppendMode::Delta
        );
        assert_eq!(
            AppendMode::parse_query_param(Some("delta")).unwrap(),
            AppendMode::Delta
        );
        assert_eq!(
            AppendMode::parse_query_param(Some("full")).unwrap(),
            AppendMode::Full
        );
        assert!(AppendMode::parse_query_param(Some("snapshot")).is_err());
    }
}
