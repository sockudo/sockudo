//! Operational endpoints: usage/stats, liveness and health checks, Prometheus
//! metrics export, the 404 fallback, and shared API-metrics recording.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::IntoResponse,
};
use serde::Serialize;
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::error::HealthStatus;
use std::{sync::Arc, time::Duration};
use sysinfo::System;
use tokio::time::timeout;
use tracing::{debug, error, info, instrument, warn};

use super::AppError;
use super::history::HistoryStatusResponse;
use super::presence_history::PresenceHistoryStatusResponse;

// --- Response Structs ---

#[derive(Serialize)]
struct MemoryStats {
    free: u64,
    used: u64,
    total: u64,
    percent: f64,
}

#[derive(Serialize)]
struct UsageResponse {
    memory: MemoryStats,
}

#[derive(Serialize, Default)]
struct OccupancyStats {
    channels: usize,
    subscriptions: usize,
    presence_channels: usize,
    presence_subscriptions: usize,
    presence_members: usize,
}

#[derive(Serialize, Default)]
struct AppStats {
    app_id: String,
    connections: usize,
    authenticated_connections: usize,
    users: usize,
    connections_with_meta: usize,
    occupancy: OccupancyStats,
}

#[derive(Serialize, Default)]
struct GlobalStats {
    apps: usize,
    connections: usize,
    authenticated_connections: usize,
    users: usize,
    connections_with_meta: usize,
    occupancy: OccupancyStats,
}

#[derive(Serialize)]
struct StatsResponse {
    memory: MemoryStats,
    history: HistoryStatusResponse,
    presence_history: PresenceHistoryStatusResponse,
    totals: GlobalStats,
    apps: Vec<AppStats>,
    /// Present only for api-role pods.
    #[serde(skip_serializing_if = "Option::is_none")]
    server_role: Option<sockudo_core::options::ServerRole>,
}

/// Records API metrics (helper async function)
#[instrument(skip(handler, incoming_request_size, outgoing_response_size), fields(app_id = %app_id))]
pub(super) async fn record_api_metrics(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    incoming_request_size: usize,
    outgoing_response_size: usize,
) {
    if let Some(metrics_arc) = handler.metrics() {
        metrics_arc.mark_api_message(app_id, incoming_request_size, outgoing_response_size);
        debug!(
            incoming_bytes = incoming_request_size,
            outgoing_bytes = outgoing_response_size,
            "Recorded API message metrics"
        );
    } else {
        debug!("metrics system not available, skipping metrics recording");
    }
}

/// GET /usage
#[instrument(skip_all, fields(service = "usage_monitor"))]
pub async fn usage() -> Result<impl IntoResponse, AppError> {
    let mut sys = System::new_all();
    sys.refresh_all();

    let total = sys.total_memory() * 1024;
    let used = sys.used_memory() * 1024;
    let free = total.saturating_sub(used);
    let percent = if total > 0 {
        (used as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    let memory_stats = MemoryStats {
        free,
        used,
        total,
        percent,
    };
    let response_payload = UsageResponse {
        memory: memory_stats,
    };

    info!(
        total_bytes = total,
        used_bytes = used,
        free_bytes = free,
        usage_percent = format!("{:.2}", percent),
        "Memory usage queried"
    );

    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /stats and /operator/stats on the client-facing listener.
///
/// Requires Pusher HTTP API signature auth (`pusher_api_auth_middleware`) and reports only
/// the signing app: one tenant's credentials never reveal other tenants' ids or occupancy
/// (GHSA-xfrj-v26g-242j). Memory and history health are process-wide and unchanged.
#[instrument(skip(handler, app), fields(service = "stats", app_id = %app.id))]
pub async fn stats(
    State(handler): State<Arc<ConnectionHandler>>,
    Extension(app): Extension<App>,
) -> Result<impl IntoResponse, AppError> {
    build_stats(&handler, Some(&app.id)).await
}

/// GET /stats on the internal metrics listener: every app, no credentials.
///
/// The metrics listener already exports every `app_id` through `/metrics`, so the
/// cross-tenant operator view adds no exposure there. Keep that listener private.
#[instrument(skip(handler), fields(service = "operator_stats"))]
pub async fn operator_stats(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    build_stats(&handler, None).await
}

/// Build the stats payload for one app (`Some`) or every app (`None`).
async fn build_stats(
    handler: &Arc<ConnectionHandler>,
    only_app: Option<&str>,
) -> Result<(StatusCode, Json<StatsResponse>), AppError> {
    let mut sys = System::new_all();
    sys.refresh_all();

    let total = sys.total_memory() * 1024;
    let used = sys.used_memory() * 1024;
    let free = total.saturating_sub(used);
    let percent = if total > 0 {
        (used as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    let memory = MemoryStats {
        free,
        used,
        total,
        percent,
    };
    let history_status = handler
        .history_store()
        .runtime_status()
        .await
        .unwrap_or_default();
    let presence_history_status = handler
        .presence_history_store()
        .runtime_status()
        .await
        .unwrap_or_default();

    let mut app_stats = Vec::new();
    let mut totals = GlobalStats::default();

    let namespaces: Vec<_> = handler
        .connection_manager()
        .get_namespaces()
        .await?
        .into_iter()
        .filter(|(app_id, _)| only_app.is_none_or(|only| only == app_id))
        .collect();
    if let Some(only) = only_app
        && namespaces.is_empty()
    {
        totals.apps = 1;
        app_stats.push(AppStats {
            app_id: only.to_string(),
            ..Default::default()
        });
    }
    for (app_id, namespace) in namespaces {
        let mut app_stat = AppStats {
            app_id,
            connections: namespace.sockets.len(),
            users: namespace.users.len(),
            ..Default::default()
        };

        let channel_names: Vec<String> = namespace
            .channels
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        app_stat.occupancy.channels = channel_names.len();
        app_stat.occupancy.subscriptions = namespace.channels.iter().map(|entry| entry.len()).sum();

        // Snapshotted first: holding this iter's shard guard across the awaits below
        // can deadlock against add_socket()'s insert on the same shard.
        let socket_refs: Vec<_> = namespace
            .sockets
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        for socket in socket_refs {
            if socket.get_user_id().await.is_some() {
                app_stat.authenticated_connections += 1;
            }
            if socket.get_connection_meta().await.is_some() {
                app_stat.connections_with_meta += 1;
            }
        }

        for channel_name in channel_names {
            if channel_name.starts_with("presence-") {
                app_stat.occupancy.presence_channels += 1;
                app_stat.occupancy.presence_subscriptions +=
                    namespace.get_channel_socket_count(&channel_name);
                app_stat.occupancy.presence_members +=
                    namespace.get_channel_members(&channel_name)?.len();
            }
        }

        totals.apps += 1;
        totals.connections += app_stat.connections;
        totals.authenticated_connections += app_stat.authenticated_connections;
        totals.users += app_stat.users;
        totals.connections_with_meta += app_stat.connections_with_meta;
        totals.occupancy.channels += app_stat.occupancy.channels;
        totals.occupancy.subscriptions += app_stat.occupancy.subscriptions;
        totals.occupancy.presence_channels += app_stat.occupancy.presence_channels;
        totals.occupancy.presence_subscriptions += app_stat.occupancy.presence_subscriptions;
        totals.occupancy.presence_members += app_stat.occupancy.presence_members;

        app_stats.push(app_stat);
    }

    app_stats.sort_by(|a, b| a.app_id.cmp(&b.app_id));

    let server_role = handler.server_options().server_role;

    Ok((
        StatusCode::OK,
        Json(StatsResponse {
            server_role: server_role.is_api().then_some(server_role),
            memory,
            history: HistoryStatusResponse {
                enabled: history_status.enabled,
                backend: history_status.backend,
                state_authority: history_status.state_authority,
                degraded_channels: history_status.degraded_channels,
                reset_required_channels: history_status.reset_required_channels,
                queue_depth: history_status.queue_depth,
            },
            presence_history: PresenceHistoryStatusResponse {
                enabled: presence_history_status.enabled,
                backend: presence_history_status.backend,
                state_authority: presence_history_status.state_authority,
                degraded_channels: presence_history_status.degraded_channels,
                reset_required_channels: presence_history_status.reset_required_channels,
                queue_depth: presence_history_status.queue_depth,
            },
            totals,
            apps: app_stats,
        }),
    ))
}

/// System health check function. Subsystem checks (adapter, cache, webhook
/// queue) run in parallel under per-check timeouts so a slow subsystem cannot
/// starve the others.
async fn check_system_health(
    handler: &Arc<ConnectionHandler>,
    timeout_duration: Duration,
) -> HealthStatus {
    let cache_enabled =
        handler.server_options().cache.driver != sockudo_core::options::CacheDriver::None;
    let webhook_integration = handler.webhook_integration();

    let adapter_fut = timeout(
        timeout_duration,
        handler.connection_manager().check_health(),
    );
    let cache_fut = async {
        if !cache_enabled {
            return None;
        }
        Some(timeout(timeout_duration, handler.cache_manager().check_health()).await)
    };
    let queue_fut = async {
        let Some(integration) = webhook_integration else {
            return None;
        };
        Some(timeout(timeout_duration, integration.check_queue_health()).await)
    };

    let (adapter_check, cache_check, queue_check) = tokio::join!(adapter_fut, cache_fut, queue_fut);

    let mut critical_issues = Vec::new();
    let mut non_critical_issues = Vec::new();

    // CRITICAL CHECK 1: Adapter health
    match adapter_check {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            critical_issues.push(format!("Adapter: {e}"));
        }
        Err(_) => {
            critical_issues.push("Adapter health check timeout".to_string());
        }
    }

    // CRITICAL CHECK 2: Cache manager health
    if let Some(cache_check) = cache_check {
        match cache_check {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                critical_issues.push(format!("Cache: {e}"));
            }
            Err(_) => {
                critical_issues.push("Cache health check timeout".to_string());
            }
        }
    }

    // NON-CRITICAL CHECK: Queue system health
    if let Some(queue_check) = queue_check {
        match queue_check {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                non_critical_issues.push(format!("Webhooks: {e}"));
            }
            Err(_) => {
                non_critical_issues.push("Webhook queue health check timeout".to_string());
            }
        }
    }

    if !critical_issues.is_empty() {
        HealthStatus::Error(critical_issues)
    } else if !non_critical_issues.is_empty() {
        HealthStatus::Degraded(non_critical_issues)
    } else {
        HealthStatus::Ok
    }
}

/// GET /live
pub async fn live() -> StatusCode {
    StatusCode::OK
}

/// GET /accept-traffic
///
/// This admission probe is intentionally separate from `/up`: memory pressure
/// stops new connections without declaring the process or its dependencies dead.
pub async fn accept_traffic(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    let snapshot = handler.memory_pressure_snapshot();
    let monitor_state = if !snapshot.enabled {
        "DISABLED"
    } else if !snapshot.sample_available {
        "UNAVAILABLE"
    } else if snapshot.shedding {
        "SHEDDING"
    } else {
        "AVAILABLE"
    };

    let (status, admission_state) = if !handler.is_accepting() {
        (StatusCode::SERVICE_UNAVAILABLE, "DRAINING")
    } else if snapshot.shedding {
        (StatusCode::SERVICE_UNAVAILABLE, "MEMORY_PRESSURE")
    } else {
        (StatusCode::OK, "ACCEPTING")
    };

    Ok(axum::http::Response::builder()
        .status(status)
        .header("X-Accept-Traffic", admission_state)
        .header("X-Memory-Pressure-Monitor", monitor_state)
        .header("X-Memory-Limit-Source", snapshot.limit_source)
        .body(admission_state.to_string())?)
}

/// GET /ready: capacity-aware load-balancer probe with the same dependency checks as `/up`.
pub async fn ready(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<axum::response::Response, AppError> {
    if handler.is_accepting() && !handler.is_capacity_ready() {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            [("X-Health-Check", "CAPACITY")],
            "CAPACITY",
        )
            .into_response());
    }
    Ok(up(None, State(handler)).await?.into_response())
}

/// GET /up or /up/{app_id}
// `debug`, matching `telemetry::trace_http_request`, which already skips the probe paths: at `info`
// every readiness probe exported a root span to OpenTelemetry.
#[instrument(level = "debug", skip(handler, app_id))]
pub async fn up(
    app_id: Option<Path<String>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<axum::http::Response<String>, AppError> {
    // Once shutdown begins, fail readiness so the orchestrator removes this pod
    // from its load-balancer endpoints and routes no new connections here while
    // existing ones drain.
    if !handler.is_accepting() {
        return Ok(axum::http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("X-Health-Check", "DRAINING")
            .body("DRAINING".to_string())?);
    }

    let timeout_duration = Duration::from_millis(handler.server_options().health_check_timeout_ms);

    // `/up/{appId}` is kept for existing readiness probes but answers exactly like `/up`.
    // Unauthenticated callers must not learn which app ids exist (or are disabled), and the
    // caller-supplied id is never used as a metric label (GHSA-xfrj-v26g-242j).
    if let Some(Path(app_id)) = app_id.as_ref() {
        debug!(
            requested_app_id_len = app_id.len(),
            "app health check received"
        );
    }
    let app_id_str = "system";
    let apps_check = timeout(timeout_duration, handler.app_manager().get_apps()).await;
    let health_status = match apps_check {
        Ok(Ok(apps)) if !apps.is_empty() => {
            debug!(apps_count = apps.len(), "found configured apps");
            check_system_health(&handler, timeout_duration).await
        }
        Ok(Ok(_)) => HealthStatus::Error(vec!["No apps configured".to_string()]),
        Ok(Err(e)) => HealthStatus::Error(vec![format!("App manager: {e}")]),
        Err(_) => HealthStatus::Error(vec![format!(
            "App manager timeout (>{}ms)",
            timeout_duration.as_millis()
        )]),
    };

    health_response(health_status, app_id_str)
}

/// GET /up/{appId} on the internal metrics listener.
///
/// Keeps the pre-5.2.1 app-specific readiness contract (`404` unknown, `503` disabled) for
/// probes and operators on the private listener. Only ids of existing apps become metric
/// labels, so cardinality stays bounded by the configured apps.
#[instrument(level = "debug", skip(handler, app_id))]
pub async fn app_health(
    Path(app_id): Path<String>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<axum::http::Response<String>, AppError> {
    if !handler.is_accepting() {
        return Ok(axum::http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("X-Health-Check", "DRAINING")
            .body("DRAINING".to_string())?);
    }

    let timeout_duration = Duration::from_millis(handler.server_options().health_check_timeout_ms);
    let app_check = timeout(timeout_duration, handler.app_manager().find_by_id(&app_id)).await;
    let (health_status, label) = match app_check {
        Ok(Ok(Some(app))) if app.enabled => (
            check_system_health(&handler, timeout_duration).await,
            app.id,
        ),
        Ok(Ok(Some(app))) => (
            HealthStatus::Error(vec!["App is disabled".to_string()]),
            app.id,
        ),
        Ok(Ok(None)) => (HealthStatus::NotFound, "system".to_string()),
        Ok(Err(e)) => (
            HealthStatus::Error(vec![format!("App manager: {e}")]),
            "system".to_string(),
        ),
        Err(_) => (
            HealthStatus::Error(vec![format!(
                "App manager timeout (>{}ms)",
                timeout_duration.as_millis()
            )]),
            "system".to_string(),
        ),
    };

    health_response(health_status, &label)
}

fn health_response(
    health_status: HealthStatus,
    metric_label: &str,
) -> Result<axum::http::Response<String>, AppError> {
    match &health_status {
        HealthStatus::Ok => {
            debug!(app_id = %metric_label, "health check passed");
        }
        HealthStatus::Degraded(reasons) => {
            warn!(
                app_id = %metric_label,
                reasons = %reasons.join(", "),
                "health check degraded"
            );
        }
        HealthStatus::Error(reasons) => {
            error!(
                app_id = %metric_label,
                reasons = %reasons.join(", "),
                "health check failed"
            );
        }
        HealthStatus::NotFound => {
            debug!("health check for non-existent app");
        }
    }

    let (status_code, status_text, header_value) = match health_status {
        HealthStatus::Ok => (StatusCode::OK, "OK", "OK"),
        HealthStatus::Degraded(_) => (StatusCode::OK, "DEGRADED", "DEGRADED"),
        HealthStatus::Error(_) => (StatusCode::SERVICE_UNAVAILABLE, "ERROR", "ERROR"),
        HealthStatus::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND", "NOT_FOUND"),
    };

    Ok(axum::http::Response::builder()
        .status(status_code)
        .header("X-Health-Check", header_value)
        .body(status_text.to_string())?)
}

/// Fallback handler for unmatched routes.
/// Returns a plain text 404.
pub async fn fallback_404(uri: Uri) -> impl IntoResponse {
    debug!(uri = %uri, "no route matched");
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/plain")],
        "404 NOT FOUND",
    )
}

/// GET /metrics (Prometheus format)
// `debug` for the same reason as `up`: at `info` every Prometheus scrape exported a root span.
#[instrument(level = "debug", skip(handler), fields(service = "metrics_exporter"))]
pub async fn metrics(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    debug!("metrics endpoint called");
    let plaintext_metrics_str = match handler.metrics().clone() {
        Some(metrics_arc) => metrics_arc.get_metrics_as_plaintext().await,
        None => {
            info!("metrics collection not enabled");
            "# Metrics collection is not enabled.\n".to_string()
        }
    };
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    debug!(
        bytes = plaintext_metrics_str.len(),
        "Successfully generated Prometheus metrics"
    );
    Ok((StatusCode::OK, response_headers, plaintext_metrics_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_handler::test_support::*;
    use sockudo_adapter::ConnectionHandlerBuilder;
    use sockudo_adapter::local_adapter::LocalAdapter;
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_cache::memory_cache_manager::MemoryCacheManager;
    use sockudo_core::app::AppManager;
    use sockudo_core::namespace::Namespace;
    use sockudo_core::options::MemoryCacheOptions;
    use sockudo_core::presence_history::{
        MemoryPresenceHistoryStore, PresenceHistoryDurableState, PresenceHistoryStreamRuntimeState,
    };
    use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

    #[tokio::test]
    async fn ready_reports_capacity_without_changing_up_and_recovers_on_disconnect() {
        use futures_util::StreamExt;
        use sockudo_adapter::ConnectionManager;
        use sockudo_protocol::{AppendMode, ProtocolVersion, WireFormat};
        use sockudo_ws::axum_integration::WebSocket;
        use sockudo_ws::client::WebSocketClient;
        use sockudo_ws::{Config, Http1};
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::net::{TcpListener, TcpStream};

        let apps = Arc::new(MemoryAppManager::new());
        apps.create_app(test_app()).await.unwrap();
        let adapter = Arc::new(LocalAdapter::new());
        adapter.init().await;
        let running = Arc::new(AtomicBool::new(true));
        let handler = Arc::new(
            ConnectionHandlerBuilder::new(
                apps,
                adapter.clone(),
                Arc::new(MemoryCacheManager::new(
                    "ready-test".into(),
                    MemoryCacheOptions::default(),
                )),
                sockudo_core::options::ServerOptions {
                    max_connections: 1,
                    ..Default::default()
                },
            )
            .local_adapter(adapter)
            .running(running.clone())
            .build(),
        );
        assert_eq!(
            ready(State(handler.clone())).await.unwrap().status(),
            StatusCode::OK
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handshake_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            sockudo_ws::handshake::server_handshake(&mut stream)
                .await
                .unwrap();
            WebSocket::from_tcp(stream, Config::default())
        });
        let (mut client, _) = WebSocketClient::<Http1>::new(Config::default())
            .connect(
                TcpStream::connect(address).await.unwrap(),
                &address.to_string(),
                "/",
                None,
            )
            .await
            .unwrap();
        let socket = handshake_task.await.unwrap();
        let socket_handler = handler.clone();
        let socket_task = tokio::spawn(async move {
            socket_handler
                .handle_socket(
                    socket,
                    "key".into(),
                    None,
                    ProtocolVersion::V1,
                    WireFormat::Json,
                    true,
                    AppendMode::Delta,
                    None,
                )
                .await
        });
        timeout(Duration::from_secs(2), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let response = ready(State(handler.clone())).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["X-Health-Check"], "CAPACITY");
        assert_eq!(
            up(None, State(handler.clone()))
                .await
                .unwrap()
                .into_response()
                .status(),
            StatusCode::OK
        );

        drop(client);
        timeout(Duration::from_secs(2), socket_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            ready(State(handler.clone())).await.unwrap().status(),
            StatusCode::OK
        );
        running.store(false, Ordering::Release);
        let response = ready(State(handler)).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["X-Health-Check"], "DRAINING");
    }

    #[tokio::test]
    async fn accept_traffic_fails_open_when_memory_admission_is_disabled() {
        let handler = Arc::new(
            ConnectionHandlerBuilder::new(
                Arc::new(MemoryAppManager::new()),
                Arc::new(LocalAdapter::new()),
                Arc::new(MemoryCacheManager::new(
                    "test".to_string(),
                    MemoryCacheOptions::default(),
                )),
                sockudo_core::options::ServerOptions::default(),
            )
            .build(),
        );

        let response = accept_traffic(State(handler))
            .await
            .unwrap()
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["X-Memory-Pressure-Monitor"], "DISABLED");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn accept_traffic_reports_memory_pressure_without_changing_readiness() {
        let mut options = sockudo_core::options::ServerOptions::default();
        options.http_api.accept_traffic.enabled = true;
        options.http_api.accept_traffic.memory_limit_bytes = Some(1);

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(
                Arc::new(MemoryAppManager::new()),
                Arc::new(LocalAdapter::new()),
                Arc::new(MemoryCacheManager::new(
                    "test".to_string(),
                    MemoryCacheOptions::default(),
                )),
                options,
            )
            .build(),
        );

        assert!(
            handler.is_accepting(),
            "memory pressure must not change /up"
        );
        assert!(handler.is_memory_pressure_shedding());
        let response = accept_traffic(State(handler))
            .await
            .unwrap()
            .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["X-Accept-Traffic"], "MEMORY_PRESSURE");
    }

    #[tokio::test]
    async fn stats_endpoint_returns_empty_totals_for_empty_server() {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(
                app_manager,
                adapter,
                cache,
                sockudo_core::options::ServerOptions::default(),
            )
            .build(),
        );

        let response = stats(State(handler), Extension(test_app()))
            .await
            .unwrap()
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn stats_endpoint_reports_non_empty_occupancy_counts() {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let concrete_adapter = Arc::new(LocalAdapter::new());

        let namespace = Arc::new(Namespace::new("app-1".to_string()));
        namespace.add_channel_to_socket(
            "chat:room-1",
            &sockudo_core::websocket::SocketId::from_string("1.1").unwrap(),
        );
        namespace.add_channel_to_socket(
            "chat:room-1",
            &sockudo_core::websocket::SocketId::from_string("1.2").unwrap(),
        );
        namespace.add_channel_to_socket(
            "presence-chat:room-1",
            &sockudo_core::websocket::SocketId::from_string("2.1").unwrap(),
        );

        concrete_adapter
            .namespaces
            .insert("app-1".to_string(), namespace);

        let adapter = concrete_adapter as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(
                app_manager,
                adapter,
                cache,
                sockudo_core::options::ServerOptions::default(),
            )
            .build(),
        );

        let response = stats(State(handler), Extension(test_app()))
            .await
            .unwrap()
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();

        assert_eq!(json["totals"]["apps"].as_u64(), Some(1));
        assert_eq!(json["totals"]["occupancy"]["channels"].as_u64(), Some(2));
        assert_eq!(
            json["totals"]["occupancy"]["subscriptions"].as_u64(),
            Some(3)
        );
        assert_eq!(
            json["totals"]["occupancy"]["presence_channels"].as_u64(),
            Some(1)
        );
        assert_eq!(
            json["apps"][0]["occupancy"]["subscriptions"].as_u64(),
            Some(3)
        );
        assert_eq!(
            json["history"]["state_authority"].as_str(),
            Some("disabled")
        );
        assert_eq!(json["history"]["reset_required_channels"].as_u64(), Some(0));
        assert_eq!(
            json["presence_history"]["state_authority"].as_str(),
            Some("disabled")
        );
        assert_eq!(
            json["presence_history"]["reset_required_channels"].as_u64(),
            Some(0)
        );
    }

    // --- GHSA-xfrj-v26g-242j -------------------------------------------------

    fn tenant_app(id: &str) -> sockudo_core::app::App {
        sockudo_core::app::App::from_policy(
            id.to_string(),
            format!("{id}-key"),
            format!("{id}-secret"),
            true,
            Default::default(),
        )
    }

    /// Pusher HTTP API signature for a GET request with no extra query parameters.
    fn signed_get_uri(app: &sockudo_core::app::App, path: &str) -> String {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();
        let query = format!(
            "auth_key={}&auth_timestamp={timestamp}&auth_version=1.0",
            app.key
        );
        let signature = sockudo_core::token::Token::new(app.key.clone(), app.secret.clone())
            .sign(&format!("GET\n{path}\n{query}"));
        format!("{path}?{query}&auth_signature={signature}")
    }

    async fn multi_tenant_handler(
        metrics: Option<Arc<dyn sockudo_core::metrics::MetricsInterface + Send + Sync>>,
    ) -> Arc<ConnectionHandler> {
        multi_tenant_handler_with_options(metrics, sockudo_core::options::ServerOptions::default())
            .await
    }

    async fn multi_tenant_handler_with_options(
        metrics: Option<Arc<dyn sockudo_core::metrics::MetricsInterface + Send + Sync>>,
        options: sockudo_core::options::ServerOptions,
    ) -> Arc<ConnectionHandler> {
        let app_manager = Arc::new(MemoryAppManager::new());
        app_manager
            .create_app(tenant_app("tenant-a"))
            .await
            .unwrap();
        app_manager
            .create_app(tenant_app("tenant-b"))
            .await
            .unwrap();
        let adapter = Arc::new(LocalAdapter::new());
        for (app_id, socket) in [("tenant-a", "1.1"), ("tenant-b", "2.1")] {
            let namespace = Arc::new(Namespace::new(app_id.to_string()));
            namespace.add_channel_to_socket(
                "orders",
                &sockudo_core::websocket::SocketId::from_string(socket).unwrap(),
            );
            adapter.namespaces.insert(app_id.to_string(), namespace);
        }
        let mut builder = ConnectionHandlerBuilder::new(
            app_manager,
            adapter,
            Arc::new(MemoryCacheManager::new(
                "test".to_string(),
                MemoryCacheOptions::default(),
            )),
            options,
        );
        if let Some(metrics) = metrics {
            builder = builder.metrics(metrics);
        }
        Arc::new(builder.build())
    }

    async fn get(router: &axum::Router, uri: &str) -> (StatusCode, String) {
        use tower::ServiceExt;
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    fn stats_router(handler: &Arc<ConnectionHandler>) -> axum::Router {
        axum::Router::new()
            .route(
                "/operator/stats",
                crate::bootstrap::router::operator_stats_route(handler),
            )
            .route(
                "/stats",
                crate::bootstrap::router::operator_stats_route(handler),
            )
            .route("/up/{appId}", axum::routing::get(up))
            .route("/up", axum::routing::get(up))
            .with_state(handler.clone())
    }

    #[tokio::test]
    async fn stats_routes_require_signed_api_auth() {
        let handler = multi_tenant_handler(None).await;
        let router = stats_router(&handler);
        for path in ["/stats", "/operator/stats"] {
            let (status, body) = get(&router, path).await;
            assert!(
                status.is_client_error(),
                "{path} without credentials returned {status}: {body}"
            );
            assert!(
                !body.contains("tenant-a") && !body.contains("tenant-b"),
                "{path} leaked tenants: {body}"
            );

            let (status, body) = get(
                &router,
                &format!("{path}?auth_key=tenant-a-key&auth_timestamp=1&auth_signature=bad"),
            )
            .await;
            assert!(
                status.is_client_error(),
                "{path} forged signature: {status}"
            );
            assert!(!body.contains("tenant-b"), "{path} leaked tenants: {body}");
        }
    }

    /// Greptile on sockudo/sockudo#558: unauthenticated callers must not make the server
    /// buffer a request body, and authenticated buffering is capped.
    #[tokio::test]
    async fn signed_api_auth_never_buffers_unbounded_bodies() {
        use tower::ServiceExt;
        let mut options = sockudo_core::options::ServerOptions::default();
        options.http_api.request_limit_in_mb = 1;
        let handler = multi_tenant_handler_with_options(None, options).await;
        let router = stats_router(&handler);

        // No credentials and a body that never ends: rejected without reading it.
        let never_ending = axum::body::Body::from_stream(futures_util::stream::pending::<
            Result<axum::body::Bytes, std::io::Error>,
        >());
        let response = timeout(
            Duration::from_secs(2),
            router.clone().oneshot(
                axum::http::Request::builder()
                    .uri("/operator/stats")
                    .body(never_ending)
                    .unwrap(),
            ),
        )
        .await
        .expect("unsigned request must be rejected before its body is read")
        .unwrap();
        assert!(response.status().is_client_error(), "{}", response.status());

        // Auth parameters present but the body exceeds request_limit_in_mb.
        let oversized = axum::body::Body::from(vec![b'x'; 2 * 1024 * 1024]);
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(
                        "/operator/stats?auth_key=tenant-a-key&auth_timestamp=1&auth_signature=bad",
                    )
                    .body(oversized)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn signed_stats_request_only_sees_the_signing_tenant() {
        let handler = multi_tenant_handler(None).await;
        let router = stats_router(&handler);
        for path in ["/stats", "/operator/stats"] {
            let (status, body) = get(&router, &signed_get_uri(&tenant_app("tenant-a"), path)).await;
            assert_eq!(status, StatusCode::OK, "{path}: {body}");
            let json: Value = sonic_rs::from_str(&body).unwrap();
            assert_eq!(json["apps"].as_array().unwrap().len(), 1, "{body}");
            assert_eq!(json["apps"][0]["app_id"].as_str(), Some("tenant-a"));
            assert_eq!(json["totals"]["apps"].as_u64(), Some(1));
            assert_eq!(
                json["totals"]["occupancy"]["subscriptions"].as_u64(),
                Some(1)
            );
            assert!(!body.contains("tenant-b"), "{path} leaked tenant-b: {body}");
        }
    }

    #[tokio::test]
    async fn app_health_probe_does_not_reveal_which_app_ids_exist() {
        let metrics =
            Arc::new(sockudo_metrics::PrometheusMetricsDriver::new(0, Some("ghsa_up_test_")).await);
        let handler = multi_tenant_handler(Some(metrics.clone())).await;
        let router = stats_router(&handler);

        let (known_status, known_body) = get(&router, "/up/tenant-a").await;
        let (system_status, system_body) = get(&router, "/up").await;
        for probe in 0..5 {
            let (status, body) = get(&router, &format!("/up/probe-{probe}")).await;
            assert_eq!(
                (status, body.as_str()),
                (known_status, known_body.as_str()),
                "unknown app ids must be indistinguishable from known ones"
            );
        }
        assert_eq!((known_status, known_body), (system_status, system_body));

        let exported =
            sockudo_core::metrics::MetricsInterface::get_metrics_as_plaintext(metrics.as_ref())
                .await;
        assert!(
            !exported.contains("probe-"),
            "unvalidated app ids must never become metric labels:\n{exported}"
        );
    }

    #[tokio::test]
    async fn metrics_listener_keeps_operator_stats_and_app_specific_health() {
        let metrics = Arc::new(
            sockudo_metrics::PrometheusMetricsDriver::new(0, Some("ghsa_internal_test_")).await,
        );
        let handler = multi_tenant_handler(Some(metrics.clone())).await;
        let router = crate::bootstrap::router::metrics_listener_router(&handler, true);

        let (status, body) = get(&router, "/stats").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let json: Value = sonic_rs::from_str(&body).unwrap();
        assert_eq!(json["totals"]["apps"].as_u64(), Some(2), "{body}");
        assert_eq!(json["apps"][0]["app_id"].as_str(), Some("tenant-a"));
        assert_eq!(json["apps"][1]["app_id"].as_str(), Some("tenant-b"));

        assert_eq!(get(&router, "/up/tenant-a").await.0, StatusCode::OK);
        assert_eq!(get(&router, "/up/probe-1").await.0, StatusCode::NOT_FOUND);
        assert_eq!(get(&router, "/usage").await.0, StatusCode::OK);
        assert_eq!(get(&router, "/metrics").await.0, StatusCode::OK);

        let exported =
            sockudo_core::metrics::MetricsInterface::get_metrics_as_plaintext(metrics.as_ref())
                .await;
        assert!(
            !exported.contains("probe-1"),
            "unknown app ids must never become metric labels:\n{exported}"
        );
    }

    #[tokio::test]
    async fn metrics_listener_omits_stats_when_usage_is_disabled() {
        let handler = multi_tenant_handler(None).await;
        let router = crate::bootstrap::router::metrics_listener_router(&handler, false);
        assert_eq!(get(&router, "/stats").await.0, StatusCode::NOT_FOUND);
        assert_eq!(get(&router, "/usage").await.0, StatusCode::NOT_FOUND);
        assert_eq!(get(&router, "/up/tenant-a").await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn stats_endpoint_reports_presence_history_health_summary() {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let presence_store = Arc::new(InspectablePresenceHistoryStore {
            inner,
            state: PresenceHistoryStreamRuntimeState {
                app_id: "app-1".to_string(),
                channel: "presence-room".to_string(),
                stream_id: Some("presence-stream-9".to_string()),
                durable_state: PresenceHistoryDurableState::ResetRequired,
                continuity_proven: false,
                reset_required: true,
                reason: Some("presence_history_reset_required_after_write_failure".to_string()),
                node_id: Some("node-a".to_string()),
                last_transition_at_ms: Some(4321),
                authoritative_source: "test_state".to_string(),
                observed_source: "test_state".to_string(),
            },
        });

        let mut options = sockudo_core::options::ServerOptions::default();
        options.presence_history.enabled = true;

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
                .presence_history_store(presence_store)
                .build(),
        );

        let response = stats(State(handler), Extension(test_app()))
            .await
            .unwrap()
            .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();

        assert_eq!(
            json["presence_history"]["state_authority"].as_str(),
            Some("test_state")
        );
        assert_eq!(
            json["presence_history"]["degraded_channels"].as_u64(),
            Some(1)
        );
        assert_eq!(
            json["presence_history"]["reset_required_channels"].as_u64(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn health_probes_do_not_count_as_api_calls() {
        let metrics = Arc::new(
            sockudo_metrics::PrometheusMetricsDriver::new(0, Some("probe_count_test_")).await,
        );
        let handler = multi_tenant_handler(Some(metrics.clone())).await;
        let router = axum::Router::new()
            .route("/up", axum::routing::get(up))
            .route("/up/{appId}", axum::routing::get(app_health))
            .with_state(handler.clone());

        assert_eq!(get(&router, "/up").await.0, StatusCode::OK);
        assert_eq!(get(&router, "/up/tenant-a").await.0, StatusCode::OK);
        assert_eq!(get(&router, "/up/probe-9").await.0, StatusCode::NOT_FOUND);

        let exported =
            sockudo_core::metrics::MetricsInterface::get_metrics_as_plaintext(metrics.as_ref())
                .await;
        // The registry is process-global: assert on this test's prefix, which renders
        // nothing unless a probe incremented the counters.
        assert!(
            !exported.contains("probe_count_test_http_calls_received_total"),
            "probes must not increment the REST API call counter:\n{exported}"
        );
        assert!(
            !exported.contains("probe_count_test_http_transmitted_bytes"),
            "probes must not increment the REST API byte counters:\n{exported}"
        );
    }
}
