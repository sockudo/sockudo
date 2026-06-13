//! Operational endpoints: usage/stats, liveness and health checks, Prometheus
//! metrics export, the 404 fallback, and shared API-metrics recording.

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::IntoResponse,
};
use serde::Serialize;
use sockudo_adapter::ConnectionHandler;
use sockudo_core::error::HealthStatus;
use std::{sync::Arc, time::Duration};
use sysinfo::System;
use tokio::time::timeout;
use tracing::{debug, error, field, info, instrument, warn};

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
        debug!(
            "{}",
            "Metrics system not available, skipping metrics recording."
        );
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

/// GET /stats
#[instrument(skip(handler), fields(service = "stats"))]
pub async fn stats(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
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

    for (app_id, namespace) in handler.connection_manager().get_namespaces().await? {
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

        for socket in namespace.sockets.iter() {
            if socket.value().get_user_id().await.is_some() {
                app_stat.authenticated_connections += 1;
            }
            if socket.value().get_connection_meta().await.is_some() {
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

    Ok((
        StatusCode::OK,
        Json(StatsResponse {
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

/// GET /up or /up/{app_id}
#[instrument(skip(handler), fields(app_id = field::Empty))]
pub async fn up(
    app_id: Option<Path<String>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    let timeout_duration = Duration::from_millis(handler.server_options().health_check_timeout_ms);

    let (health_status, app_id_str) = if let Some(Path(app_id)) = app_id {
        tracing::Span::current().record("app_id", &app_id);
        debug!("Health check received for app_id: {}", app_id);

        let app_check = timeout(timeout_duration, handler.app_manager().find_by_id(&app_id)).await;

        let app_status = match app_check {
            Ok(Ok(Some(app))) if app.enabled => {
                check_system_health(&handler, timeout_duration).await
            }
            Ok(Ok(Some(_))) => HealthStatus::Error(vec!["App is disabled".to_string()]),
            Ok(Ok(None)) => HealthStatus::NotFound,
            Ok(Err(e)) => HealthStatus::Error(vec![format!("App manager: {e}")]),
            Err(_) => HealthStatus::Error(vec![format!(
                "App manager timeout (>{}ms)",
                timeout_duration.as_millis()
            )]),
        };

        (app_status, app_id)
    } else {
        debug!("General health check received (no app_id)");

        let apps_check = timeout(timeout_duration, handler.app_manager().get_apps()).await;

        let app_status = match apps_check {
            Ok(Ok(apps)) if !apps.is_empty() => {
                debug!("Found {} configured apps", apps.len());
                check_system_health(&handler, timeout_duration).await
            }
            Ok(Ok(_)) => HealthStatus::Error(vec!["No apps configured".to_string()]),
            Ok(Err(e)) => HealthStatus::Error(vec![format!("App manager: {e}")]),
            Err(_) => HealthStatus::Error(vec![format!(
                "App manager timeout (>{}ms)",
                timeout_duration.as_millis()
            )]),
        };

        (app_status, "system".to_string())
    };

    match &health_status {
        HealthStatus::Ok => {
            debug!("Health check passed for {}", app_id_str);
        }
        HealthStatus::Degraded(reasons) => {
            warn!(
                "Health check degraded for {}: {}",
                app_id_str,
                reasons.join(", ")
            );
        }
        HealthStatus::Error(reasons) => {
            error!(
                "Health check failed for {}: {}",
                app_id_str,
                reasons.join(", ")
            );
        }
        HealthStatus::NotFound => {
            warn!("Health check for non-existent app_id: {}", app_id_str);
        }
    }

    let (status_code, status_text, header_value) = match health_status {
        HealthStatus::Ok => (StatusCode::OK, "OK", "OK"),
        HealthStatus::Degraded(_) => (StatusCode::OK, "DEGRADED", "DEGRADED"),
        HealthStatus::Error(_) => (StatusCode::SERVICE_UNAVAILABLE, "ERROR", "ERROR"),
        HealthStatus::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND", "NOT_FOUND"),
    };

    if handler.metrics().is_some() {
        let response_size = status_text.len();
        record_api_metrics(&handler, &app_id_str, 0, response_size).await;
    }

    let response_val = axum::http::Response::builder()
        .status(status_code)
        .header("X-Health-Check", header_value)
        .body(status_text.to_string())?;

    Ok(response_val)
}

/// Fallback handler for unmatched routes.
/// Returns a plain text 404.
pub async fn fallback_404(uri: Uri) -> impl IntoResponse {
    debug!("No route matched for: {}", uri);
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/plain")],
        "404 NOT FOUND",
    )
}

/// GET /metrics (Prometheus format)
#[instrument(skip(handler), fields(service = "metrics_exporter"))]
pub async fn metrics(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    debug!("{}", "Metrics endpoint called");
    let plaintext_metrics_str = match handler.metrics().clone() {
        Some(metrics_arc) => metrics_arc.get_metrics_as_plaintext().await,
        None => {
            info!(
                "{}",
                "No metrics data available (metrics collection is not enabled)."
            );
            "# Metrics collection is not enabled.\n".to_string()
        }
    };
    #[cfg(feature = "push")]
    let plaintext_metrics_str = {
        let mut plaintext_metrics_str = plaintext_metrics_str;
        plaintext_metrics_str.push_str(&crate::push_http::push_metrics_plaintext());
        plaintext_metrics_str
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
    use sonic_rs::{JsonValueTrait, Value};

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

        let response = stats(State(handler)).await.unwrap().into_response();
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

        let response = stats(State(handler)).await.unwrap().into_response();
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

        let response = stats(State(handler)).await.unwrap().into_response();
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
}
