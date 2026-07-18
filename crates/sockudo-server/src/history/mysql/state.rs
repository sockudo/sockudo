use super::*;

pub(super) fn is_truncated_by_retention(
    bounds: &HistoryQueryBounds,
    retained: &HistoryRetentionStats,
) -> bool {
    if let (Some(start_serial), Some(oldest_serial)) = (bounds.start_serial, retained.oldest_serial)
        && start_serial < oldest_serial
    {
        return true;
    }
    if let (Some(start_time_ms), Some(oldest_time_ms)) =
        (bounds.start_time_ms, retained.oldest_published_at_ms)
        && start_time_ms < oldest_time_ms
    {
        return true;
    }
    bounds.start_serial.is_none()
        && bounds.start_time_ms.is_none()
        && retained
            .oldest_serial
            .is_some_and(|oldest_serial| oldest_serial > 1)
}

pub(super) fn parse_history_durable_state(raw: &str) -> HistoryDurableState {
    match raw {
        "healthy" => HistoryDurableState::Healthy,
        "reset_required" => HistoryDurableState::ResetRequired,
        "degraded" => HistoryDurableState::Degraded,
        _ => HistoryDurableState::Degraded,
    }
}

pub(super) fn degraded_channel_key(app_id: &str, channel: &str) -> String {
    format!("{app_id}\0{channel}")
}

pub(super) fn degraded_cache_key(app_id: &str, channel: &str) -> String {
    format!("history:degraded:{app_id}:{channel}")
}

pub(super) fn resolve_runtime_state(
    durable_state: HistoryStreamRuntimeState,
    local_hint: Option<HistoryDegradedState>,
    cache_hint: Option<HistoryDegradedState>,
) -> HistoryStreamRuntimeState {
    let newest_hint = [local_hint, cache_hint]
        .into_iter()
        .flatten()
        .max_by_key(|hint| hint.last_transition_at_ms);
    let durable_transition_at = durable_state.last_transition_at_ms.unwrap_or_default();

    if let Some(hint) = newest_hint
        && hint.last_transition_at_ms > durable_transition_at
    {
        return HistoryStreamRuntimeState {
            app_id: durable_state.app_id,
            channel: durable_state.channel,
            stream_id: durable_state.stream_id,
            durable_state: hint.durable_state,
            recovery_allowed: hint.durable_state.recovery_allowed(),
            reset_required: hint.durable_state.reset_required(),
            reason: Some(hint.reason),
            node_id: hint.node_id,
            last_transition_at_ms: Some(hint.last_transition_at_ms),
            authoritative_source: durable_state.authoritative_source,
            observed_source: hint.observed_source.to_string(),
        };
    }

    durable_state
}

pub(super) fn increment_app_queue_depth(
    queue_depth_by_app: &DashMap<String, usize>,
    app_id: &str,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
) {
    let mut depth = 1usize;
    if let Some(mut entry) = queue_depth_by_app.get_mut(app_id) {
        *entry += 1;
        depth = *entry;
    } else {
        queue_depth_by_app.insert(app_id.to_string(), 1);
    }
    if let Some(metrics) = metrics {
        metrics.update_history_queue_depth(app_id, depth);
    }
}

pub(super) fn decrement_app_queue_depth(
    queue_depth_by_app: &DashMap<String, usize>,
    app_id: &str,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
) {
    let depth = if let Some(mut entry) = queue_depth_by_app.get_mut(app_id) {
        if *entry > 1 {
            *entry -= 1;
            *entry
        } else {
            drop(entry);
            queue_depth_by_app.remove(app_id);
            0
        }
    } else {
        0
    };
    if let Some(metrics) = metrics {
        metrics.update_history_queue_depth(app_id, depth);
    }
}

pub(super) struct DegradeRequest<'a> {
    pub(super) app_id: &'a str,
    pub(super) channel: &'a str,
    pub(super) reason: &'a str,
    pub(super) node_id: Option<String>,
}

pub(super) async fn mark_channel_degraded(
    pool: &MySqlPool,
    tables: &HistoryTables,
    degraded_channels: &DashMap<String, HistoryDegradedState>,
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
    request: DegradeRequest<'_>,
) {
    let now_ms = sockudo_core::history::now_ms();
    let state = HistoryDegradedState {
        app_id: request.app_id.to_string(),
        channel: request.channel.to_string(),
        durable_state: HistoryDurableState::Degraded,
        reason: request.reason.to_string(),
        node_id: request.node_id,
        last_transition_at_ms: now_ms,
        observed_source: "local_memory_hint",
    };
    degraded_channels.insert(
        degraded_channel_key(request.app_id, request.channel),
        state.clone(),
    );
    if let Some(cache) = cache_manager {
        let _ = cache
            .set(
                &degraded_cache_key(request.app_id, request.channel),
                &sonic_rs::to_string(&sonic_rs::json!({
                    "app_id": state.app_id,
                    "channel": state.channel,
                    "durable_state": state.durable_state.as_str(),
                    "reason": state.reason,
                    "node_id": state.node_id,
                    "last_transition_at_ms": state.last_transition_at_ms,
                }))
                .unwrap_or_else(|_| "{}".to_string()),
                3600,
            )
            .await;
    }
    let update_sql = format!(
        "UPDATE {} SET durable_state = ?, durable_state_reason = ?, durable_state_node_id = ?, durable_state_changed_at_ms = ? WHERE app_id = ? AND channel = ? AND IFNULL(durable_state_changed_at_ms, 0) <= ?",
        tables.streams
    );
    if let Err(err) = sqlx::query(sqlx::AssertSqlSafe(update_sql.as_str()))
        .bind(state.durable_state.as_str())
        .bind(&state.reason)
        .bind(&state.node_id)
        .bind(state.last_transition_at_ms)
        .bind(request.app_id)
        .bind(request.channel)
        .bind(state.last_transition_at_ms)
        .execute(pool)
        .await
    {
        error!(app_id = %request.app_id, channel = %request.channel, error = %err, "failed to persist mysql history degraded state");
    }
    if let Some(metrics) = metrics {
        let _ = refresh_history_state_metrics(pool, tables, metrics, request.app_id).await;
    }
}

pub(super) async fn get_cached_channel_degraded(
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    app_id: &str,
    channel: &str,
) -> Result<Option<HistoryDegradedState>> {
    if let Some(cache) = cache_manager
        && let Some(raw) = cache.get(&degraded_cache_key(app_id, channel)).await?
    {
        let value: sonic_rs::Value = sonic_rs::from_str(&raw).map_err(|e| {
            Error::Internal(format!("Failed to parse degraded MySQL history state: {e}"))
        })?;
        return Ok(Some(HistoryDegradedState {
            app_id: value
                .get("app_id")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or(app_id)
                .to_string(),
            channel: value
                .get("channel")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or(channel)
                .to_string(),
            durable_state: value
                .get("durable_state")
                .and_then(sonic_rs::Value::as_str)
                .map(parse_history_durable_state)
                .unwrap_or(HistoryDurableState::Degraded),
            reason: value
                .get("reason")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or("history_stream_degraded")
                .to_string(),
            node_id: value
                .get("node_id")
                .and_then(sonic_rs::Value::as_str)
                .map(str::to_string),
            last_transition_at_ms: value
                .get("last_transition_at_ms")
                .and_then(sonic_rs::Value::as_i64)
                .unwrap_or_default(),
            observed_source: "shared_cache_hint",
        }));
    }
    Ok(None)
}

pub(super) async fn refresh_history_state_metrics(
    pool: &MySqlPool,
    tables: &HistoryTables,
    metrics: &(dyn MetricsInterface + Send + Sync),
    app_id: &str,
) -> Result<()> {
    let sql = format!(
        "SELECT COALESCE(SUM(CASE WHEN durable_state <> 'healthy' THEN 1 ELSE 0 END), 0) AS degraded_channels, COALESCE(SUM(CASE WHEN durable_state = 'reset_required' THEN 1 ELSE 0 END), 0) AS reset_required_channels FROM {} WHERE app_id = ?",
        tables.streams
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(app_id)
        .fetch_one(pool)
        .await
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to refresh MySQL history state metrics: {e}"
            ))
        })?;
    metrics
        .update_history_degraded_channels(app_id, row.get::<i64, _>("degraded_channels") as usize);
    metrics.update_history_reset_required_channels(
        app_id,
        row.get::<i64, _>("reset_required_channels") as usize,
    );
    Ok(())
}
