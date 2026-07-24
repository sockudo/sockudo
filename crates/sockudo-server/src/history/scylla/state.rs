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

pub(super) fn lwt_applied(result: scylla::response::query_result::QueryResult) -> Result<bool> {
    let rows = result
        .into_rows_result()
        .map_err(|e| Error::Internal(format!("Failed to decode ScyllaDB LWT result: {e}")))?;
    match rows.column_specs().len() {
        1 => rows
            .single_row::<LwtApplyOnlyRow>()
            .map(|row| row.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB LWT apply-only row: {e}"
                ))
            }),
        3 => rows
            .single_row::<LwtConditionalRow>()
            .map(|row| row.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB LWT conditional row: {e}"
                ))
            }),
        16 => rows
            .single_row::<LwtStreamRow>()
            .map(|row| row.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB LWT stream row: {e}"
                ))
            }),
        columns => Err(Error::Internal(format!(
            "Unexpected ScyllaDB LWT result shape with {columns} columns"
        ))),
    }
}

pub(super) fn map_scylla_lwt_error(operation: &str, error: impl std::fmt::Display) -> Error {
    let error_text = error.to_string();
    if error_text.contains("not yet supported with tablets") {
        return Error::Configuration(format!(
            "ScyllaDB history cannot {operation}: the keyspace uses tablets, but this backend requires tablets disabled for LWT-based serial reservation"
        ));
    }
    Error::Internal(format!(
        "Failed to {operation} in ScyllaDB history: {error_text}"
    ))
}

pub(super) struct DegradeRequest<'a> {
    pub(super) app_id: &'a str,
    pub(super) channel: &'a str,
    pub(super) reason: &'a str,
    pub(super) node_id: Option<String>,
}

pub(super) async fn mark_channel_degraded(
    session: &Session,
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

    if let Ok(Some(current)) = {
        let query = format!(
            "SELECT stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms FROM {} WHERE app_id = ? AND channel = ?",
            tables.streams_fq()
        );
        async {
            session
                .query_unpaged(query, (request.app_id, request.channel))
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to read ScyllaDB stream before degrade: {e}"
                    ))
                })?
                .into_rows_result()
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to decode ScyllaDB stream before degrade: {e}"
                    ))
                })?
                .maybe_first_row::<StreamRow>()
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to deserialize ScyllaDB stream before degrade: {e}"
                    ))
                })
                .map(|row| {
                    row.map(|row| HistoryStreamRecord {
                        stream_id: row.stream_id,
                        next_serial: row.next_serial as u64,
                        retained_messages: row.retained_messages as u64,
                        retained_bytes: row.retained_bytes as u64,
                        oldest_serial: row.oldest_available_serial.map(|value| value as u64),
                        newest_serial: row.newest_available_serial.map(|value| value as u64),
                        oldest_published_at_ms: row.oldest_available_published_at_ms,
                        newest_published_at_ms: row.newest_available_published_at_ms,
                        durable_state: parse_history_durable_state(&row.durable_state),
                        durable_state_reason: row.durable_state_reason,
                        durable_state_node_id: row.durable_state_node_id,
                        durable_state_changed_at_ms: row.durable_state_changed_at_ms,
                    })
                })
        }
        .await
    } {
        let retained = current.retention_stats();
        let query = format!(
            "INSERT INTO {} (app_id, channel, stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tables.streams_fq()
        );
        if let Err(err) = session
            .query_unpaged(
                query,
                (
                    request.app_id,
                    request.channel,
                    current.stream_id.as_str(),
                    current.next_serial as i64,
                    state.durable_state.as_str(),
                    state.reason.as_str(),
                    state.node_id.as_deref(),
                    Some(state.last_transition_at_ms),
                    retained.retained_messages as i64,
                    retained.retained_bytes as i64,
                    retained.oldest_serial.map(|value| value as i64),
                    retained.newest_serial.map(|value| value as i64),
                    retained.oldest_published_at_ms,
                    retained.newest_published_at_ms,
                    now_ms,
                ),
            )
            .await
        {
            error!(app_id = %request.app_id, channel = %request.channel, error = %err, "failed to persist scylladb history degraded state");
        }
    }
    if let Some(metrics) = metrics {
        let _ = refresh_history_state_metrics(session, tables, metrics, request.app_id).await;
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
            Error::Internal(format!(
                "Failed to parse degraded ScyllaDB history state: {e}"
            ))
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
    session: &Session,
    tables: &HistoryTables,
    metrics: &(dyn MetricsInterface + Send + Sync),
    app_id: &str,
) -> Result<()> {
    let query = format!(
        "SELECT durable_state FROM {} WHERE app_id = ?",
        tables.streams_fq()
    );
    let pager = session
        .query_iter(query, (app_id,))
        .await
        .map_err(|e| Error::Internal(format!("Failed to stream ScyllaDB history metrics: {e}")))?;
    let mut rows_stream = pager
        .rows_stream::<DurableStateRow>()
        .map_err(|e| Error::Internal(format!("Failed to decode ScyllaDB history metrics: {e}")))?;
    let mut degraded = 0usize;
    let mut reset_required = 0usize;
    while let Some(row) = rows_stream
        .try_next()
        .await
        .map_err(|e| Error::Internal(format!("Failed to read ScyllaDB history metrics: {e}")))?
    {
        let state = parse_history_durable_state(&row.durable_state);
        if state != HistoryDurableState::Healthy {
            degraded += 1;
        }
        if state == HistoryDurableState::ResetRequired {
            reset_required += 1;
        }
    }
    metrics.update_history_degraded_channels(app_id, degraded);
    metrics.update_history_reset_required_channels(app_id, reset_required);
    Ok(())
}
