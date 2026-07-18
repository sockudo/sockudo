use super::*;

pub(super) fn validate_identifier(identifier: &str, field_name: &str) -> Result<()> {
    let valid = identifier.chars().enumerate().all(|(idx, ch)| {
        if idx == 0 {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        }
    });
    if valid {
        Ok(())
    } else {
        Err(Error::Internal(format!(
            "Invalid SurrealDB {field_name} '{identifier}'. Use only letters, numbers, and underscores."
        )))
    }
}

pub(super) fn deterministic_key<I, S>(parts: I) -> String
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let mut key = String::new();
    for part in parts {
        let part = part.as_ref();
        key.push_str(&part.len().to_string());
        key.push(':');
        key.push_str(part);
        key.push('|');
    }
    key
}

pub(super) fn retained_from_stream_record(stream: &StoredStreamRecord) -> HistoryRetentionStats {
    HistoryRetentionStats {
        stream_id: Some(stream.stream_id.clone()),
        retained_messages: stream.retained_messages.max(0) as u64,
        retained_bytes: stream.retained_bytes.max(0) as u64,
        oldest_serial: stream.oldest_available_serial.map(|value| value as u64),
        newest_serial: stream.newest_available_serial.map(|value| value as u64),
        oldest_published_at_ms: stream.oldest_available_published_at_ms,
        newest_published_at_ms: stream.newest_available_published_at_ms,
    }
}

pub(super) fn parse_history_durable_state(raw: &str) -> HistoryDurableState {
    match raw {
        "healthy" => HistoryDurableState::Healthy,
        "reset_required" => HistoryDurableState::ResetRequired,
        "degraded" => HistoryDurableState::Degraded,
        _ => HistoryDurableState::Degraded,
    }
}

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

pub(super) struct DegradeRequest<'a> {
    pub(super) app_id: &'a str,
    pub(super) channel: &'a str,
    pub(super) reason: &'a str,
    pub(super) node_id: Option<String>,
}

pub(super) async fn mark_channel_degraded(
    db: &Surreal<Any>,
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

    let resource = (
        tables.streams.clone(),
        deterministic_key([request.app_id, request.channel].into_iter()),
    );
    let current: std::result::Result<Option<StoredStreamRecord>, _> =
        db.select(resource.clone()).await;
    match current {
        Ok(Some(stream)) => {
            let updated = StoredStreamRecord {
                durable_state: state.durable_state.as_str().to_string(),
                durable_state_reason: Some(state.reason.clone()),
                durable_state_node_id: state.node_id.clone(),
                durable_state_changed_at_ms: Some(state.last_transition_at_ms),
                updated_at_ms: now_ms,
                ..stream
            };
            let upsert_result: std::result::Result<Option<StoredStreamRecord>, _> =
                db.upsert(resource).content(updated).await;
            if let Err(err) = upsert_result {
                error!(app_id = %request.app_id, channel = %request.channel, error = %err, "failed to persist surrealdb history degraded state");
            }
        }
        Ok(None) => {}
        Err(err) => {
            error!(app_id = %request.app_id, channel = %request.channel, error = %err, "failed to load surrealdb history stream before degrade");
        }
    }
    if let Some(metrics) = metrics {
        let _ = refresh_history_state_metrics(db, tables, metrics, request.app_id).await;
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
                "Failed to parse degraded SurrealDB history state: {e}"
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
    db: &Surreal<Any>,
    tables: &HistoryTables,
    metrics: &(dyn MetricsInterface + Send + Sync),
    app_id: &str,
) -> Result<()> {
    let mut response = db
        .query(format!(
            "SELECT durable_state FROM {} WHERE app_id = $app_id",
            tables.streams
        ))
        .bind(("app_id", app_id.to_string()))
        .await
        .map_err(|e| Error::Internal(format!("Failed to query SurrealDB history metrics: {e}")))?;
    let rows: Vec<DurableStateRow> = response
        .take(0usize)
        .map_err(|e| Error::Internal(format!("Failed to decode SurrealDB history metrics: {e}")))?;
    let mut degraded_channels = 0usize;
    let mut reset_required_channels = 0usize;
    for row in rows {
        let state = parse_history_durable_state(&row.durable_state);
        if state != HistoryDurableState::Healthy {
            degraded_channels += 1;
        }
        if state == HistoryDurableState::ResetRequired {
            reset_required_channels += 1;
        }
    }
    metrics.update_history_degraded_channels(app_id, degraded_channels);
    metrics.update_history_reset_required_channels(app_id, reset_required_channels);
    Ok(())
}
