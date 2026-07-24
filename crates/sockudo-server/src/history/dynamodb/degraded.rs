use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use dashmap::DashMap;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{HistoryDurableState, HistoryStreamRuntimeState};
use sockudo_core::metrics::MetricsInterface;
use sonic_rs::JsonValueTrait;
use std::sync::Arc;
use tracing::error;

use super::items::parse_history_durable_state;
use super::{DynamoDbHistoryStore, HistoryDegradedState, HistoryTables, StoredStreamRecord};

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
    client: &Client,
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

    let response = client
        .get_item()
        .table_name(&tables.streams)
        .key(
            "stream_key",
            AttributeValue::S(DynamoDbHistoryStore::stream_key(
                request.app_id,
                request.channel,
            )),
        )
        .send()
        .await;
    match response {
        Ok(output) => {
            if let Some(item) = output.item
                && let Ok(stream) = DynamoDbHistoryStore::stream_from_item(item)
            {
                let updated = StoredStreamRecord {
                    durable_state: state.durable_state,
                    durable_state_reason: Some(state.reason.clone()),
                    durable_state_node_id: state.node_id.clone(),
                    durable_state_changed_at_ms: Some(state.last_transition_at_ms),
                    updated_at_ms: now_ms,
                    ..stream
                };
                let put_result = client
                    .put_item()
                    .table_name(&tables.streams)
                    .set_item(Some(DynamoDbHistoryStore::stream_item(
                        &DynamoDbHistoryStore::stream_key(request.app_id, request.channel),
                        &updated,
                    )))
                    .send()
                    .await;
                if let Err(err) = put_result {
                    error!(app_id = %request.app_id, channel = %request.channel, error = %err, "failed to persist dynamodb history degraded state");
                }
            }
        }
        Err(err) => {
            error!(app_id = %request.app_id, channel = %request.channel, error = %err, "failed to load dynamodb history stream before degrade");
        }
    }
    if let Some(metrics) = metrics {
        let _ = refresh_history_state_metrics(client, tables, metrics, request.app_id).await;
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
                "Failed to parse degraded DynamoDB history state: {e}"
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
    client: &Client,
    tables: &HistoryTables,
    metrics: &(dyn MetricsInterface + Send + Sync),
    app_id: &str,
) -> Result<()> {
    let mut degraded = 0usize;
    let mut reset_required = 0usize;
    let mut start_key = None;
    loop {
        let response = client
            .scan()
            .table_name(&tables.streams)
            .filter_expression("app_id = :app_id")
            .expression_attribute_values(":app_id", AttributeValue::S(app_id.to_string()))
            .set_exclusive_start_key(start_key)
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to scan DynamoDB history metrics: {e}"))
            })?;
        for item in response.items() {
            let stream = DynamoDbHistoryStore::stream_from_item(item.clone())?;
            if stream.durable_state != HistoryDurableState::Healthy {
                degraded += 1;
            }
            if stream.durable_state == HistoryDurableState::ResetRequired {
                reset_required += 1;
            }
        }
        start_key = response.last_evaluated_key;
        if start_key.is_none() {
            break;
        }
    }
    metrics.update_history_degraded_channels(app_id, degraded);
    metrics.update_history_reset_required_channels(app_id, reset_required);
    Ok(())
}
