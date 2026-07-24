//! Event publish endpoints (`/events`, `/batch_events`) plus publish-side
//! idempotency helpers.

use axum::{
    Json,
    extract::{Extension, Path, Query, RawQuery, State},
    http::{HeaderMap, StatusCode, Uri},
    response::IntoResponse,
};
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::auth::EventQuery;
use sockudo_protocol::messages::{BatchPusherApiMessage, PusherApiMessage, is_ai_event};
use sonic_rs::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, field, instrument, warn};

use super::AppError;
use super::system::record_api_metrics;

mod processor;

use processor::process_single_event_parallel;

/// Resolve the idempotency key from the request body field or the `X-Idempotency-Key` header.
/// Body field takes precedence. Returns `None` when idempotency is disabled or no key was
/// provided.
fn resolve_idempotency_key(
    body_key: &Option<String>,
    headers: &HeaderMap,
    config: &sockudo_core::options::IdempotencyConfig,
) -> Result<Option<String>, AppError> {
    if !config.enabled {
        return Ok(None);
    }

    let key = body_key.clone().or_else(|| {
        headers
            .get("x-idempotency-key")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    });

    if let Some(ref k) = key {
        if k.is_empty() {
            return Err(AppError::InvalidInput(
                "Idempotency key must not be empty".to_string(),
            ));
        }
        if k.len() > config.max_key_length {
            return Err(AppError::InvalidInput(format!(
                "Idempotency key exceeds maximum length of {} characters",
                config.max_key_length
            )));
        }
    }

    Ok(key)
}

#[cfg(feature = "push")]
fn push_rule_target_channels(event: &PusherApiMessage) -> Vec<String> {
    if let Some(channels) = event.channels.as_ref() {
        return channels.clone();
    }
    event.channel.iter().cloned().collect()
}

/// Build the cache key used for idempotency storage.
fn idempotency_cache_key(app_id: &str, key: &str) -> String {
    format!("app:{}:idempotency:{}", app_id, key)
}

pub(super) fn idempotency_ttl(app: &App, handler: &ConnectionHandler) -> u64 {
    app.resolved_idempotency(&handler.server_options().idempotency)
        .ttl_seconds
}

/// Merge per-app idempotency overrides with the global config.
/// Only fields explicitly set at the app level take precedence.
/// POST /apps/{app_id}/events
#[instrument(skip_all, fields(app_id = %app_id))]
#[allow(clippy::too_many_arguments)]
pub async fn events(
    Path(app_id): Path<String>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app): Extension<App>,
    #[cfg(feature = "push")] Extension(push_store): Extension<sockudo_push::DynPushStore>,
    #[cfg(feature = "push")] Extension(push_queue): Extension<sockudo_push::DynPushQueue>,
    #[cfg(feature = "push")] Extension(push_admission): Extension<
        Arc<crate::bootstrap::push::PushAdmissionSnapshot>,
    >,
    State(handler): State<Arc<ConnectionHandler>>,
    headers: HeaderMap,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
    Json(event_payload): Json<PusherApiMessage>,
) -> Result<impl IntoResponse, AppError> {
    let start_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as f64
        / 1_000_000.0;

    let incoming_request_size_bytes = sonic_rs::to_vec(&event_payload)?.len();

    let idempotency_config = app.resolved_idempotency(&handler.server_options().idempotency);
    let idempotency_key = resolve_idempotency_key(
        &event_payload.idempotency_key,
        &headers,
        &idempotency_config,
    )?;

    // Check for cached response or claim the idempotency key atomically
    if let Some(ref key) = idempotency_key {
        if let Some(metrics_arc) = handler.metrics() {
            metrics_arc.mark_idempotency_publish(&app_id);
        }
        let cache_key = idempotency_cache_key(&app_id, key);
        let ttl = idempotency_config.ttl_seconds;

        // First, check if there's already a completed response
        match handler.cache_manager().get(&cache_key).await {
            Ok(Some(cached)) if cached != "__processing__" => {
                if let Some(metrics_arc) = handler.metrics() {
                    metrics_arc.mark_idempotency_duplicate(&app_id);
                }
                debug!(idempotency_key = %key, "Returning cached idempotent response");
                let response_payload: Value =
                    sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({ "ok": true }));
                let outgoing_response_size_bytes = sonic_rs::to_vec(&response_payload)?.len();
                record_api_metrics(
                    &handler,
                    &app_id,
                    incoming_request_size_bytes,
                    outgoing_response_size_bytes,
                )
                .await;
                return Ok((StatusCode::OK, Json(response_payload)));
            }
            Ok(Some(_)) => {
                // Another request is processing — wait briefly then return cached result
                for _ in 0..6 {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    match handler.cache_manager().get(&cache_key).await {
                        Ok(Some(cached)) if cached != "__processing__" => {
                            if let Some(metrics_arc) = handler.metrics() {
                                metrics_arc.mark_idempotency_duplicate(&app_id);
                            }
                            let response_payload: Value = sonic_rs::from_str(&cached)
                                .unwrap_or_else(|_| json!({ "ok": true }));
                            let outgoing_response_size_bytes =
                                sonic_rs::to_vec(&response_payload)?.len();
                            record_api_metrics(
                                &handler,
                                &app_id,
                                incoming_request_size_bytes,
                                outgoing_response_size_bytes,
                            )
                            .await;
                            return Ok((StatusCode::OK, Json(response_payload)));
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(idempotency_key = %key, %error, "Idempotency coordination failed while waiting for the active request");
                            return Err(AppError::ServiceUnavailable(
                                "Idempotency coordination is temporarily unavailable".to_string(),
                            ));
                        }
                    }
                }
                debug!(idempotency_key = %key, "Idempotent request is still in progress");
                return Err(AppError::Backpressure {
                    message: "Idempotent publish is still in progress".to_string(),
                    retry_after_seconds: 1,
                });
            }
            Ok(None) => {
                // Try to claim this key atomically
                match handler
                    .cache_manager()
                    .set_if_not_exists(&cache_key, "__processing__", ttl)
                    .await
                {
                    Ok(false) => {
                        // Another request claimed it — wait for their result
                        for _ in 0..6 {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            match handler.cache_manager().get(&cache_key).await {
                                Ok(Some(cached)) if cached != "__processing__" => {
                                    if let Some(metrics_arc) = handler.metrics() {
                                        metrics_arc.mark_idempotency_duplicate(&app_id);
                                    }
                                    let response_payload: Value = sonic_rs::from_str(&cached)
                                        .unwrap_or_else(|_| json!({ "ok": true }));
                                    let outgoing_response_size_bytes =
                                        sonic_rs::to_vec(&response_payload)?.len();
                                    record_api_metrics(
                                        &handler,
                                        &app_id,
                                        incoming_request_size_bytes,
                                        outgoing_response_size_bytes,
                                    )
                                    .await;
                                    return Ok((StatusCode::OK, Json(response_payload)));
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(idempotency_key = %key, %error, "Idempotency coordination failed while waiting for the active request");
                                    return Err(AppError::ServiceUnavailable(
                                        "Idempotency coordination is temporarily unavailable"
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                        debug!(idempotency_key = %key, "Idempotent request is still in progress");
                        return Err(AppError::Backpressure {
                            message: "Idempotent publish is still in progress".to_string(),
                            retry_after_seconds: 1,
                        });
                    }
                    Ok(true) => {
                        // We claimed it — proceed to process below
                    }
                    Err(error) => {
                        warn!(idempotency_key = %key, %error, "Failed to claim idempotency key; rejecting publish");
                        return Err(AppError::ServiceUnavailable(
                            "Idempotency coordination is temporarily unavailable".to_string(),
                        ));
                    }
                }
            }
            Err(error) => {
                warn!(idempotency_key = %key, %error, "Failed to check idempotency cache; rejecting publish");
                return Err(AppError::ServiceUnavailable(
                    "Idempotency coordination is temporarily unavailable".to_string(),
                ));
            }
        }
    }

    let need_channel_info = event_payload.info.is_some()
        || event_payload.message_id.is_some()
        || event_payload.name.as_deref().is_some_and(is_ai_event);

    #[cfg(feature = "push")]
    let push_rule_requests = {
        let rules = &handler.server_options().push_rules;
        if rules.is_empty() {
            Vec::new()
        } else {
            let channels = push_rule_target_channels(&event_payload);
            crate::push_http::build_channel_push_rule_requests(
                &app_id,
                event_payload.name.as_deref(),
                event_payload.data.as_ref(),
                &channels,
                rules,
            )?
        }
    };

    #[cfg(feature = "push")]
    if let Some(extras_push) = event_payload
        .extras
        .as_ref()
        .and_then(|extras| extras.push.as_ref())
    {
        if let Some(channel) = event_payload.channel.as_deref() {
            crate::push_http::enqueue_v2_channel_push_from_extras(
                &app_id,
                channel,
                extras_push,
                &push_store,
                &push_queue,
                &push_admission,
            )
            .await?;
        }
        if let Some(channels) = event_payload.channels.as_ref() {
            for channel in channels {
                crate::push_http::enqueue_v2_channel_push_from_extras(
                    &app_id,
                    channel,
                    extras_push,
                    &push_store,
                    &push_queue,
                    &push_admission,
                )
                .await?;
            }
        }
    }

    let channels_info_map = process_single_event_parallel(
        &handler,
        &app,
        event_payload,
        need_channel_info,
        Some(start_time_ms),
        idempotency_key.clone(),
    )
    .await?;

    #[cfg(feature = "push")]
    crate::push_http::spawn_channel_push_rule_requests(
        app_id.clone(),
        push_rule_requests,
        push_store.clone(),
        push_queue.clone(),
        push_admission.clone(),
    );

    let response_payload = if need_channel_info && !channels_info_map.is_empty() {
        json!({
            "channels": channels_info_map
        })
    } else {
        json!({ "ok": true })
    };

    // Store final response in idempotency cache (overwrites __processing__ sentinel)
    if let Some(ref key) = idempotency_key {
        let cache_key = idempotency_cache_key(&app_id, key);
        let ttl = idempotency_config.ttl_seconds;
        if let Ok(serialized) = sonic_rs::to_string(&response_payload)
            && let Err(e) = handler
                .cache_manager()
                .set(&cache_key, &serialized, ttl)
                .await
        {
            warn!(idempotency_key = %key, error = %e, "Failed to store idempotency response in cache");
        }
    }

    let outgoing_response_size_bytes = sonic_rs::to_vec(&response_payload)?.len();
    record_api_metrics(
        &handler,
        &app_id,
        incoming_request_size_bytes,
        outgoing_response_size_bytes,
    )
    .await;

    Ok((StatusCode::OK, Json(response_payload)))
}

/// POST /apps/{app_id}/batch_events
#[instrument(skip_all, fields(app_id = %app_id, batch_len = field::Empty))]
#[allow(clippy::too_many_arguments)]
pub async fn batch_events(
    Path(app_id): Path<String>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app_config): Extension<App>,
    #[cfg(feature = "push")] Extension(push_store): Extension<sockudo_push::DynPushStore>,
    #[cfg(feature = "push")] Extension(push_queue): Extension<sockudo_push::DynPushQueue>,
    #[cfg(feature = "push")] Extension(push_admission): Extension<
        Arc<crate::bootstrap::push::PushAdmissionSnapshot>,
    >,
    State(handler): State<Arc<ConnectionHandler>>,
    headers: HeaderMap,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
    Json(batch_message_payload): Json<BatchPusherApiMessage>,
) -> Result<impl IntoResponse, AppError> {
    let start_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as f64
        / 1_000_000.0;

    let body_bytes = sonic_rs::to_vec(&batch_message_payload)?;

    // Batch-level idempotency: check X-Idempotency-Key header
    let idempotency_config = app_config.resolved_idempotency(&handler.server_options().idempotency);
    let idempotency_key = resolve_idempotency_key(&None, &headers, &idempotency_config)?;

    if let Some(ref key) = idempotency_key {
        if let Some(metrics_arc) = handler.metrics() {
            metrics_arc.mark_idempotency_publish(&app_id);
        }
        let cache_key = idempotency_cache_key(&app_id, key);
        let ttl = idempotency_config.ttl_seconds;
        match handler.cache_manager().get(&cache_key).await {
            Ok(Some(cached)) if cached != "__processing__" => {
                if let Some(metrics_arc) = handler.metrics() {
                    metrics_arc.mark_idempotency_duplicate(&app_id);
                }
                debug!(idempotency_key = %key, "Returning cached idempotent batch response");
                let response_payload: Value =
                    sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({}));
                let outgoing_response_size_bytes = sonic_rs::to_vec(&response_payload)?.len();
                record_api_metrics(
                    &handler,
                    &app_id,
                    body_bytes.len(),
                    outgoing_response_size_bytes,
                )
                .await;
                return Ok((StatusCode::OK, Json(response_payload)));
            }
            Ok(Some(_)) | Ok(None) => {
                // Claim key atomically or wait for concurrent request
                match handler
                    .cache_manager()
                    .set_if_not_exists(&cache_key, "__processing__", ttl)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        for _ in 0..6 {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            match handler.cache_manager().get(&cache_key).await {
                                Ok(Some(cached)) if cached != "__processing__" => {
                                    if let Some(metrics_arc) = handler.metrics() {
                                        metrics_arc.mark_idempotency_duplicate(&app_id);
                                    }
                                    let response_payload: Value =
                                        sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({}));
                                    let outgoing_response_size_bytes =
                                        sonic_rs::to_vec(&response_payload)?.len();
                                    record_api_metrics(
                                        &handler,
                                        &app_id,
                                        body_bytes.len(),
                                        outgoing_response_size_bytes,
                                    )
                                    .await;
                                    return Ok((StatusCode::OK, Json(response_payload)));
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(idempotency_key = %key, %error, "Batch idempotency coordination failed while waiting for the active request");
                                    return Err(AppError::ServiceUnavailable(
                                        "Idempotency coordination is temporarily unavailable"
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                        debug!(idempotency_key = %key, "Idempotent batch request is still in progress");
                        return Err(AppError::Backpressure {
                            message: "Idempotent batch publish is still in progress".to_string(),
                            retry_after_seconds: 1,
                        });
                    }
                    Err(error) => {
                        warn!(idempotency_key = %key, %error, "Failed to claim batch idempotency key; rejecting publish");
                        return Err(AppError::ServiceUnavailable(
                            "Idempotency coordination is temporarily unavailable".to_string(),
                        ));
                    }
                }
            }
            Err(error) => {
                warn!(idempotency_key = %key, %error, "Failed to check batch idempotency cache; rejecting publish");
                return Err(AppError::ServiceUnavailable(
                    "Idempotency coordination is temporarily unavailable".to_string(),
                ));
            }
        }
    }

    let batch_events_vec = batch_message_payload.batch;
    let batch_len = batch_events_vec.len();
    tracing::Span::current().record("batch_len", batch_len);
    debug!("Received batch events request with {} events", batch_len);

    for (i, event) in batch_events_vec.iter().enumerate().take(3) {
        debug!("Batch event #{}: tags={:?}", i, event.tags);
    }

    if let Some(max_batch) = app_config.event_batch_size_limit()
        && batch_len > max_batch as usize
    {
        return Err(AppError::LimitExceeded(format!(
            "Batch size ({batch_len}) exceeds limit ({max_batch})"
        )));
    }

    let incoming_request_size_bytes = body_bytes.len();
    let mut any_message_requests_info = false;

    for single_event_message in &batch_events_vec {
        if single_event_message.info.is_some()
            || single_event_message.message_id.is_some()
            || single_event_message
                .name
                .as_deref()
                .is_some_and(is_ai_event)
        {
            any_message_requests_info = true;
            break;
        }
    }

    let mut processed_event_data = Vec::with_capacity(batch_len);

    for single_event_message in batch_events_vec {
        // Per-event idempotency: skip events whose idempotency key has already been seen
        if let Some(ref evt_key) = single_event_message.idempotency_key
            && idempotency_config.enabled
            && !evt_key.is_empty()
        {
            if let Some(metrics_arc) = handler.metrics() {
                metrics_arc.mark_idempotency_publish(&app_id);
            }
            let evt_cache_key = idempotency_cache_key(&app_id, evt_key);
            match handler.cache_manager().get(&evt_cache_key).await {
                Ok(Some(_)) => {
                    if let Some(metrics_arc) = handler.metrics() {
                        metrics_arc.mark_idempotency_duplicate(&app_id);
                    }
                    debug!(idempotency_key = %evt_key, "Skipping duplicate batch event");
                    processed_event_data.push((single_event_message, HashMap::new()));
                    continue;
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(idempotency_key = %evt_key, %error, "Failed to check event idempotency cache; rejecting batch");
                    return Err(AppError::ServiceUnavailable(
                        "Idempotency coordination is temporarily unavailable".to_string(),
                    ));
                }
            }
        }

        let should_collect_info_for_this_event = single_event_message.info.is_some()
            || single_event_message.message_id.is_some()
            || single_event_message
                .name
                .as_deref()
                .is_some_and(is_ai_event);

        #[cfg(feature = "push")]
        let push_rule_requests = {
            let rules = &handler.server_options().push_rules;
            if rules.is_empty() {
                Vec::new()
            } else {
                let channels = push_rule_target_channels(&single_event_message);
                crate::push_http::build_channel_push_rule_requests(
                    &app_id,
                    single_event_message.name.as_deref(),
                    single_event_message.data.as_ref(),
                    &channels,
                    rules,
                )?
            }
        };

        #[cfg(feature = "push")]
        if let Some(extras_push) = single_event_message
            .extras
            .as_ref()
            .and_then(|extras| extras.push.as_ref())
        {
            if let Some(channel) = single_event_message.channel.as_deref() {
                crate::push_http::enqueue_v2_channel_push_from_extras(
                    &app_id,
                    channel,
                    extras_push,
                    &push_store,
                    &push_queue,
                    &push_admission,
                )
                .await?;
            }
            if let Some(channels) = single_event_message.channels.as_ref() {
                for channel in channels {
                    crate::push_http::enqueue_v2_channel_push_from_extras(
                        &app_id,
                        channel,
                        extras_push,
                        &push_store,
                        &push_queue,
                        &push_admission,
                    )
                    .await?;
                }
            }
        }

        let channel_info_map = process_single_event_parallel(
            &handler,
            &app_config,
            single_event_message.clone(),
            should_collect_info_for_this_event,
            Some(start_time_ms),
            single_event_message.idempotency_key.clone(),
        )
        .await?;

        #[cfg(feature = "push")]
        crate::push_http::spawn_channel_push_rule_requests(
            app_id.clone(),
            push_rule_requests,
            push_store.clone(),
            push_queue.clone(),
            push_admission.clone(),
        );

        // Store per-event idempotency key
        if let Some(ref evt_key) = single_event_message.idempotency_key
            && idempotency_config.enabled
            && !evt_key.is_empty()
        {
            let evt_cache_key = idempotency_cache_key(&app_id, evt_key);
            let _ = handler
                .cache_manager()
                .set(&evt_cache_key, "1", idempotency_config.ttl_seconds)
                .await;
        }

        processed_event_data.push((single_event_message, channel_info_map));
    }

    let mut batch_response_info_vec = Vec::with_capacity(batch_len);

    if any_message_requests_info {
        for (original_msg, channel_info_map_for_event) in processed_event_data {
            if let Some(main_channel_for_event) = original_msg
                .channel
                .as_ref()
                .or_else(|| original_msg.channels.as_ref().and_then(|chs| chs.first()))
            {
                batch_response_info_vec.push(
                    channel_info_map_for_event
                        .get(main_channel_for_event)
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                );
            } else {
                batch_response_info_vec.push(json!({}));
            }
        }
    }

    let final_response_payload = if any_message_requests_info {
        json!({ "batch": batch_response_info_vec })
    } else {
        json!({})
    };

    // Store batch response in idempotency cache
    if let Some(ref key) = idempotency_key {
        let cache_key = idempotency_cache_key(&app_id, key);
        let ttl = idempotency_config.ttl_seconds;
        if let Ok(serialized) = sonic_rs::to_string(&final_response_payload)
            && let Err(e) = handler
                .cache_manager()
                .set(&cache_key, &serialized, ttl)
                .await
        {
            warn!(idempotency_key = %key, error = %e, "Failed to store batch idempotency response in cache");
        }
    }

    let outgoing_response_size_bytes_vec = sonic_rs::to_vec(&final_response_payload)?;
    record_api_metrics(
        &handler,
        &app_id,
        incoming_request_size_bytes,
        outgoing_response_size_bytes_vec.len(),
    )
    .await;
    debug!("{}", "Batch events processed successfully");
    Ok((StatusCode::OK, Json(final_response_payload)))
}

#[cfg(test)]
mod idempotency_tests;

#[cfg(all(test, feature = "push"))]
mod tests;
