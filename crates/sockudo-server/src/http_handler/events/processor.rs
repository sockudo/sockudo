use futures_util::future::join_all;
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::channel_manager::ChannelManager;
use sockudo_core::app::App;
use sockudo_core::utils::{self, validate_channel_name};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::constants::EVENT_NAME_MAX_LENGTH as DEFAULT_EVENT_NAME_MAX_LENGTH;
use sockudo_protocol::messages::{ApiMessageData, MessageData, PusherApiMessage, PusherMessage};
use sonic_rs::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{debug, error, field, instrument, warn};

use super::super::AppError;
use super::super::ai::validate_ai_http_publish;

/// Helper to build cache payload string
fn build_cache_payload(
    event_name: &str,
    event_data: &Value,
    channel: &str,
) -> Result<String, sonic_rs::Error> {
    sonic_rs::to_string(&json!({
        "event": event_name,
        "channel": channel,
        "data": event_data,
    }))
}

#[derive(Clone, Copy)]
pub(super) struct MessageIdIdempotencyContext {
    pub(super) enabled: bool,
    pub(super) ttl_seconds: u64,
}

/// Helper to process a single event and return channel info if requested
#[instrument(skip(handler, event_data, app, start_time_ms, message_id_idempotency), fields(app_id = app.id, event_name = field::Empty))]
pub(super) async fn process_single_event_parallel(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    event_data: PusherApiMessage,
    collect_info: bool,
    start_time_ms: Option<f64>,
    idempotency_key: Option<String>,
    message_id_idempotency: MessageIdIdempotencyContext,
) -> Result<HashMap<String, Value>, AppError> {
    let PusherApiMessage {
        name,
        data: event_payload_data,
        channels,
        channel,
        socket_id: original_socket_id_str,
        info,
        tags,
        delta: delta_flag,
        idempotency_key: _,
        message_id,
        extras,
    } = event_data;

    let event_name_str = name
        .as_deref()
        .ok_or_else(|| AppError::InvalidInput("Event name is required".to_string()))?;
    tracing::Span::current().record("event_name", event_name_str);

    let max_event_name_len = app
        .event_name_limit()
        .unwrap_or(DEFAULT_EVENT_NAME_MAX_LENGTH as u32);
    if event_name_str.len() > max_event_name_len as usize {
        return Err(AppError::LimitExceeded(format!(
            "Event name '{event_name_str}' exceeds maximum length of {max_event_name_len}"
        )));
    }

    if let Some(max_payload_kb) = app.event_payload_limit_kb() {
        let value_for_size_calc = match &event_payload_data {
            Some(ApiMessageData::String(s)) => json!(s),
            Some(ApiMessageData::Json(j_val)) => j_val.clone(),
            None => json!(null),
        };
        let payload_size_bytes = utils::data_to_bytes_flexible(vec![value_for_size_calc]);
        if payload_size_bytes > (max_payload_kb as usize * 1024) {
            return Err(AppError::PayloadTooLarge(format!(
                "Event payload size ({payload_size_bytes} bytes) for event '{event_name_str}' exceeds limit ({max_payload_kb}KB)"
            )));
        }
    }

    let mapped_socket_id: Option<SocketId> = original_socket_id_str
        .as_ref()
        .and_then(|s| SocketId::from_string(s).ok());

    let target_channels: Vec<String> = match channels {
        Some(ch_list) if !ch_list.is_empty() => {
            if let Some(max_ch_at_once) = app.event_channels_at_once_limit()
                && ch_list.len() > max_ch_at_once as usize
            {
                return Err(AppError::LimitExceeded(format!(
                    "Number of channels ({}) exceeds limit ({})",
                    ch_list.len(),
                    max_ch_at_once
                )));
            }
            ch_list
        }
        None => match channel {
            Some(ch_str) => vec![ch_str],
            None => {
                warn!("{}", "Missing 'channels' or 'channel' in event");
                return Err(AppError::MissingChannelInfo);
            }
        },
        Some(_) => {
            warn!("{}", "Empty 'channels' list provided in event");
            return Err(AppError::MissingChannelInfo);
        }
    };

    let event_name_owned = event_name_str.to_string();
    let message_name = Arc::new(name);
    let message_data = Arc::new(match event_payload_data {
        Some(ApiMessageData::String(s)) => MessageData::String(s),
        Some(ApiMessageData::Json(j_val)) => MessageData::String(j_val.to_string()),
        None => MessageData::String("null".to_string()),
    });
    let message_tags: Arc<Option<std::collections::BTreeMap<String, String>>> =
        Arc::new(tags.map(|h| h.into_iter().collect()));
    let message_idempotency_key = Arc::new(idempotency_key);
    let message_extras = Arc::new(extras);

    let channel_processing_futures = target_channels.into_iter().map(|target_channel_str| {
        let handler_clone = Arc::clone(handler);
        let name_for_task = Arc::clone(&message_name);
        let payload_for_task = Arc::clone(&message_data);
        let socket_id_for_task = mapped_socket_id;
        let info_for_task = info.clone();
        let event_name_for_task = event_name_owned.clone();
        let tags_for_task = Arc::clone(&message_tags);
        let delta_flag_for_task = delta_flag;
        let message_id_for_task = message_id.clone();
        let idempotency_key_for_task = Arc::clone(&message_idempotency_key);
        let extras_for_task = Arc::clone(&message_extras);

        async move {
            debug!(channel = %target_channel_str, "Processing channel for event (parallel task)");

            validate_channel_name(app, &target_channel_str).await?;
            validate_ai_http_publish(
                handler_clone.as_ref(),
                app,
                &target_channel_str,
                event_name_for_task.as_str(),
                message_id_for_task.as_deref(),
                extras_for_task.as_ref().as_ref(),
            )
            .await?;

            if let Some(message_id) = message_id_for_task.as_deref()
                && message_id_idempotency.enabled
            {
                let cache_key = message_id_cache_key(&app.id, &target_channel_str, message_id);
                match handler_clone.cache_manager().get(&cache_key).await {
                    Ok(Some(cached)) if cached != "__processing__" => {
                        if let Some(metrics) = handler_clone.metrics() {
                            metrics.mark_idempotency_duplicate(&app.id);
                        }
                        let cached_ack = sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({}));
                        return Ok(Some((target_channel_str.clone(), cached_ack)));
                    }
                    Ok(Some(_)) => {
                        if let Some(cached_ack) =
                            wait_for_message_id_ack(handler_clone.as_ref(), &cache_key).await
                        {
                            if let Some(metrics) = handler_clone.metrics() {
                                metrics.mark_idempotency_duplicate(&app.id);
                            }
                            return Ok(Some((target_channel_str.clone(), cached_ack)));
                        }
                        return Err(AppError::Backpressure {
                            message: "message_id duplicate is still processing".to_string(),
                            retry_after_seconds: 1,
                        });
                    }
                    Ok(None) => {
                        if let Some(metrics) = handler_clone.metrics() {
                            metrics.mark_idempotency_publish(&app.id);
                        }
                        match handler_clone
                            .cache_manager()
                            .set_if_not_exists(
                                &cache_key,
                                "__processing__",
                                message_id_idempotency.ttl_seconds,
                            )
                            .await
                        {
                            Ok(true) => {}
                            Ok(false) => {
                                if let Some(cached_ack) =
                                    wait_for_message_id_ack(handler_clone.as_ref(), &cache_key)
                                        .await
                                {
                                    if let Some(metrics) = handler_clone.metrics() {
                                        metrics.mark_idempotency_duplicate(&app.id);
                                    }
                                    return Ok(Some((target_channel_str.clone(), cached_ack)));
                                }
                                return Err(AppError::Backpressure {
                                    message: "message_id duplicate is still processing".to_string(),
                                    retry_after_seconds: 1,
                                });
                            }
                            Err(e) => {
                                warn!(message_id = %message_id, error = %e, "Failed to claim message_id idempotency key, proceeding without dedup");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(message_id = %message_id, error = %e, "Failed to check message_id idempotency cache, proceeding without dedup");
                    }
                }
            }

            let _message_to_send = PusherMessage {
                channel: Some(target_channel_str.clone()),
                name: None,
                event: (*name_for_task).clone(),
                data: Some((*payload_for_task).clone()),
                user_id: None,
                tags: (*tags_for_task).clone(),
                sequence: None,
                conflation_key: None,
                message_id: if handler_clone.server_options().connection_recovery.enabled {
                    message_id_for_task
                        .clone()
                        .or_else(|| Some(uuid::Uuid::new_v4().to_string()))
                } else {
                    message_id_for_task.clone()
                },
                stream_id: None,
                serial: None,
                idempotency_key: (*idempotency_key_for_task).clone(),
                extras: (*extras_for_task).clone(),
                delta_sequence: None,
                delta_conflation_key: None,
            };
            let timestamp_ms = start_time_ms;

            let force_full = matches!(delta_flag_for_task, Some(false));
            let publish_ack = handler_clone
                .publish_to_channel_with_timing(
                    app,
                    &target_channel_str,
                    _message_to_send,
                    socket_id_for_task.as_ref(),
                    timestamp_ms,
                    force_full,
                )
                .await?;

            let mut collected_channel_specific_info: Option<(String, Value)> = None;
            if collect_info {
                let is_presence = target_channel_str.starts_with("presence-");
                let mut current_channel_info_map = sonic_rs::Object::new();

                if let Some(ack) = publish_ack {
                    current_channel_info_map.insert("message_serial", json!(ack.message_serial));
                    current_channel_info_map.insert("history_serial", json!(ack.history_serial));
                    current_channel_info_map.insert("delivery_serial", json!(ack.delivery_serial));
                    current_channel_info_map.insert("version_serial", json!(ack.version_serial));
                }

                if is_presence && info_for_task.as_deref().is_some_and(|s| s.contains("user_count")) {
                    match ChannelManager::get_channel_members(
                        handler_clone.connection_manager(),
                        &app.id,
                        &target_channel_str
                    )
                    .await
                    {
                        Ok(members_map) => {
                            current_channel_info_map
                                .insert("user_count", json!(members_map.len()));
                        }
                        Err(e) => {
                            warn!(
                                "Failed to get user count for channel {}: {} (internal error: {:?})",
                                target_channel_str, e, e
                            );
                        }
                    }
                }

                if info_for_task
                    .as_deref()
                    .is_some_and(|s| s.contains("subscription_count"))
                {
                    let count = handler_clone
                        .connection_manager()
                        .get_channel_socket_count(&app.id, &target_channel_str)
                        .await;
                    current_channel_info_map.insert("subscription_count", json!(count));
                }

                if !current_channel_info_map.is_empty() {
                    if let Some(message_id) = message_id_for_task.as_deref()
                        && message_id_idempotency.enabled
                        && let Ok(serialized) =
                            sonic_rs::to_string(&current_channel_info_map.clone().into_value())
                    {
                        let cache_key = message_id_cache_key(&app.id, &target_channel_str, message_id);
                        let _ = handler_clone
                            .cache_manager()
                            .set(&cache_key, &serialized, message_id_idempotency.ttl_seconds)
                            .await;
                    }
                    collected_channel_specific_info = Some((
                        target_channel_str.clone(),
                        current_channel_info_map.into_value(),
                    ));
                }
            }

            // Handle caching for cacheable channels.
            if utils::is_cache_channel(&target_channel_str) {
                let message_data = sonic_rs::to_value(&*payload_for_task)
                    .map_err(AppError::SerializationError)?;
                match build_cache_payload(&event_name_for_task, &message_data, &target_channel_str) {
                    Ok(cache_payload_str) => {
                        let cache_key_str =
                            format!("app:{}:channel:{}:cache_miss", &app.id, target_channel_str);

                        match handler_clone
                            .cache_manager()
                            .set(&cache_key_str, &cache_payload_str, 3600)
                            .await
                        {
                            Ok(_) => {
                                debug!(channel = %target_channel_str, cache_key = %cache_key_str, "Cached event for channel");
                            }
                            Err(e) => {
                                error!(channel = %target_channel_str, cache_key = %cache_key_str, error = %e, "Failed to cache event (internal error: {:?})", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!(channel = %target_channel_str, error = %e, "Failed to serialize event data for caching");
                    }
                }
            }
            Ok(collected_channel_specific_info)
        }
    });

    let results: Vec<Result<Option<(String, Value)>, AppError>> =
        join_all(channel_processing_futures).await;

    let mut final_channels_info_map = HashMap::new();
    for result in results {
        match result {
            Ok(Some((channel_name, info_value))) => {
                final_channels_info_map.insert(channel_name, info_value);
            }
            Ok(None) => {}
            Err(e) => {
                return Err(e);
            }
        }
    }

    Ok(final_channels_info_map)
}

async fn wait_for_message_id_ack(handler: &ConnectionHandler, cache_key: &str) -> Option<Value> {
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(Some(cached)) = handler.cache_manager().get(cache_key).await
            && cached != "__processing__"
        {
            return Some(sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({})));
        }
    }
    None
}

fn message_id_cache_key(app_id: &str, channel: &str, message_id: &str) -> String {
    format!("app:{app_id}:channel:{channel}:message_id:{message_id}")
}
