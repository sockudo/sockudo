//! AI Transport validation, caps, counters, and channel stats for the HTTP API.

use axum::http::StatusCode;
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::history::{HistoryDirection, HistoryQueryBounds, HistoryReadRequest};
use sockudo_core::version_store::{
    StoredVersionRecord, VersionStoreDirection, VersionStoreReadRequest,
};
use sockudo_core::versioned_messages::MessageAction as CoreMessageAction;
use sockudo_protocol::messages::{
    AI_ERROR_EVENT_NOT_PERMITTED, AI_ERROR_HEADER_TOO_LARGE, AI_ERROR_INVALID_TRANSPORT_HEADER,
    AI_ERROR_MUTABLE_NOT_PERMITTED, AI_ERROR_PAYLOAD_TOO_LARGE, AI_MESSAGE_ID_MAX_BYTES,
    AiHeaderValidationError, AiPublishTrust, MessageData, MessageExtras, PusherMessage,
    is_ai_agent_publish_event, is_ai_event,
};
use sonic_rs::{Value, json};
use std::sync::Arc;

use super::AppError;

pub(super) async fn ai_channel_stats(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    app_id: &str,
    channel: &str,
) -> Result<Option<Value>, AppError> {
    if !handler
        .server_options()
        .ai_transport
        .matches_channel(channel)
    {
        return Ok(None);
    }

    let history_policy = app.resolved_history(channel, &handler.server_options().history);
    let last_history_serial = if history_policy.enabled {
        let page = handler
            .history_store()
            .read_page(HistoryReadRequest {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                direction: HistoryDirection::NewestFirst,
                limit: 1,
                cursor: None,
                bounds: HistoryQueryBounds::default(),
            })
            .await?;
        page.items.first().map(|item| item.serial)
    } else {
        None
    };

    let latest_messages = if handler.server_options().versioned_messages.enabled {
        handler
            .version_store()
            .latest_by_history(app_id, channel)
            .await?
    } else {
        Vec::new()
    };
    let active_streams = handler.ai_active_stream_count(app_id, channel).await?;

    Ok(Some(json!({
        "active_streams": active_streams,
        "last_history_serial": last_history_serial,
        "message_count": latest_messages.len()
    })))
}

fn ai_validation_app_error(error: AiHeaderValidationError) -> AppError {
    let status = match error.code {
        AI_ERROR_HEADER_TOO_LARGE => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::BAD_REQUEST,
    };

    AppError::AiTransport {
        status,
        code: error.code,
        name: error.name,
        message: error.message,
    }
}

fn record_ai_rejection(handler: &ConnectionHandler, app_id: &str, code: u32) {
    if let Some(metrics) = handler.metrics() {
        metrics.mark_ai_transport_rejected(app_id, code);
    }
}

fn ai_payload_too_large(message: impl Into<String>) -> AppError {
    AppError::AiTransport {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        code: AI_ERROR_PAYLOAD_TOO_LARGE,
        name: "payload_too_large",
        message: message.into(),
    }
}

pub(super) fn mutable_not_permitted() -> AppError {
    AppError::AiTransport {
        status: StatusCode::FORBIDDEN,
        code: AI_ERROR_MUTABLE_NOT_PERMITTED,
        name: "mutable_not_permitted",
        message: "mutations not permitted on this channel".to_string(),
    }
}

fn message_data_bytes(data: Option<&MessageData>) -> Result<usize, AppError> {
    match data {
        None => Ok(0),
        Some(MessageData::String(value)) => Ok(value.len()),
        Some(other) => sonic_rs::to_vec(other)
            .map(|bytes| bytes.len())
            .map_err(AppError::from),
    }
}

fn ai_transport_applies(handler: &ConnectionHandler, channel: &str) -> bool {
    handler
        .server_options()
        .ai_transport
        .matches_channel(channel)
}

pub(super) async fn validate_ai_append_caps(
    handler: &ConnectionHandler,
    current: &StoredVersionRecord,
    append_bytes: usize,
) -> Result<(), AppError> {
    if !ai_transport_applies(handler, &current.channel) {
        return Ok(());
    }

    let config = &handler.server_options().ai_transport;
    let current_bytes = message_data_bytes(current.message.data.as_ref())?;
    let next_bytes = current_bytes.saturating_add(append_bytes);
    if next_bytes > config.max_accumulated_message_bytes {
        record_ai_rejection(handler, &current.app_id, AI_ERROR_PAYLOAD_TOO_LARGE);
        return Err(ai_payload_too_large(format!(
            "accumulated message content exceeds {} bytes",
            config.max_accumulated_message_bytes
        )));
    }

    Ok(())
}

fn ai_append_count_key(current: &StoredVersionRecord) -> String {
    format!(
        "ai_transport:append_count:{}:{}:{}",
        current.app_id,
        current.channel,
        current.message_serial().as_str()
    )
}

fn ai_counter_ttl_seconds(handler: &ConnectionHandler) -> u64 {
    handler
        .server_options()
        .versioned_messages
        .retention_window_seconds
        .max(1)
}

pub(super) async fn reserve_ai_append_capacity(
    handler: &ConnectionHandler,
    current: &StoredVersionRecord,
) -> Result<Option<String>, AppError> {
    if !ai_transport_applies(handler, &current.channel) {
        return Ok(None);
    }

    let key = ai_append_count_key(current);
    if handler.cache_manager().get(&key).await?.is_none() {
        let page = handler
            .version_store()
            .get_versions(VersionStoreReadRequest {
                app_id: current.app_id.clone(),
                channel: current.channel.clone(),
                message_serial: current.message_serial().clone(),
                direction: VersionStoreDirection::OldestFirst,
                limit: handler
                    .server_options()
                    .ai_transport
                    .max_appends_per_message
                    .saturating_add(1),
                cursor: None,
            })
            .await?;
        let append_count = page
            .items
            .iter()
            .filter(|record| record.message.action == CoreMessageAction::Append)
            .count();
        handler
            .cache_manager()
            .set(
                &key,
                &append_count.to_string(),
                ai_counter_ttl_seconds(handler),
            )
            .await?;
    }

    let next = handler
        .cache_manager()
        .increment_by(&key, 1, ai_counter_ttl_seconds(handler))
        .await?;
    let max_appends = handler
        .server_options()
        .ai_transport
        .max_appends_per_message as i64;
    if next > max_appends {
        let _ = handler
            .cache_manager()
            .increment_by(&key, -1, ai_counter_ttl_seconds(handler))
            .await;
        record_ai_rejection(handler, &current.app_id, AI_ERROR_PAYLOAD_TOO_LARGE);
        return Err(ai_payload_too_large(format!(
            "message append count exceeds {}",
            handler
                .server_options()
                .ai_transport
                .max_appends_per_message
        )));
    }

    Ok(Some(key))
}

pub(super) async fn rollback_ai_append_capacity(handler: &ConnectionHandler, key: Option<&str>) {
    if let Some(key) = key {
        let _ = handler
            .cache_manager()
            .increment_by(key, -1, ai_counter_ttl_seconds(handler))
            .await;
    }
}

pub(super) fn validate_ai_update_caps(
    handler: &ConnectionHandler,
    app_id: &str,
    channel: &str,
    data: Option<&MessageData>,
) -> Result<(), AppError> {
    if !ai_transport_applies(handler, channel) {
        return Ok(());
    }
    let max_bytes = handler
        .server_options()
        .ai_transport
        .max_accumulated_message_bytes;
    let bytes = message_data_bytes(data)?;
    if bytes > max_bytes {
        record_ai_rejection(handler, app_id, AI_ERROR_PAYLOAD_TOO_LARGE);
        return Err(ai_payload_too_large(format!(
            "accumulated message content exceeds {max_bytes} bytes"
        )));
    }
    Ok(())
}

pub(super) async fn validate_ai_http_publish(
    handler: &ConnectionHandler,
    app: &App,
    channel: &str,
    event_name: &str,
    message_id: Option<&str>,
    extras: Option<&MessageExtras>,
) -> Result<(), AppError> {
    let ai_channel = handler
        .server_options()
        .ai_transport
        .matches_channel(channel);
    let has_ai_headers = extras.and_then(|extras| extras.ai.as_ref()).is_some();
    if !ai_channel && !has_ai_headers && !is_ai_event(event_name) {
        return Ok(());
    }

    if !ai_channel {
        record_ai_rejection(handler, &app.id, AI_ERROR_EVENT_NOT_PERMITTED);
        return Err(AppError::AiTransport {
            status: StatusCode::FORBIDDEN,
            code: AI_ERROR_EVENT_NOT_PERMITTED,
            name: "ai_event_not_permitted",
            message: "AI Transport is not enabled for this channel".to_string(),
        });
    }

    if is_ai_event(event_name)
        && let Some(metrics) = handler.metrics()
    {
        metrics.mark_ai_transport_validated(&app.id, event_name);
    }

    if is_ai_agent_publish_event(event_name) {
        // HTTP API requests are signed with the app secret before reaching this handler.
    }

    if let Some(message_id) = message_id {
        if message_id.is_empty() {
            record_ai_rejection(handler, &app.id, AI_ERROR_INVALID_TRANSPORT_HEADER);
            return Err(AppError::AiTransport {
                status: StatusCode::BAD_REQUEST,
                code: AI_ERROR_INVALID_TRANSPORT_HEADER,
                name: "ai_invalid_transport_header",
                message: "message_id must not be empty".to_string(),
            });
        }
        if message_id.len() > AI_MESSAGE_ID_MAX_BYTES {
            record_ai_rejection(handler, &app.id, AI_ERROR_HEADER_TOO_LARGE);
            return Err(AppError::AiTransport {
                status: StatusCode::BAD_REQUEST,
                code: AI_ERROR_HEADER_TOO_LARGE,
                name: "ai_header_too_large",
                message: format!("message_id exceeds {AI_MESSAGE_ID_MAX_BYTES} bytes"),
            });
        }
    }

    if let Some(extras) = extras {
        extras.validate_ai_headers().map_err(|error| {
            record_ai_rejection(handler, &app.id, error.code);
            ai_validation_app_error(error)
        })?;
        let probe = PusherMessage {
            event: Some(event_name.to_string()),
            channel: Some(channel.to_string()),
            data: None,
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: message_id.map(ToOwned::to_owned),
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: Some(extras.clone()),
            delta_sequence: None,
            delta_conflation_key: None,
        };
        sockudo_adapter::handler::validation::validate_ai_client_id_headers(
            &probe,
            None,
            AiPublishTrust::TrustedApp,
        )
        .map_err(|error| {
            record_ai_rejection(handler, &app.id, error.code);
            ai_validation_app_error(error)
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests;
