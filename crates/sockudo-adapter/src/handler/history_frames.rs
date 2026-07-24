use super::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryCursor, HistoryDirection, HistoryItem, HistoryQueryBounds, HistoryReadRequest,
    HistoryRetentionStats,
};
use sockudo_core::message_envelope::decode_stored_message_payload;
use sockudo_core::version_store::StoredVersionRecord;
use sockudo_core::versioned_messages::MessageSerial;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::ProtocolVersion;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sockudo_protocol::versioned_messages::{
    MessageAction as ProtocolMessageAction, MessageVersionMetadata, VersionedRealtimeMessage,
    extract_runtime_message_serial,
};
use sonic_rs::{JsonValueTrait, Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const DEFAULT_HISTORY_LIMIT: usize = 100;
const MAX_CLIENT_HISTORY_LIMIT: usize = 1000;

#[derive(Debug, Clone)]
struct ChannelHistoryFrameRequest {
    channel: String,
    direction: HistoryDirection,
    limit: usize,
    cursor: Option<HistoryCursor>,
    bounds: HistoryQueryBounds,
    until_attach: bool,
}

impl ConnectionHandler {
    pub(crate) async fn handle_channel_history_request(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let _permit = self.acquire_channel_history_permit(socket_id)?;
        let mut request = parse_channel_history_frame(message)?;

        let connection = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
            .ok_or(Error::ConnectionNotFound)?;

        if connection.protocol_version != ProtocolVersion::V2 {
            return Err(Error::Protocol(
                "channel_history is only supported on protocol V2".to_string(),
            ));
        }

        self.verify_channel_subscription(socket_id, app_config, &request.channel)
            .await?;
        self.validate_v2_channel_access(socket_id, app_config, &request.channel)
            .await?;
        self.validate_v2_capability(socket_id, app_config, &request.channel, "history")
            .await?;

        let history_policy =
            app_config.resolved_history(&request.channel, &self.server_options().history);
        if !history_policy.enabled {
            return Err(Error::Channel(format!(
                "Durable history is disabled by policy for channel '{}'",
                request.channel
            )));
        }

        request.limit = request
            .limit
            .min(history_policy.max_page_size)
            .min(MAX_CLIENT_HISTORY_LIMIT);
        if request.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "History limit must be greater than 0".to_string(),
            ));
        }

        let attach_serial = connection.attach_serial(&request.channel);
        if request.until_attach {
            let attach_serial = attach_serial.ok_or_else(|| {
                Error::InvalidMessageFormat(format!(
                    "No attach_serial is recorded for channel '{}'",
                    request.channel
                ))
            })?;
            request.bounds.end_serial = Some(
                request
                    .bounds
                    .end_serial
                    .map_or(attach_serial, |end| end.min(attach_serial)),
            );
        }

        let stream_state = self
            .history_store()
            .stream_runtime_state(&app_config.id, &request.channel)
            .await?;
        let page = self
            .history_store()
            .read_page(HistoryReadRequest {
                app_id: app_config.id.clone(),
                channel: request.channel.clone(),
                direction: request.direction,
                limit: request.limit,
                cursor: request.cursor,
                bounds: request.bounds.clone(),
            })
            .await?;

        let latest_by_serial = self
            .latest_versions_for_history_items(app_config, &request.channel, &page.items)
            .await?;
        let mut items = Vec::with_capacity(page.items.len());
        for item in page.items {
            let latest = history_item_message_serial(&item)?
                .as_ref()
                .and_then(|serial| latest_by_serial.get(serial));
            items.push(self.history_item_response(item, latest)?);
        }

        let response_payload = json!({
            "items": items,
            "direction": request.direction.as_str(),
            "limit": request.limit,
            "has_more": page.has_more,
            "next_cursor": page.next_cursor.and_then(|cursor| cursor.encode().ok()),
            "bounds": {
                "start_serial": request.bounds.start_serial,
                "end_serial": request.bounds.end_serial,
                "start_time_ms": request.bounds.start_time_ms,
                "end_time_ms": request.bounds.end_time_ms,
                "until_attach": request.until_attach,
                "attach_serial": attach_serial,
            },
            "continuity": build_history_continuity_payload(&page.retained, page.complete, page.truncated_by_retention),
            "stream_state": {
                "stream_id": stream_state.stream_id,
                "durable_state": stream_state.durable_state.as_str(),
                "recovery_allowed": stream_state.recovery_allowed,
                "reset_required": stream_state.reset_required,
                "reason": stream_state.reason,
                "node_id": stream_state.node_id,
                "last_transition_at_ms": stream_state.last_transition_at_ms,
                "authoritative_source": stream_state.authoritative_source,
                "observed_source": stream_state.observed_source,
            },
        });

        self.send_message_to_socket(
            &app_config.id,
            socket_id,
            PusherMessage {
                event: Some("sockudo:channel_history".to_string()),
                channel: Some(request.channel),
                data: Some(MessageData::Json(response_payload)),
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: None,
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
        )
        .await
    }

    fn acquire_channel_history_permit(&self, socket_id: &SocketId) -> Result<OwnedSemaphorePermit> {
        let semaphore: Arc<Semaphore> = self
            .history_request_limits
            .entry(*socket_id)
            .or_insert_with(|| Arc::new(Semaphore::new(2)))
            .value()
            .clone();

        semaphore.try_acquire_owned().map_err(|_| {
            Error::ClientEvent(
                "Too many concurrent channel_history requests for this connection".to_string(),
            )
        })
    }

    pub(crate) async fn history_items_to_realtime_messages(
        &self,
        app_config: &App,
        channel: &str,
        items: &[HistoryItem],
    ) -> Result<Vec<PusherMessage>> {
        let latest_by_serial = self
            .latest_versions_for_history_items(app_config, channel, items)
            .await?;
        let version_stream_id = if latest_by_serial.is_empty() {
            None
        } else {
            self.version_store()
                .stream_state(&app_config.id, channel)
                .await?
                .stream_id
        };

        items
            .iter()
            .map(|item| {
                let raw_message = decode_stored_message_payload(item.payload_bytes.as_ref())
                    .map(|payload| payload.message)
                    .map_err(|e| {
                        Error::InvalidMessageFormat(format!("Invalid stored history payload: {e}"))
                    })?;
                let latest = extract_runtime_message_serial(&raw_message)
                    .map(MessageSerial::new)
                    .transpose()?
                    .as_ref()
                    .and_then(|serial| latest_by_serial.get(serial));
                Ok(match latest {
                    Some(latest) => self.build_runtime_message_from_record(
                        latest,
                        version_stream_id
                            .clone()
                            .or_else(|| raw_message.stream_id.clone()),
                    ),
                    None => raw_message,
                })
            })
            .collect()
    }

    async fn latest_versions_for_history_items(
        &self,
        app_config: &App,
        channel: &str,
        items: &[HistoryItem],
    ) -> Result<BTreeMap<MessageSerial, StoredVersionRecord>> {
        if !self.server_options().versioned_messages.enabled {
            return Ok(BTreeMap::new());
        }

        let message_serials = items
            .iter()
            .map(history_item_message_serial)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        self.version_store()
            .get_latest_batch(&app_config.id, channel, &message_serials)
            .await
    }

    fn history_item_response(
        &self,
        item: HistoryItem,
        latest: Option<&StoredVersionRecord>,
    ) -> Result<Value> {
        let mut event_name = item.event_name.clone();
        let mut operation_kind = item.operation_kind.clone();
        let mut payload_size_bytes = item.payload_size_bytes;
        let message = self.history_item_to_response_message(&item, latest)?;

        if let Some(message_event) = message
            .get("event")
            .and_then(|value| value.as_str())
            .map(str::to_string)
        {
            event_name = Some(message_event);
        }
        if let Some(action) = message.get("action").and_then(|value| value.as_str()) {
            operation_kind = action.to_string();
        }
        if let Ok(bytes) = sonic_rs::to_vec(&message) {
            payload_size_bytes = bytes.len();
        }

        Ok(json!({
            "stream_id": item.stream_id,
            "serial": item.serial,
            "published_at_ms": item.published_at_ms,
            "message_id": item.message_id,
            "event_name": event_name,
            "operation_kind": operation_kind,
            "payload_size_bytes": payload_size_bytes,
            "message": message,
        }))
    }

    fn history_item_to_response_message(
        &self,
        item: &HistoryItem,
        latest: Option<&StoredVersionRecord>,
    ) -> Result<Value> {
        let raw_message = decode_stored_message_payload(item.payload_bytes.as_ref())
            .map(|payload| payload.message)
            .map_err(|e| {
                Error::InvalidMessageFormat(format!("Invalid stored history payload: {e}"))
            })?;

        if !self.server_options().versioned_messages.enabled {
            return sonic_rs::to_value(&raw_message).map_err(Error::from);
        }

        let Some(_message_serial) = extract_runtime_message_serial(&raw_message) else {
            return sonic_rs::to_value(&raw_message).map_err(Error::from);
        };

        let Some(latest) = latest else {
            return sonic_rs::to_value(&raw_message).map_err(Error::from);
        };

        let action = protocol_action(latest.message.action);
        let message = VersionedRealtimeMessage {
            message: PusherMessage {
                event: Some(action.v2_event_name()),
                channel: Some(latest.channel.clone()),
                data: latest.message.data.clone(),
                name: latest.message.name.clone(),
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: raw_message.stream_id.clone(),
                serial: Some(latest.delivery_serial()),
                idempotency_key: None,
                extras: latest.message.extras.clone(),
                delta_sequence: None,
                delta_conflation_key: None,
            },
            action,
            message_serial: latest.message_serial().as_str().to_string(),
            history_serial: Some(latest.history_serial()),
            delivery_serial: Some(latest.delivery_serial()),
            version: Some(MessageVersionMetadata {
                serial: latest.version_serial().as_str().to_string(),
                client_id: latest.message.version.client_id.clone(),
                timestamp_ms: latest.message.version.timestamp_ms,
                description: latest.message.version.description.clone(),
                metadata: latest.message.version.metadata.clone(),
            }),
        };

        sonic_rs::to_value(&message).map_err(Error::from)
    }
}

fn history_item_message_serial(item: &HistoryItem) -> Result<Option<MessageSerial>> {
    let raw_message = decode_stored_message_payload(item.payload_bytes.as_ref())
        .map(|payload| payload.message)
        .map_err(|e| Error::InvalidMessageFormat(format!("Invalid stored history payload: {e}")))?;
    extract_runtime_message_serial(&raw_message)
        .map(MessageSerial::new)
        .transpose()
}

fn parse_channel_history_frame(message: &PusherMessage) -> Result<ChannelHistoryFrameRequest> {
    let root = match &message.data {
        Some(MessageData::String(value)) => sonic_rs::from_str(value).map_err(|e| {
            Error::InvalidMessageFormat(format!("Invalid channel_history data JSON: {e}"))
        })?,
        Some(MessageData::Json(value)) => value.clone(),
        _ => {
            return Err(Error::InvalidMessageFormat(
                "Missing data in channel_history message".to_string(),
            ));
        }
    };

    let params = root.get("params").unwrap_or(&root);
    let channel = root
        .get("channel")
        .or_else(|| params.get("channel"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::InvalidMessageFormat("Missing channel in channel_history message".to_string())
        })?;

    let direction = parse_history_direction(
        params
            .get("direction")
            .or_else(|| root.get("direction"))
            .and_then(Value::as_str),
    )?;
    let limit = params
        .get("limit")
        .or_else(|| root.get("limit"))
        .and_then(Value::as_u64)
        .map(|limit| limit as usize)
        .unwrap_or(DEFAULT_HISTORY_LIMIT)
        .min(MAX_CLIENT_HISTORY_LIMIT);
    let cursor = match params
        .get("cursor")
        .or_else(|| root.get("cursor"))
        .and_then(Value::as_str)
    {
        Some(encoded) => Some(HistoryCursor::decode(encoded)?),
        None => None,
    };
    let bounds = HistoryQueryBounds {
        start_serial: serial_param(params, &root, "start_serial")
            .or_else(|| serial_param(params, &root, "start")),
        end_serial: serial_param(params, &root, "end_serial")
            .or_else(|| serial_param(params, &root, "end")),
        start_time_ms: i64_param(params, &root, "start_time_ms"),
        end_time_ms: i64_param(params, &root, "end_time_ms"),
    };
    let until_attach = params
        .get("until_attach")
        .or_else(|| params.get("untilAttach"))
        .or_else(|| root.get("until_attach"))
        .or_else(|| root.get("untilAttach"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    Ok(ChannelHistoryFrameRequest {
        channel: channel.to_string(),
        direction,
        limit,
        cursor,
        bounds,
        until_attach,
    })
}

fn parse_history_direction(value: Option<&str>) -> Result<HistoryDirection> {
    match value.unwrap_or("backwards") {
        "newest_first" | "backwards" | "reverse" => Ok(HistoryDirection::NewestFirst),
        "oldest_first" | "forwards" | "forward" => Ok(HistoryDirection::OldestFirst),
        other => Err(Error::InvalidMessageFormat(format!(
            "Unsupported history direction: {other}"
        ))),
    }
}

fn serial_param(params: &Value, root: &Value, name: &str) -> Option<u64> {
    params
        .get(name)
        .or_else(|| root.get(name))
        .and_then(Value::as_u64)
}

fn i64_param(params: &Value, root: &Value, name: &str) -> Option<i64> {
    params
        .get(name)
        .or_else(|| root.get(name))
        .and_then(Value::as_i64)
}

fn build_history_continuity_payload(
    retained: &HistoryRetentionStats,
    complete: bool,
    truncated_by_retention: bool,
) -> Value {
    json!({
        "stream_id": retained.stream_id,
        "oldest_available_serial": retained.oldest_serial,
        "newest_available_serial": retained.newest_serial,
        "oldest_available_published_at_ms": retained.oldest_published_at_ms,
        "newest_available_published_at_ms": retained.newest_published_at_ms,
        "retained_messages": retained.retained_messages,
        "retained_bytes": retained.retained_bytes,
        "complete": complete,
        "truncated_by_retention": truncated_by_retention,
    })
}

fn protocol_action(
    action: sockudo_core::versioned_messages::MessageAction,
) -> ProtocolMessageAction {
    match action {
        sockudo_core::versioned_messages::MessageAction::Create => ProtocolMessageAction::Create,
        sockudo_core::versioned_messages::MessageAction::Update => ProtocolMessageAction::Update,
        sockudo_core::versioned_messages::MessageAction::Delete => ProtocolMessageAction::Delete,
        sockudo_core::versioned_messages::MessageAction::Append => ProtocolMessageAction::Append,
        sockudo_core::versioned_messages::MessageAction::Summary => ProtocolMessageAction::Summary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history_message(data: Value) -> PusherMessage {
        PusherMessage {
            event: Some("channel_history".to_string()),
            channel: None,
            data: Some(MessageData::Json(data)),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        }
    }

    #[test]
    fn channel_history_frame_defaults_to_current_contract() {
        let request = parse_channel_history_frame(&history_message(json!({
            "channel": "chat"
        })))
        .unwrap();

        assert_eq!(request.channel, "chat");
        assert_eq!(request.direction, HistoryDirection::NewestFirst);
        assert_eq!(request.limit, DEFAULT_HISTORY_LIMIT);
        assert!(!request.until_attach);
        assert_eq!(request.bounds, HistoryQueryBounds::default());
    }

    #[test]
    fn channel_history_frame_accepts_additive_v2_fields_and_aliases() {
        let request = parse_channel_history_frame(&history_message(json!({
            "channel": "chat",
            "params": {
                "limit": 2000,
                "direction": "forwards",
                "start": 2,
                "end_serial": 9,
                "untilAttach": true
            }
        })))
        .unwrap();

        assert_eq!(request.channel, "chat");
        assert_eq!(request.direction, HistoryDirection::OldestFirst);
        assert_eq!(request.limit, MAX_CLIENT_HISTORY_LIMIT);
        assert_eq!(request.bounds.start_serial, Some(2));
        assert_eq!(request.bounds.end_serial, Some(9));
        assert!(request.until_attach);
    }
}
