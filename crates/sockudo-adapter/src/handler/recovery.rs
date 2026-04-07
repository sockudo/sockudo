use super::ConnectionHandler;
use bytes::Bytes;
use serde::Deserialize;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{HistoryDirection, HistoryQueryBounds, HistoryReadRequest};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sonic_rs::json;
use std::collections::HashMap;
use tracing::warn;

use crate::replay_buffer::ReplayLookup;

#[derive(Debug, Clone)]
struct ResumePosition {
    serial: u64,
    stream_id: Option<String>,
    last_message_id: Option<String>,
    legacy_serial_only: bool,
}

#[derive(Debug, Clone)]
struct ResumeFailure {
    code: &'static str,
    reason: &'static str,
    expected_stream_id: Option<String>,
    current_stream_id: Option<String>,
    oldest_available_serial: Option<u64>,
    newest_available_serial: Option<u64>,
}

#[derive(Debug, Clone)]
enum ResumeOutcome {
    Recovered {
        source: &'static str,
        count: usize,
        messages: Vec<Bytes>,
    },
    Failed(ResumeFailure),
}

impl ConnectionHandler {
    /// Handle a `pusher:resume` event from a reconnecting client.
    ///
    /// V2 clients should send `channel_positions` with `{ stream_id, serial }`.
    /// Legacy `channel_serials` is still accepted for the hot-buffer fast path only.
    pub async fn handle_resume(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let replay_buffer = match &self.replay_buffer {
            Some(rb) => rb,
            None => {
                self.send_message_to_socket(
                    &app_config.id,
                    socket_id,
                    PusherMessage::error(
                        4301,
                        "Connection recovery is not enabled".to_string(),
                        None,
                    ),
                )
                .await?;
                return Ok(());
            }
        };

        let channel_positions = parse_resume_positions(message)?;
        if channel_positions.is_empty() {
            self.send_message_to_socket(
                &app_config.id,
                socket_id,
                PusherMessage::error(4302, "No channel recovery positions provided".to_string(), None),
            )
            .await?;
            return Ok(());
        }

        let mut recovered = Vec::new();
        let mut failed = Vec::new();

        for (channel, position) in channel_positions {
            let outcome = self
                .resume_channel(socket_id, app_config, replay_buffer, &channel, &position)
                .await;

            match outcome {
                ResumeOutcome::Recovered {
                    source,
                    count,
                    messages,
                } => {
                    if let Err(failure) = self
                        .send_replayed_bytes(socket_id, &app_config.id, messages)
                        .await
                    {
                        self.send_resume_failed(socket_id, app_config, &channel, &failure)
                            .await
                            .ok();
                        failed.push(json!({
                            "channel": channel,
                            "code": failure.code,
                            "reason": failure.reason,
                            "expected_stream_id": failure.expected_stream_id,
                            "current_stream_id": failure.current_stream_id,
                            "oldest_available_serial": failure.oldest_available_serial,
                            "newest_available_serial": failure.newest_available_serial,
                        }));
                        if let Some(metrics) = self.metrics.as_ref() {
                            metrics.mark_history_recovery_failure(&app_config.id, failure.code);
                        }
                        continue;
                    }
                    if let Some(metrics) = self.metrics.as_ref() {
                        metrics.mark_history_recovery_success(&app_config.id, source);
                    }
                    recovered.push(json!({
                        "channel": channel,
                        "source": source,
                        "replayed": count,
                    }));
                }
                ResumeOutcome::Failed(failure) => {
                    self.send_resume_failed(socket_id, app_config, &channel, &failure)
                        .await
                        .ok();
                    if let Some(metrics) = self.metrics.as_ref() {
                        metrics.mark_history_recovery_failure(&app_config.id, failure.code);
                    }
                    failed.push(json!({
                        "channel": channel,
                        "code": failure.code,
                        "reason": failure.reason,
                        "expected_stream_id": failure.expected_stream_id,
                        "current_stream_id": failure.current_stream_id,
                        "oldest_available_serial": failure.oldest_available_serial,
                        "newest_available_serial": failure.newest_available_serial,
                    }));
                }
            }
        }

        let success_msg = PusherMessage {
            event: Some(sockudo_protocol::constants::EVENT_RESUME_SUCCESS.to_string()),
            channel: None,
            data: Some(MessageData::String(
                json!({
                    "recovered": recovered,
                    "failed": failed,
                })
                .to_string(),
            )),
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
        };
        self.send_message_to_socket(&app_config.id, socket_id, success_msg)
            .await?;

        Ok(())
    }

    async fn resume_channel(
        &self,
        _socket_id: &SocketId,
        app_config: &App,
        replay_buffer: &crate::replay_buffer::ReplayBuffer,
        channel: &str,
        position: &ResumePosition,
    ) -> ResumeOutcome {
        match replay_buffer.get_messages_after_position(
            &app_config.id,
            channel,
            position.stream_id.as_deref(),
            position.serial,
        ) {
            ReplayLookup::Recovered(messages) => {
                let count = messages.len();
                return ResumeOutcome::Recovered {
                    source: "hot",
                    count,
                    messages,
                };
            }
            ReplayLookup::StreamReset { current_stream_id } => {
                return ResumeOutcome::Failed(ResumeFailure {
                    code: "stream_reset",
                    reason: "stream_id_mismatch",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id,
                    oldest_available_serial: None,
                    newest_available_serial: None,
                });
            }
            ReplayLookup::Expired => {}
        }

        if position.legacy_serial_only {
            return ResumeOutcome::Failed(ResumeFailure {
                code: "continuity_unverifiable",
                reason: "legacy_serial_only_position_cannot_use_cold_recovery",
                expected_stream_id: None,
                current_stream_id: None,
                oldest_available_serial: None,
                newest_available_serial: None,
            });
        }

        if !self.server_options().history.enabled {
            return ResumeOutcome::Failed(ResumeFailure {
                code: "position_expired",
                reason: "hot_replay_expired_and_durable_history_disabled",
                expected_stream_id: position.stream_id.clone(),
                current_stream_id: None,
                oldest_available_serial: None,
                newest_available_serial: None,
            });
        }

        match self
            .collect_resume_from_history(app_config, channel, position)
            .await
        {
            Ok(messages) => ResumeOutcome::Recovered {
                source: "cold",
                count: messages.len(),
                messages,
            },
            Err(failure) => ResumeOutcome::Failed(failure),
        }
    }

    async fn collect_resume_from_history(
        &self,
        app_config: &App,
        channel: &str,
        position: &ResumePosition,
    ) -> std::result::Result<Vec<Bytes>, ResumeFailure> {
        let mut cursor = None;
        let mut recovered_messages = Vec::new();
        let bounds = HistoryQueryBounds {
            start_serial: Some(position.serial.saturating_add(1)),
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
        };
        let mut first_page = true;

        loop {
            let page = self
                .history_store()
                .read_page(HistoryReadRequest {
                    app_id: app_config.id.clone(),
                    channel: channel.to_string(),
                    direction: HistoryDirection::OldestFirst,
                    limit: self.server_options().history.max_page_size,
                    cursor: cursor.clone(),
                    bounds: bounds.clone(),
                })
                .await
                .map_err(|_| ResumeFailure {
                    code: "persistence_unavailable",
                    reason: "durable_history_read_failed",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: None,
                    oldest_available_serial: None,
                    newest_available_serial: None,
                })?;

            if first_page {
                first_page = false;

                if page.retained.stream_id.as_deref() != position.stream_id.as_deref() {
                    return Err(ResumeFailure {
                        code: "stream_reset",
                        reason: "durable_stream_id_mismatch",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: page.retained.stream_id.clone(),
                        oldest_available_serial: page.retained.oldest_serial,
                        newest_available_serial: page.retained.newest_serial,
                    });
                }

                if page.truncated_by_retention {
                    return Err(ResumeFailure {
                        code: "position_expired",
                        reason: "retained_history_floor_is_ahead_of_requested_serial",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: page.retained.stream_id.clone(),
                        oldest_available_serial: page.retained.oldest_serial,
                        newest_available_serial: page.retained.newest_serial,
                    });
                }
            }

            let count_this_page = page.items.len();
            for item in page.items {
                if position
                    .last_message_id
                    .as_ref()
                    .is_some_and(|last_id| item.message_id.as_ref() == Some(last_id))
                {
                    continue;
                }
                recovered_messages.push(item.payload_bytes);
            }

            if !page.has_more {
                if count_this_page == 0
                    && page
                        .retained
                        .newest_serial
                        .is_some_and(|newest| newest < position.serial)
                {
                    return Ok(Vec::new());
                }
                break;
            }
            cursor = page.next_cursor;
        }

        Ok(recovered_messages)
    }

    async fn send_resume_failed(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
        failure: &ResumeFailure,
    ) -> Result<()> {
        let fail_msg = PusherMessage {
            event: Some(sockudo_protocol::constants::EVENT_RESUME_FAILED.to_string()),
            channel: Some(channel.to_string()),
            data: Some(MessageData::String(
                json!({
                    "code": failure.code,
                    "reason": failure.reason,
                    "expected_stream_id": failure.expected_stream_id,
                    "current_stream_id": failure.current_stream_id,
                    "oldest_available_serial": failure.oldest_available_serial,
                    "newest_available_serial": failure.newest_available_serial,
                })
                .to_string(),
            )),
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
        };
        self.send_message_to_socket(&app_config.id, socket_id, fail_msg)
            .await
    }

    /// Send pre-serialized message bytes directly to a socket.
    async fn send_raw_bytes_to_socket(
        &self,
        socket_id: &SocketId,
        app_id: &str,
        bytes: Bytes,
    ) -> Result<()> {
        if let Some(conn) = self
            .connection_manager
            .get_connection(socket_id, app_id)
            .await
        {
            conn.send_broadcast(bytes)
        } else {
            Err(Error::ConnectionClosed(format!(
                "Socket {} not found during replay",
                socket_id
            )))
        }
    }

    async fn send_replayed_bytes(
        &self,
        socket_id: &SocketId,
        app_id: &str,
        messages: Vec<Bytes>,
    ) -> std::result::Result<(), ResumeFailure> {
        send_replayed_bytes_impl(self, socket_id, app_id, messages).await
    }
}

async fn send_replayed_bytes_impl(
    handler: &ConnectionHandler,
    socket_id: &SocketId,
    app_id: &str,
    messages: Vec<Bytes>,
) -> std::result::Result<(), ResumeFailure> {
    for bytes in messages {
        if let Err(err) = handler.send_raw_bytes_to_socket(socket_id, app_id, bytes).await {
            warn!("Failed to replay message to socket {}: {}", socket_id, err);
            return Err(ResumeFailure {
                code: "persistence_unavailable",
                reason: "failed_to_deliver_recovery_payload",
                expected_stream_id: None,
                current_stream_id: None,
                oldest_available_serial: None,
                newest_available_serial: None,
            });
        }
    }
    Ok(())
}

fn parse_resume_positions(message: &PusherMessage) -> Result<HashMap<String, ResumePosition>> {
    let data_str = match &message.data {
        Some(MessageData::String(s)) => s.clone(),
        Some(MessageData::Json(v)) => v.to_string(),
        _ => {
            return Err(Error::InvalidMessageFormat(
                "Missing data in resume message".to_string(),
            ));
        }
    };

    #[derive(Deserialize)]
    struct ChannelPosition {
        serial: u64,
        stream_id: Option<String>,
        last_message_id: Option<String>,
    }

    #[derive(Deserialize)]
    struct ResumeData {
        channel_positions: Option<HashMap<String, ChannelPosition>>,
        channel_serials: Option<HashMap<String, u64>>,
    }

    let resume_data: ResumeData = sonic_rs::from_str(&data_str)
        .map_err(|e| Error::InvalidMessageFormat(format!("Invalid resume data JSON: {e}")))?;

    if let Some(channel_positions) = resume_data.channel_positions {
        return Ok(channel_positions
            .into_iter()
            .map(|(channel, position)| {
                (
                    channel,
                    ResumePosition {
                        serial: position.serial,
                        stream_id: position.stream_id,
                        last_message_id: position.last_message_id,
                        legacy_serial_only: false,
                    },
                )
            })
            .collect());
    }

    resume_data
        .channel_serials
        .map(|positions| {
            positions
                .into_iter()
                .map(|(channel, serial)| {
                    (
                        channel,
                        ResumePosition {
                            serial,
                            stream_id: None,
                            last_message_id: None,
                            legacy_serial_only: true,
                        },
                    )
                })
                .collect()
        })
        .ok_or_else(|| {
            Error::InvalidMessageFormat(
                "Missing channel_positions or channel_serials in resume data".to_string(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use crate::ConnectionManager;
    use crate::local_adapter::LocalAdapter;
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_core::app::AppManager;
    use sockudo_core::cache::CacheManager;
    use sockudo_core::history::{HistoryAppendRecord, MemoryHistoryStore, MemoryHistoryStoreConfig};
    use sockudo_core::metrics::MetricsInterface;
    use sockudo_core::options::ServerOptions;
    use sonic_rs::Value;
    use std::sync::Arc;
    use std::time::Duration;

    struct TestCache;

    #[async_trait]
    impl CacheManager for TestCache {
        async fn has(&self, _key: &str) -> Result<bool> { Ok(false) }
        async fn get(&self, _key: &str) -> Result<Option<String>> { Ok(None) }
        async fn set(&self, _key: &str, _value: &str, _ttl_seconds: u64) -> Result<()> { Ok(()) }
        async fn remove(&self, _key: &str) -> Result<()> { Ok(()) }
        async fn disconnect(&self) -> Result<()> { Ok(()) }
        async fn ttl(&self, _key: &str) -> Result<Option<Duration>> { Ok(None) }
    }

    struct TestMetrics;

    #[async_trait]
    impl MetricsInterface for TestMetrics {
        async fn init(&self) -> Result<()> { Ok(()) }
        fn mark_new_connection(&self, _app_id: &str, _socket_id: &SocketId) {}
        fn mark_disconnection(&self, _app_id: &str, _socket_id: &SocketId) {}
        fn mark_connection_error(&self, _app_id: &str, _error_type: &str) {}
        fn mark_rate_limit_check(&self, _app_id: &str, _limiter_type: &str) {}
        fn mark_rate_limit_check_with_context(&self, _app_id: &str, _limiter_type: &str, _request_context: &str) {}
        fn mark_rate_limit_triggered(&self, _app_id: &str, _limiter_type: &str) {}
        fn mark_rate_limit_triggered_with_context(&self, _app_id: &str, _limiter_type: &str, _request_context: &str) {}
        fn mark_channel_subscription(&self, _app_id: &str, _channel_type: &str) {}
        fn mark_channel_unsubscription(&self, _app_id: &str, _channel_type: &str) {}
        fn update_active_channels(&self, _app_id: &str, _channel_type: &str, _count: i64) {}
        fn mark_api_message(&self, _app_id: &str, _incoming_message_size: usize, _sent_message_size: usize) {}
        fn mark_ws_message_sent(&self, _app_id: &str, _sent_message_size: usize) {}
        fn mark_ws_messages_sent_batch(&self, _app_id: &str, _sent_message_size: usize, _count: usize) {}
        fn mark_ws_message_received(&self, _app_id: &str, _message_size: usize) {}
        fn track_horizontal_adapter_resolve_time(&self, _app_id: &str, _time_ms: f64) {}
        fn track_horizontal_adapter_resolved_promises(&self, _app_id: &str, _resolved: bool) {}
        fn mark_horizontal_adapter_request_sent(&self, _app_id: &str) {}
        fn mark_horizontal_adapter_request_received(&self, _app_id: &str) {}
        fn mark_horizontal_adapter_response_received(&self, _app_id: &str) {}
        fn track_broadcast_latency(&self, _app_id: &str, _channel_name: &str, _recipient_count: usize, _latency_ms: f64) {}
        fn track_horizontal_delta_compression(&self, _app_id: &str, _channel_name: &str, _enabled: bool) {}
        fn track_delta_compression_bandwidth(&self, _app_id: &str, _channel_name: &str, _original_bytes: usize, _compressed_bytes: usize) {}
        fn track_delta_compression_full_message(&self, _app_id: &str, _channel_name: &str) {}
        fn track_delta_compression_delta_message(&self, _app_id: &str, _channel_name: &str) {}
        async fn get_metrics_as_plaintext(&self) -> String { String::new() }
        async fn get_metrics_as_json(&self) -> Value { sonic_rs::json!({}) }
        async fn clear(&self) {}
    }

    fn build_handler(history_enabled: bool) -> ConnectionHandler {
        build_handler_with_history_store(
            history_enabled,
            Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
        )
    }

    fn build_handler_with_history_store(
        history_enabled: bool,
        history_store: Arc<dyn sockudo_core::history::HistoryStore + Send + Sync>,
    ) -> ConnectionHandler {
        let mut options = ServerOptions::default();
        options.connection_recovery.enabled = true;
        options.history.enabled = history_enabled;
        options.history.max_page_size = 100;

        ConnectionHandler::builder(
            Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>,
            Arc::new(LocalAdapter::new()) as Arc<dyn ConnectionManager + Send + Sync>,
            Arc::new(TestCache),
            options,
        )
        .metrics(Arc::new(TestMetrics))
        .history_store(history_store)
        .build()
    }

    async fn append_history(
        handler: &ConnectionHandler,
        app_id: &str,
        channel: &str,
        serials: &[u64],
        stream_id: &str,
    ) {
        let base_ts = sockudo_core::history::now_ms();
        for serial in serials {
            handler
                .history_store()
                .append(HistoryAppendRecord {
                    app_id: app_id.to_string(),
                    channel: channel.to_string(),
                    stream_id: stream_id.to_string(),
                    serial: *serial,
                    published_at_ms: base_ts + *serial as i64,
                    message_id: Some(format!("msg-{serial}")),
                    event_name: Some("evt".to_string()),
                    operation_kind: "append".to_string(),
                    payload_bytes: Bytes::from(
                        sonic_rs::to_vec(&PusherMessage {
                            event: Some("evt".to_string()),
                            channel: Some(channel.to_string()),
                            data: None,
                            name: None,
                            user_id: None,
                            tags: None,
                            sequence: None,
                            conflation_key: None,
                            message_id: Some(format!("msg-{serial}")),
                            stream_id: Some(stream_id.to_string()),
                            serial: Some(*serial),
                            idempotency_key: None,
                            extras: None,
                            delta_sequence: None,
                            delta_conflation_key: None,
                        })
                        .unwrap(),
                    ),
                })
                .await
                .unwrap();
        }
    }

    fn test_app(app_id: &str) -> App {
        App::from_policy(
            app_id.to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            Default::default(),
        )
    }

    fn resume_message(payload: sonic_rs::Value) -> PusherMessage {
        PusherMessage {
            event: Some(sockudo_protocol::constants::EVENT_RESUME.to_string()),
            channel: None,
            data: Some(MessageData::String(payload.to_string())),
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
    fn parse_resume_positions_supports_legacy_serials() {
        let positions = parse_resume_positions(&resume_message(json!({
            "channel_serials": { "chat": 42 }
        })))
        .unwrap();
        let position = positions.get("chat").unwrap();
        assert_eq!(position.serial, 42);
        assert!(position.stream_id.is_none());
        assert!(position.legacy_serial_only);
    }

    #[test]
    fn parse_resume_positions_supports_continuity_tokens() {
        let positions = parse_resume_positions(&resume_message(json!({
            "channel_positions": {
                "chat": {
                    "serial": 42,
                    "stream_id": "stream-1",
                    "last_message_id": "msg-42"
                }
            }
        })))
        .unwrap();
        let position = positions.get("chat").unwrap();
        assert_eq!(position.serial, 42);
        assert_eq!(position.stream_id.as_deref(), Some("stream-1"));
        assert_eq!(position.last_message_id.as_deref(), Some("msg-42"));
        assert!(!position.legacy_serial_only);
    }

    #[tokio::test]
    async fn hot_recovery_success_uses_replay_buffer() {
        let handler = build_handler(true);
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        replay_buffer.store("app", "chat", Some("stream-1"), 2, Bytes::from_static(b"two"));
        replay_buffer.store("app", "chat", Some("stream-1"), 3, Bytes::from_static(b"three"));
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, count, .. } => {
                assert_eq!(source, "hot");
                assert_eq!(count, 2);
            }
            other => panic!("expected hot recovery, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cold_recovery_success_uses_durable_history() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, count, .. } => {
                assert_eq!(source, "cold");
                assert_eq!(count, 2);
            }
            other => panic!("expected cold recovery, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn epoch_mismatch_fails_explicitly() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[2, 3], "stream-new").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-old".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => assert_eq!(failure.code, "stream_reset"),
            other => panic!("expected failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn retention_gap_fails_explicitly() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[5, 6], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => assert_eq!(failure.code, "position_expired"),
            other => panic!("expected retention-gap failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn per_channel_recovery_outcomes_are_independent() {
        let handler = build_handler(true);
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        replay_buffer.store("app", "good", Some("stream-good"), 2, Bytes::from_static(b"two"));
        append_history(&handler, "app", "bad", &[5, 6], "stream-bad").await;
        let app = test_app("app");

        let good = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "good",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-good".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;
        let bad = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "bad",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-bad".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        assert!(matches!(good, ResumeOutcome::Recovered { .. }));
        assert!(matches!(bad, ResumeOutcome::Failed(_)));
    }

    #[tokio::test]
    async fn shared_history_store_allows_cross_node_cold_recovery() {
        let shared_history =
            Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let node_a = build_handler_with_history_store(true, shared_history.clone());
        let node_b = build_handler_with_history_store(true, shared_history);
        let app = test_app("app");

        append_history(&node_a, "app", "chat", &[2, 3], "stream-1").await;

        let outcome = node_b
            .resume_channel(
                &SocketId::new(),
                &app,
                node_b.replay_buffer().unwrap(),
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, count, .. } => {
                assert_eq!(source, "cold");
                assert_eq!(count, 2);
            }
            other => panic!("expected shared-store cold recovery, got {other:?}"),
        }
    }
}
