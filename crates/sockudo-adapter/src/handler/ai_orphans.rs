use super::ConnectionHandler;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::now_ms;
use sockudo_core::version_store::StoredVersionRecord;
use sockudo_core::versioned_messages::{
    FieldPatch, MessageAction as CoreMessageAction, MessageFieldDelta, MessageSerial,
    VersionMetadata, VersionSerial,
};
use sockudo_protocol::messages::{AiExtras, MessageExtras};
use sonic_rs::{Deserialize, Serialize, json};
use std::collections::HashMap;
use tracing::{debug, info, warn};

const ACTIVE_STREAM_PREFIX: &str = "ai_transport:active_stream:";
const ORPHAN_CLAIM_PREFIX: &str = "ai_transport:orphan_claim:";
const ORPHAN_SWEEP_SCAN_LIMIT: usize = 4096;
const ORPHAN_CLAIM_TTL_SECONDS: u64 = 120;
const ACTIVE_STREAM_TTL_SAFETY_SECONDS: u64 = 60;
const AI_STREAM_CANCELLED_WEBHOOK_REASON: &str = "orphan_timeout";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveAiStreamEntry {
    app_id: String,
    channel: String,
    message_serial: String,
    version_serial: String,
    history_serial: u64,
    delivery_serial: u64,
    last_seen_ms: i64,
}

impl ActiveAiStreamEntry {
    fn from_record(record: &StoredVersionRecord, last_seen_ms: i64) -> Self {
        Self {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            message_serial: record.message_serial().as_str().to_string(),
            version_serial: record.version_serial().as_str().to_string(),
            history_serial: record.history_serial(),
            delivery_serial: record.delivery_serial(),
            last_seen_ms,
        }
    }
}

impl ConnectionHandler {
    pub async fn record_ai_stream_activity(
        &self,
        app_id: &str,
        channel: &str,
        record: &StoredVersionRecord,
    ) -> Result<()> {
        if !self.server_options().ai_transport.matches_channel(channel) {
            return Ok(());
        }

        let key = active_stream_key(app_id, channel, record.message_serial().as_str());
        if record_ai_status(record) == Some("streaming") {
            let was_absent = !self.cache_manager().has(&key).await?;
            let entry = ActiveAiStreamEntry::from_record(record, now_ms());
            let value = sonic_rs::to_string(&entry).map_err(Error::from)?;
            self.cache_manager()
                .set(&key, &value, active_stream_ttl_seconds(self))
                .await?;
            if was_absent {
                self.cache_manager()
                    .increment_by(
                        &active_stream_count_key(app_id, channel),
                        1,
                        active_stream_ttl_seconds(self),
                    )
                    .await?;
            }
        } else {
            let was_present = self.cache_manager().has(&key).await?;
            if let Err(error) = self.cache_manager().remove(&key).await {
                debug!(
                    app_id = %app_id,
                    channel = %channel,
                    message_serial = %record.message_serial().as_str(),
                    error = %error,
                    "AI active stream registry entry was already absent"
                );
            } else if was_present {
                let next = self
                    .cache_manager()
                    .increment_by(
                        &active_stream_count_key(app_id, channel),
                        -1,
                        active_stream_ttl_seconds(self),
                    )
                    .await?;
                if next < 0 {
                    self.cache_manager()
                        .set(
                            &active_stream_count_key(app_id, channel),
                            "0",
                            active_stream_ttl_seconds(self),
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }

    pub async fn ai_active_stream_count(&self, app_id: &str, channel: &str) -> Result<usize> {
        let key = active_stream_count_key(app_id, channel);
        if self.cache_manager().get(&key).await?.is_none() {
            let bootstrapped = self
                .version_store()
                .latest_by_history(app_id, channel)
                .await?
                .iter()
                .filter(|record| record_ai_status(record) == Some("streaming"))
                .count();
            self.cache_manager()
                .set(
                    &key,
                    &bootstrapped.to_string(),
                    active_stream_ttl_seconds(self),
                )
                .await?;
        }

        Ok(self
            .cache_manager()
            .get(&key)
            .await?
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0))
    }

    pub async fn sweep_ai_stream_orphans_once(&self, now_ms: i64) -> Result<usize> {
        if !self.server_options().ai_transport.enabled {
            return Ok(0);
        }

        let orphan_ttl_ms = self
            .server_options()
            .ai_transport
            .rollup
            .orphan_ttl_ms
            .min(i64::MAX as u64) as i64;
        let mut closed = 0usize;
        let mut cursor = None;

        loop {
            let page = self
                .cache_manager()
                .scan_prefix_page(ACTIVE_STREAM_PREFIX, cursor, ORPHAN_SWEEP_SCAN_LIMIT)
                .await?;

            for (key, value) in page.entries {
                let entry: ActiveAiStreamEntry = match sonic_rs::from_str(&value) {
                    Ok(entry) => entry,
                    Err(error) => {
                        warn!(
                            key = %key,
                            error = %error,
                            "invalid AI active stream registry entry"
                        );
                        continue;
                    }
                };

                if now_ms.saturating_sub(entry.last_seen_ms) < orphan_ttl_ms {
                    continue;
                }

                if self.try_close_ai_stream_orphan(&entry, now_ms).await? {
                    closed += 1;
                }
            }

            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        Ok(closed)
    }

    async fn try_close_ai_stream_orphan(
        &self,
        entry: &ActiveAiStreamEntry,
        now_ms: i64,
    ) -> Result<bool> {
        let claim_key = orphan_claim_key(&entry.app_id, &entry.channel, &entry.message_serial);
        let process_id = self.server_options().instance.process_id.as_str();
        let claim_value = format!("{process_id}:{now_ms}");
        let claimed = self
            .cache_manager()
            .set_if_not_exists(&claim_key, &claim_value, ORPHAN_CLAIM_TTL_SECONDS)
            .await?;
        if !claimed {
            return Ok(false);
        }

        let message_serial = MessageSerial::new(entry.message_serial.clone())?;
        let Some(current) = self
            .version_store()
            .get_latest(&entry.app_id, &entry.channel, &message_serial)
            .await?
        else {
            remove_active_stream_key(self, entry).await;
            return Ok(false);
        };

        if current.version_serial().as_str() != entry.version_serial
            || current.delivery_serial() != entry.delivery_serial
        {
            self.record_ai_stream_activity(&entry.app_id, &entry.channel, &current)
                .await?;
            return Ok(false);
        }

        if record_ai_status(&current) != Some("streaming") {
            remove_active_stream_key(self, entry).await;
            return Ok(false);
        }

        let Some(app) = self.app_manager().find_by_id(&entry.app_id).await? else {
            warn!(
                app_id = %entry.app_id,
                channel = %entry.channel,
                message_serial = %entry.message_serial,
                "cannot close AI stream orphan because app no longer exists"
            );
            return Ok(false);
        };

        let reservation = self
            .version_store()
            .reserve_delivery_position_after(
                &entry.app_id,
                &entry.channel,
                current.delivery_serial(),
            )
            .await?;
        let cancelled = current.message.apply_mutation(
            CoreMessageAction::Update,
            VersionMetadata {
                serial: VersionSerial::new(self.next_version_serial())?,
                client_id: None,
                timestamp_ms: now_ms,
                description: Some("AI stream orphan timeout".to_string()),
                metadata: Some(json!({
                    "system": "ai_transport_orphan_janitor",
                    "reason": AI_STREAM_CANCELLED_WEBHOOK_REASON,
                })),
            },
            reservation.delivery_serial,
            MessageFieldDelta {
                extras: FieldPatch::Replace(cancelled_extras(&current)),
                ..MessageFieldDelta::default()
            },
        )?;
        let cancelled = StoredVersionRecord {
            app_id: current.app_id.clone(),
            channel: current.channel.clone(),
            original_client_id: current.original_client_id.clone(),
            message: cancelled,
        };

        self.version_store()
            .append_version(cancelled.clone())
            .await?;
        self.record_ai_stream_activity(&entry.app_id, &entry.channel, &cancelled)
            .await?;

        let runtime_message =
            self.build_runtime_message_from_record(&cancelled, Some(reservation.stream_id));
        self.broadcast_to_channel_force_full(&app, &entry.channel, runtime_message, None, None)
            .await?;

        if let Some(webhooks) = self.webhook_integration().as_ref()
            && let Err(error) = webhooks
                .send_ai_stream_orphaned(
                    &app,
                    &entry.channel,
                    &entry.message_serial,
                    AI_STREAM_CANCELLED_WEBHOOK_REASON,
                )
                .await
        {
            warn!(
                app_id = %entry.app_id,
                channel = %entry.channel,
                message_serial = %entry.message_serial,
                error = %error,
                "failed to emit AI stream orphaned webhook"
            );
        }

        info!(
            app_id = %entry.app_id,
            channel = %entry.channel,
            message_serial = %entry.message_serial,
            version_serial = %cancelled.version_serial().as_str(),
            delivery_serial = cancelled.delivery_serial(),
            "closed orphaned AI stream"
        );

        Ok(true)
    }
}

fn active_stream_key(app_id: &str, channel: &str, message_serial: &str) -> String {
    format!("{ACTIVE_STREAM_PREFIX}{app_id}:{channel}:{message_serial}")
}

fn active_stream_count_key(app_id: &str, channel: &str) -> String {
    format!("ai_transport:active_stream_count:{app_id}:{channel}")
}

fn orphan_claim_key(app_id: &str, channel: &str, message_serial: &str) -> String {
    format!("{ORPHAN_CLAIM_PREFIX}{app_id}:{channel}:{message_serial}")
}

fn active_stream_ttl_seconds(handler: &ConnectionHandler) -> u64 {
    handler
        .server_options()
        .ai_transport
        .rollup
        .orphan_ttl_ms
        .saturating_mul(2)
        .saturating_add(999)
        / 1_000
        + ACTIVE_STREAM_TTL_SAFETY_SECONDS
}

fn record_ai_status(record: &StoredVersionRecord) -> Option<&str> {
    record
        .message
        .extras
        .as_ref()
        .and_then(|extras| extras.ai_transport_headers())
        .and_then(|headers| headers.status())
}

fn cancelled_extras(current: &StoredVersionRecord) -> MessageExtras {
    let mut extras = current.message.extras.clone().unwrap_or_default();
    let ai = extras.ai.get_or_insert_with(AiExtras::default);
    let transport = ai.transport.get_or_insert_with(HashMap::new);
    transport.insert("status".to_string(), "cancelled".to_string());
    transport.insert(
        "error-code".to_string(),
        AI_STREAM_CANCELLED_WEBHOOK_REASON.to_string(),
    );
    transport.insert(
        "error-message".to_string(),
        "stream cancelled after orphan timeout".to_string(),
    );
    extras
}

async fn remove_active_stream_key(handler: &ConnectionHandler, entry: &ActiveAiStreamEntry) {
    let key = active_stream_key(&entry.app_id, &entry.channel, &entry.message_serial);
    if let Err(error) = handler.cache_manager().remove(&key).await {
        debug!(
            app_id = %entry.app_id,
            channel = %entry.channel,
            message_serial = %entry.message_serial,
            error = %error,
            "AI active stream registry key was already removed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_manager::ConnectionManager;
    use crate::local_adapter::LocalAdapter;
    use async_trait::async_trait;
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_core::app::{App, AppManager, AppPolicy};
    use sockudo_core::cache::CacheManager;
    use sockudo_core::history::{HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig};
    use sockudo_core::options::{AiTransportChannelConfig, ServerOptions};
    use sockudo_core::version_store::{
        MemoryVersionStore, StoredVersionRecord, VersionStore, VersionWriteReservation,
    };
    use sockudo_core::versioned_messages::{MessageIdentity, ReplayPosition, VersionedMessage};
    use sockudo_protocol::messages::{MessageData, MessageExtras};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    const APP_ID: &str = "app";
    const CHANNEL: &str = "ai-chat";
    const MESSAGE_SERIAL: &str = "msg-1";

    #[derive(Default)]
    struct TestCache {
        entries: Mutex<BTreeMap<String, String>>,
    }

    #[async_trait]
    impl CacheManager for TestCache {
        async fn has(&self, key: &str) -> Result<bool> {
            Ok(self.entries.lock().await.contains_key(key))
        }

        async fn get(&self, key: &str) -> Result<Option<String>> {
            Ok(self.entries.lock().await.get(key).cloned())
        }

        async fn set(&self, key: &str, value: &str, _ttl_seconds: u64) -> Result<()> {
            self.entries
                .lock()
                .await
                .insert(key.to_string(), value.to_string());
            Ok(())
        }

        async fn remove(&self, key: &str) -> Result<()> {
            self.entries.lock().await.remove(key);
            Ok(())
        }

        async fn disconnect(&self) -> Result<()> {
            self.entries.lock().await.clear();
            Ok(())
        }

        async fn ttl(&self, _key: &str) -> Result<Option<Duration>> {
            Ok(None)
        }

        async fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
            Ok(self
                .entries
                .lock()
                .await
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .take(limit)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }

        async fn set_if_not_exists(
            &self,
            key: &str,
            value: &str,
            _ttl_seconds: u64,
        ) -> Result<bool> {
            let mut entries = self.entries.lock().await;
            if entries.contains_key(key) {
                return Ok(false);
            }
            entries.insert(key.to_string(), value.to_string());
            Ok(true)
        }
    }

    async fn build_handler(
        app_manager: Arc<MemoryAppManager>,
        cache: Arc<TestCache>,
        version_store: Arc<MemoryVersionStore>,
    ) -> ConnectionHandler {
        let adapter = Arc::new(LocalAdapter::new());
        let mut options = ServerOptions::default();
        options.ai_transport.enabled = true;
        options.ai_transport.channels = vec![AiTransportChannelConfig {
            prefix: "ai-".to_string(),
        }];
        options.ai_transport.rollup.orphan_ttl_ms = 1_000;
        options.history.enabled = true;
        options.versioned_messages.enabled = true;

        ConnectionHandler::builder(
            app_manager as Arc<dyn AppManager + Send + Sync>,
            adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
            cache as Arc<dyn CacheManager + Send + Sync>,
            options,
        )
        .local_adapter(adapter)
        .history_store(
            Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()))
                as Arc<dyn HistoryStore + Send + Sync>,
        )
        .version_store(version_store as Arc<dyn VersionStore + Send + Sync>)
        .build()
    }

    fn streaming_extras() -> MessageExtras {
        let mut transport = HashMap::new();
        transport.insert("status".to_string(), "streaming".to_string());
        MessageExtras {
            ai: Some(AiExtras {
                transport: Some(transport),
                ..AiExtras::default()
            }),
            ..MessageExtras::default()
        }
    }

    fn record(delivery_serial: u64, status_extras: MessageExtras) -> StoredVersionRecord {
        StoredVersionRecord {
            app_id: APP_ID.to_string(),
            channel: CHANNEL.to_string(),
            original_client_id: Some("client".to_string()),
            message: VersionedMessage {
                action: CoreMessageAction::Create,
                identity: MessageIdentity {
                    message_serial: MessageSerial::new(MESSAGE_SERIAL).unwrap(),
                    history_serial: 1,
                },
                replay_position: ReplayPosition { delivery_serial },
                version: VersionMetadata {
                    serial: VersionSerial::new(format!(
                        "{:020}:fixture:{:020}",
                        delivery_serial, delivery_serial
                    ))
                    .unwrap(),
                    client_id: Some("client".to_string()),
                    timestamp_ms: 1,
                    description: None,
                    metadata: None,
                },
                name: Some("response".to_string()),
                data: Some(MessageData::String("partial".to_string())),
                extras: Some(status_extras),
            },
        }
    }

    #[tokio::test]
    async fn janitor_closes_streaming_orphan_once_from_another_node() {
        let app_manager = Arc::new(MemoryAppManager::new());
        app_manager
            .create_app(App::from_policy(
                APP_ID.to_string(),
                "key".to_string(),
                "secret".to_string(),
                true,
                AppPolicy::default(),
            ))
            .await
            .unwrap();
        let cache = Arc::new(TestCache::default());
        let version_store = Arc::new(MemoryVersionStore::new());
        let first_reservation = VersionWriteReservation {
            stream_id: "stream".to_string(),
            delivery_serial: 1,
        };
        let initial = record(first_reservation.delivery_serial, streaming_extras());
        version_store.append_version(initial.clone()).await.unwrap();

        let node_a = build_handler(app_manager.clone(), cache.clone(), version_store.clone()).await;
        let node_b = build_handler(app_manager, cache, version_store.clone()).await;

        node_a
            .record_ai_stream_activity(APP_ID, CHANNEL, &initial)
            .await
            .unwrap();

        let closed = node_b
            .sweep_ai_stream_orphans_once(now_ms().saturating_add(2_000))
            .await
            .unwrap();
        assert_eq!(closed, 1);

        let latest = version_store
            .get_latest(
                APP_ID,
                CHANNEL,
                &MessageSerial::new(MESSAGE_SERIAL).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.message.action, CoreMessageAction::Update);
        assert_eq!(
            latest
                .message
                .extras
                .as_ref()
                .and_then(|extras| extras.ai_transport_headers())
                .and_then(|headers| headers.status()),
            Some("cancelled")
        );

        let closed_again = node_b
            .sweep_ai_stream_orphans_once(now_ms().saturating_add(3_000))
            .await
            .unwrap();
        assert_eq!(closed_again, 0);
    }

    #[tokio::test]
    async fn janitor_refreshes_registry_when_latest_version_advanced() {
        let app_manager = Arc::new(MemoryAppManager::new());
        app_manager
            .create_app(App::from_policy(
                APP_ID.to_string(),
                "key".to_string(),
                "secret".to_string(),
                true,
                AppPolicy::default(),
            ))
            .await
            .unwrap();
        let cache = Arc::new(TestCache::default());
        let version_store = Arc::new(MemoryVersionStore::new());
        let initial = record(1, streaming_extras());
        let advanced = record(2, streaming_extras());
        version_store.append_version(initial.clone()).await.unwrap();
        version_store
            .append_version(advanced.clone())
            .await
            .unwrap();

        let node = build_handler(app_manager, cache, version_store.clone()).await;
        node.record_ai_stream_activity(APP_ID, CHANNEL, &initial)
            .await
            .unwrap();

        let closed = node
            .sweep_ai_stream_orphans_once(now_ms().saturating_add(2_000))
            .await
            .unwrap();
        assert_eq!(closed, 0);

        let latest = version_store
            .get_latest(
                APP_ID,
                CHANNEL,
                &MessageSerial::new(MESSAGE_SERIAL).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.delivery_serial(), 2);
        assert_eq!(record_ai_status(&latest), Some("streaming"));
    }
}
