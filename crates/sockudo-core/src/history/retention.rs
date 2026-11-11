use super::store::HistoryStore;
use super::time::now_ms;
use super::types::*;
use crate::app::AppManager;
use crate::error::Result;
use crate::options::HistoryConfig;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::warn;

/// Enforces the history retention window on reads.
///
/// Backends prune by age only when a channel is written to and in the
/// periodic purge job, so an entry can stay stored for up to the retention
/// window plus the purge interval. This decorator drops entries older than
/// the channel's resolved `retention_window_seconds` from every page and marks
/// the page `truncated_by_retention`, so history reads never serve them and
/// cold recovery fails closed with `position_expired` instead of replaying
/// them.
pub struct RetentionWindowHistoryStore {
    inner: Arc<dyn HistoryStore + Send + Sync>,
    app_manager: Arc<dyn AppManager + Send + Sync>,
    global: HistoryConfig,
}

impl RetentionWindowHistoryStore {
    pub fn new(
        inner: Arc<dyn HistoryStore + Send + Sync>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        global: HistoryConfig,
    ) -> Self {
        Self {
            inner,
            app_manager,
            global,
        }
    }

    /// Oldest `published_at_ms` still inside the retention window.
    ///
    /// A window of `0` expires everything, matching how the backends prune on
    /// write. Global `[history] retention_window_seconds = 0` is rejected at
    /// config validation; an app or namespace override of `0` keeps this
    /// meaning. A failed app lookup fails the read: substituting the global
    /// window could serve entries past a shorter app window.
    async fn cutoff_ms(&self, app_id: &str, channel: &str) -> Result<i64> {
        let retention_window_seconds = match self.app_manager.find_by_id(app_id).await {
            Ok(Some(app)) => {
                app.resolved_history(channel, &self.global)
                    .retention_window_seconds
            }
            Ok(None) => self.global.retention_window_seconds,
            Err(error) => {
                warn!(
                    app_id = %app_id,
                    error = %error,
                    "failed to resolve history retention policy"
                );
                return Err(error);
            }
        };
        let window_ms =
            i64::try_from(retention_window_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX);
        Ok(now_ms().saturating_sub(window_ms))
    }
}

/// Removes expired items from `page` and marks it truncated only when it
/// actually lost items, so a read (or resume) that starts after the expired
/// entries is unaffected while they wait for the purge. Newest-first
/// pagination stops only when a whole page expired: publish timestamps are
/// taken before serials are allocated, so a fresh entry can sit behind an
/// expired one and must stay reachable. Oldest-first pagination always
/// continues to the fresh entries.
fn drop_expired_items(page: &mut HistoryPage, direction: HistoryDirection, cutoff_ms: i64) {
    let before = page.items.len();
    page.items.retain(|item| item.published_at_ms >= cutoff_ms);
    if page.items.len() == before {
        return;
    }

    page.truncated_by_retention = true;
    page.complete = false;
    if direction == HistoryDirection::NewestFirst && page.items.is_empty() {
        page.has_more = false;
        page.next_cursor = None;
    }
}

/// The stored floor names the oldest row still stored, which can predate the
/// window while it waits for the purge job. When it does, the readable floor is
/// unknown, so continuity reports null instead of an entry reads skip. While
/// the stored floor is inside the window it is readable, so it stays.
fn window_retained_floor(retained: &mut HistoryRetentionStats, cutoff_ms: i64) {
    if retained
        .oldest_published_at_ms
        .is_some_and(|oldest| oldest < cutoff_ms)
    {
        retained.oldest_serial = None;
        retained.oldest_published_at_ms = None;
    }
}

#[async_trait]
impl HistoryStore for RetentionWindowHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        self.inner.reserve_publish_position(app_id, channel).await
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        self.inner.append(record).await
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        let direction = request.direction;
        let cutoff_ms = self.cutoff_ms(&request.app_id, &request.channel).await?;
        let mut page = self.inner.read_page(request).await?;
        drop_expired_items(&mut page, direction, cutoff_ms);
        window_retained_floor(&mut page.retained, cutoff_ms);
        Ok(page)
    }

    async fn channel_head(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        self.inner.channel_head(app_id, channel).await
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        self.inner.stream_runtime_state(app_id, channel).await
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        let cutoff_ms = self.cutoff_ms(app_id, channel).await?;
        let mut inspection = self.inner.stream_inspection(app_id, channel).await?;
        window_retained_floor(&mut inspection.retained, cutoff_ms);
        Ok(inspection)
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<HistoryResetResult> {
        self.inner
            .reset_stream(app_id, channel, reason, requested_by)
            .await
    }

    async fn purge_stream(
        &self,
        app_id: &str,
        channel: &str,
        request: HistoryPurgeRequest,
    ) -> Result<HistoryPurgeResult> {
        self.inner.purge_stream(app_id, channel, request).await
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        self.inner.runtime_status().await
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        self.inner.purge_before(before_ms, batch_size).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::error::Error;
    use crate::history::{MemoryHistoryStore, MemoryHistoryStoreConfig};
    use bytes::Bytes;
    use std::time::Duration;

    struct OneAppManager {
        app: App,
        fail_lookup: bool,
    }

    #[async_trait]
    impl AppManager for OneAppManager {
        async fn init(&self) -> Result<()> {
            Ok(())
        }
        async fn create_app(&self, _config: App) -> Result<()> {
            Ok(())
        }
        async fn update_app(&self, _config: App) -> Result<()> {
            Ok(())
        }
        async fn delete_app(&self, _app_id: &str) -> Result<()> {
            Ok(())
        }
        async fn get_apps(&self) -> Result<Vec<App>> {
            Ok(vec![self.app.clone()])
        }
        async fn find_by_key(&self, _key: &str) -> Result<Option<App>> {
            Ok(Some(self.app.clone()))
        }
        async fn find_by_id(&self, _app_id: &str) -> Result<Option<App>> {
            if self.fail_lookup {
                return Err(Error::Internal("app manager unavailable".to_string()));
            }
            Ok(Some(self.app.clone()))
        }
        async fn check_health(&self) -> Result<()> {
            Ok(())
        }
    }

    async fn store_with_entries(published_at_ms: &[i64]) -> RetentionWindowHistoryStore {
        store_with_window(published_at_ms, 120, false).await
    }

    async fn store_with_window(
        published_at_ms: &[i64],
        retention_window_seconds: u64,
        fail_lookup: bool,
    ) -> RetentionWindowHistoryStore {
        // The inner store keeps everything, like a backend between purges.
        let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig {
            retention_window: Duration::from_secs(365 * 24 * 3600),
            max_messages_per_channel: None,
            max_bytes_per_channel: None,
        }));
        for &published in published_at_ms {
            let reservation = inner.reserve_publish_position("app", "chan").await.unwrap();
            inner
                .append(HistoryAppendRecord {
                    app_id: "app".to_string(),
                    channel: "chan".to_string(),
                    stream_id: reservation.stream_id,
                    serial: reservation.serial,
                    published_at_ms: published,
                    message_id: None,
                    event_name: Some("e".to_string()),
                    operation_kind: "append".to_string(),
                    payload_bytes: Bytes::from_static(b"{}"),
                    retention: HistoryRetentionPolicy {
                        retention_window_seconds: 365 * 24 * 3600,
                        max_messages_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }
        let app = App {
            id: "app".to_string(),
            ..App::default()
        };
        let global = HistoryConfig {
            enabled: true,
            retention_window_seconds,
            ..HistoryConfig::default()
        };
        let app_manager = OneAppManager { app, fail_lookup };
        RetentionWindowHistoryStore::new(inner, Arc::new(app_manager), global)
    }

    fn request(direction: HistoryDirection) -> HistoryReadRequest {
        HistoryReadRequest {
            app_id: "app".to_string(),
            channel: "chan".to_string(),
            direction,
            limit: 100,
            cursor: None,
            bounds: HistoryQueryBounds::default(),
        }
    }

    #[tokio::test]
    async fn reads_skip_entries_older_than_retention_window() {
        let now = now_ms();
        let store = store_with_entries(&[now - 130_000, now - 125_000, now - 1_000]).await;

        let page = store
            .read_page(request(HistoryDirection::NewestFirst))
            .await
            .unwrap();
        assert_eq!(
            page.items.iter().map(|i| i.serial).collect::<Vec<_>>(),
            vec![3]
        );
        assert!(page.truncated_by_retention);
        assert!(!page.complete);
        assert!(!page.has_more);
        assert_eq!(page.retained.oldest_serial, None);

        let page = store
            .read_page(request(HistoryDirection::OldestFirst))
            .await
            .unwrap();
        assert_eq!(
            page.items.iter().map(|i| i.serial).collect::<Vec<_>>(),
            vec![3]
        );
        assert!(page.truncated_by_retention);
    }

    #[tokio::test]
    async fn reads_starting_after_expired_entries_are_not_truncated() {
        let now = now_ms();
        let store = store_with_entries(&[now - 130_000, now - 2_000, now - 1_000]).await;

        // A resume from serial 1 asks for 2.. which is still retained.
        let mut resume = request(HistoryDirection::OldestFirst);
        resume.bounds.start_serial = Some(2);
        let page = store.read_page(resume).await.unwrap();
        assert_eq!(
            page.items.iter().map(|i| i.serial).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(!page.truncated_by_retention);

        // A resume from serial 0 needs the expired serial 1 and must fail closed.
        let mut resume = request(HistoryDirection::OldestFirst);
        resume.bounds.start_serial = Some(1);
        let page = store.read_page(resume).await.unwrap();
        assert!(page.truncated_by_retention);
        assert_eq!(page.items.first().map(|i| i.serial), Some(2));
    }

    #[tokio::test]
    async fn reads_inside_retention_window_are_unchanged() {
        let now = now_ms();
        let store = store_with_entries(&[now - 2_000, now - 1_000]).await;

        let page = store
            .read_page(request(HistoryDirection::OldestFirst))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert!(!page.truncated_by_retention);
        assert!(page.complete);
    }

    #[tokio::test]
    async fn expired_entry_between_fresh_entries_is_not_served() {
        // Publish timestamps are not monotonic in serial, so the physical
        // floor being fresh proves nothing about the rest of the page.
        let now = now_ms();
        let store = store_with_entries(&[now - 1_000, now - 130_000, now - 500]).await;

        let page = store
            .read_page(request(HistoryDirection::OldestFirst))
            .await
            .unwrap();
        assert_eq!(
            page.items.iter().map(|i| i.serial).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(page.truncated_by_retention);
        assert!(!page.complete);
        // The stored floor (serial 1) is inside the window, so it is also the
        // first readable entry even though serial 2 expired.
        assert_eq!(page.retained.oldest_serial, Some(1));
    }

    #[tokio::test]
    async fn newest_first_continues_past_a_partly_expired_page() {
        // Serial 2 expired while 1 and 3 are fresh: publish timestamps are not
        // monotonic in serial, so pagination must keep reaching serial 1.
        let now = now_ms();
        let store = store_with_entries(&[now - 1_000, now - 130_000, now - 500]).await;

        let mut first = request(HistoryDirection::NewestFirst);
        first.limit = 2;
        let page = store.read_page(first).await.unwrap();
        assert_eq!(
            page.items.iter().map(|i| i.serial).collect::<Vec<_>>(),
            vec![3]
        );
        assert!(page.has_more);
        assert!(page.next_cursor.is_some());

        let mut second = request(HistoryDirection::NewestFirst);
        second.limit = 2;
        second.cursor = page.next_cursor;
        let page = store.read_page(second).await.unwrap();
        assert_eq!(
            page.items.iter().map(|i| i.serial).collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[tokio::test]
    async fn newest_first_stops_at_a_fully_expired_page() {
        let now = now_ms();
        let store = store_with_entries(&[now - 130_000, now - 125_000]).await;

        let page = store
            .read_page(request(HistoryDirection::NewestFirst))
            .await
            .unwrap();
        assert!(page.items.is_empty());
        assert!(page.truncated_by_retention);
        assert!(!page.has_more);
        assert!(page.next_cursor.is_none());
    }

    #[tokio::test]
    async fn continuity_hides_the_floor_while_rows_await_purge() {
        let now = now_ms();
        let store = store_with_entries(&[now - 130_000, now - 2_000, now - 1_000]).await;

        let page = store
            .read_page(request(HistoryDirection::NewestFirst))
            .await
            .unwrap();
        assert_eq!(page.retained.oldest_serial, None);
        assert_eq!(page.retained.oldest_published_at_ms, None);

        let inspection = store.stream_inspection("app", "chan").await.unwrap();
        assert_eq!(inspection.retained.oldest_serial, None);
        assert_eq!(inspection.retained.oldest_published_at_ms, None);
        assert_eq!(inspection.retained.newest_serial, Some(3));
    }

    #[tokio::test]
    async fn app_lookup_failure_fails_the_read() {
        let store = store_with_window(&[now_ms() - 1_000], 120, true).await;

        let error = store
            .read_page(request(HistoryDirection::NewestFirst))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Internal(_)));
    }

    #[tokio::test]
    async fn zero_window_expires_every_entry() {
        let now = now_ms();
        let store = store_with_window(&[now - 2_000, now - 1_000], 0, false).await;

        let page = store
            .read_page(request(HistoryDirection::NewestFirst))
            .await
            .unwrap();
        assert!(page.items.is_empty());
        assert!(page.truncated_by_retention);
        assert_eq!(page.retained.oldest_serial, None);
        assert_eq!(page.retained.oldest_published_at_ms, None);
    }
}
