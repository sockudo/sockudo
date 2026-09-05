//! Advisory calendar indexes. Canonical minute buckets retain their V1 keys and wire shape.
use super::*;

const INDEX_VERSION: u8 = 1;
const BACKFILL_PAGE: usize = 256;
const MAX_INDEX_MONTHS: usize = 4_096;

#[derive(Default, Deserialize, Serialize)]
struct CalendarRoot {
    version: u8,
    ready: bool,
    cursor: Option<String>,
    months: BTreeSet<String>,
}

impl CacheStatsStore {
    fn index_prefix(app: &str) -> String {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(app.as_bytes());
        format!("stats-index:v1:{encoded}:")
    }

    fn index_root_key(app: &str) -> String {
        format!("{}root", Self::index_prefix(app))
    }

    async fn update_index_value<T>(
        &self,
        key: &str,
        mut update: impl FnMut(&mut T) -> Result<(), StatsError> + Send,
    ) -> Result<(), StatsError>
    where
        T: Default + Serialize + for<'de> Deserialize<'de> + Send,
    {
        for _ in 0..self.cas_retries.max(1) {
            let previous = self.cache.get(key).await.map_err(store_error)?;
            let mut value: T = previous
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(store_error)?
                .unwrap_or_default();
            update(&mut value)?;
            let encoded = serde_json::to_string(&value).map_err(store_error)?;
            let applied = match previous {
                Some(previous) => {
                    self.cache
                        .compare_and_swap(key, &previous, &encoded, self.retention_seconds)
                        .await
                }
                None => {
                    self.cache
                        .set_if_not_exists(key, &encoded, self.retention_seconds)
                        .await
                }
            }
            .map_err(store_error)?;
            if applied {
                return Ok(());
            }
        }
        Err(StatsError::Store(
            "stats index compare-and-swap retries exhausted".to_owned(),
        ))
    }

    // Called after the durable canonical write and before acknowledging that observation. Refresh
    // the child first and each parent afterwards, so no acknowledged child outlives its index.
    async fn index_bucket(&self, bucket: &StatsBucket) -> Result<(), StatsError> {
        let minute = &bucket.interval_id;
        parse_interval_start(minute)?;
        if minute.len() != 16 {
            return Err(StatsError::Store(
                "invalid canonical stats minute".to_owned(),
            ));
        }
        let prefix = Self::index_prefix(&bucket.app_id);
        for (parent, child) in [(13, 16), (10, 13), (7, 10)] {
            let key = format!("{prefix}{}", &minute[..parent]);
            let entry = minute[..child].to_owned();
            self.update_index_value::<BTreeSet<String>>(&key, |entries| {
                entries.insert(entry.clone());
                Ok(())
            })
            .await?;
        }
        self.update_index_value::<CalendarRoot>(&Self::index_root_key(&bucket.app_id), |root| {
            root.version = INDEX_VERSION;
            root.months.insert(minute[..7].to_owned());
            if root.months.len() > MAX_INDEX_MONTHS {
                return Err(StatsError::Store(
                    "stats index exceeded the bounded month directory".to_owned(),
                ));
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn index_after_write(
        &self,
        buckets: &[StatsBucket],
    ) -> Result<(), StatsError> {
        for bucket in buckets {
            self.index_bucket(bucket).await?;
        }
        Ok(())
    }

    async fn invalidate_index(&self, app: &str) -> Result<(), StatsError> {
        let key = Self::index_root_key(app);
        for _ in 0..self.cas_retries.max(1) {
            let Some(previous) = self.cache.get(&key).await.map_err(store_error)? else {
                return Ok(());
            };
            if self
                .cache
                .compare_and_remove(&key, &previous)
                .await
                .map_err(store_error)?
            {
                return Ok(());
            }
        }
        Err(StatsError::Store(
            "stats index invalidation retries exhausted".to_owned(),
        ))
    }

    async fn read_index_root(&self, app: &str) -> Result<Option<CalendarRoot>, StatsError> {
        let encoded = self
            .cache
            .get(&Self::index_root_key(app))
            .await
            .map_err(store_error)?;
        let root: Option<CalendarRoot> = encoded
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(store_error)?;
        if root.as_ref().is_some_and(|root| {
            root.version != INDEX_VERSION || root.months.len() > MAX_INDEX_MONTHS
        }) {
            return Err(StatsError::Store("invalid stats index metadata".to_owned()));
        }
        Ok(root)
    }

    async fn backfill_index_page(&self, app: &str) -> Result<bool, StatsError> {
        let root = self.read_index_root(app).await?;
        if root.as_ref().is_some_and(|root| root.ready) {
            return Ok(true);
        }
        let cursor = root.and_then(|root| root.cursor);
        let page = self
            .cache
            .scan_prefix_page(&Self::app_prefix(app), cursor.clone(), BACKFILL_PAGE)
            .await
            .map_err(store_error)?;
        for (_, encoded) in page.entries {
            let bucket: StatsBucket = serde_json::from_str(&encoded).map_err(store_error)?;
            if bucket.app_id != app {
                return Err(StatsError::Store("stats index app mismatch".to_owned()));
            }
            self.index_bucket(&bucket).await?;
        }
        let next = page.next_cursor;
        self.update_index_value::<CalendarRoot>(&Self::index_root_key(app), |root| {
            root.version = INDEX_VERSION;
            // A concurrent reader can repeat this page, but cannot rewind a newer checkpoint.
            if !root.ready && root.cursor == cursor {
                root.cursor = next.clone();
                root.ready = next.is_none();
            }
            Ok(())
        })
        .await?;
        Ok(self
            .read_index_root(app)
            .await?
            .is_some_and(|root| root.ready))
    }

    async fn index_children(
        &self,
        app: &str,
        prefix: &str,
    ) -> Result<BTreeSet<String>, StatsError> {
        let encoded = self
            .cache
            .get(&format!("{}{prefix}", Self::index_prefix(app)))
            .await
            .map_err(store_error)?;
        let encoded = encoded.ok_or(StatsError::IndexIncomplete)?;
        if encoded.len() > 4_096 {
            return Err(StatsError::Store(
                "oversized stats calendar node".to_owned(),
            ));
        }
        let entries: BTreeSet<String> = serde_json::from_str(&encoded).map_err(store_error)?;
        if entries.len() > 60 || entries.iter().any(|entry| !entry.starts_with(prefix)) {
            return Err(StatsError::Store("invalid stats calendar node".to_owned()));
        }
        Ok(entries)
    }

    async fn indexed_buckets(
        &self,
        app: &str,
        root: &CalendarRoot,
        bounds: (&str, &str),
        direction: StatsDirection,
        unit: StatsUnit,
        limit: usize,
    ) -> Result<Vec<StatsBucket>, StatsError> {
        let (lower, upper) = bounds;
        let mut buckets = Vec::new();
        let mut last_unit = None;
        let mut units = 0;
        let mut visited = 0usize;
        // Only metadata is read for excluded months/days/hours. Canonical reads stop after the
        // first minute of the extra rollup, once all minutes of each returned rollup are present.
        for month in ordered(&root.months, direction) {
            if !intersects(month, lower, upper) {
                continue;
            }
            for day in ordered(&self.index_children(app, month).await?, direction) {
                if !intersects(day, lower, upper) {
                    continue;
                }
                for hour in ordered(&self.index_children(app, day).await?, direction) {
                    if !intersects(hour, lower, upper) {
                        continue;
                    }
                    for minute in ordered(&self.index_children(app, hour).await?, direction) {
                        if minute.as_str() < lower || minute.as_str() >= upper {
                            continue;
                        }
                        visited += 1;
                        if visited > self.max_scan_entries {
                            return Err(StatsError::Store(
                                "stats query exceeded the bounded interval scan".to_owned(),
                            ));
                        }
                        let key = format!("{}{minute}", Self::app_prefix(app));
                        let Some(encoded) = self.cache.get(&key).await.map_err(store_error)? else {
                            continue;
                        };
                        let bucket: StatsBucket =
                            serde_json::from_str(&encoded).map_err(store_error)?;
                        if bucket.app_id != app || bucket.interval_id != *minute {
                            return Err(StatsError::Store(
                                "stats index bucket mismatch".to_owned(),
                            ));
                        }
                        let current_unit = unit.rollup_id(minute)?;
                        if last_unit.as_ref() != Some(&current_unit) {
                            units += 1;
                            last_unit = Some(current_unit);
                        }
                        buckets.push(bucket);
                        if units > limit {
                            return Ok(buckets);
                        }
                    }
                }
            }
        }
        Ok(buckets)
    }

    pub(super) async fn read_indexed_range(
        &self,
        app: &str,
        query: &StatsQuery,
    ) -> Result<Option<StatsRead>, StatsError> {
        match self.try_read_indexed_range(app, query).await {
            Err(StatsError::IndexIncomplete) => {
                // Eviction/expiry is not evidence that canonical minutes disappeared. Drop
                // readiness, serve canonical data, and restart bounded backfill next time.
                self.invalidate_index(app).await?;
                Ok(None)
            }
            result => result,
        }
    }

    async fn try_read_indexed_range(
        &self,
        app: &str,
        query: &StatsQuery,
    ) -> Result<Option<StatsRead>, StatsError> {
        if !self.backfill_index_page(app).await? {
            return Ok(None);
        }
        let Some(root) = self.read_index_root(app).await? else {
            return Ok(None);
        };
        let cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;
        if let Some(cursor) = &cursor {
            validate_cursor(cursor, app, query)?;
        }
        let horizon = if let Some(cursor) = &cursor {
            Some(cursor.horizon.clone())
        } else {
            self.indexed_buckets(
                app,
                &root,
                ("", "~"),
                StatsDirection::Backwards,
                StatsUnit::Minute,
                0,
            )
            .await?
            .into_iter()
            .next()
            .map(|bucket| bucket.interval_id)
        };
        let mut lower = query.start.clone().unwrap_or_default();
        let mut upper = query
            .end
            .as_ref()
            .map(|end| format!("{end}~"))
            .unwrap_or_else(|| "~".to_owned());
        if let Some(cursor) = &cursor {
            upper = upper.min(format!("{}~", cursor.horizon));
            match query.direction {
                StatsDirection::Forwards => lower = lower.max(format!("{}~", cursor.after)),
                StatsDirection::Backwards => upper = upper.min(cursor.after.clone()),
            }
        }
        let buckets = if lower >= upper {
            Vec::new()
        } else {
            self.indexed_buckets(
                app,
                &root,
                (&lower, &upper),
                query.direction,
                query.unit,
                query.limit,
            )
            .await?
        };
        Ok(Some(StatsRead { buckets, horizon }))
    }
}

fn ordered(
    values: &BTreeSet<String>,
    direction: StatsDirection,
) -> Box<dyn Iterator<Item = &String> + Send + '_> {
    match direction {
        StatsDirection::Backwards => Box::new(values.iter().rev()),
        StatsDirection::Forwards => Box::new(values.iter()),
    }
}
fn intersects(prefix: &str, lower: &str, upper: &str) -> bool {
    prefix < upper && format!("{prefix}~").as_str() >= lower
}
fn store_error(error: impl std::fmt::Display) -> StatsError {
    StatsError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_cache::memory_cache_manager::MemoryCacheManager;
    use sockudo_core::options::MemoryCacheOptions;

    fn config(max_scan: usize) -> StatsRuntimeConfig {
        StatsRuntimeConfig {
            queue_capacity: 16,
            flush_interval: Duration::ZERO,
            retention_seconds: 60,
            max_scan_entries: max_scan,
            cas_retries: 32,
        }
    }

    async fn exercise_calendar_index(cache: Arc<dyn CacheManager>) {
        let app = "calendar";
        let legacy = CacheStatsStore::new(cache.clone(), config(1_000));
        let start = parse_interval_start("2026-02-03:00:00")
            .unwrap()
            .timestamp_millis();
        let buckets: Vec<_> = (0..600)
            .map(|minute| {
                let mut bucket = StatsBucket::new(app, start + minute * 60_000).unwrap();
                bucket.add("count", 1, FieldAggregation::Sum);
                bucket
            })
            .collect();
        // Seed the exact old canonical shape without inventing ready index metadata.
        for bucket in &buckets {
            cache
                .set(
                    &CacheStatsStore::bucket_key(bucket),
                    &serde_json::to_string(bucket).unwrap(),
                    60,
                )
                .await
                .unwrap();
        }
        let query =
            StatsQuery::parse("minute", Some("backwards"), None, None, Some(2), None).unwrap();
        // Each fresh store resumes persisted backfill, while incomplete reads keep the old
        // bounded scan behavior (here the deliberately tiny five-interval limit rejects it).
        let mut failures = 0;
        let mut checkpoints = BTreeSet::new();
        loop {
            let restarted = CacheStatsStore::new(cache.clone(), config(5));
            if let Ok(read) = restarted.read_range(app, &query).await {
                assert_eq!(read.buckets.len(), 3);
                assert_eq!(read.buckets[0].interval_id, "2026-02-03:09:59");
                break;
            }
            failures += 1;
            // Redis SCAN's cursor covers the whole database, including unrelated prefixes.
            // Require progress at every bounded step instead of assuming 600 matching rows
            // imply fewer than20 physical pages in a shared integration fixture.
            let root = restarted.read_index_root(app).await.unwrap().unwrap();
            assert!(!root.ready, "a ready index must satisfy this range");
            let checkpoint = root
                .cursor
                .expect("incomplete migration has a continuation");
            assert!(
                checkpoints.insert(checkpoint),
                "backfill checkpoint did not advance"
            );
            assert!(
                failures < 1_024,
                "backfill did not finish its bounded fixture walk"
            );
        }
        assert!(
            failures >= 2,
            "backfill must respect its 256-entry page bound"
        );
        let indexed = CacheStatsStore::new(cache.clone(), config(1_000));
        for unit in [
            StatsUnit::Minute,
            StatsUnit::Hour,
            StatsUnit::Day,
            StatsUnit::Month,
        ] {
            for direction in [StatsDirection::Backwards, StatsDirection::Forwards] {
                let query = StatsQuery {
                    unit,
                    direction,
                    start: None,
                    end: None,
                    limit: 2,
                    cursor: None,
                };
                let read = indexed.read_range(app, &query).await.unwrap();
                let expected = MemoryStatsStore::new(60, 1_000);
                expected.put(&buckets).await.unwrap();
                let expected = expected.read_range(app, &query).await.unwrap();
                assert_eq!(read.buckets, expected.buckets);
                assert_eq!(read.horizon, expected.horizon);
            }
        }
        let aggregator = StatsAggregator::new(Some(cache.clone()), config(1_000));
        let first = aggregator.query(app, &query, start).await.unwrap();
        let cursor = first.next_cursor.clone().unwrap();
        let late = StatsBucket::new(app, start + 900 * 60_000).unwrap();
        indexed.put(&[late]).await.unwrap();
        let second = aggregator
            .query(
                app,
                &StatsQuery {
                    cursor: Some(cursor),
                    ..query.clone()
                },
                start,
            )
            .await
            .unwrap();
        assert_eq!(second.items[0].interval_id, "2026-02-03:09:57");
        assert_eq!(second.items[1].interval_id, "2026-02-03:09:56");
        // An evicted calendar node must not make previously durable minutes invisible.
        cache
            .remove(&format!(
                "{}2026-02-03:09",
                CacheStatsStore::index_prefix(app)
            ))
            .await
            .unwrap();
        let narrow = StatsQuery::parse(
            "minute",
            None,
            Some("2026-02-03:09:58"),
            Some("2026-02-03:09:59"),
            Some(2),
            None,
        )
        .unwrap();
        let page = aggregator.query(app, &narrow, start).await.unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].interval_id, "2026-02-03:09:59");
        assert!(indexed.read_index_root(app).await.unwrap().is_none());
        // The legacy list contract still reads every canonical value after current writes.
        legacy
            .put(&[StatsBucket::new(app, start + 901 * 60_000).unwrap()])
            .await
            .unwrap();
        assert_eq!(legacy.list(app).await.unwrap().len(), 602);
        assert!(!indexed.read_index_root(app).await.unwrap().unwrap().ready);
    }

    #[tokio::test]
    async fn calendar_index_resumes_legacy_backfill_bounds_reads_and_recovers_eviction() {
        let cache = Arc::new(MemoryCacheManager::new(
            "calendar-index-test".to_owned(),
            MemoryCacheOptions::default(),
        ));
        exercise_calendar_index(cache).await;
    }

    #[tokio::test]
    #[ignore = "requires local Redis fixture"]
    async fn redis_calendar_index_resumes_backfill_and_preserves_legacy_reads() {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_owned());
        let prefix = format!("ably-calendar-{}", uuid::Uuid::new_v4());
        let cache = sockudo_cache::RedisCacheManager::with_url(&url, Some(&prefix))
            .await
            .unwrap();
        exercise_calendar_index(Arc::new(cache)).await;
    }
}
