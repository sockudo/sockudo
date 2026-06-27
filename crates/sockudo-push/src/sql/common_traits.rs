use super::helpers::*;
#[cfg(feature = "mysql")]
use super::stores::MySqlPushStore;
#[cfg(feature = "postgres")]
use super::stores::PostgresPushStore;
use crate::domain::{
    DeleteDeviceOutcome, DeliveryEvent, NotificationTemplate, ProviderCredential, PublishLogEvent,
    PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    IdempotencyRecord, OperatorInvalidationEvent, Page, PushCredentialStore,
    PushDeliveryEventStore, PushFanoutShardStore, PushIdempotencyStore, PushOperatorEventStore,
    PushPublishLogStore, PushPublishStatusStore, PushScheduleStore, PushSchedulerLockStore,
    PushStorageResult, PushTemplateStore, ScheduledPushJob, SchedulerLock,
};
use async_trait::async_trait;
use sqlx::Row;

macro_rules! impl_common_sql_traits {
    ($store:ty, $pool:ident, $postgres:expr, $bind:literal, $json_cast:expr, $json_text:expr) => {
        #[async_trait]
        impl PushCredentialStore for $store {
            async fn put_credential(&self, credential: ProviderCredential) -> PushStorageResult<()> {
                credential.validate()?;
                let bytes = to_json_vec(&credential)?;
                let q = sql_query(format!(
                    "INSERT INTO push_provider_credentials (app_id, credential_id, provider, version, active, encrypted_material, dek_ciphertext, kek_ref, metadata, created_at_ms, updated_at_ms) VALUES ({0}, {0}, {0}, {0}, TRUE, {0}, {0}, {0}, {1}, {0}, {0}) {2}",
                    $bind,
                    $json_cast,
                    upsert_credential_clause($postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&credential.app_id)
                    .bind(&credential.credential_id)
                    .bind(provider_label(credential.provider))
                    .bind(credential.version as i64)
                    .bind(bytes)
                    .bind(b"push-v1".as_slice())
                    .bind("serialized-provider-credential")
                    .bind("{}")
                    .bind(now_ms_i64())
                    .bind(now_ms_i64())
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }

            async fn get_credential(&self, app_id: &str, credential_id: &str) -> PushStorageResult<Option<ProviderCredential>> {
                let q = sql_query(format!(
                    "SELECT encrypted_material FROM push_provider_credentials WHERE app_id = {0} AND credential_id = {0}",
                    $bind
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(credential_id)
                    .fetch_optional(&self.$pool)
                    .await
                    .map_err(sql_error)?
                    .map(|row| from_json_slice(row.try_get::<Vec<u8>, _>("encrypted_material").map_err(sql_error)?.as_slice()))
                    .transpose()
            }

            async fn list_credentials(&self, app_id: &str, limit: usize, cursor: Option<PushCursor>) -> PushStorageResult<Page<ProviderCredential>> {
                let start = cursor_position(cursor, app_id)?;
                let q = sql_query(format!(
                    "SELECT credential_id, encrypted_material FROM push_provider_credentials WHERE app_id = {0} AND credential_id > {0} ORDER BY credential_id LIMIT {0}",
                    $bind
                ), $postgres);
                let rows = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(start.unwrap_or_default())
                    .bind(limit_plus_one(limit))
                    .fetch_all(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                page_from_sql_rows(app_id, PushCursorKind::Credential, rows, limit, |row| {
                    Ok((
                        row.try_get::<String, _>("credential_id").map_err(sql_error)?,
                        from_json_slice(row.try_get::<Vec<u8>, _>("encrypted_material").map_err(sql_error)?.as_slice())?,
                    ))
                })
            }
        }

        #[async_trait]
        impl PushTemplateStore for $store {
            async fn put_template(&self, template: NotificationTemplate) -> PushStorageResult<()> {
                template.validate()?;
                let q = sql_query(format!(
                    "INSERT INTO push_notification_templates (app_id, template_id, default_locale, locales_json, provider_overrides_json, created_at_ms, updated_at_ms) VALUES ({0}, {0}, {0}, {1}, {1}, {0}, {0}) {2}",
                    $bind,
                    $json_cast,
                    upsert_template_clause($postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&template.app_id)
                    .bind(&template.template_id)
                    .bind(&template.default_locale)
                    .bind(to_json_string(&template.locales)?)
                    .bind(to_json_string(&template.provider_overrides)?)
                    .bind(now_ms_i64())
                    .bind(now_ms_i64())
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }

            async fn get_template(&self, app_id: &str, template_id: &str) -> PushStorageResult<Option<NotificationTemplate>> {
                let q = sql_query(format!(
                    "SELECT app_id, template_id, default_locale, {1} AS locales_json, {2} AS provider_overrides_json FROM push_notification_templates WHERE app_id = {0} AND template_id = {0}",
                    $bind, json_text_expr("locales_json", $json_text), json_text_expr("provider_overrides_json", $json_text)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(template_id)
                    .fetch_optional(&self.$pool)
                    .await
                    .map_err(sql_error)?
                    .map(|row| template_from_row(&row))
                    .transpose()
            }

            async fn list_templates(&self, app_id: &str, limit: usize, cursor: Option<PushCursor>) -> PushStorageResult<Page<NotificationTemplate>> {
                let start = cursor_position(cursor, app_id)?;
                let q = sql_query(format!(
                    "SELECT app_id, template_id, default_locale, {1} AS locales_json, {2} AS provider_overrides_json FROM push_notification_templates WHERE app_id = {0} AND template_id > {0} ORDER BY template_id LIMIT {0}",
                    $bind, json_text_expr("locales_json", $json_text), json_text_expr("provider_overrides_json", $json_text)
                ), $postgres);
                let rows = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(start.unwrap_or_default())
                    .bind(limit_plus_one(limit))
                    .fetch_all(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                page_from_sql_rows(app_id, PushCursorKind::Template, rows, limit, |row| {
                    Ok((row.try_get::<String, _>("template_id").map_err(sql_error)?, template_from_row(row)?))
                })
            }

            async fn delete_template(&self, app_id: &str, template_id: &str) -> PushStorageResult<DeleteDeviceOutcome> {
                let q = sql_query(format!("DELETE FROM push_notification_templates WHERE app_id = {0} AND template_id = {0}", $bind), $postgres);
                delete_result(sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(app_id).bind(template_id).execute(&self.$pool).await.map_err(sql_error)?.rows_affected())
            }
        }

        #[async_trait]
        impl PushPublishStatusStore for $store {
            async fn put_publish_status(&self, status: PublishStatus) -> PushStorageResult<()> {
                let q = sql_query(format!(
                    "INSERT INTO push_publish_status (app_id, publish_id, state, counters_json, error_reason, created_at_ms, updated_at_ms) VALUES ({0}, {0}, {0}, {1}, {0}, {0}, {0}) {2}",
                    $bind, $json_cast, upsert_status_clause($postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&status.app_id)
                    .bind(&status.publish_id)
                    .bind(format!("{:?}", status.state).to_ascii_lowercase())
                    .bind(to_json_string(&status)?)
                    .bind(&status.error_reason)
                    .bind(now_ms_i64())
                    .bind(now_ms_i64())
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }

            async fn get_publish_status(&self, app_id: &str, publish_id: &str) -> PushStorageResult<Option<PublishStatus>> {
                let q = sql_query(format!("SELECT {1} AS counters_json FROM push_publish_status WHERE app_id = {0} AND publish_id = {0}", $bind, json_text_expr("counters_json", $json_text)), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(publish_id)
                    .fetch_optional(&self.$pool)
                    .await
                    .map_err(sql_error)?
                    .map(|row| from_json_str(&row.try_get::<String, _>("counters_json").map_err(sql_error)?))
                    .transpose()
            }
        }

        #[async_trait]
        impl PushPublishLogStore for $store {
            async fn append_publish_log_event(&self, event: PublishLogEvent) -> PushStorageResult<()> {
                let q = sql_query(format!(
                    "INSERT INTO push_publish_log (app_id, publish_id, occurred_at_ms, event_id, event_json) VALUES ({0}, {0}, {0}, {0}, {1}) {2}",
                    $bind, $json_cast, upsert_publish_log_clause($postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&event.app_id)
                    .bind(&event.publish_id)
                    .bind(event.occurred_at_ms as i64)
                    .bind(&event.event_id)
                    .bind(to_json_string(&event)?)
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }

            async fn list_publish_log_events(&self, app_id: &str, limit: usize, cursor: Option<PushCursor>) -> PushStorageResult<Page<PublishLogEvent>> {
                let start = cursor_position(cursor, app_id)?.unwrap_or_default();
                let (cursor_ms, cursor_event_id) = parse_ts_id_cursor(&start);
                let q = sql_query(format!(
                    "SELECT occurred_at_ms, event_id, {1} AS event_json FROM push_publish_log WHERE app_id = {0} AND (occurred_at_ms, event_id) > ({0}, {0}) ORDER BY occurred_at_ms, event_id LIMIT {0}",
                    $bind, json_text_expr("event_json", $json_text)
                ), $postgres);
                let rows = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(cursor_ms)
                    .bind(cursor_event_id)
                    .bind(limit_plus_one(limit))
                    .fetch_all(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                page_from_sql_rows(app_id, PushCursorKind::PublishLog, rows, limit, |row| {
                    let occurred: i64 = row.try_get("occurred_at_ms").map_err(sql_error)?;
                    let event_id: String = row.try_get("event_id").map_err(sql_error)?;
                    Ok((format!("{occurred:020}:{event_id}"), from_json_str(&row.try_get::<String, _>("event_json").map_err(sql_error)?)?))
                })
            }
        }

        #[async_trait]
        impl PushFanoutShardStore for $store {
            async fn put_fanout_shard(&self, shard: ShardJob) -> PushStorageResult<()> {
                let q = sql_query(format!(
                    "INSERT INTO push_fanout_shards (app_id, publish_id, shard_id, shard_json, updated_at_ms) VALUES ({0}, {0}, {0}, {1}, {0}) {2}",
                    $bind, $json_cast, upsert_shard_clause($postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&shard.app_id)
                    .bind(&shard.publish_id)
                    .bind(&shard.shard_id)
                    .bind(to_json_string(&shard)?)
                    .bind(now_ms_i64())
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }

            async fn get_fanout_shard(&self, app_id: &str, publish_id: &str, shard_id: &str) -> PushStorageResult<Option<ShardJob>> {
                let q = sql_query(format!("SELECT {1} AS shard_json FROM push_fanout_shards WHERE app_id = {0} AND publish_id = {0} AND shard_id = {0}", $bind, json_text_expr("shard_json", $json_text)), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(publish_id)
                    .bind(shard_id)
                    .fetch_optional(&self.$pool)
                    .await
                    .map_err(sql_error)?
                    .map(|row| from_json_str(&row.try_get::<String, _>("shard_json").map_err(sql_error)?))
                    .transpose()
            }
        }

        #[async_trait]
        impl PushScheduleStore for $store {
            async fn put_scheduled_job(&self, job: ScheduledPushJob) -> PushStorageResult<()> {
                let q = sql_query(format!(
                    "INSERT INTO push_scheduled_jobs (app_id, publish_id, due_minute_ms, due_at_ms, payload_json, state, created_at_ms, updated_at_ms) VALUES ({0}, {0}, {0}, {0}, {1}, 'pending', {0}, {0}) {2}",
                    $bind, $json_cast, upsert_schedule_clause($postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&job.app_id)
                    .bind(&job.publish_id)
                    .bind(job.due_minute_ms as i64)
                    .bind(job.due_at_ms as i64)
                    .bind(to_json_string(&job.payload_json)?)
                    .bind(now_ms_i64())
                    .bind(now_ms_i64())
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }

            async fn get_scheduled_job(&self, app_id: &str, publish_id: &str) -> PushStorageResult<Option<ScheduledPushJob>> {
                let q = sql_query(format!("SELECT app_id, publish_id, due_minute_ms, due_at_ms, {1} AS payload_json FROM push_scheduled_jobs WHERE app_id = {0} AND publish_id = {0}", $bind, json_text_expr("payload_json", $json_text)), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(publish_id)
                    .fetch_optional(&self.$pool)
                    .await
                    .map_err(sql_error)?
                    .map(|row| scheduled_from_row(&row))
                    .transpose()
            }

            async fn delete_scheduled_job(&self, app_id: &str, publish_id: &str) -> PushStorageResult<DeleteDeviceOutcome> {
                let q = sql_query(format!("DELETE FROM push_scheduled_jobs WHERE app_id = {0} AND publish_id = {0}", $bind), $postgres);
                delete_result(sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(app_id).bind(publish_id).execute(&self.$pool).await.map_err(sql_error)?.rows_affected())
            }

            async fn list_scheduled_apps(&self) -> PushStorageResult<Vec<String>> {
                let rows = sqlx::query("SELECT DISTINCT app_id FROM push_scheduled_jobs WHERE state = 'pending' ORDER BY app_id")
                    .fetch_all(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                rows.into_iter()
                    .map(|row| row.try_get("app_id").map_err(sql_error))
                    .collect()
            }

            async fn list_due_scheduled_jobs(&self, app_id: &str, due_minute_ms: u64, limit: usize, cursor: Option<PushCursor>) -> PushStorageResult<Page<ScheduledPushJob>> {
                let start = cursor_position(cursor, app_id)?.unwrap_or_default();
                let (cursor_due_ms, cursor_publish_id) = parse_ts_id_cursor(&start);
                let q = sql_query(format!(
                    "SELECT app_id, publish_id, due_minute_ms, due_at_ms, {1} AS payload_json FROM push_scheduled_jobs WHERE app_id = {0} AND due_minute_ms <= {0} AND (due_at_ms, publish_id) > ({0}, {0}) ORDER BY due_at_ms, publish_id LIMIT {0}",
                    $bind, json_text_expr("payload_json", $json_text)
                ), $postgres);
                let rows = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(due_minute_ms as i64)
                    .bind(cursor_due_ms)
                    .bind(cursor_publish_id)
                    .bind(limit_plus_one(limit))
                    .fetch_all(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                page_from_sql_rows(app_id, PushCursorKind::ScheduledJob, rows, limit, |row| {
                    let due: i64 = row.try_get("due_at_ms").map_err(sql_error)?;
                    let publish_id: String = row.try_get("publish_id").map_err(sql_error)?;
                    Ok((format!("{due:020}:{publish_id}"), scheduled_from_row(row)?))
                })
            }
        }

        #[async_trait]
        impl PushDeliveryEventStore for $store {
            async fn append_delivery_event(&self, event: DeliveryEvent) -> PushStorageResult<()> {
                let q = sql_query(format!(
                    "INSERT INTO push_delivery_events (app_id, publish_id, occurred_at_ms, event_id, provider, outcome, result_json) VALUES ({0}, {0}, {0}, {0}, {0}, {0}, {1}) {2}",
                    $bind, $json_cast, upsert_delivery_event_clause($postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&event.app_id)
                    .bind(&event.publish_id)
                    .bind(event.occurred_at_ms as i64)
                    .bind(&event.event_id)
                    .bind(provider_label(event.result.provider))
                    .bind(format!("{:?}", event.result.outcome).to_ascii_lowercase())
                    .bind(to_json_string(&event)?)
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }

            async fn list_delivery_events(&self, app_id: &str, publish_id: &str, limit: usize, cursor: Option<PushCursor>) -> PushStorageResult<Page<DeliveryEvent>> {
                let start = cursor_position(cursor, app_id)?.unwrap_or_default();
                let (cursor_ms, cursor_event_id) = parse_ts_id_cursor(&start);
                let q = sql_query(format!(
                    "SELECT occurred_at_ms, event_id, {1} AS result_json FROM push_delivery_events WHERE app_id = {0} AND publish_id = {0} AND (occurred_at_ms, event_id) > ({0}, {0}) ORDER BY occurred_at_ms, event_id LIMIT {0}",
                    $bind, json_text_expr("result_json", $json_text)
                ), $postgres);
                let rows = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(publish_id)
                    .bind(cursor_ms)
                    .bind(cursor_event_id)
                    .bind(limit_plus_one(limit))
                    .fetch_all(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                page_from_sql_rows(app_id, PushCursorKind::DeliveryEvent, rows, limit, |row| {
                    let occurred: i64 = row.try_get("occurred_at_ms").map_err(sql_error)?;
                    let event_id: String = row.try_get("event_id").map_err(sql_error)?;
                    Ok((format!("{occurred:020}:{event_id}"), from_json_str(&row.try_get::<String, _>("result_json").map_err(sql_error)?)?))
                })
            }

            async fn purge_delivery_events_before(&self, app_id: &str, before_ms: u64) -> PushStorageResult<u64> {
                let q = sql_query(format!("DELETE FROM push_delivery_events WHERE app_id = {0} AND occurred_at_ms < {0}", $bind), $postgres);
                Ok(sqlx::query(sqlx::AssertSqlSafe(q.as_str())).bind(app_id).bind(before_ms as i64).execute(&self.$pool).await.map_err(sql_error)?.rows_affected())
            }
        }

        #[async_trait]
        impl PushIdempotencyStore for $store {
            async fn put_idempotency_record_if_absent(&self, record: IdempotencyRecord) -> PushStorageResult<bool> {
                let q = sql_query(format!(
                    "INSERT INTO push_idempotency (app_id, idempotency_key, publish_id, expires_at_ms, created_at_ms) VALUES ({0}, {0}, {0}, {0}, {0}) {1}",
                    $bind, ignore_conflict_clause($postgres)
                ), $postgres);
                let result = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&record.app_id)
                    .bind(&record.key)
                    .bind(&record.publish_id)
                    .bind(record.expires_at_ms as i64)
                    .bind(now_ms_i64())
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(result.rows_affected() > 0)
            }

            async fn get_idempotency_record(&self, app_id: &str, key: &str) -> PushStorageResult<Option<IdempotencyRecord>> {
                let q = sql_query(format!("SELECT app_id, idempotency_key, publish_id, expires_at_ms FROM push_idempotency WHERE app_id = {0} AND idempotency_key = {0}", $bind), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(key)
                    .fetch_optional(&self.$pool)
                    .await
                    .map_err(sql_error)?
                    .map(|row| Ok(IdempotencyRecord {
                        app_id: row.try_get("app_id").map_err(sql_error)?,
                        key: row.try_get("idempotency_key").map_err(sql_error)?,
                        publish_id: row.try_get("publish_id").map_err(sql_error)?,
                        expires_at_ms: row.try_get::<i64, _>("expires_at_ms").map_err(sql_error)? as u64,
                    }))
                    .transpose()
            }
        }

        #[async_trait]
        impl PushSchedulerLockStore for $store {
            async fn acquire_scheduler_lock(&self, lock: SchedulerLock, now_ms: u64) -> PushStorageResult<bool> {
                if $postgres {
                    let q = sql_query(
                        "INSERT INTO push_scheduler_locks (app_id, publish_id, owner_id, expires_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?) ON CONFLICT (app_id, publish_id) DO UPDATE SET owner_id = EXCLUDED.owner_id, expires_at_ms = EXCLUDED.expires_at_ms, updated_at_ms = EXCLUDED.updated_at_ms WHERE push_scheduler_locks.expires_at_ms <= ? OR push_scheduler_locks.owner_id = ? RETURNING owner_id".to_owned(),
                        true,
                    );
                    let row = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                        .bind(&lock.app_id)
                        .bind(&lock.publish_id)
                        .bind(&lock.owner_id)
                        .bind(lock.expires_at_ms as i64)
                        .bind(now_ms as i64)
                        .bind(now_ms as i64)
                        .bind(&lock.owner_id)
                        .fetch_optional(&self.$pool)
                        .await
                        .map_err(sql_error)?;
                    return Ok(row.is_some());
                }

                let update = sqlx::query(
                    "UPDATE push_scheduler_locks SET owner_id = ?, expires_at_ms = ?, updated_at_ms = ? WHERE app_id = ? AND publish_id = ? AND (expires_at_ms <= ? OR owner_id = ?)",
                )
                    .bind(&lock.owner_id)
                    .bind(lock.expires_at_ms as i64)
                    .bind(now_ms as i64)
                    .bind(&lock.app_id)
                    .bind(&lock.publish_id)
                    .bind(now_ms as i64)
                    .bind(&lock.owner_id)
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                if update.rows_affected() > 0 {
                    return Ok(true);
                }

                let insert = sqlx::query(
                    "INSERT IGNORE INTO push_scheduler_locks (app_id, publish_id, owner_id, expires_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?)",
                )
                    .bind(&lock.app_id)
                    .bind(&lock.publish_id)
                    .bind(&lock.owner_id)
                    .bind(lock.expires_at_ms as i64)
                    .bind(now_ms as i64)
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(insert.rows_affected() > 0)
            }

            async fn release_scheduler_lock(&self, app_id: &str, publish_id: &str, owner_id: &str) -> PushStorageResult<()> {
                let q = sql_query(format!("DELETE FROM push_scheduler_locks WHERE app_id = {0} AND publish_id = {0} AND owner_id = {0}", $bind), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(publish_id)
                    .bind(owner_id)
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }
        }

        #[async_trait]
        impl PushOperatorEventStore for $store {
            async fn append_operator_invalidation(&self, event: OperatorInvalidationEvent) -> PushStorageResult<()> {
                let q = sql_query(format!(
                    "INSERT INTO push_operator_invalidations (app_id, occurred_at_ms, event_id, subject) VALUES ({0}, {0}, {0}, {0}) {1}",
                    $bind, ignore_conflict_clause($postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&event.app_id)
                    .bind(event.occurred_at_ms as i64)
                    .bind(&event.event_id)
                    .bind(&event.subject)
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                Ok(())
            }

            async fn list_operator_invalidations(&self, app_id: &str, limit: usize, cursor: Option<PushCursor>) -> PushStorageResult<Page<OperatorInvalidationEvent>> {
                let start = cursor_position(cursor, app_id)?.unwrap_or_default();
                let (cursor_ms, cursor_event_id) = parse_ts_id_cursor(&start);
                let q = sql_query(format!(
                    "SELECT app_id, occurred_at_ms, event_id, subject FROM push_operator_invalidations WHERE app_id = {0} AND (occurred_at_ms, event_id) > ({0}, {0}) ORDER BY occurred_at_ms, event_id LIMIT {0}",
                    $bind
                ), $postgres);
                let rows = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(cursor_ms)
                    .bind(cursor_event_id)
                    .bind(limit_plus_one(limit))
                    .fetch_all(&self.$pool)
                    .await
                    .map_err(sql_error)?;
                page_from_sql_rows(app_id, PushCursorKind::OperatorInvalidation, rows, limit, |row| {
                    let occurred: i64 = row.try_get("occurred_at_ms").map_err(sql_error)?;
                    let event_id: String = row.try_get("event_id").map_err(sql_error)?;
                    Ok((format!("{occurred:020}:{event_id}"), OperatorInvalidationEvent {
                        app_id: row.try_get("app_id").map_err(sql_error)?,
                        event_id,
                        subject: row.try_get("subject").map_err(sql_error)?,
                        occurred_at_ms: occurred as u64,
                    }))
                })
            }
        }
    };
}

#[cfg(feature = "postgres")]
impl_common_sql_traits!(PostgresPushStore, pool, true, "?", "?::jsonb", "?::text");
#[cfg(feature = "mysql")]
impl_common_sql_traits!(
    MySqlPushStore,
    pool,
    false,
    "?",
    "CAST(? AS JSON)",
    "CAST(? AS CHAR)"
);
