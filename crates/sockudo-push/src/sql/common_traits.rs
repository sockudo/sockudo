use super::helpers::*;
#[cfg(feature = "mysql")]
use super::stores::MySqlPushStore;
#[cfg(feature = "postgres")]
use super::stores::PostgresPushStore;
use crate::cleanup::{PushCleanupReport, PushCleanupRequest};
use crate::domain::{
    DeleteDeviceOutcome, DeliveryEvent, NotificationTemplate, ProviderCredential, PublishLogEvent,
    PublishStatus, PushCursor, PushCursorKind, ShardJob,
};
use crate::storage::{
    IdempotencyRecord, OperatorInvalidationEvent, Page, PublishStatusCasOutcome, PushCleanupStore,
    PushCredentialStore, PushDeliveryEventStore, PushFanoutShardStore, PushIdempotencyStore,
    PushOperatorEventStore, PushPublishLogStore, PushPublishStatusStore, PushScheduleStore,
    PushSchedulerLockStore, PushStorageError, PushStorageResult, PushTemplateStore,
    ScheduledPushJob, SchedulerLock, VersionedPublishStatus,
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
            async fn create_publish_status_if_absent(
                &self,
                status: PublishStatus,
            ) -> PushStorageResult<PublishStatusCasOutcome> {
                let now_ms = now_ms_i64();
                let q = sql_query(format!(
                    "INSERT INTO push_publish_status (app_id, publish_id, state, counters_json, error_reason, created_at_ms, updated_at_ms, revision) VALUES ({0}, {0}, {0}, {1}, {0}, {0}, {0}, {0})",
                    $bind, $json_cast
                ), $postgres);
                let result = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&status.app_id)
                    .bind(&status.publish_id)
                    .bind(format!("{:?}", status.state).to_ascii_lowercase())
                    .bind(to_json_string(&status)?)
                    .bind(&status.error_reason)
                    .bind(now_ms)
                    .bind(now_ms)
                    .bind(1_i64)
                    .execute(&self.$pool)
                    .await;

                match result {
                    Ok(_) => Ok(PublishStatusCasOutcome::Inserted { revision: 1 }),
                    Err(error) if is_unique_violation(&error) => {
                        Ok(PublishStatusCasOutcome::Conflict)
                    }
                    Err(error) => Err(sql_error(error)),
                }
            }

            async fn get_versioned_publish_status(
                &self,
                app_id: &str,
                publish_id: &str,
            ) -> PushStorageResult<Option<VersionedPublishStatus>> {
                let q = sql_query(format!(
                    "SELECT {1} AS counters_json, {2} AS revision, {3} AS updated_at_ms FROM push_publish_status WHERE app_id = {0} AND publish_id = {0}",
                    $bind,
                    json_text_expr("counters_json", $json_text),
                    signed_i64_expr("revision", $postgres),
                    signed_i64_expr("updated_at_ms", $postgres)
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(publish_id)
                    .fetch_optional(&self.$pool)
                    .await
                    .map_err(sql_error)?
                    .map(|row| {
                        let status: PublishStatus = from_json_str(
                            &row.try_get::<String, _>("counters_json")
                                .map_err(sql_error)?,
                        )?;
                        if status.app_id != app_id || status.publish_id != publish_id {
                            return Err(PushStorageError::Backend(
                                "publish status row identity does not match stored payload"
                                    .to_owned(),
                            ));
                        }
                        Ok(VersionedPublishStatus {
                            status,
                            revision: positive_sql_u64(
                                row.try_get::<i64, _>("revision").map_err(sql_error)?,
                                "publish status revision",
                            )?,
                            updated_at_ms: nonnegative_sql_u64(
                                row.try_get::<i64, _>("updated_at_ms").map_err(sql_error)?,
                                "publish status updated_at_ms",
                            )?,
                        })
                    })
                    .transpose()
            }

            async fn compare_and_swap_publish_status(
                &self,
                expected: &VersionedPublishStatus,
                next: PublishStatus,
            ) -> PushStorageResult<PublishStatusCasOutcome> {
                if expected.status.app_id != next.app_id
                    || expected.status.publish_id != next.publish_id
                {
                    return Err(PushStorageError::Backend(
                        "publish status compare-and-swap identity mismatch".to_owned(),
                    ));
                }

                let expected_revision = sql_i64_revision(expected.revision)?;
                let expected_updated_at_ms = sql_i64_timestamp(
                    expected.updated_at_ms,
                    "publish status expected updated_at_ms",
                )?;
                let next_revision_u64 = expected.revision.checked_add(1).ok_or_else(|| {
                    PushStorageError::Backend(
                        "publish status revision exhausted storage range".to_owned(),
                    )
                })?;
                let next_revision = sql_i64_revision(next_revision_u64)?;
                let updated_at_ms = next_status_updated_at_ms(expected.updated_at_ms);
                let q = sql_query(format!(
                    "UPDATE push_publish_status SET state = {0}, counters_json = {1}, error_reason = {0}, updated_at_ms = {0}, revision = {0} WHERE app_id = {0} AND publish_id = {0} AND revision = {0} AND updated_at_ms = {0}",
                    $bind, $json_cast
                ), $postgres);
                let updated = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(format!("{:?}", next.state).to_ascii_lowercase())
                    .bind(to_json_string(&next)?)
                    .bind(&next.error_reason)
                    .bind(updated_at_ms)
                    .bind(next_revision)
                    .bind(&next.app_id)
                    .bind(&next.publish_id)
                    .bind(expected_revision)
                    .bind(expected_updated_at_ms)
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?
                    .rows_affected();

                if updated == 1 {
                    return Ok(PublishStatusCasOutcome::Updated {
                        revision: next_revision_u64,
                    });
                }
                if updated > 1 {
                    return Err(PushStorageError::Backend(
                        "publish status compare-and-swap updated multiple rows".to_owned(),
                    ));
                }

                let q = sql_query(
                    format!(
                        "SELECT {1} AS revision FROM push_publish_status WHERE app_id = {0} AND publish_id = {0}",
                        $bind,
                        signed_i64_expr("revision", $postgres)
                    ),
                    $postgres,
                );
                let current_revision = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(&next.app_id)
                    .bind(&next.publish_id)
                    .fetch_optional(&self.$pool)
                    .await
                    .map_err(sql_error)?;

                Ok(if current_revision.is_some() {
                    PublishStatusCasOutcome::Conflict
                } else {
                    PublishStatusCasOutcome::Missing
                })
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
                let prune = sql_query(format!(
                    "DELETE FROM push_idempotency WHERE app_id = {0} AND idempotency_key = {0} AND expires_at_ms >= 1000000000000 AND expires_at_ms <= {0}",
                    $bind
                ), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(prune.as_str()))
                    .bind(&record.app_id)
                    .bind(&record.key)
                    .bind(now_ms_i64())
                    .execute(&self.$pool)
                    .await
                    .map_err(sql_error)?;
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
                let q = sql_query(format!("SELECT app_id, idempotency_key, publish_id, expires_at_ms FROM push_idempotency WHERE app_id = {0} AND idempotency_key = {0} AND (expires_at_ms < 1000000000000 OR expires_at_ms > {0})", $bind), $postgres);
                sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                    .bind(app_id)
                    .bind(key)
                    .bind(now_ms_i64())
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

        #[async_trait]
        impl PushCleanupStore for $store {
            async fn cleanup_expired_push_data(&self, request: PushCleanupRequest) -> PushStorageResult<PushCleanupReport> {
                let mut report = PushCleanupReport::default();
                let mut remaining = request.policy.max_deleted_per_tick;

                if remaining > 0 {
                    if let Some(cutoff_ms) = request.publish_status_cutoff_ms() {
                        let limit = request.limit_for(remaining) as i64;
                        let raw = if $postgres {
                            "WITH doomed AS (
                                SELECT app_id, publish_id, revision
                                FROM push_publish_status
                                WHERE updated_at_ms < ?
                                  AND state IN ('succeeded', 'partiallysucceeded', 'failed', 'expired', 'cancelled', 'quotaexceeded', 'deadlettered')
                                ORDER BY updated_at_ms, app_id, publish_id
                                LIMIT ?
                             )
                             DELETE FROM push_publish_status AS status
                             USING doomed
                             WHERE status.app_id = doomed.app_id
                               AND status.publish_id = doomed.publish_id
                               AND status.revision = doomed.revision"
                                .to_owned()
                        } else {
                            "DELETE s
                             FROM push_publish_status s
                             JOIN (
                                SELECT app_id, publish_id, revision
                                FROM push_publish_status
                                WHERE updated_at_ms < ?
                                  AND state IN ('succeeded', 'partiallysucceeded', 'failed', 'expired', 'cancelled', 'quotaexceeded', 'deadlettered')
                                ORDER BY updated_at_ms, app_id, publish_id
                                LIMIT ?
                             ) doomed ON doomed.app_id = s.app_id
                                AND doomed.publish_id = s.publish_id
                                AND doomed.revision = s.revision"
                                .to_owned()
                        };
                        let q = sql_query(raw, $postgres);
                        let deleted = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                            .bind(cutoff_ms as i64)
                            .bind(limit)
                            .execute(&self.$pool)
                            .await
                            .map_err(sql_error)?
                            .rows_affected();
                        report.publish_statuses.record(deleted, deleted);
                        remaining = remaining.saturating_sub(deleted as usize);
                    }
                }

                if remaining > 0 {
                    if let Some(cutoff_ms) = request.delivery_event_cutoff_ms() {
                        let limit = request.limit_for(remaining) as i64;
                        let raw = if $postgres {
                            "WITH doomed AS (
                                SELECT app_id, publish_id, occurred_at_ms, event_id
                                FROM push_delivery_events
                                WHERE occurred_at_ms < ?
                                ORDER BY occurred_at_ms, app_id, publish_id, event_id
                                LIMIT ?
                             )
                             DELETE FROM push_delivery_events
                             WHERE (app_id, publish_id, occurred_at_ms, event_id) IN (
                                SELECT app_id, publish_id, occurred_at_ms, event_id FROM doomed
                             )"
                                .to_owned()
                        } else {
                            "DELETE e
                             FROM push_delivery_events e
                             JOIN (
                                SELECT app_id, publish_id, occurred_at_ms, event_id
                                FROM push_delivery_events
                                WHERE occurred_at_ms < ?
                                ORDER BY occurred_at_ms, app_id, publish_id, event_id
                                LIMIT ?
                             ) doomed
                               ON doomed.app_id = e.app_id
                              AND doomed.publish_id = e.publish_id
                              AND doomed.occurred_at_ms = e.occurred_at_ms
                              AND doomed.event_id = e.event_id"
                                .to_owned()
                        };
                        let q = sql_query(raw, $postgres);
                        let deleted = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                            .bind(cutoff_ms as i64)
                            .bind(limit)
                            .execute(&self.$pool)
                            .await
                            .map_err(sql_error)?
                            .rows_affected();
                        report.delivery_events.record(deleted, deleted);
                        remaining = remaining.saturating_sub(deleted as usize);
                    }
                }

                if remaining > 0 {
                    let limit = request.limit_for(remaining) as i64;
                    let raw = if $postgres {
                        "WITH doomed AS (
                            SELECT app_id, idempotency_key
                            FROM push_idempotency
                            WHERE expires_at_ms >= 1000000000000 AND expires_at_ms <= ?
                            ORDER BY expires_at_ms, app_id, idempotency_key
                            LIMIT ?
                         )
                         DELETE FROM push_idempotency
                         WHERE (app_id, idempotency_key) IN (SELECT app_id, idempotency_key FROM doomed)"
                            .to_owned()
                    } else {
                        "DELETE i
                         FROM push_idempotency i
                         JOIN (
                            SELECT app_id, idempotency_key
                            FROM push_idempotency
                            WHERE expires_at_ms >= 1000000000000 AND expires_at_ms <= ?
                            ORDER BY expires_at_ms, app_id, idempotency_key
                            LIMIT ?
                         ) doomed ON doomed.app_id = i.app_id AND doomed.idempotency_key = i.idempotency_key"
                            .to_owned()
                    };
                    let q = sql_query(raw, $postgres);
                    let deleted = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                        .bind(request.now_ms as i64)
                        .bind(limit)
                        .execute(&self.$pool)
                        .await
                        .map_err(sql_error)?
                        .rows_affected();
                    report.idempotency_records.record(deleted, deleted);
                    remaining = remaining.saturating_sub(deleted as usize);
                }

                if remaining > 0 {
                    let limit = request.limit_for(remaining) as i64;
                    let raw = if $postgres {
                        "WITH doomed AS (
                            SELECT app_id, publish_id
                            FROM push_scheduler_locks
                            WHERE expires_at_ms <= ?
                            ORDER BY expires_at_ms, app_id, publish_id
                            LIMIT ?
                         )
                         DELETE FROM push_scheduler_locks
                         WHERE (app_id, publish_id) IN (SELECT app_id, publish_id FROM doomed)"
                            .to_owned()
                    } else {
                        "DELETE l
                         FROM push_scheduler_locks l
                         JOIN (
                            SELECT app_id, publish_id
                            FROM push_scheduler_locks
                            WHERE expires_at_ms <= ?
                            ORDER BY expires_at_ms, app_id, publish_id
                            LIMIT ?
                         ) doomed ON doomed.app_id = l.app_id AND doomed.publish_id = l.publish_id"
                            .to_owned()
                    };
                    let q = sql_query(raw, $postgres);
                    let deleted = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                        .bind(request.now_ms as i64)
                        .bind(limit)
                        .execute(&self.$pool)
                        .await
                        .map_err(sql_error)?
                        .rows_affected();
                    report.scheduler_locks.record(deleted, deleted);
                    remaining = remaining.saturating_sub(deleted as usize);
                }

                if remaining > 0 {
                    if let Some(cutoff_ms) = request.operator_event_cutoff_ms() {
                        let limit = request.limit_for(remaining) as i64;
                        let raw = if $postgres {
                            "WITH doomed AS (
                                SELECT app_id, occurred_at_ms, event_id
                                FROM push_operator_invalidations
                                WHERE occurred_at_ms < ?
                                ORDER BY occurred_at_ms, app_id, event_id
                                LIMIT ?
                             )
                             DELETE FROM push_operator_invalidations
                             WHERE (app_id, occurred_at_ms, event_id) IN (
                                SELECT app_id, occurred_at_ms, event_id FROM doomed
                             )"
                                .to_owned()
                        } else {
                            "DELETE oi
                             FROM push_operator_invalidations oi
                             JOIN (
                                SELECT app_id, occurred_at_ms, event_id
                                FROM push_operator_invalidations
                                WHERE occurred_at_ms < ?
                                ORDER BY occurred_at_ms, app_id, event_id
                                LIMIT ?
                             ) doomed
                               ON doomed.app_id = oi.app_id
                              AND doomed.occurred_at_ms = oi.occurred_at_ms
                              AND doomed.event_id = oi.event_id"
                                .to_owned()
                        };
                        let q = sql_query(raw, $postgres);
                        let deleted = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                            .bind(cutoff_ms as i64)
                            .bind(limit)
                            .execute(&self.$pool)
                            .await
                            .map_err(sql_error)?
                            .rows_affected();
                        report.operator_invalidations.record(deleted, deleted);
                        remaining = remaining.saturating_sub(deleted as usize);
                    }
                }

                if remaining > 0 {
                    if let Some(cutoff_ms) = request.dead_letter_cutoff_ms() {
                        let limit = request.limit_for(remaining) as i64;
                        let raw = if $postgres {
                            "WITH doomed AS (
                                SELECT app_id, publish_id, dead_letter_id
                                FROM push_dead_letters
                                WHERE occurred_at_ms < ?
                                ORDER BY occurred_at_ms, app_id, publish_id, dead_letter_id
                                LIMIT ?
                             )
                             DELETE FROM push_dead_letters
                             WHERE (app_id, publish_id, dead_letter_id) IN (
                                SELECT app_id, publish_id, dead_letter_id FROM doomed
                             )"
                                .to_owned()
                        } else {
                            "DELETE dl
                             FROM push_dead_letters dl
                             JOIN (
                                SELECT app_id, publish_id, dead_letter_id
                                FROM push_dead_letters
                                WHERE occurred_at_ms < ?
                                ORDER BY occurred_at_ms, app_id, publish_id, dead_letter_id
                                LIMIT ?
                             ) doomed
                               ON doomed.app_id = dl.app_id
                              AND doomed.publish_id = dl.publish_id
                              AND doomed.dead_letter_id = dl.dead_letter_id"
                                .to_owned()
                        };
                        let q = sql_query(raw, $postgres);
                        let deleted = sqlx::query(sqlx::AssertSqlSafe(q.as_str()))
                            .bind(cutoff_ms as i64)
                            .bind(limit)
                            .execute(&self.$pool)
                            .await
                            .map_err(sql_error)?
                            .rows_affected();
                        report.dead_letters.record(deleted, deleted);
                    }
                }

                Ok(report)
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
