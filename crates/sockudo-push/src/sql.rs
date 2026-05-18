use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::Row;

use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    ProviderCredential, PublishLogEvent, PublishStatus, PushCursor, PushCursorKind,
    PushProviderKind, ShardJob,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, EXPECTED_PUSH_SCHEMA_VERSION,
    IdempotencyRecord, OperatorInvalidationEvent, Page, PushCredentialStore,
    PushDeliveryEventStore, PushDeviceStore, PushFanoutShardStore, PushIdempotencyStore,
    PushOperatorEventStore, PushPublishLogStore, PushPublishStatusStore, PushScheduleStore,
    PushSchedulerLockStore, PushStorageError, PushStorageResult, PushSubscriptionStore,
    PushTemplateStore, ScheduledPushJob, SchedulerLock,
};

#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PostgresPushStore {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresPushStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn assert_schema_version(&self) -> PushStorageResult<()> {
        let version: i32 = sqlx::query_scalar(
            "SELECT version FROM push_schema_version ORDER BY version DESC LIMIT 1",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(sql_error)?;
        assert_expected_schema_version(version as u32)
    }
}

#[cfg(feature = "mysql")]
#[derive(Clone)]
pub struct MySqlPushStore {
    pool: sqlx::MySqlPool,
}

#[cfg(feature = "mysql")]
impl MySqlPushStore {
    pub fn new(pool: sqlx::MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn assert_schema_version(&self) -> PushStorageResult<()> {
        let version: u32 = sqlx::query_scalar(
            "SELECT version FROM push_schema_version ORDER BY version DESC LIMIT 1",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(sql_error)?;
        assert_expected_schema_version(version)
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl PushDeviceStore for PostgresPushStore {
    async fn upsert_device(
        &self,
        device: DeviceDetails,
    ) -> PushStorageResult<DeviceRegistrationOutcome> {
        upsert_device_pg(&self.pool, device).await
    }

    async fn get_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<Option<DeviceDetails>> {
        get_device_pg(&self.pool, app_id, device_id).await
    }

    async fn delete_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        delete_device_pg(&self.pool, app_id, device_id).await
    }

    async fn list_devices(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        list_devices_pg(&self.pool, app_id, limit, cursor).await
    }

    async fn delete_devices_by_client(
        &self,
        app_id: &str,
        client_id: &str,
    ) -> PushStorageResult<u64> {
        let mut tx = self.pool.begin().await.map_err(sql_error)?;
        sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = $1 AND client_id = $2")
            .bind(app_id)
            .bind(client_id)
            .execute(&mut *tx)
            .await
            .map_err(sql_error)?;
        let result = sqlx::query("DELETE FROM push_devices WHERE app_id = $1 AND client_id = $2")
            .bind(app_id)
            .bind(client_id)
            .execute(&mut *tx)
            .await
            .map_err(sql_error)?;
        tx.commit().await.map_err(sql_error)?;
        Ok(result.rows_affected())
    }

    async fn list_stale_devices(
        &self,
        app_id: &str,
        day_bucket: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        list_stale_devices_pg(&self.pool, app_id, day_bucket, limit, cursor).await
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl PushDeviceStore for MySqlPushStore {
    async fn upsert_device(
        &self,
        device: DeviceDetails,
    ) -> PushStorageResult<DeviceRegistrationOutcome> {
        upsert_device_mysql(&self.pool, device).await
    }

    async fn get_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<Option<DeviceDetails>> {
        get_device_mysql(&self.pool, app_id, device_id).await
    }

    async fn delete_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        delete_device_mysql(&self.pool, app_id, device_id).await
    }

    async fn list_devices(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        list_devices_mysql(&self.pool, app_id, limit, cursor).await
    }

    async fn delete_devices_by_client(
        &self,
        app_id: &str,
        client_id: &str,
    ) -> PushStorageResult<u64> {
        let mut tx = self.pool.begin().await.map_err(sql_error)?;
        sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = ? AND client_id = ?")
            .bind(app_id)
            .bind(client_id)
            .execute(&mut *tx)
            .await
            .map_err(sql_error)?;
        let result = sqlx::query("DELETE FROM push_devices WHERE app_id = ? AND client_id = ?")
            .bind(app_id)
            .bind(client_id)
            .execute(&mut *tx)
            .await
            .map_err(sql_error)?;
        tx.commit().await.map_err(sql_error)?;
        Ok(result.rows_affected())
    }

    async fn list_stale_devices(
        &self,
        app_id: &str,
        day_bucket: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<DeviceDetails>> {
        list_stale_devices_mysql(&self.pool, app_id, day_bucket, limit, cursor).await
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl PushSubscriptionStore for PostgresPushStore {
    async fn upsert_subscription(
        &self,
        subscription: ChannelSubscription,
    ) -> PushStorageResult<()> {
        subscription.validate()?;
        sqlx::query(
            r#"
            INSERT INTO push_channel_subscribers
                (app_id, channel, device_id, client_id, provider, token_hash, credential_version, recipient_json, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, $8)
            ON CONFLICT (app_id, channel, device_id) DO UPDATE SET
                client_id = EXCLUDED.client_id,
                provider = EXCLUDED.provider,
                token_hash = EXCLUDED.token_hash,
                credential_version = EXCLUDED.credential_version,
                updated_at_ms = EXCLUDED.updated_at_ms
            "#,
        )
        .bind(&subscription.app_id)
        .bind(&subscription.channel)
        .bind(&subscription.device_id)
        .bind(&subscription.client_id)
        .bind(provider_label(subscription.provider))
        .bind(&subscription.token_hash)
        .bind(subscription.credential_version.map(|v| v as i64))
        .bind(now_ms_i64())
        .execute(&self.pool)
        .await
        .map_err(sql_error)?;
        Ok(())
    }

    async fn delete_subscription(
        &self,
        app_id: &str,
        channel: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        delete_subscription_pg(&self.pool, app_id, channel, device_id).await
    }

    async fn list_channel_subscribers(
        &self,
        app_id: &str,
        channel: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        list_channel_subscribers_pg(&self.pool, app_id, channel, limit, cursor).await
    }

    async fn list_device_channels(
        &self,
        app_id: &str,
        device_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        list_device_channels_pg(&self.pool, app_id, device_id, limit, cursor).await
    }

    async fn list_subscriptions(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        list_subscriptions_pg(&self.pool, app_id, limit, cursor).await
    }

    async fn list_subscription_channels(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<String>> {
        list_subscription_channels_pg(&self.pool, app_id, limit, cursor).await
    }

    async fn delete_subscriptions_by_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<u64> {
        let result = sqlx::query(
            "DELETE FROM push_channel_subscribers WHERE app_id = $1 AND device_id = $2",
        )
        .bind(app_id)
        .bind(device_id)
        .execute(&self.pool)
        .await
        .map_err(sql_error)?;
        Ok(result.rows_affected())
    }

    async fn delete_subscriptions_by_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> PushStorageResult<u64> {
        let result =
            sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = $1 AND channel = $2")
                .bind(app_id)
                .bind(channel)
                .execute(&self.pool)
                .await
                .map_err(sql_error)?;
        Ok(result.rows_affected())
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl PushSubscriptionStore for MySqlPushStore {
    async fn upsert_subscription(
        &self,
        subscription: ChannelSubscription,
    ) -> PushStorageResult<()> {
        subscription.validate()?;
        sqlx::query(
            r#"
            INSERT INTO push_channel_subscribers
                (app_id, channel, device_id, client_id, provider, token_hash, credential_version, recipient_json, updated_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, CAST('{}' AS JSON), ?)
            ON DUPLICATE KEY UPDATE
                client_id = VALUES(client_id),
                provider = VALUES(provider),
                token_hash = VALUES(token_hash),
                credential_version = VALUES(credential_version),
                updated_at_ms = VALUES(updated_at_ms)
            "#,
        )
        .bind(&subscription.app_id)
        .bind(&subscription.channel)
        .bind(&subscription.device_id)
        .bind(&subscription.client_id)
        .bind(provider_label(subscription.provider))
        .bind(&subscription.token_hash)
        .bind(subscription.credential_version.map(|v| v as i64))
        .bind(now_ms_i64())
        .execute(&self.pool)
        .await
        .map_err(sql_error)?;
        Ok(())
    }

    async fn delete_subscription(
        &self,
        app_id: &str,
        channel: &str,
        device_id: &str,
    ) -> PushStorageResult<DeleteDeviceOutcome> {
        delete_subscription_mysql(&self.pool, app_id, channel, device_id).await
    }

    async fn list_channel_subscribers(
        &self,
        app_id: &str,
        channel: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        list_channel_subscribers_mysql(&self.pool, app_id, channel, limit, cursor).await
    }

    async fn list_device_channels(
        &self,
        app_id: &str,
        device_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        list_device_channels_mysql(&self.pool, app_id, device_id, limit, cursor).await
    }

    async fn list_subscriptions(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<ChannelSubscription>> {
        list_subscriptions_mysql(&self.pool, app_id, limit, cursor).await
    }

    async fn list_subscription_channels(
        &self,
        app_id: &str,
        limit: usize,
        cursor: Option<PushCursor>,
    ) -> PushStorageResult<Page<String>> {
        list_subscription_channels_mysql(&self.pool, app_id, limit, cursor).await
    }

    async fn delete_subscriptions_by_device(
        &self,
        app_id: &str,
        device_id: &str,
    ) -> PushStorageResult<u64> {
        let result =
            sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = ? AND device_id = ?")
                .bind(app_id)
                .bind(device_id)
                .execute(&self.pool)
                .await
                .map_err(sql_error)?;
        Ok(result.rows_affected())
    }

    async fn delete_subscriptions_by_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> PushStorageResult<u64> {
        let result =
            sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = ? AND channel = ?")
                .bind(app_id)
                .bind(channel)
                .execute(&self.pool)
                .await
                .map_err(sql_error)?;
        Ok(result.rows_affected())
    }
}

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
                sqlx::query(&q)
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
                sqlx::query(&q)
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
                let rows = sqlx::query(&q)
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
                sqlx::query(&q)
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
                sqlx::query(&q)
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
                let rows = sqlx::query(&q)
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
                delete_result(sqlx::query(&q).bind(app_id).bind(template_id).execute(&self.$pool).await.map_err(sql_error)?.rows_affected())
            }
        }

        #[async_trait]
        impl PushPublishStatusStore for $store {
            async fn put_publish_status(&self, status: PublishStatus) -> PushStorageResult<()> {
                let q = sql_query(format!(
                    "INSERT INTO push_publish_status (app_id, publish_id, state, counters_json, error_reason, created_at_ms, updated_at_ms) VALUES ({0}, {0}, {0}, {1}, {0}, {0}, {0}) {2}",
                    $bind, $json_cast, upsert_status_clause($postgres)
                ), $postgres);
                sqlx::query(&q)
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
                sqlx::query(&q)
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
                sqlx::query(&q)
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
                let rows = sqlx::query(&q)
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
                sqlx::query(&q)
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
                sqlx::query(&q)
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
                sqlx::query(&q)
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
                sqlx::query(&q)
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
                delete_result(sqlx::query(&q).bind(app_id).bind(publish_id).execute(&self.$pool).await.map_err(sql_error)?.rows_affected())
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
                let rows = sqlx::query(&q)
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
                sqlx::query(&q)
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
                let rows = sqlx::query(&q)
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
                Ok(sqlx::query(&q).bind(app_id).bind(before_ms as i64).execute(&self.$pool).await.map_err(sql_error)?.rows_affected())
            }
        }

        #[async_trait]
        impl PushIdempotencyStore for $store {
            async fn put_idempotency_record_if_absent(&self, record: IdempotencyRecord) -> PushStorageResult<bool> {
                let q = sql_query(format!(
                    "INSERT INTO push_idempotency (app_id, idempotency_key, publish_id, expires_at_ms, created_at_ms) VALUES ({0}, {0}, {0}, {0}, {0}) {1}",
                    $bind, ignore_conflict_clause($postgres)
                ), $postgres);
                let result = sqlx::query(&q)
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
                sqlx::query(&q)
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
                    let row = sqlx::query(&q)
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
                sqlx::query(&q)
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
                sqlx::query(&q)
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
                let rows = sqlx::query(&q)
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

#[cfg(feature = "postgres")]
async fn upsert_device_pg(
    pool: &sqlx::PgPool,
    device: DeviceDetails,
) -> PushStorageResult<DeviceRegistrationOutcome> {
    device.validate()?;
    let token_hash = device.push.recipient.token_hash();
    let existing = get_device_pg(pool, &device.app_id, &device.id).await?;
    let now = now_ms_i64();
    let row = sqlx::query(
        r#"
        INSERT INTO push_devices
            (app_id, device_id, client_id, form_factor, platform, metadata, device_secret_hash, timezone, locale, last_active_at_ms, push_state, failure_count, error_reason, transport_type, token_hash, recipient_json, credential_version, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb, NULL, $17, $18)
        ON CONFLICT (app_id, device_id) DO UPDATE SET
            client_id = EXCLUDED.client_id,
            form_factor = EXCLUDED.form_factor,
            platform = EXCLUDED.platform,
            metadata = EXCLUDED.metadata,
            device_secret_hash = EXCLUDED.device_secret_hash,
            timezone = EXCLUDED.timezone,
            locale = EXCLUDED.locale,
            last_active_at_ms = EXCLUDED.last_active_at_ms,
            push_state = EXCLUDED.push_state,
            failure_count = EXCLUDED.failure_count,
            error_reason = EXCLUDED.error_reason,
            transport_type = EXCLUDED.transport_type,
            token_hash = EXCLUDED.token_hash,
            recipient_json = EXCLUDED.recipient_json,
            updated_at_ms = EXCLUDED.updated_at_ms
        WHERE
            push_devices.client_id IS DISTINCT FROM EXCLUDED.client_id OR
            push_devices.form_factor IS DISTINCT FROM EXCLUDED.form_factor OR
            push_devices.platform IS DISTINCT FROM EXCLUDED.platform OR
            push_devices.metadata IS DISTINCT FROM EXCLUDED.metadata OR
            push_devices.device_secret_hash IS DISTINCT FROM EXCLUDED.device_secret_hash OR
            push_devices.timezone IS DISTINCT FROM EXCLUDED.timezone OR
            push_devices.locale IS DISTINCT FROM EXCLUDED.locale OR
            push_devices.last_active_at_ms IS DISTINCT FROM EXCLUDED.last_active_at_ms OR
            push_devices.push_state IS DISTINCT FROM EXCLUDED.push_state OR
            push_devices.failure_count IS DISTINCT FROM EXCLUDED.failure_count OR
            push_devices.error_reason IS DISTINCT FROM EXCLUDED.error_reason OR
            push_devices.transport_type IS DISTINCT FROM EXCLUDED.transport_type OR
            push_devices.token_hash IS DISTINCT FROM EXCLUDED.token_hash OR
            push_devices.recipient_json IS DISTINCT FROM EXCLUDED.recipient_json
        RETURNING 1 AS touched
        "#,
    )
    .bind(&device.app_id)
    .bind(&device.id)
    .bind(&device.client_id)
    .bind(enum_label(&device.form_factor)?)
    .bind(enum_label(&device.platform)?)
    .bind(to_json_string(&device.metadata)?)
    .bind(device.device_secret.expose_secret())
    .bind(&device.timezone)
    .bind(&device.locale)
    .bind(device.last_active_at_ms as i64)
    .bind(enum_label(&device.push.state)?)
    .bind(device.push.failure_count as i32)
    .bind(&device.push.error_reason)
    .bind(provider_label(device.push.recipient.provider()))
    .bind(&token_hash)
    .bind(to_json_string(&device.push.recipient)?)
    .bind(now)
    .bind(now)
    .fetch_optional(pool)
    .await
    .map_err(sql_error)?;
    let change = match row {
        None => DeviceRegistrationChange::Unchanged,
        Some(_) if existing.is_none() => DeviceRegistrationChange::Inserted,
        Some(_) => DeviceRegistrationChange::Updated,
    };
    Ok(DeviceRegistrationOutcome { change, token_hash })
}

#[cfg(feature = "mysql")]
async fn upsert_device_mysql(
    pool: &sqlx::MySqlPool,
    device: DeviceDetails,
) -> PushStorageResult<DeviceRegistrationOutcome> {
    device.validate()?;
    let token_hash = device.push.recipient.token_hash();
    let now = now_ms_i64();
    let result = sqlx::query(
        r#"
        INSERT INTO push_devices
            (app_id, device_id, client_id, form_factor, platform, metadata, device_secret_hash, timezone, locale, last_active_at_ms, push_state, failure_count, error_reason, transport_type, token_hash, recipient_json, credential_version, created_at_ms, updated_at_ms)
        VALUES (?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), NULL, ?, ?)
        ON DUPLICATE KEY UPDATE
            client_id = VALUES(client_id),
            form_factor = VALUES(form_factor),
            platform = VALUES(platform),
            metadata = VALUES(metadata),
            device_secret_hash = VALUES(device_secret_hash),
            timezone = VALUES(timezone),
            locale = VALUES(locale),
            last_active_at_ms = VALUES(last_active_at_ms),
            push_state = VALUES(push_state),
            failure_count = VALUES(failure_count),
            error_reason = VALUES(error_reason),
            transport_type = VALUES(transport_type),
            token_hash = VALUES(token_hash),
            recipient_json = VALUES(recipient_json),
            updated_at_ms = VALUES(updated_at_ms)
        "#,
    )
    .bind(&device.app_id)
    .bind(&device.id)
    .bind(&device.client_id)
    .bind(enum_label(&device.form_factor)?)
    .bind(enum_label(&device.platform)?)
    .bind(to_json_string(&device.metadata)?)
    .bind(device.device_secret.expose_secret())
    .bind(&device.timezone)
    .bind(&device.locale)
    .bind(device.last_active_at_ms as i64)
    .bind(enum_label(&device.push.state)?)
    .bind(device.push.failure_count as i32)
    .bind(&device.push.error_reason)
    .bind(provider_label(device.push.recipient.provider()))
    .bind(&token_hash)
    .bind(to_json_string(&device.push.recipient)?)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .map_err(sql_error)?;
    let change = match result.rows_affected() {
        1 => DeviceRegistrationChange::Inserted,
        0 => DeviceRegistrationChange::Unchanged,
        _ => DeviceRegistrationChange::Updated,
    };
    Ok(DeviceRegistrationOutcome { change, token_hash })
}

#[cfg(feature = "postgres")]
async fn get_device_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    device_id: &str,
) -> PushStorageResult<Option<DeviceDetails>> {
    sqlx::query(
        r#"SELECT app_id, device_id, client_id, form_factor, platform, metadata::text AS metadata, device_secret_hash, timezone, locale, last_active_at_ms, push_state, failure_count, error_reason, recipient_json::text AS recipient_json FROM push_devices WHERE app_id = $1 AND device_id = $2"#,
    )
    .bind(app_id)
    .bind(device_id)
    .fetch_optional(pool)
    .await
    .map_err(sql_error)?
    .map(|row| device_from_row(&row))
    .transpose()
}

#[cfg(feature = "mysql")]
async fn get_device_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    device_id: &str,
) -> PushStorageResult<Option<DeviceDetails>> {
    sqlx::query(
        r#"SELECT app_id, device_id, client_id, form_factor, platform, CAST(metadata AS CHAR) AS metadata, device_secret_hash, timezone, locale, last_active_at_ms, push_state, failure_count, error_reason, CAST(recipient_json AS CHAR) AS recipient_json FROM push_devices WHERE app_id = ? AND device_id = ?"#,
    )
    .bind(app_id)
    .bind(device_id)
    .fetch_optional(pool)
    .await
    .map_err(sql_error)?
    .map(|row| device_from_row(&row))
    .transpose()
}

#[cfg(feature = "postgres")]
async fn delete_device_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    device_id: &str,
) -> PushStorageResult<DeleteDeviceOutcome> {
    sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = $1 AND device_id = $2")
        .bind(app_id)
        .bind(device_id)
        .execute(pool)
        .await
        .map_err(sql_error)?;
    delete_result(
        sqlx::query("DELETE FROM push_devices WHERE app_id = $1 AND device_id = $2")
            .bind(app_id)
            .bind(device_id)
            .execute(pool)
            .await
            .map_err(sql_error)?
            .rows_affected(),
    )
}

#[cfg(feature = "mysql")]
async fn delete_device_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    device_id: &str,
) -> PushStorageResult<DeleteDeviceOutcome> {
    sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = ? AND device_id = ?")
        .bind(app_id)
        .bind(device_id)
        .execute(pool)
        .await
        .map_err(sql_error)?;
    delete_result(
        sqlx::query("DELETE FROM push_devices WHERE app_id = ? AND device_id = ?")
            .bind(app_id)
            .bind(device_id)
            .execute(pool)
            .await
            .map_err(sql_error)?
            .rows_affected(),
    )
}

#[cfg(feature = "postgres")]
async fn list_devices_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<DeviceDetails>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let rows = sqlx::query(r#"SELECT app_id, device_id, client_id, form_factor, platform, metadata::text AS metadata, device_secret_hash, timezone, locale, last_active_at_ms, push_state, failure_count, error_reason, recipient_json::text AS recipient_json FROM push_devices WHERE app_id = $1 AND device_id > $2 ORDER BY device_id LIMIT $3"#)
        .bind(app_id)
        .bind(start)
        .bind(limit_plus_one(limit))
        .fetch_all(pool)
        .await
        .map_err(sql_error)?;
    page_from_sql_rows(app_id, PushCursorKind::Device, rows, limit, |row| {
        Ok((
            row.try_get::<String, _>("device_id").map_err(sql_error)?,
            device_from_row(row)?,
        ))
    })
}

#[cfg(feature = "mysql")]
async fn list_devices_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<DeviceDetails>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let rows = sqlx::query(r#"SELECT app_id, device_id, client_id, form_factor, platform, CAST(metadata AS CHAR) AS metadata, device_secret_hash, timezone, locale, last_active_at_ms, push_state, failure_count, error_reason, CAST(recipient_json AS CHAR) AS recipient_json FROM push_devices WHERE app_id = ? AND device_id > ? ORDER BY device_id LIMIT ?"#)
        .bind(app_id)
        .bind(start)
        .bind(limit_plus_one(limit))
        .fetch_all(pool)
        .await
        .map_err(sql_error)?;
    page_from_sql_rows(app_id, PushCursorKind::Device, rows, limit, |row| {
        Ok((
            row.try_get::<String, _>("device_id").map_err(sql_error)?,
            device_from_row(row)?,
        ))
    })
}

#[cfg(feature = "postgres")]
async fn list_stale_devices_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    day_bucket: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<DeviceDetails>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let (start_ms, end_ms) = day_bucket_range(day_bucket)?;
    let (cursor_ms, cursor_device_id) = parse_ts_id_cursor(&start);
    let rows = sqlx::query(r#"SELECT app_id, device_id, client_id, form_factor, platform, metadata::text AS metadata, device_secret_hash, timezone, locale, last_active_at_ms, push_state, failure_count, error_reason, recipient_json::text AS recipient_json FROM push_devices WHERE app_id = $1 AND last_active_at_ms >= $2 AND last_active_at_ms < $3 AND (last_active_at_ms, device_id) > ($4, $5) ORDER BY last_active_at_ms, device_id LIMIT $6"#)
        .bind(app_id)
        .bind(start_ms)
        .bind(end_ms)
        .bind(cursor_ms)
        .bind(cursor_device_id)
        .bind(limit_plus_one(limit))
        .fetch_all(pool)
        .await
        .map_err(sql_error)?;
    page_from_sql_rows(app_id, PushCursorKind::Device, rows, limit, |row| {
        let ts: i64 = row.try_get("last_active_at_ms").map_err(sql_error)?;
        let device_id: String = row.try_get("device_id").map_err(sql_error)?;
        Ok((format!("{ts:020}:{device_id}"), device_from_row(row)?))
    })
}

#[cfg(feature = "mysql")]
async fn list_stale_devices_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    day_bucket: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<DeviceDetails>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let (start_ms, end_ms) = day_bucket_range(day_bucket)?;
    let (cursor_ms, cursor_device_id) = parse_ts_id_cursor(&start);
    let rows = sqlx::query(r#"SELECT app_id, device_id, client_id, form_factor, platform, CAST(metadata AS CHAR) AS metadata, device_secret_hash, timezone, locale, last_active_at_ms, push_state, failure_count, error_reason, CAST(recipient_json AS CHAR) AS recipient_json FROM push_devices WHERE app_id = ? AND last_active_at_ms >= ? AND last_active_at_ms < ? AND (last_active_at_ms, device_id) > (?, ?) ORDER BY last_active_at_ms, device_id LIMIT ?"#)
        .bind(app_id)
        .bind(start_ms)
        .bind(end_ms)
        .bind(cursor_ms)
        .bind(cursor_device_id)
        .bind(limit_plus_one(limit))
        .fetch_all(pool)
        .await
        .map_err(sql_error)?;
    page_from_sql_rows(app_id, PushCursorKind::Device, rows, limit, |row| {
        let ts: i64 = row.try_get("last_active_at_ms").map_err(sql_error)?;
        let device_id: String = row.try_get("device_id").map_err(sql_error)?;
        Ok((format!("{ts:020}:{device_id}"), device_from_row(row)?))
    })
}

#[cfg(feature = "postgres")]
async fn delete_subscription_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    channel: &str,
    device_id: &str,
) -> PushStorageResult<DeleteDeviceOutcome> {
    delete_result(sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = $1 AND channel = $2 AND device_id = $3").bind(app_id).bind(channel).bind(device_id).execute(pool).await.map_err(sql_error)?.rows_affected())
}

#[cfg(feature = "mysql")]
async fn delete_subscription_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    channel: &str,
    device_id: &str,
) -> PushStorageResult<DeleteDeviceOutcome> {
    delete_result(sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = ? AND channel = ? AND device_id = ?").bind(app_id).bind(channel).bind(device_id).execute(pool).await.map_err(sql_error)?.rows_affected())
}

#[cfg(feature = "postgres")]
async fn list_channel_subscribers_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    channel: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<ChannelSubscription>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let rows = sqlx::query("SELECT app_id, channel, device_id, client_id, provider, token_hash, credential_version FROM push_channel_subscribers WHERE app_id = $1 AND channel = $2 AND device_id > $3 ORDER BY device_id LIMIT $4")
        .bind(app_id).bind(channel).bind(start).bind(limit_plus_one(limit)).fetch_all(pool).await.map_err(sql_error)?;
    page_from_sql_rows(
        app_id,
        PushCursorKind::ChannelSubscription,
        rows,
        limit,
        |row| {
            Ok((
                row.try_get::<String, _>("device_id").map_err(sql_error)?,
                subscription_from_row(row)?,
            ))
        },
    )
}

#[cfg(feature = "mysql")]
async fn list_channel_subscribers_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    channel: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<ChannelSubscription>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let rows = sqlx::query("SELECT app_id, channel, device_id, client_id, provider, token_hash, credential_version FROM push_channel_subscribers WHERE app_id = ? AND channel = ? AND device_id > ? ORDER BY device_id LIMIT ?")
        .bind(app_id).bind(channel).bind(start).bind(limit_plus_one(limit)).fetch_all(pool).await.map_err(sql_error)?;
    page_from_sql_rows(
        app_id,
        PushCursorKind::ChannelSubscription,
        rows,
        limit,
        |row| {
            Ok((
                row.try_get::<String, _>("device_id").map_err(sql_error)?,
                subscription_from_row(row)?,
            ))
        },
    )
}

#[cfg(feature = "postgres")]
async fn list_device_channels_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    device_id: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<ChannelSubscription>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let rows = sqlx::query("SELECT app_id, channel, device_id, client_id, provider, token_hash, credential_version FROM push_channel_subscribers WHERE app_id = $1 AND device_id = $2 AND channel > $3 ORDER BY channel LIMIT $4")
        .bind(app_id).bind(device_id).bind(start).bind(limit_plus_one(limit)).fetch_all(pool).await.map_err(sql_error)?;
    page_from_sql_rows(
        app_id,
        PushCursorKind::ChannelSubscription,
        rows,
        limit,
        |row| {
            Ok((
                row.try_get::<String, _>("channel").map_err(sql_error)?,
                subscription_from_row(row)?,
            ))
        },
    )
}

#[cfg(feature = "mysql")]
async fn list_device_channels_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    device_id: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<ChannelSubscription>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let rows = sqlx::query("SELECT app_id, channel, device_id, client_id, provider, token_hash, credential_version FROM push_channel_subscribers WHERE app_id = ? AND device_id = ? AND channel > ? ORDER BY channel LIMIT ?")
        .bind(app_id).bind(device_id).bind(start).bind(limit_plus_one(limit)).fetch_all(pool).await.map_err(sql_error)?;
    page_from_sql_rows(
        app_id,
        PushCursorKind::ChannelSubscription,
        rows,
        limit,
        |row| {
            Ok((
                row.try_get::<String, _>("channel").map_err(sql_error)?,
                subscription_from_row(row)?,
            ))
        },
    )
}

#[cfg(feature = "postgres")]
async fn list_subscriptions_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<ChannelSubscription>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let (cursor_channel, cursor_device_id) = parse_channel_device_cursor(&start);
    let rows = sqlx::query("SELECT app_id, channel, device_id, client_id, provider, token_hash, credential_version FROM push_channel_subscribers WHERE app_id = $1 AND (channel, device_id) > ($2, $3) ORDER BY channel, device_id LIMIT $4")
        .bind(app_id).bind(cursor_channel).bind(cursor_device_id).bind(limit_plus_one(limit)).fetch_all(pool).await.map_err(sql_error)?;
    page_from_sql_rows(
        app_id,
        PushCursorKind::ChannelSubscription,
        rows,
        limit,
        |row| {
            let channel: String = row.try_get("channel").map_err(sql_error)?;
            let device_id: String = row.try_get("device_id").map_err(sql_error)?;
            Ok((
                format!("{channel}:{device_id}"),
                subscription_from_row(row)?,
            ))
        },
    )
}

#[cfg(feature = "mysql")]
async fn list_subscriptions_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<ChannelSubscription>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let (cursor_channel, cursor_device_id) = parse_channel_device_cursor(&start);
    let rows = sqlx::query("SELECT app_id, channel, device_id, client_id, provider, token_hash, credential_version FROM push_channel_subscribers WHERE app_id = ? AND (channel, device_id) > (?, ?) ORDER BY channel, device_id LIMIT ?")
        .bind(app_id).bind(cursor_channel).bind(cursor_device_id).bind(limit_plus_one(limit)).fetch_all(pool).await.map_err(sql_error)?;
    page_from_sql_rows(
        app_id,
        PushCursorKind::ChannelSubscription,
        rows,
        limit,
        |row| {
            let channel: String = row.try_get("channel").map_err(sql_error)?;
            let device_id: String = row.try_get("device_id").map_err(sql_error)?;
            Ok((
                format!("{channel}:{device_id}"),
                subscription_from_row(row)?,
            ))
        },
    )
}

#[cfg(feature = "postgres")]
async fn list_subscription_channels_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<String>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let rows = sqlx::query("SELECT DISTINCT channel FROM push_channel_subscribers WHERE app_id = $1 AND channel > $2 ORDER BY channel LIMIT $3")
        .bind(app_id).bind(start).bind(limit_plus_one(limit)).fetch_all(pool).await.map_err(sql_error)?;
    page_from_sql_rows(
        app_id,
        PushCursorKind::ChannelSubscription,
        rows,
        limit,
        |row| {
            let channel: String = row.try_get("channel").map_err(sql_error)?;
            Ok((channel.clone(), channel))
        },
    )
}

#[cfg(feature = "mysql")]
async fn list_subscription_channels_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    limit: usize,
    cursor: Option<PushCursor>,
) -> PushStorageResult<Page<String>> {
    let start = cursor_position(cursor, app_id)?.unwrap_or_default();
    let rows = sqlx::query("SELECT DISTINCT channel FROM push_channel_subscribers WHERE app_id = ? AND channel > ? ORDER BY channel LIMIT ?")
        .bind(app_id).bind(start).bind(limit_plus_one(limit)).fetch_all(pool).await.map_err(sql_error)?;
    page_from_sql_rows(
        app_id,
        PushCursorKind::ChannelSubscription,
        rows,
        limit,
        |row| {
            let channel: String = row.try_get("channel").map_err(sql_error)?;
            Ok((channel.clone(), channel))
        },
    )
}

fn cursor_position(cursor: Option<PushCursor>, app_id: &str) -> PushStorageResult<Option<String>> {
    match cursor {
        Some(cursor) if cursor.app_id != app_id => {
            Err(crate::domain::PushDomainError::CursorAppMismatch {
                expected: app_id.to_owned(),
                found: cursor.app_id,
            }
            .into())
        }
        Some(cursor) => Ok(Some(cursor.position)),
        None => Ok(None),
    }
}

fn day_bucket_range(day_bucket: &str) -> PushStorageResult<(i64, i64)> {
    let day = day_bucket
        .parse::<i64>()
        .map_err(|_| PushStorageError::Backend("invalid push day bucket".to_owned()))?;
    let start = day.saturating_mul(86_400_000);
    Ok((start, start.saturating_add(86_400_000)))
}

fn parse_ts_id_cursor(cursor: &str) -> (i64, String) {
    let Some((timestamp, id)) = cursor.split_once(':') else {
        return (-1, String::new());
    };
    (timestamp.parse::<i64>().unwrap_or(-1), id.to_owned())
}

fn parse_channel_device_cursor(cursor: &str) -> (String, String) {
    let Some((channel, device_id)) = cursor.split_once(':') else {
        return (String::new(), String::new());
    };
    (channel.to_owned(), device_id.to_owned())
}

fn page_from_sql_rows<R, T, F>(
    app_id: &str,
    kind: PushCursorKind,
    rows: Vec<R>,
    limit: usize,
    mut f: F,
) -> PushStorageResult<Page<T>>
where
    F: FnMut(&R) -> PushStorageResult<(String, T)>,
{
    let mut items = Vec::new();
    let mut last = None;
    for row in rows.iter().take(limit) {
        let (position, item) = f(row)?;
        last = Some(position);
        items.push(item);
    }
    let next_cursor = if rows.len() > limit {
        last.map(|position| PushCursor {
            app_id: app_id.to_owned(),
            kind,
            position,
            issued_at_ms: 0,
        })
    } else {
        None
    };
    Ok(Page { items, next_cursor })
}

fn device_from_row<R>(row: &R) -> PushStorageResult<DeviceDetails>
where
    R: Row,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> Option<String>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i32: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    Ok(DeviceDetails {
        app_id: row.try_get("app_id").map_err(sql_error)?,
        id: row.try_get("device_id").map_err(sql_error)?,
        client_id: row.try_get("client_id").map_err(sql_error)?,
        form_factor: enum_from_label(&row.try_get::<String, _>("form_factor").map_err(sql_error)?)?,
        platform: enum_from_label(&row.try_get::<String, _>("platform").map_err(sql_error)?)?,
        metadata: from_json_str(&row.try_get::<String, _>("metadata").map_err(sql_error)?)?,
        device_secret: crate::domain::SecretString::new(
            row.try_get::<String, _>("device_secret_hash")
                .map_err(sql_error)?,
        )
        .map_err(PushStorageError::Validation)?,
        timezone: row.try_get("timezone").map_err(sql_error)?,
        locale: row.try_get("locale").map_err(sql_error)?,
        last_active_at_ms: row
            .try_get::<i64, _>("last_active_at_ms")
            .map_err(sql_error)? as u64,
        push: crate::domain::DevicePushDetails {
            recipient: from_json_str(
                &row.try_get::<String, _>("recipient_json")
                    .map_err(sql_error)?,
            )?,
            state: enum_from_label(&row.try_get::<String, _>("push_state").map_err(sql_error)?)?,
            failure_count: row.try_get::<i32, _>("failure_count").map_err(sql_error)? as u32,
            error_reason: row.try_get("error_reason").map_err(sql_error)?,
        },
        push_rate_policy: None,
    })
}

fn subscription_from_row<R>(row: &R) -> PushStorageResult<ChannelSubscription>
where
    R: Row,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> Option<String>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> Option<i64>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    Ok(ChannelSubscription {
        app_id: row.try_get("app_id").map_err(sql_error)?,
        channel: row.try_get("channel").map_err(sql_error)?,
        device_id: row.try_get("device_id").map_err(sql_error)?,
        client_id: row.try_get("client_id").map_err(sql_error)?,
        provider: provider_from_label(&row.try_get::<String, _>("provider").map_err(sql_error)?)?,
        token_hash: row.try_get("token_hash").map_err(sql_error)?,
        credential_version: row
            .try_get::<Option<i64>, _>("credential_version")
            .map_err(sql_error)?
            .map(|v| v as u64),
    })
}

fn template_from_row<R>(row: &R) -> PushStorageResult<NotificationTemplate>
where
    R: Row,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    Ok(NotificationTemplate {
        app_id: row.try_get("app_id").map_err(sql_error)?,
        template_id: row.try_get("template_id").map_err(sql_error)?,
        default_locale: row.try_get("default_locale").map_err(sql_error)?,
        locales: from_json_str(
            &row.try_get::<String, _>("locales_json")
                .map_err(sql_error)?,
        )?,
        provider_overrides: from_json_str(
            &row.try_get::<String, _>("provider_overrides_json")
                .map_err(sql_error)?,
        )?,
    })
}

fn scheduled_from_row<R>(row: &R) -> PushStorageResult<ScheduledPushJob>
where
    R: Row,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    Ok(ScheduledPushJob {
        app_id: row.try_get("app_id").map_err(sql_error)?,
        publish_id: row.try_get("publish_id").map_err(sql_error)?,
        due_at_ms: row.try_get::<i64, _>("due_at_ms").map_err(sql_error)? as u64,
        due_minute_ms: row.try_get::<i64, _>("due_minute_ms").map_err(sql_error)? as u64,
        payload_json: from_json_str(
            &row.try_get::<String, _>("payload_json")
                .map_err(sql_error)?,
        )?,
    })
}

fn enum_label<T: Serialize>(value: &T) -> PushStorageResult<String> {
    serde_json::to_value(value)
        .map_err(json_error)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| {
            PushStorageError::Backend("enum label did not serialize to string".to_owned())
        })
}

fn enum_from_label<T: DeserializeOwned>(label: &str) -> PushStorageResult<T> {
    serde_json::from_value(serde_json::Value::String(label.to_owned())).map_err(json_error)
}

fn provider_label(provider: PushProviderKind) -> &'static str {
    match provider {
        PushProviderKind::Fcm => "fcm",
        PushProviderKind::Apns => "apns",
        PushProviderKind::WebPush => "webPush",
        PushProviderKind::Hms => "hms",
        PushProviderKind::Wns => "wns",
    }
}

fn provider_from_label(label: &str) -> PushStorageResult<PushProviderKind> {
    enum_from_label(label)
}

fn to_json_string<T: Serialize>(value: &T) -> PushStorageResult<String> {
    let data = serde_json::to_value(value).map_err(json_error)?;
    serde_json::to_string(&serde_json::json!({ "_v": 1, "data": data })).map_err(json_error)
}

fn to_json_vec<T: Serialize>(value: &T) -> PushStorageResult<Vec<u8>> {
    let data = serde_json::to_value(value).map_err(json_error)?;
    serde_json::to_vec(&serde_json::json!({ "_v": 1, "data": data })).map_err(json_error)
}

fn from_json_str<T: DeserializeOwned>(value: &str) -> PushStorageResult<T> {
    from_versioned_json_value(serde_json::from_str(value).map_err(json_error)?)
}

fn from_json_slice<T: DeserializeOwned>(value: &[u8]) -> PushStorageResult<T> {
    from_versioned_json_value(serde_json::from_slice(value).map_err(json_error)?)
}

fn from_versioned_json_value<T: DeserializeOwned>(
    value: serde_json::Value,
) -> PushStorageResult<T> {
    if let serde_json::Value::Object(mut object) = value {
        if object.get("_v").and_then(serde_json::Value::as_u64) == Some(1)
            && let Some(data) = object.remove("data")
        {
            return serde_json::from_value(data).map_err(json_error);
        }
        return serde_json::from_value(serde_json::Value::Object(object)).map_err(json_error);
    }
    serde_json::from_value(value).map_err(json_error)
}

fn json_error(error: serde_json::Error) -> PushStorageError {
    PushStorageError::Backend(format!("push SQL JSON mapping failed: {error}"))
}

fn sql_error(error: sqlx::Error) -> PushStorageError {
    PushStorageError::Backend(format!("push SQL backend error: {error}"))
}

fn assert_expected_schema_version(version: u32) -> PushStorageResult<()> {
    if version == EXPECTED_PUSH_SCHEMA_VERSION {
        return Ok(());
    }
    Err(PushStorageError::Backend(format!(
        "push SQL schema version mismatch: expected {}, found {version}",
        EXPECTED_PUSH_SCHEMA_VERSION
    )))
}

fn delete_result(rows: u64) -> PushStorageResult<DeleteDeviceOutcome> {
    Ok(if rows > 0 {
        DeleteDeviceOutcome::Deleted
    } else {
        DeleteDeviceOutcome::NotFound
    })
}

fn limit_plus_one(limit: usize) -> i64 {
    limit.saturating_add(1).max(1) as i64
}

fn now_ms_i64() -> i64 {
    crate::pipeline::now_ms().min(i64::MAX as u64) as i64
}

fn json_text_expr(column: &str, template: &str) -> String {
    template.replace('?', column).replace("$1", column)
}

fn sql_query(raw: String, postgres: bool) -> String {
    if !postgres {
        return raw;
    }

    let mut next = 1usize;
    let mut converted = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch == '?' {
            converted.push('$');
            converted.push_str(&next.to_string());
            next += 1;
        } else {
            converted.push(ch);
        }
    }
    converted
}

fn upsert_credential_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, credential_id) DO UPDATE SET provider = EXCLUDED.provider, version = EXCLUDED.version, encrypted_material = EXCLUDED.encrypted_material, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE provider = VALUES(provider), version = VALUES(version), encrypted_material = VALUES(encrypted_material), updated_at_ms = VALUES(updated_at_ms)"
    }
}

fn upsert_template_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, template_id) DO UPDATE SET default_locale = EXCLUDED.default_locale, locales_json = EXCLUDED.locales_json, provider_overrides_json = EXCLUDED.provider_overrides_json, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE default_locale = VALUES(default_locale), locales_json = VALUES(locales_json), provider_overrides_json = VALUES(provider_overrides_json), updated_at_ms = VALUES(updated_at_ms)"
    }
}

fn upsert_status_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, publish_id) DO UPDATE SET state = EXCLUDED.state, counters_json = EXCLUDED.counters_json, error_reason = EXCLUDED.error_reason, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE state = VALUES(state), counters_json = VALUES(counters_json), error_reason = VALUES(error_reason), updated_at_ms = VALUES(updated_at_ms)"
    }
}

fn upsert_publish_log_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, occurred_at_ms, event_id) DO NOTHING"
    } else {
        "ON DUPLICATE KEY UPDATE event_json = event_json"
    }
}

fn upsert_shard_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, publish_id, shard_id) DO UPDATE SET shard_json = EXCLUDED.shard_json, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE shard_json = VALUES(shard_json), updated_at_ms = VALUES(updated_at_ms)"
    }
}

fn upsert_schedule_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, publish_id) DO UPDATE SET due_minute_ms = EXCLUDED.due_minute_ms, due_at_ms = EXCLUDED.due_at_ms, payload_json = EXCLUDED.payload_json, state = EXCLUDED.state, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE due_minute_ms = VALUES(due_minute_ms), due_at_ms = VALUES(due_at_ms), payload_json = VALUES(payload_json), state = VALUES(state), updated_at_ms = VALUES(updated_at_ms)"
    }
}

fn upsert_delivery_event_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, publish_id, occurred_at_ms, event_id) DO UPDATE SET result_json = EXCLUDED.result_json"
    } else {
        "ON DUPLICATE KEY UPDATE result_json = VALUES(result_json)"
    }
}

fn ignore_conflict_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT DO NOTHING"
    } else {
        "ON DUPLICATE KEY UPDATE idempotency_key = idempotency_key"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_query_conversion_numbers_each_placeholder() {
        let sql = sql_query(
            "INSERT INTO t (a, b, c) VALUES (?, ?::jsonb, ?) WHERE ? = ?".to_owned(),
            true,
        );

        assert_eq!(
            sql,
            "INSERT INTO t (a, b, c) VALUES ($1, $2::jsonb, $3) WHERE $4 = $5"
        );
    }

    #[test]
    fn mysql_query_conversion_leaves_placeholders_unchanged() {
        let sql = sql_query("SELECT ? AS a, CAST(? AS JSON) AS b".to_owned(), false);

        assert_eq!(sql, "SELECT ? AS a, CAST(? AS JSON) AS b");
    }
}
