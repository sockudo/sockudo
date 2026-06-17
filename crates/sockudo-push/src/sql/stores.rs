#![allow(unused_imports)]
use super::device::*;
use super::helpers::*;
use crate::domain::{ChannelSubscription, DeleteDeviceOutcome, DeviceDetails, PushCursor};
use crate::storage::{
    DeviceRegistrationOutcome, Page, PushDeviceStore, PushStorageResult, PushSubscriptionStore,
};
use async_trait::async_trait;

#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PostgresPushStore {
    pub(super) pool: sqlx::PgPool,
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
    pub(super) pool: sqlx::MySqlPool,
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
