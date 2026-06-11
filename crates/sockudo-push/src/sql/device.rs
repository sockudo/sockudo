use super::helpers::*;
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeviceDetails, PushCursor, PushCursorKind,
};
use crate::storage::{
    DeviceRegistrationChange, DeviceRegistrationOutcome, Page, PushStorageResult,
};
use sqlx::Row;

#[cfg(feature = "postgres")]
pub(super) async fn upsert_device_pg(
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
pub(super) async fn upsert_device_mysql(
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
pub(super) async fn get_device_pg(
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
pub(super) async fn get_device_mysql(
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
pub(super) async fn delete_device_pg(
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
pub(super) async fn delete_device_mysql(
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
pub(super) async fn list_devices_pg(
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
pub(super) async fn list_devices_mysql(
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
pub(super) async fn list_stale_devices_pg(
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
pub(super) async fn list_stale_devices_mysql(
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
pub(super) async fn delete_subscription_pg(
    pool: &sqlx::PgPool,
    app_id: &str,
    channel: &str,
    device_id: &str,
) -> PushStorageResult<DeleteDeviceOutcome> {
    delete_result(sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = $1 AND channel = $2 AND device_id = $3").bind(app_id).bind(channel).bind(device_id).execute(pool).await.map_err(sql_error)?.rows_affected())
}

#[cfg(feature = "mysql")]
pub(super) async fn delete_subscription_mysql(
    pool: &sqlx::MySqlPool,
    app_id: &str,
    channel: &str,
    device_id: &str,
) -> PushStorageResult<DeleteDeviceOutcome> {
    delete_result(sqlx::query("DELETE FROM push_channel_subscribers WHERE app_id = ? AND channel = ? AND device_id = ?").bind(app_id).bind(channel).bind(device_id).execute(pool).await.map_err(sql_error)?.rows_affected())
}

#[cfg(feature = "postgres")]
pub(super) async fn list_channel_subscribers_pg(
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
pub(super) async fn list_channel_subscribers_mysql(
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
pub(super) async fn list_device_channels_pg(
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
pub(super) async fn list_device_channels_mysql(
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
pub(super) async fn list_subscriptions_pg(
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
pub(super) async fn list_subscriptions_mysql(
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
pub(super) async fn list_subscription_channels_pg(
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
pub(super) async fn list_subscription_channels_mysql(
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
