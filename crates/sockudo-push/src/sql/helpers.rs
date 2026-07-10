#![allow(unused_imports)]
use crate::domain::{
    ChannelSubscription, DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, NotificationTemplate,
    PushCursor, PushCursorKind, PushProviderKind, from_json_value, to_json_value,
};
use crate::storage::{
    EXPECTED_PUSH_SCHEMA_VERSION, Page, PushStorageError, PushStorageResult, ScheduledPushJob,
};
use serde::{Serialize, de::DeserializeOwned};
use sonic_rs::prelude::*;
use sqlx::Row;

pub(super) fn cursor_position(
    cursor: Option<PushCursor>,
    app_id: &str,
) -> PushStorageResult<Option<String>> {
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

pub(super) fn day_bucket_range(day_bucket: &str) -> PushStorageResult<(i64, i64)> {
    let day = day_bucket
        .parse::<i64>()
        .map_err(|_| PushStorageError::Backend("invalid push day bucket".to_owned()))?;
    let start = day.saturating_mul(86_400_000);
    Ok((start, start.saturating_add(86_400_000)))
}

pub(super) fn parse_ts_id_cursor(cursor: &str) -> (i64, String) {
    let Some((timestamp, id)) = cursor.split_once(':') else {
        return (-1, String::new());
    };
    (timestamp.parse::<i64>().unwrap_or(-1), id.to_owned())
}

pub(super) fn parse_channel_device_cursor(cursor: &str) -> (String, String) {
    let Some((channel, device_id)) = cursor.split_once(':') else {
        return (String::new(), String::new());
    };
    (channel.to_owned(), device_id.to_owned())
}

pub(super) fn page_from_sql_rows<R, T, F>(
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

pub(super) fn device_from_row<R>(row: &R) -> PushStorageResult<DeviceDetails>
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

pub(super) fn subscription_from_row<R>(row: &R) -> PushStorageResult<ChannelSubscription>
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

pub(super) fn template_from_row<R>(row: &R) -> PushStorageResult<NotificationTemplate>
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

pub(super) fn scheduled_from_row<R>(row: &R) -> PushStorageResult<ScheduledPushJob>
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

pub(super) fn enum_label<T: Serialize>(value: &T) -> PushStorageResult<String> {
    sonic_rs::to_value(value)
        .map_err(json_error)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| {
            PushStorageError::Backend("enum label did not serialize to string".to_owned())
        })
}

pub(super) fn enum_from_label<T: DeserializeOwned>(label: &str) -> PushStorageResult<T> {
    let value = sonic_rs::Value::from(label);
    sonic_rs::from_value(&value).map_err(json_error)
}

pub(super) fn provider_label(provider: PushProviderKind) -> &'static str {
    match provider {
        PushProviderKind::Fcm => "fcm",
        PushProviderKind::Apns => "apns",
        PushProviderKind::WebPush => "webPush",
        PushProviderKind::Hms => "hms",
        PushProviderKind::Wns => "wns",
    }
}

pub(super) fn provider_from_label(label: &str) -> PushStorageResult<PushProviderKind> {
    enum_from_label(label)
}

pub(super) fn to_json_string<T: Serialize>(value: &T) -> PushStorageResult<String> {
    let data = to_json_value(value).map_err(json_error)?;
    sonic_rs::to_string(&sonic_rs::json!({ "_v": 1, "data": data })).map_err(json_error)
}

pub(super) fn to_json_vec<T: Serialize>(value: &T) -> PushStorageResult<Vec<u8>> {
    let data = to_json_value(value).map_err(json_error)?;
    sonic_rs::to_vec(&sonic_rs::json!({ "_v": 1, "data": data })).map_err(json_error)
}

pub(super) fn from_json_str<T: DeserializeOwned>(value: &str) -> PushStorageResult<T> {
    let value: sonic_rs::Value = sonic_rs::from_str(value).map_err(json_error)?;
    from_versioned_json_value(value)
}

pub(super) fn from_json_slice<T: DeserializeOwned>(value: &[u8]) -> PushStorageResult<T> {
    let value: sonic_rs::Value = sonic_rs::from_slice(value).map_err(json_error)?;
    from_versioned_json_value(value)
}

pub(super) fn from_versioned_json_value<T: DeserializeOwned>(
    mut value: sonic_rs::Value,
) -> PushStorageResult<T> {
    if let Some(object) = value.as_object_mut()
        && object.get(&"_v").and_then(sonic_rs::Value::as_u64) == Some(1)
        && let Some(data) = object.remove(&"data")
    {
        return from_json_value(&data).map_err(json_error);
    }
    from_json_value(&value).map_err(json_error)
}

pub(super) fn json_error(error: sonic_rs::Error) -> PushStorageError {
    PushStorageError::Backend(format!("push SQL JSON mapping failed: {error}"))
}

pub(super) fn sql_error(error: sqlx::Error) -> PushStorageError {
    PushStorageError::Backend(format!("push SQL backend error: {error}"))
}

pub(super) fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.is_unique_violation())
}

pub(super) fn positive_sql_u64(value: i64, field: &str) -> PushStorageResult<u64> {
    if value <= 0 {
        return Err(PushStorageError::Backend(format!(
            "push SQL {field} must be positive"
        )));
    }
    Ok(value as u64)
}

pub(super) fn nonnegative_sql_u64(value: i64, field: &str) -> PushStorageResult<u64> {
    u64::try_from(value)
        .map_err(|_| PushStorageError::Backend(format!("push SQL {field} must be nonnegative")))
}

pub(super) fn sql_i64_revision(revision: u64) -> PushStorageResult<i64> {
    if revision == 0 {
        return Err(PushStorageError::Backend(
            "publish status revision must be positive".to_owned(),
        ));
    }
    i64::try_from(revision).map_err(|_| {
        PushStorageError::Backend("publish status revision exhausted storage range".to_owned())
    })
}

pub(super) fn sql_i64_timestamp(timestamp_ms: u64, field: &'static str) -> PushStorageResult<i64> {
    i64::try_from(timestamp_ms)
        .map_err(|_| PushStorageError::Backend(format!("{field} exceeds SQL storage range")))
}

pub(super) fn next_status_updated_at_ms(previous: u64) -> i64 {
    let next = crate::pipeline::now_ms()
        .max(previous.saturating_add(1))
        .min(i64::MAX as u64);
    next as i64
}

pub(super) fn assert_expected_schema_version(version: u32) -> PushStorageResult<()> {
    if version == EXPECTED_PUSH_SCHEMA_VERSION {
        return Ok(());
    }
    Err(PushStorageError::Backend(format!(
        "push SQL schema version mismatch: expected {}, found {version}",
        EXPECTED_PUSH_SCHEMA_VERSION
    )))
}

pub(super) fn delete_result(rows: u64) -> PushStorageResult<DeleteDeviceOutcome> {
    Ok(if rows > 0 {
        DeleteDeviceOutcome::Deleted
    } else {
        DeleteDeviceOutcome::NotFound
    })
}

pub(super) fn limit_plus_one(limit: usize) -> i64 {
    limit.saturating_add(1).max(1) as i64
}

pub(super) fn now_ms_i64() -> i64 {
    crate::pipeline::now_ms().min(i64::MAX as u64) as i64
}

pub(super) fn json_text_expr(column: &str, template: &str) -> String {
    template.replace('?', column).replace("$1", column)
}

pub(super) fn signed_i64_expr(column: &str, postgres: bool) -> String {
    if postgres {
        column.to_owned()
    } else {
        format!("CAST({column} AS SIGNED)")
    }
}

pub(super) fn sql_query(raw: String, postgres: bool) -> String {
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

pub(super) fn upsert_credential_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, credential_id) DO UPDATE SET provider = EXCLUDED.provider, version = EXCLUDED.version, encrypted_material = EXCLUDED.encrypted_material, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE provider = VALUES(provider), version = VALUES(version), encrypted_material = VALUES(encrypted_material), updated_at_ms = VALUES(updated_at_ms)"
    }
}

pub(super) fn upsert_template_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, template_id) DO UPDATE SET default_locale = EXCLUDED.default_locale, locales_json = EXCLUDED.locales_json, provider_overrides_json = EXCLUDED.provider_overrides_json, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE default_locale = VALUES(default_locale), locales_json = VALUES(locales_json), provider_overrides_json = VALUES(provider_overrides_json), updated_at_ms = VALUES(updated_at_ms)"
    }
}

pub(super) fn upsert_publish_log_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, occurred_at_ms, event_id) DO NOTHING"
    } else {
        "ON DUPLICATE KEY UPDATE event_json = event_json"
    }
}

pub(super) fn upsert_shard_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, publish_id, shard_id) DO UPDATE SET shard_json = EXCLUDED.shard_json, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE shard_json = VALUES(shard_json), updated_at_ms = VALUES(updated_at_ms)"
    }
}

pub(super) fn upsert_schedule_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, publish_id) DO UPDATE SET due_minute_ms = EXCLUDED.due_minute_ms, due_at_ms = EXCLUDED.due_at_ms, payload_json = EXCLUDED.payload_json, state = EXCLUDED.state, updated_at_ms = EXCLUDED.updated_at_ms"
    } else {
        "ON DUPLICATE KEY UPDATE due_minute_ms = VALUES(due_minute_ms), due_at_ms = VALUES(due_at_ms), payload_json = VALUES(payload_json), state = VALUES(state), updated_at_ms = VALUES(updated_at_ms)"
    }
}

pub(super) fn upsert_delivery_event_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT (app_id, publish_id, occurred_at_ms, event_id) DO UPDATE SET result_json = EXCLUDED.result_json"
    } else {
        "ON DUPLICATE KEY UPDATE result_json = VALUES(result_json)"
    }
}

pub(super) fn ignore_conflict_clause(postgres: bool) -> &'static str {
    if postgres {
        "ON CONFLICT DO NOTHING"
    } else {
        "ON DUPLICATE KEY UPDATE idempotency_key = idempotency_key"
    }
}
