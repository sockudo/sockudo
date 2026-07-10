use crate::domain::{
    DeleteDeviceOutcome, DeliveryEvent, DeviceDetails, PushCursor, PushCursorKind, from_json_value,
    to_json_value,
};
use crate::storage::{
    DeviceRegistrationChange, Page, PushStorageError, PushStorageResult, ScheduledPushJob,
};
use serde::{Serialize, de::DeserializeOwned};
use sonic_rs::prelude::*;

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

pub(super) fn page_from_rows<T: Clone>(
    app_id: &str,
    kind: PushCursorKind,
    mut rows: Vec<(String, T)>,
    limit: usize,
    start_after: Option<String>,
) -> Page<T> {
    let limit = limit.max(1);
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    let filtered = rows
        .into_iter()
        .filter(|(position, _)| start_after.as_ref().is_none_or(|start| position > start))
        .collect::<Vec<_>>();

    let mut items = Vec::with_capacity(limit.min(filtered.len()));
    let mut last_position = None;
    for (position, item) in filtered.iter().take(limit) {
        last_position = Some(position.clone());
        items.push(item.clone());
    }

    let next_cursor = if filtered.len() > limit {
        last_position.map(|position| PushCursor {
            app_id: app_id.to_owned(),
            kind,
            position,
            issued_at_ms: 0,
        })
    } else {
        None
    };

    Page { items, next_cursor }
}

pub(super) fn registration_change(
    existing: Option<&DeviceDetails>,
    incoming: &DeviceDetails,
) -> DeviceRegistrationChange {
    match existing {
        None => DeviceRegistrationChange::Inserted,
        Some(existing) if existing == incoming => DeviceRegistrationChange::Unchanged,
        Some(_) => DeviceRegistrationChange::Updated,
    }
}

pub(super) fn delete_outcome(deleted: bool) -> PushStorageResult<DeleteDeviceOutcome> {
    Ok(if deleted {
        DeleteDeviceOutcome::Deleted
    } else {
        DeleteDeviceOutcome::NotFound
    })
}

pub(super) fn limit_plus_one(limit: usize) -> usize {
    limit.saturating_add(1).max(1)
}

pub(super) fn day_bucket_for_ms(timestamp_ms: u64) -> String {
    (timestamp_ms / 86_400_000).to_string()
}

pub(super) fn family_app(family: &'static str, app_id: &str) -> String {
    format!("{family}#{app_id}")
}

#[cfg(feature = "dynamodb")]
pub(super) fn document_key(pk: &str, sk: &str) -> String {
    format!("{pk}\0{sk}")
}

pub(super) fn scheduled_due_position(job: &ScheduledPushJob) -> String {
    format!("{:020}:{}", job.due_at_ms, job.publish_id)
}

pub(super) fn delivery_event_position(event: &DeliveryEvent) -> String {
    format!("{:020}:{}", event.occurred_at_ms, event.event_id)
}

pub(super) fn status_updated_position(updated_at_ms: u64, publish_id: &str) -> String {
    format!("{updated_at_ms:020}:{publish_id}")
}

pub(super) fn parse_status_updated_position(position: &str) -> Option<(u64, String)> {
    let (timestamp, publish_id) = position.split_once(':')?;
    Some((timestamp.parse().ok()?, publish_id.to_owned()))
}

pub(super) fn to_json_string<T: Serialize>(value: &T) -> PushStorageResult<String> {
    let data = to_json_value(value).map_err(json_error)?;
    sonic_rs::to_string(&sonic_rs::json!({ "_v": 1, "data": data })).map_err(json_error)
}

pub(super) fn from_json_str<T: DeserializeOwned>(value: &str) -> PushStorageResult<T> {
    let mut value: sonic_rs::Value = sonic_rs::from_str(value).map_err(json_error)?;
    if let Some(object) = value.as_object_mut()
        && object.get(&"_v").and_then(sonic_rs::Value::as_u64) == Some(1)
        && let Some(data) = object.remove(&"data")
    {
        return from_json_value(&data).map_err(json_error);
    }
    from_json_value(&value).map_err(json_error)
}

pub(super) fn json_error(error: sonic_rs::Error) -> PushStorageError {
    PushStorageError::Backend(format!("push document JSON mapping failed: {error}"))
}

#[cfg(any(feature = "surrealdb", feature = "scylladb"))]
pub(super) fn validate_identifier(value: &str, label: &'static str) -> PushStorageResult<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(PushStorageError::Backend(format!(
            "{label} must contain only ASCII letters, numbers, and underscores"
        )));
    }
    Ok(())
}

#[cfg(feature = "dynamodb")]
pub(super) fn take_dynamodb_string(
    item: &mut std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    name: &str,
) -> PushStorageResult<String> {
    item.remove(name)
        .ok_or_else(|| PushStorageError::Backend(format!("DynamoDB push item missing {name}")))?
        .as_s()
        .map(String::to_owned)
        .map_err(|_| {
            PushStorageError::Backend(format!("DynamoDB push item {name} is not a string"))
        })
}

#[cfg(feature = "dynamodb")]
pub(super) fn dynamodb_error<E: std::fmt::Display>(error: E) -> PushStorageError {
    PushStorageError::Backend(format!("push DynamoDB backend error: {error}"))
}

#[cfg(feature = "dynamodb")]
pub(super) fn dynamodb_build_error<E: std::fmt::Display>(error: E) -> PushStorageError {
    PushStorageError::Backend(format!("push DynamoDB schema build error: {error}"))
}

#[cfg(feature = "surrealdb")]
pub(super) fn surreal_error<E: std::fmt::Display>(error: E) -> PushStorageError {
    PushStorageError::Backend(format!("push SurrealDB backend error: {error}"))
}

#[cfg(feature = "surrealdb")]
pub(super) fn surreal_conflict_error<E: std::fmt::Display>(error: &E) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("already") || message.contains("duplicate") || message.contains("unique")
}

#[cfg(feature = "scylladb")]
pub(super) fn scylla_error<E: std::fmt::Display>(error: E) -> PushStorageError {
    PushStorageError::Backend(format!("push ScyllaDB backend error: {error}"))
}
