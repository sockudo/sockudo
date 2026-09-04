//! The tool table. Each entry maps validated arguments onto one Sockudo HTTP
//! API route or a small local computation.
//!
//! Naming: `sockudo_<verb>_<noun>`. Scopes: `read` for inspection, `write` for
//! publishing and mutation, `admin` for destructive or connection-affecting
//! operations. Destructive tools additionally require `confirm: true`.

use http::{HeaderName, HeaderValue};
use serde_json::{Map, Value, json};

use super::args::{Args, validate_direction, validate_limit};
use super::{ToolContext, ToolFuture, ToolKind, ToolSpec};
use crate::api::{ApiRequest, Endpoint, signing};
use crate::auth::Scope;
use crate::error::{ToolError, json_result};

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn string(description: &str) -> Value {
    json!({ "type": "string", "description": description })
}

fn integer(description: &str) -> Value {
    json!({ "type": "integer", "description": description })
}

fn boolean(description: &str) -> Value {
    json!({ "type": "boolean", "description": description })
}

fn any(description: &str) -> Value {
    json!({ "description": description })
}

fn object(description: &str) -> Value {
    json!({ "type": "object", "description": description, "additionalProperties": true })
}

fn string_array(description: &str) -> Value {
    json!({ "type": "array", "items": { "type": "string" }, "description": description })
}

fn app_id() -> Value {
    string("Sockudo app id. Discover apps with sockudo_list_apps.")
}

fn channel() -> Value {
    string("Channel name, e.g. `orders`, `private-user-42`, `presence-room-1`.")
}

fn message_serial() -> Value {
    string("Versioned message serial (`message_serial` from history or publish receipts).")
}

fn direction() -> Value {
    json!({
        "type": "string",
        "enum": ["newest_first", "oldest_first"],
        "description": "Page ordering. Default newest_first."
    })
}

fn confirm(operation: &str) -> Value {
    boolean(&format!(
        "Must be true. Acknowledges that `{operation}` is destructive and irreversible."
    ))
}

fn reason() -> Value {
    string("Human-readable justification recorded in the audit log and server response.")
}

fn history_query_properties() -> Map<String, Value> {
    let mut properties = Map::new();
    properties.insert("app_id".into(), app_id());
    properties.insert("channel".into(), channel());
    properties.insert(
        "limit".into(),
        integer("Page size; capped by server policy."),
    );
    properties.insert("direction".into(), direction());
    properties.insert(
        "cursor".into(),
        string("Opaque cursor from a previous page."),
    );
    properties.insert(
        "start_serial".into(),
        integer("Inclusive lower serial bound."),
    );
    properties.insert(
        "end_serial".into(),
        integer("Inclusive upper serial bound."),
    );
    properties.insert(
        "start_time_ms".into(),
        integer("Inclusive lower bound, Unix ms."),
    );
    properties.insert(
        "end_time_ms".into(),
        integer("Inclusive upper bound, Unix ms."),
    );
    properties
}

fn copy_fields(args: &Args<'_>, body: &mut Map<String, Value>, keys: &[&str]) {
    for key in keys {
        if let Some(value) = args.get(key) {
            body.insert((*key).to_string(), value.clone());
        }
    }
}

fn history_query(args: &Args<'_>, request: ApiRequest) -> Result<ApiRequest, ToolError> {
    let limit = validate_limit(args.opt_u64("limit")?, "limit")?;
    let direction = validate_direction(args.opt_str("direction")?)?;
    Ok(request
        .query_opt("limit", limit)
        .query_opt("direction", direction)
        .query_opt("cursor", args.opt_str("cursor")?)
        .query_opt("start_serial", args.opt_u64("start_serial")?)
        .query_opt("end_serial", args.opt_u64("end_serial")?)
        .query_opt("start_time_ms", args.opt_i64("start_time_ms")?)
        .query_opt("end_time_ms", args.opt_i64("end_time_ms")?))
}

fn require_confirm(args: &Args<'_>, operation: &str) -> Result<(), ToolError> {
    if args.flag("confirm")? {
        Ok(())
    } else {
        Err(ToolError::invalid(format!(
            "'{operation}' is destructive; set confirm=true after stating the reason"
        )))
    }
}

fn presence_channel(args: &Args<'_>) -> Result<String, ToolError> {
    let channel = args.channel()?;
    if channel.starts_with("presence-") {
        Ok(channel)
    } else {
        Err(ToolError::invalid(format!(
            "'{channel}' is not a presence channel (presence-* prefix required)"
        )))
    }
}

fn idempotency_header(args: &Args<'_>, request: ApiRequest) -> Result<ApiRequest, ToolError> {
    match args.opt_str("idempotency_key")? {
        Some(key) => {
            let value = HeaderValue::from_str(key)
                .map_err(|_| ToolError::invalid("'idempotency_key' contains invalid characters"))?;
            Ok(request.header(HeaderName::from_static("x-idempotency-key"), value))
        }
        None => Ok(request),
    }
}

// ---------------------------------------------------------------------------
// API-backed builders
// ---------------------------------------------------------------------------

fn list_channels(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::Channels {
        app_id: args.app_id()?,
    })
    .query_opt("filter_by_prefix", args.opt_str("filter_by_prefix")?)
    .query_opt("info", args.opt_str("info")?))
}

fn get_channel(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::Channel {
        app_id: args.app_id()?,
        channel: args.channel()?,
    })
    .query_opt("info", args.opt_str("info")?))
}

fn get_presence_users(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::ChannelUsers {
        app_id: args.app_id()?,
        channel: presence_channel(args)?,
    }))
}

fn event_body(args: &Args<'_>) -> Result<Map<String, Value>, ToolError> {
    let name = args.str("name")?;
    if name.len() > 200 {
        return Err(ToolError::invalid("'name' must be at most 200 bytes"));
    }
    let channel = args.opt_str("channel")?;
    let channels = args.opt_str_array("channels")?;
    match (channel, &channels) {
        (None, None) => return Err(ToolError::invalid("provide 'channel' or 'channels'")),
        (Some(_), Some(_)) => {
            return Err(ToolError::invalid(
                "use either 'channel' or 'channels', not both",
            ));
        }
        (None, Some(list)) if list.is_empty() => {
            return Err(ToolError::invalid("'channels' must not be empty"));
        }
        _ => {}
    }
    let mut body = Map::new();
    body.insert("name".into(), Value::String(name.to_string()));
    if let Some(channel) = channel {
        body.insert("channel".into(), Value::String(channel.to_string()));
    }
    if let Some(channels) = channels {
        body.insert("channels".into(), json!(channels));
    }
    // Pusher publishes `data` as a string that clients JSON-parse. Objects,
    // arrays, numbers, and booleans are therefore serialized to their JSON
    // text; strings pass through untouched. Absent data is published as `null`.
    match args.get("data") {
        None => {}
        Some(Value::String(text)) => {
            body.insert("data".into(), Value::String(text.clone()));
        }
        Some(other) => {
            let text = serde_json::to_string(other)
                .map_err(|error| ToolError::Internal(format!("cannot encode data: {error}")))?;
            body.insert("data".into(), Value::String(text));
        }
    }
    copy_fields(
        args,
        &mut body,
        &[
            "socket_id",
            "info",
            "idempotency_key",
            "message_id",
            "tags",
            "delta",
            "extras",
        ],
    );
    Ok(body)
}

fn trigger_event(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    let app_id = args.app_id()?;
    let body = event_body(args)?;
    ApiRequest::new(Endpoint::Events { app_id })
        .json(&body)
        .map_err(ToolError::from)
}

fn trigger_batch_events(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    let app_id = args.app_id()?;
    let events = args
        .opt_array("events")?
        .ok_or_else(|| ToolError::invalid("'events' is required"))?;
    if events.is_empty() {
        return Err(ToolError::invalid("'events' must not be empty"));
    }
    let mut batch = Vec::with_capacity(events.len());
    for (index, event) in events.iter().enumerate() {
        let object = event
            .as_object()
            .ok_or_else(|| ToolError::invalid(format!("'events[{index}]' must be an object")))?;
        let body = event_body(&Args::new(object))
            .map_err(|error| ToolError::invalid(format!("events[{index}]: {error}")))?;
        batch.push(Value::Object(body));
    }
    let request =
        ApiRequest::new(Endpoint::BatchEvents { app_id }).json(&json!({ "batch": batch }))?;
    idempotency_header(args, request)
}

fn get_history(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    history_query(
        args,
        ApiRequest::new(Endpoint::History {
            app_id: args.app_id()?,
            channel: args.channel()?,
        }),
    )
}

fn get_history_state(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::HistoryState {
        app_id: args.app_id()?,
        channel: args.channel()?,
    }))
}

fn reset_history(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    require_confirm(args, "reset_history")?;
    let channel = args.channel()?;
    let body = json!({
        "confirm_channel": channel,
        "confirm_operation": "reset",
        "reason": args.str("reason")?,
        "requested_by": args.opt_str("requested_by")?,
    });
    Ok(ApiRequest::new(Endpoint::HistoryReset {
        app_id: args.app_id()?,
        channel,
    })
    .json(&body)?)
}

fn purge_history(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    require_confirm(args, "purge_history")?;
    let channel = args.channel()?;
    let mode = args.str("mode")?;
    if !matches!(mode, "all" | "before_serial" | "before_time_ms") {
        return Err(ToolError::invalid(
            "'mode' must be one of all, before_serial, before_time_ms",
        ));
    }
    let before_serial = args.opt_u64("before_serial")?;
    let before_time_ms = args.opt_i64("before_time_ms")?;
    if mode == "before_serial" && before_serial.is_none() {
        return Err(ToolError::invalid(
            "'before_serial' is required for mode before_serial",
        ));
    }
    if mode == "before_time_ms" && before_time_ms.is_none() {
        return Err(ToolError::invalid(
            "'before_time_ms' is required for mode before_time_ms",
        ));
    }
    let body = json!({
        "confirm_channel": channel,
        "confirm_operation": "purge",
        "mode": mode,
        "before_serial": before_serial,
        "before_time_ms": before_time_ms,
        "reason": args.str("reason")?,
        "requested_by": args.opt_str("requested_by")?,
    });
    Ok(ApiRequest::new(Endpoint::HistoryPurge {
        app_id: args.app_id()?,
        channel,
    })
    .json(&body)?)
}

fn get_message(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::Message {
        app_id: args.app_id()?,
        channel: args.channel()?,
        message_serial: args.str("message_serial")?.to_string(),
    }))
}

fn list_message_versions(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    let limit = validate_limit(args.opt_u64("limit")?, "limit")?;
    let direction = validate_direction(args.opt_str("direction")?)?;
    Ok(ApiRequest::new(Endpoint::MessageVersions {
        app_id: args.app_id()?,
        channel: args.channel()?,
        message_serial: args.str("message_serial")?.to_string(),
    })
    .query_opt("limit", limit)
    .query_opt("direction", direction)
    .query_opt("cursor", args.opt_str("cursor")?))
}

fn mutation_body(args: &Args<'_>) -> Map<String, Value> {
    args.passthrough(&["app_id", "channel", "message_serial"])
}

fn update_message(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    let body = mutation_body(args);
    if !["name", "data", "extras", "clear_fields"]
        .iter()
        .any(|key| body.contains_key(*key))
    {
        return Err(ToolError::invalid(
            "provide at least one of name, data, extras, clear_fields",
        ));
    }
    Ok(ApiRequest::new(Endpoint::UpdateMessage {
        app_id: args.app_id()?,
        channel: args.channel()?,
        message_serial: args.str("message_serial")?.to_string(),
    })
    .json(&body)?)
}

fn delete_message(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::DeleteMessage {
        app_id: args.app_id()?,
        channel: args.channel()?,
        message_serial: args.str("message_serial")?.to_string(),
    })
    .json(&mutation_body(args))?)
}

fn append_message(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    if !args.has("data") {
        return Err(ToolError::invalid("'data' is required"));
    }
    Ok(ApiRequest::new(Endpoint::AppendMessage {
        app_id: args.app_id()?,
        channel: args.channel()?,
        message_serial: args.str("message_serial")?.to_string(),
    })
    .json(&mutation_body(args))?)
}

fn list_annotations(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    let limit = validate_limit(args.opt_u64("limit")?, "limit")?;
    Ok(ApiRequest::new(Endpoint::ListAnnotations {
        app_id: args.app_id()?,
        channel: args.channel()?,
        message_serial: args.str("message_serial")?.to_string(),
    })
    .query_opt("type", args.opt_str("annotation_type")?)
    .query_opt("limit", limit)
    .query_opt("from_serial", args.opt_str("from_serial")?)
    .query_opt("socket_id", args.opt_str("socket_id")?))
}

fn publish_annotation(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    if args.opt_u64("count")? == Some(0) {
        return Err(ToolError::invalid("'count' must be greater than 0"));
    }
    // The annotation endpoint uses camelCase body keys.
    let body = json!({
        "type": args.str("annotation_type")?,
        "name": args.opt_str("name")?,
        "clientId": args.opt_str("client_id")?,
        "socketId": args.opt_str("socket_id")?,
        "count": args.opt_u64("count")?,
        "data": args.get("data"),
        "encoding": args.opt_str("encoding")?,
    });
    Ok(ApiRequest::new(Endpoint::PublishAnnotation {
        app_id: args.app_id()?,
        channel: args.channel()?,
        message_serial: args.str("message_serial")?.to_string(),
    })
    .json(&body)?)
}

fn delete_annotation(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::DeleteAnnotation {
        app_id: args.app_id()?,
        channel: args.channel()?,
        message_serial: args.str("message_serial")?.to_string(),
        annotation_serial: args.str("annotation_serial")?.to_string(),
    })
    .query_opt("socket_id", args.opt_str("socket_id")?))
}

fn get_presence_history(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    history_query(
        args,
        ApiRequest::new(Endpoint::PresenceHistory {
            app_id: args.app_id()?,
            channel: presence_channel(args)?,
        }),
    )
}

fn get_presence_history_state(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::PresenceHistoryState {
        app_id: args.app_id()?,
        channel: presence_channel(args)?,
    }))
}

fn get_presence_snapshot(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::PresenceHistorySnapshot {
        app_id: args.app_id()?,
        channel: presence_channel(args)?,
    })
    .query_opt("at_time_ms", args.opt_i64("at_time_ms")?)
    .query_opt("at_serial", args.opt_u64("at_serial")?))
}

fn reset_presence_history(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    require_confirm(args, "reset_presence_history")?;
    let channel = presence_channel(args)?;
    let body = json!({
        "confirm_channel": channel,
        "confirm_operation": "reset",
        "reason": args.str("reason")?,
        "requested_by": args.opt_str("requested_by")?,
    });
    Ok(ApiRequest::new(Endpoint::PresenceHistoryReset {
        app_id: args.app_id()?,
        channel,
    })
    .json(&body)?)
}

fn terminate_user_connections(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    require_confirm(args, "terminate_user_connections")?;
    Ok(ApiRequest::new(Endpoint::TerminateUserConnections {
        app_id: args.app_id()?,
        user_id: args.str("user_id")?.to_string(),
    }))
}

fn force_reconnect_user(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    require_confirm(args, "force_reconnect_user")?;
    Ok(ApiRequest::new(Endpoint::ForceReconnectUser {
        app_id: args.app_id()?,
        user_id: args.str("user_id")?.to_string(),
    }))
}

fn revoke_capability_tokens(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    require_confirm(args, "revoke_capability_tokens")?;
    if !args.has("jti") && !args.has("client_id") {
        return Err(ToolError::invalid("provide 'jti', 'client_id', or both"));
    }
    let body = json!({
        "jti": args.opt_str("jti")?,
        "client_id": args.opt_str("client_id")?,
        "expires_at": args.opt_i64("expires_at")?,
        "ttl_seconds": args.opt_u64("ttl_seconds")?,
        "reason": args.opt_str("reason")?,
    });
    Ok(ApiRequest::new(Endpoint::Revocations {
        app_id: args.app_id()?,
    })
    .json(&body)?)
}

/// Push request bodies use camelCase keys; tool arguments are snake_case.
/// Only top-level keys are converted; nested recipient/payload objects are
/// documented as the HTTP API shape.
fn camel_case_keys(map: Map<String, Value>) -> Map<String, Value> {
    map.into_iter()
        .map(|(key, value)| {
            let mut out = String::with_capacity(key.len());
            let mut upper = false;
            for ch in key.chars() {
                if ch == '_' {
                    upper = true;
                } else if upper {
                    out.extend(ch.to_uppercase());
                    upper = false;
                } else {
                    out.push(ch);
                }
            }
            (out, value)
        })
        .collect()
}

fn push_publish(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    args.object("payload")?;
    let recipients = args
        .opt_array("recipients")?
        .ok_or_else(|| ToolError::invalid("'recipients' is required"))?;
    if recipients.is_empty() {
        return Err(ToolError::invalid("'recipients' must not be empty"));
    }
    Ok(ApiRequest::new(Endpoint::PushPublish {
        app_id: args.app_id()?,
    })
    .json(&camel_case_keys(args.passthrough(&["app_id"])))?)
}

fn push_batch_publish(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    let requests = args
        .opt_array("requests")?
        .ok_or_else(|| ToolError::invalid("'requests' is required"))?;
    if requests.is_empty() {
        return Err(ToolError::invalid("'requests' must not be empty"));
    }
    let mut body = Vec::with_capacity(requests.len());
    for (index, request) in requests.iter().enumerate() {
        let object = request
            .as_object()
            .ok_or_else(|| ToolError::invalid(format!("'requests[{index}]' must be an object")))?;
        if !object.contains_key("payload") || !object.contains_key("recipients") {
            return Err(ToolError::invalid(format!(
                "'requests[{index}]' needs 'payload' and 'recipients'"
            )));
        }
        body.push(Value::Object(camel_case_keys(object.clone())));
    }
    Ok(ApiRequest::new(Endpoint::PushBatchPublish {
        app_id: args.app_id()?,
    })
    .json(&body)?)
}

fn push_publish_status(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::PushPublishStatus {
        app_id: args.app_id()?,
        publish_id: args.str("publish_id")?.to_string(),
    }))
}

fn pagination(args: &Args<'_>, request: ApiRequest) -> Result<ApiRequest, ToolError> {
    let limit = validate_limit(args.opt_u64("limit")?, "limit")?;
    Ok(request
        .query_opt("limit", limit)
        .query_opt("cursor", args.opt_str("cursor")?))
}

fn list_push_devices(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    pagination(
        args,
        ApiRequest::new(Endpoint::PushDevices {
            app_id: args.app_id()?,
        }),
    )
}

fn get_push_device(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::PushDevice {
        app_id: args.app_id()?,
        device_id: args.str("device_id")?.to_string(),
    }))
}

fn list_push_channel_subscriptions(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    let request = ApiRequest::new(Endpoint::PushChannelSubscriptions {
        app_id: args.app_id()?,
    })
    .query_opt("channel", args.opt_str("channel")?)
    .query_opt("deviceId", args.opt_str("device_id")?);
    pagination(args, request)
}

fn list_push_subscription_channels(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    pagination(
        args,
        ApiRequest::new(Endpoint::PushSubscriptionChannels {
            app_id: args.app_id()?,
        }),
    )
}

fn list_push_dead_letters(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    let request = ApiRequest::new(Endpoint::PushDeadLetters {
        app_id: args.app_id()?,
    })
    .query_opt("provider", args.opt_str("provider")?)
    .query_opt("sinceMs", args.opt_u64("since_ms")?)
    .query_opt("untilMs", args.opt_u64("until_ms")?);
    pagination(args, request)
}

fn replay_push_dead_letter(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    require_confirm(args, "replay_push_dead_letter")?;
    Ok(ApiRequest::new(Endpoint::PushReplayDeadLetter {
        app_id: args.app_id()?,
        dead_letter_id: args.str("dead_letter_id")?.to_string(),
    }))
}

fn delete_push_scheduled_job(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    require_confirm(args, "delete_push_scheduled_job")?;
    Ok(ApiRequest::new(Endpoint::PushDeleteScheduledJob {
        app_id: args.app_id()?,
        job_id: args.str("job_id")?.to_string(),
    }))
}

fn list_push_credentials(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    pagination(
        args,
        ApiRequest::new(Endpoint::PushCredentials {
            app_id: args.app_id()?,
        }),
    )
}

fn list_push_templates(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    pagination(
        args,
        ApiRequest::new(Endpoint::PushTemplates {
            app_id: args.app_id()?,
        }),
    )
}

fn get_push_template(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::PushTemplate {
        app_id: args.app_id()?,
        template_id: args.str("template_id")?.to_string(),
    }))
}

fn server_health(args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(match args.opt_str("app_id")? {
        Some(app_id) if !app_id.is_empty() => ApiRequest::new(Endpoint::UpApp {
            app_id: app_id.to_string(),
        }),
        _ => ApiRequest::new(Endpoint::Up),
    })
}

fn server_stats(_args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::OperatorStats))
}

fn server_usage(_args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::Usage))
}

fn server_accept_traffic(_args: &Args<'_>) -> Result<ApiRequest, ToolError> {
    Ok(ApiRequest::new(Endpoint::AcceptTraffic))
}

// ---------------------------------------------------------------------------
// Custom tools
// ---------------------------------------------------------------------------

fn list_apps<'a>(ctx: ToolContext<'a>, _args: Args<'a>) -> ToolFuture<'a> {
    Box::pin(async move {
        let apps = ctx.core.api().credentials().list_apps().await?;
        let visible: Vec<_> = apps
            .into_iter()
            .filter(|app| ctx.principal.apps.allows(&app.id))
            .collect();
        json_result(&json!({ "apps": visible, "count": visible.len() }))
    })
}

fn get_app<'a>(ctx: ToolContext<'a>, args: Args<'a>) -> ToolFuture<'a> {
    Box::pin(async move {
        let app_id = args.app_id()?;
        ctx.principal.require_app(&app_id)?;
        match ctx.core.api().credentials().describe_app(&app_id).await? {
            Some(app) => json_result(&app),
            None => Err(ToolError::Api(crate::api::ApiError::UnknownApp(app_id))),
        }
    })
}

fn server_info<'a>(ctx: ToolContext<'a>, _args: Args<'a>) -> ToolFuture<'a> {
    Box::pin(async move { json_result(&ctx.core.describe(ctx.principal)) })
}

fn server_metrics<'a>(ctx: ToolContext<'a>, args: Args<'a>) -> ToolFuture<'a> {
    Box::pin(async move {
        let filter = args.opt_str("filter")?;
        let max_lines = args.opt_u64("max_lines")?.unwrap_or(400).clamp(1, 5000) as usize;
        let Some(introspection) = ctx.core.introspection() else {
            return Err(ToolError::Internal(
                "metrics are not reachable from this MCP deployment".to_string(),
            ));
        };
        let Some(text) = introspection.metrics_text().await? else {
            return Err(ToolError::Internal(
                "metrics are disabled on this server".to_string(),
            ));
        };
        let mut lines = Vec::new();
        let mut total = 0usize;
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            if let Some(filter) = filter
                && !line.contains(filter)
            {
                continue;
            }
            total += 1;
            if lines.len() < max_lines {
                lines.push(line);
            }
        }
        let truncated = total > lines.len();
        json_result(&json!({
            "format": "prometheus-text-0.0.4",
            "filter": filter,
            "returned_lines": lines.len(),
            "matching_lines": total,
            "truncated": truncated,
            "metrics": lines.join("\n"),
        }))
    })
}

fn sign_channel_auth<'a>(ctx: ToolContext<'a>, args: Args<'a>) -> ToolFuture<'a> {
    Box::pin(async move {
        let app_id = args.app_id()?;
        ctx.principal.require_app(&app_id)?;
        let socket_id = args.str("socket_id")?;
        let channel = args.channel()?;
        if !(channel.starts_with("private-") || channel.starts_with("presence-")) {
            return Err(ToolError::invalid(
                "only private-* and presence-* channels require authorization signatures",
            ));
        }
        let channel_data = match args.get("channel_data") {
            None => None,
            Some(Value::String(raw)) => Some(raw.clone()),
            Some(other) => Some(serde_json::to_string(other).map_err(|error| {
                ToolError::Internal(format!("cannot encode channel_data: {error}"))
            })?),
        };
        if channel.starts_with("presence-") && channel_data.is_none() {
            return Err(ToolError::invalid(
                "presence channels require 'channel_data' with a user_id",
            ));
        }
        let credentials = ctx
            .core
            .api()
            .credentials()
            .resolve(&app_id)
            .await?
            .ok_or_else(|| crate::api::ApiError::UnknownApp(app_id.clone()))?;
        let auth =
            signing::channel_auth(&credentials, socket_id, &channel, channel_data.as_deref());
        json_result(&json!({
            "auth": auth,
            "channel_data": channel_data,
            "channel": channel,
            "socket_id": socket_id,
        }))
    })
}

fn sign_user_auth<'a>(ctx: ToolContext<'a>, args: Args<'a>) -> ToolFuture<'a> {
    Box::pin(async move {
        let app_id = args.app_id()?;
        ctx.principal.require_app(&app_id)?;
        let socket_id = args.str("socket_id")?;
        let user_data = match args.get("user_data") {
            Some(Value::String(raw)) => raw.clone(),
            Some(Value::Object(object)) => {
                if !object.contains_key("id") {
                    return Err(ToolError::invalid("'user_data' must contain an 'id' field"));
                }
                serde_json::to_string(object).map_err(|error| {
                    ToolError::Internal(format!("cannot encode user_data: {error}"))
                })?
            }
            _ => {
                return Err(ToolError::invalid(
                    "'user_data' (object or JSON string) is required",
                ));
            }
        };
        let credentials = ctx
            .core
            .api()
            .credentials()
            .resolve(&app_id)
            .await?
            .ok_or_else(|| crate::api::ApiError::UnknownApp(app_id.clone()))?;
        let auth = signing::user_auth(&credentials, socket_id, &user_data);
        json_result(&json!({ "auth": auth, "user_data": user_data, "socket_id": socket_id }))
    })
}

fn verify_webhook_signature<'a>(ctx: ToolContext<'a>, args: Args<'a>) -> ToolFuture<'a> {
    Box::pin(async move {
        let app_id = args.app_id()?;
        ctx.principal.require_app(&app_id)?;
        let body = match args.get("body") {
            Some(Value::String(raw)) => raw.clone(),
            Some(other) => serde_json::to_string(other)
                .map_err(|error| ToolError::Internal(format!("cannot encode body: {error}")))?,
            None => return Err(ToolError::invalid("'body' is required")),
        };
        let signature = args.str("signature")?;
        let credentials = ctx
            .core
            .api()
            .credentials()
            .resolve(&app_id)
            .await?
            .ok_or_else(|| crate::api::ApiError::UnknownApp(app_id.clone()))?;
        let valid = signing::webhook_signature_valid(&credentials, &body, signature);
        let key_matches = args
            .opt_str("key")?
            .is_none_or(|key| key == credentials.key);
        json_result(&json!({
            "valid": valid && key_matches,
            "signature_valid": valid,
            "key_matches": key_matches,
        }))
    })
}

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

struct Entry {
    name: &'static str,
    title: &'static str,
    description: &'static str,
    scope: Scope,
    hints: (bool, bool, bool),
    schema: Value,
    kind: ToolKind,
}

const READ: (bool, bool, bool) = (true, false, true);
const WRITE: (bool, bool, bool) = (false, false, false);
const WRITE_IDEMPOTENT: (bool, bool, bool) = (false, false, true);
const DESTRUCTIVE: (bool, bool, bool) = (false, true, false);

fn entry(
    name: &'static str,
    title: &'static str,
    description: &'static str,
    scope: Scope,
    hints: (bool, bool, bool),
    schema: Value,
    kind: ToolKind,
) -> Entry {
    Entry {
        name,
        title,
        description,
        scope,
        hints,
        schema,
        kind,
    }
}

/// Every tool the server can expose. Called once per process.
pub fn specs() -> Vec<ToolSpec> {
    let mut table: Vec<Entry> = Vec::with_capacity(48);

    // --- discovery & server -------------------------------------------------
    table.push(entry(
        "sockudo_list_apps", "List apps",
        "List the Sockudo apps this MCP token may access, with public keys and sanitized policy. Never returns secrets. Call this first to discover valid app_id values.",
        Scope::Read, READ, schema(json!({}), &[]), ToolKind::Custom(list_apps),
    ));
    table.push(entry(
        "sockudo_get_app", "Get app",
        "Describe one app: enabled flag, limits, feature policy, channel namespaces, history/presence-history policy. Secrets are never included.",
        Scope::Read, READ, schema(json!({ "app_id": app_id() }), &["app_id"]), ToolKind::Custom(get_app),
    ));
    table.push(entry(
        "sockudo_server_info", "Server info",
        "Describe this Sockudo MCP deployment: server version, transport mode, enabled features, tool count, and the caller's scopes.",
        Scope::Read, READ, schema(json!({}), &[]), ToolKind::Custom(server_info),
    ));
    table.push(entry(
        "sockudo_server_health", "Server health",
        "Run the /up health check (adapter, cache, webhook queue). Pass app_id to also verify that app resolves. Non-200 responses are returned as errors with the server's diagnostics.",
        Scope::Read, READ, schema(json!({ "app_id": string("Optional app id to verify.") }), &[]), ToolKind::Api(server_health),
    ));
    table.push(entry(
        "sockudo_server_accept_traffic", "Accept-traffic status",
        "Check whether this node currently accepts new WebSocket connections (memory-pressure admission control and drain state).",
        Scope::Read, READ, schema(json!({}), &[]), ToolKind::Api(server_accept_traffic),
    ));
    table.push(entry(
        "sockudo_server_stats", "Server stats",
        "Operator statistics: memory, per-app connections and users, channel occupancy, presence members, durable history and presence-history health. Requires http_api.usage_enabled on the server.",
        Scope::Read, READ, schema(json!({}), &[]), ToolKind::Api(server_stats),
    ));
    table.push(entry(
        "sockudo_server_usage",
        "Server memory usage",
        "Process memory usage snapshot from /usage. Requires http_api.usage_enabled.",
        Scope::Read,
        READ,
        schema(json!({}), &[]),
        ToolKind::Api(server_usage),
    ));
    table.push(entry(
        "sockudo_server_metrics", "Prometheus metrics",
        "Read Prometheus metrics text. Use filter to select a metric family (e.g. `broadcast_latency`, `history_`, `push_`) and keep responses small.",
        Scope::Read, READ,
        schema(json!({
            "filter": string("Substring filter applied to each metrics line."),
            "max_lines": integer("Maximum lines to return (default 400, max 5000).")
        }), &[]),
        ToolKind::Custom(server_metrics),
    ));

    // --- channels ------------------------------------------------------------
    table.push(entry(
        "sockudo_list_channels", "List channels",
        "List occupied channels for an app. Use filter_by_prefix to narrow (e.g. `presence-`). info may be `subscription_count`, `user_count` (presence only), or both comma-separated.",
        Scope::Read, READ,
        schema(json!({
            "app_id": app_id(),
            "filter_by_prefix": string("Only channels starting with this prefix."),
            "info": string("Comma-separated: subscription_count, user_count.")
        }), &["app_id"]),
        ToolKind::Api(list_channels),
    ));
    table.push(entry(
        "sockudo_get_channel", "Get channel",
        "Read one channel's state: occupancy, subscription_count, user_count (presence), cache payload (cache channels), AI stream stats when applicable.",
        Scope::Read, READ,
        schema(json!({
            "app_id": app_id(),
            "channel": channel(),
            "info": string("Comma-separated: subscription_count, user_count, cache.")
        }), &["app_id", "channel"]),
        ToolKind::Api(get_channel),
    ));
    table.push(entry(
        "sockudo_get_presence_users",
        "Presence members",
        "List current members (user ids) of a presence channel.",
        Scope::Read,
        READ,
        schema(
            json!({ "app_id": app_id(), "channel": channel() }),
            &["app_id", "channel"],
        ),
        ToolKind::Api(get_presence_users),
    ));

    // --- publish -------------------------------------------------------------
    table.push(entry(
        "sockudo_trigger_event", "Publish event",
        "Publish an event to one channel or up to the app's channels-at-once limit. data may be any JSON value. Set idempotency_key when a retry is possible. socket_id suppresses echo to that connection. info=subscription_count returns per-channel counts and publish receipts (message_serial, history_serial).",
        Scope::Write, WRITE_IDEMPOTENT,
        schema(json!({
            "app_id": app_id(),
            "name": string("Event name (max 200 bytes). Client events must start with `client-`; server events must not."),
            "channel": channel(),
            "channels": string_array("Alternative to channel: publish the same event to several channels."),
            "data": any("Event payload. Strings are sent verbatim; objects, arrays, and numbers are serialized to a JSON string, which Pusher clients parse."),
            "socket_id": string("Connection to exclude from delivery (echo suppression)."),
            "info": string("Optional response info: subscription_count, user_count."),
            "idempotency_key": string("Deduplicate retries within the idempotency TTL."),
            "message_id": string("Client-supplied idempotent create key for versioned/AI messages."),
            "tags": object("Tag map for V2 tag filtering."),
            "delta": boolean("Force (true) or skip (false) delta compression for this publish."),
            "extras": object("Protocol V2 extras (e.g. ai, push, ephemeral).")
        }), &["app_id", "name"]),
        ToolKind::Api(trigger_event),
    ));
    table.push(entry(
        "sockudo_trigger_batch_events", "Publish batch",
        "Publish several events in one request. Each item has the same shape as sockudo_trigger_event (without app_id). Batch size is bounded by app policy.",
        Scope::Write, WRITE_IDEMPOTENT,
        schema(json!({
            "app_id": app_id(),
            "events": { "type": "array", "items": { "type": "object", "additionalProperties": true }, "description": "Events to publish." },
            "idempotency_key": string("Batch-level idempotency key sent as X-Idempotency-Key.")
        }), &["app_id", "events"]),
        ToolKind::Api(trigger_batch_events),
    ));

    // --- history -------------------------------------------------------------
    table.push(entry(
        "sockudo_get_history", "Read history",
        "Page through durable channel history with continuity metadata (stream_id, serials, retention). Versioned messages are returned as their latest winner.",
        Scope::Read, READ, schema(Value::Object(history_query_properties()), &["app_id", "channel"]),
        ToolKind::Api(get_history),
    ));
    table.push(entry(
        "sockudo_get_history_state", "History stream state",
        "Inspect a channel's durable history stream: next serial, retained messages/bytes, degraded or reset_required flags and their reason.",
        Scope::Read, READ, schema(json!({ "app_id": app_id(), "channel": channel() }), &["app_id", "channel"]),
        ToolKind::Api(get_history_state),
    ));
    table.push(entry(
        "sockudo_reset_history", "Reset history stream",
        "DESTRUCTIVE: rotate a channel's history stream_id and drop retained messages. Connected clients lose recovery continuity. Requires admin scope, confirm=true, and a reason.",
        Scope::Admin, DESTRUCTIVE,
        schema(json!({
            "app_id": app_id(), "channel": channel(),
            "reason": reason(),
            "requested_by": string("Operator or agent identity to record."),
            "confirm": confirm("reset_history")
        }), &["app_id", "channel", "reason", "confirm"]),
        ToolKind::Api(reset_history),
    ));
    table.push(entry(
        "sockudo_purge_history", "Purge history",
        "DESTRUCTIVE: delete retained history for a channel without rotating continuity state. mode=all, before_serial (needs before_serial), or before_time_ms (needs before_time_ms). Requires admin scope, confirm=true, and a reason.",
        Scope::Admin, DESTRUCTIVE,
        schema(json!({
            "app_id": app_id(), "channel": channel(),
            "mode": { "type": "string", "enum": ["all", "before_serial", "before_time_ms"], "description": "Purge selector." },
            "before_serial": integer("Purge messages with serial < before_serial."),
            "before_time_ms": integer("Purge messages published before this Unix ms timestamp."),
            "reason": reason(),
            "requested_by": string("Operator or agent identity to record."),
            "confirm": confirm("purge_history")
        }), &["app_id", "channel", "mode", "reason", "confirm"]),
        ToolKind::Api(purge_history),
    ));

    // --- versioned messages -------------------------------------------------
    table.push(entry(
        "sockudo_get_message",
        "Get versioned message",
        "Fetch the latest version of a mutable (versioned) message by message_serial.",
        Scope::Read,
        READ,
        schema(
            json!({ "app_id": app_id(), "channel": channel(), "message_serial": message_serial() }),
            &["app_id", "channel", "message_serial"],
        ),
        ToolKind::Api(get_message),
    ));
    table.push(entry(
        "sockudo_list_message_versions",
        "List message versions",
        "Page through every version (create, update, delete, append) of a versioned message.",
        Scope::Read,
        READ,
        schema(
            json!({
                "app_id": app_id(), "channel": channel(), "message_serial": message_serial(),
                "limit": integer("Page size."), "direction": direction(),
                "cursor": string("version_serial cursor from a previous page.")
            }),
            &["app_id", "channel", "message_serial"],
        ),
        ToolKind::Api(list_message_versions),
    ));
    table.push(entry(
        "sockudo_update_message", "Update message",
        "Publish a new version of a versioned message replacing name/data/extras (or clearing fields). Runs with server privilege unless socket_id names an acting connection, whose identity and capabilities then apply.",
        Scope::Write, WRITE,
        schema(json!({
            "app_id": app_id(), "channel": channel(), "message_serial": message_serial(),
            "name": string("New event name."),
            "data": any("New payload."),
            "extras": object("New extras."),
            "clear_fields": string_array("Fields to clear: name, data, extras."),
            "description": string("Version description."),
            "metadata": object("Version metadata."),
            "op_id": string("Idempotent operation id."),
            "client_id": string("Requested actor client id."),
            "socket_id": string("Acting connection (Protocol V2).")
        }), &["app_id", "channel", "message_serial"]),
        ToolKind::Api(update_message),
    ));
    table.push(entry(
        "sockudo_delete_message", "Delete message",
        "Publish a delete version (tombstone) for a versioned message. Optional data/extras describe the deletion.",
        Scope::Write, WRITE,
        schema(json!({
            "app_id": app_id(), "channel": channel(), "message_serial": message_serial(),
            "data": any("Tombstone payload."),
            "extras": object("Tombstone extras."),
            "clear_fields": string_array("Fields to clear."),
            "description": string("Version description."),
            "metadata": object("Version metadata."),
            "op_id": string("Idempotent operation id."),
            "client_id": string("Requested actor client id."),
            "socket_id": string("Acting connection (Protocol V2).")
        }), &["app_id", "channel", "message_serial"]),
        ToolKind::Api(delete_message),
    ));
    table.push(entry(
        "sockudo_append_message",
        "Append to message",
        "Append a data fragment to a streaming (AI Transport) versioned message.",
        Scope::Write,
        WRITE,
        schema(
            json!({
                "app_id": app_id(), "channel": channel(), "message_serial": message_serial(),
                "data": string("Fragment to append."),
                "extras": object("Fragment extras."),
                "description": string("Version description."),
                "metadata": object("Version metadata."),
                "op_id": string("Idempotent operation id."),
                "client_id": string("Requested actor client id."),
                "socket_id": string("Acting connection (Protocol V2).")
            }),
            &["app_id", "channel", "message_serial", "data"],
        ),
        ToolKind::Api(append_message),
    ));

    // --- annotations --------------------------------------------------------
    table.push(entry(
        "sockudo_list_annotations", "List annotations",
        "List annotation events (reactions, flags, ...) attached to a versioned message, optionally filtered by annotation_type.",
        Scope::Read, READ,
        schema(json!({
            "app_id": app_id(), "channel": channel(), "message_serial": message_serial(),
            "annotation_type": string("Filter by annotation type (e.g. `reaction:distinct.v1`)."),
            "limit": integer("Page size."),
            "from_serial": string("Return events after this annotation serial."),
            "socket_id": string("Subscriber connection to authorize as (Protocol V2).")
        }), &["app_id", "channel", "message_serial"]),
        ToolKind::Api(list_annotations),
    ));
    table.push(entry(
        "sockudo_publish_annotation", "Publish annotation",
        "Attach an annotation to a versioned message. annotation_type selects the summarizer (e.g. `reaction:distinct.v1`, `flag:total.v1`).",
        Scope::Write, WRITE,
        schema(json!({
            "app_id": app_id(), "channel": channel(), "message_serial": message_serial(),
            "annotation_type": string("Annotation type."),
            "name": string("Annotation name (e.g. the emoji or flag)."),
            "client_id": string("Acting client id."),
            "socket_id": string("Acting connection (Protocol V2)."),
            "count": integer("Count for multiple/total summarizers (> 0)."),
            "data": any("Annotation payload."),
            "encoding": string("Payload encoding.")
        }), &["app_id", "channel", "message_serial", "annotation_type"]),
        ToolKind::Api(publish_annotation),
    ));
    table.push(entry(
        "sockudo_delete_annotation", "Delete annotation",
        "Delete an annotation by its serial. Idempotent: deleting twice returns the existing delete event.",
        Scope::Write, WRITE_IDEMPOTENT,
        schema(json!({
            "app_id": app_id(), "channel": channel(), "message_serial": message_serial(),
            "annotation_serial": string("Serial of the annotation.create event to delete."),
            "socket_id": string("Acting connection (Protocol V2).")
        }), &["app_id", "channel", "message_serial", "annotation_serial"]),
        ToolKind::Api(delete_annotation),
    ));

    // --- presence history ---------------------------------------------------
    table.push(entry(
        "sockudo_get_presence_history", "Presence history",
        "Page through presence transitions (member_added, member_updated, member_removed) for a presence channel.",
        Scope::Read, READ, schema(Value::Object(history_query_properties()), &["app_id", "channel"]),
        ToolKind::Api(get_presence_history),
    ));
    table.push(entry(
        "sockudo_get_presence_history_state", "Presence history state",
        "Inspect the presence-history stream for a channel: retained events, continuity, degraded state.",
        Scope::Read, READ, schema(json!({ "app_id": app_id(), "channel": channel() }), &["app_id", "channel"]),
        ToolKind::Api(get_presence_history_state),
    ));
    table.push(entry(
        "sockudo_get_presence_snapshot", "Presence snapshot",
        "Reconstruct presence membership at a point in time (at_time_ms or at_serial) by replaying presence history. Without bounds returns the latest derived state.",
        Scope::Read, READ,
        schema(json!({
            "app_id": app_id(), "channel": channel(),
            "at_time_ms": integer("Membership as of this Unix ms timestamp (inclusive)."),
            "at_serial": integer("Membership as of this presence-history serial (inclusive).")
        }), &["app_id", "channel"]),
        ToolKind::Api(get_presence_snapshot),
    ));
    table.push(entry(
        "sockudo_reset_presence_history", "Reset presence history",
        "DESTRUCTIVE: rotate and clear a presence channel's history stream. Requires admin scope, confirm=true, and a reason.",
        Scope::Admin, DESTRUCTIVE,
        schema(json!({
            "app_id": app_id(), "channel": channel(),
            "reason": reason(),
            "requested_by": string("Operator or agent identity to record."),
            "confirm": confirm("reset_presence_history")
        }), &["app_id", "channel", "reason", "confirm"]),
        ToolKind::Api(reset_presence_history),
    ));

    // --- connections & tokens -----------------------------------------------
    table.push(entry(
        "sockudo_terminate_user_connections", "Terminate user connections",
        "Disconnect every active socket for a user id across the cluster. Requires admin scope and confirm=true.",
        Scope::Admin, DESTRUCTIVE,
        schema(json!({
            "app_id": app_id(),
            "user_id": string("Authenticated user id."),
            "confirm": confirm("terminate_user_connections")
        }), &["app_id", "user_id", "confirm"]),
        ToolKind::Api(terminate_user_connections),
    ));
    table.push(entry(
        "sockudo_force_reconnect_user", "Force user reconnect",
        "Close a user's sockets with code 4200 so clients reconnect (e.g. after permission changes). Requires admin scope and confirm=true.",
        Scope::Admin, DESTRUCTIVE,
        schema(json!({
            "app_id": app_id(),
            "user_id": string("Authenticated user id."),
            "confirm": confirm("force_reconnect_user")
        }), &["app_id", "user_id", "confirm"]),
        ToolKind::Api(force_reconnect_user),
    ));
    table.push(entry(
        "sockudo_revoke_capability_tokens", "Revoke capability tokens",
        "Revoke Protocol V2 capability tokens by jti and/or client_id; matching sockets receive sockudo:token_expired and close. Requires admin scope and confirm=true.",
        Scope::Admin, DESTRUCTIVE,
        schema(json!({
            "app_id": app_id(),
            "jti": string("Token id to revoke."),
            "client_id": string("Revoke every token for this client id."),
            "expires_at": integer("Unix seconds until which the revocation is remembered."),
            "ttl_seconds": integer("Alternative to expires_at."),
            "reason": string("Reason recorded with the revocation."),
            "confirm": confirm("revoke_capability_tokens")
        }), &["app_id", "confirm"]),
        ToolKind::Api(revoke_capability_tokens),
    ));

    // --- push -----------------------------------------------------------------
    table.push(entry(
        "sockudo_push_publish", "Send push notification",
        "Enqueue a push notification. recipients target devices, client ids, or channels; payload holds the notification body (title, body, data, per-provider sections). Returns a publish_id to track with sockudo_push_publish_status.",
        Scope::Write, WRITE_IDEMPOTENT,
        schema(json!({
            "app_id": app_id(),
            "publish_id": string("Client-chosen idempotent publish id."),
            "recipients": { "type": "array", "items": { "type": "object", "additionalProperties": true }, "description": "Recipient selectors (deviceId, clientId, channel, ...)." },
            "payload": object("Notification payload."),
            "provider_overrides": { "type": "array", "items": { "type": "object", "additionalProperties": true }, "description": "Per-provider payload overrides." },
            "sync": boolean("Wait for provider outcomes before returning."),
            "not_before_ms": integer("Schedule delivery no earlier than this Unix ms."),
            "expires_at_ms": integer("Drop if not delivered by this Unix ms.")
        }), &["app_id", "recipients", "payload"]),
        ToolKind::Api(push_publish),
    ));
    table.push(entry(
        "sockudo_push_batch_publish", "Send push batch",
        "Enqueue several push publishes at once. Each request has the sockudo_push_publish shape without app_id.",
        Scope::Write, WRITE_IDEMPOTENT,
        schema(json!({
            "app_id": app_id(),
            "requests": { "type": "array", "items": { "type": "object", "additionalProperties": true }, "description": "Publish requests." }
        }), &["app_id", "requests"]),
        ToolKind::Api(push_batch_publish),
    ));
    table.push(entry(
        "sockudo_push_publish_status", "Push publish status",
        "Read durable delivery status for a push publish id: per-provider outcomes, counts, retries.",
        Scope::Read, READ,
        schema(json!({ "app_id": app_id(), "publish_id": string("Publish id returned by sockudo_push_publish.") }), &["app_id", "publish_id"]),
        ToolKind::Api(push_publish_status),
    ));
    table.push(entry(
        "sockudo_list_push_devices", "List push devices",
        "List registered push devices (tokens are redacted by the server).",
        Scope::Read, READ,
        schema(json!({ "app_id": app_id(), "limit": integer("Page size."), "cursor": string("Pagination cursor.") }), &["app_id"]),
        ToolKind::Api(list_push_devices),
    ));
    table.push(entry(
        "sockudo_get_push_device",
        "Get push device",
        "Read one push device registration.",
        Scope::Read,
        READ,
        schema(
            json!({ "app_id": app_id(), "device_id": string("Device registration id.") }),
            &["app_id", "device_id"],
        ),
        ToolKind::Api(get_push_device),
    ));
    table.push(entry(
        "sockudo_list_push_channel_subscriptions",
        "List push channel subscriptions",
        "List device subscriptions to push channels, optionally filtered by channel or device_id.",
        Scope::Read,
        READ,
        schema(
            json!({
                "app_id": app_id(),
                "channel": string("Filter by channel."),
                "device_id": string("Filter by device id."),
                "limit": integer("Page size."), "cursor": string("Pagination cursor.")
            }),
            &["app_id"],
        ),
        ToolKind::Api(list_push_channel_subscriptions),
    ));
    table.push(entry(
        "sockudo_list_push_subscription_channels", "List push channels",
        "List channels that have at least one push subscription.",
        Scope::Read, READ,
        schema(json!({ "app_id": app_id(), "limit": integer("Page size."), "cursor": string("Pagination cursor.") }), &["app_id"]),
        ToolKind::Api(list_push_subscription_channels),
    ));
    table.push(entry(
        "sockudo_list_push_dead_letters", "List push dead letters",
        "List push deliveries that exhausted retries, optionally filtered by provider and time window.",
        Scope::Read, READ,
        schema(json!({
            "app_id": app_id(),
            "provider": string("Filter by provider (fcm, apns, webpush, hms, wns)."),
            "since_ms": integer("Only dead letters recorded at or after this Unix ms."),
            "until_ms": integer("Only dead letters recorded at or before this Unix ms."),
            "limit": integer("Page size."), "cursor": string("Pagination cursor.")
        }), &["app_id"]),
        ToolKind::Api(list_push_dead_letters),
    ));
    table.push(entry(
        "sockudo_replay_push_dead_letter",
        "Replay push dead letter",
        "Re-enqueue a dead-lettered push delivery. Requires admin scope and confirm=true.",
        Scope::Admin,
        DESTRUCTIVE,
        schema(
            json!({
                "app_id": app_id(),
                "dead_letter_id": string("Dead letter id."),
                "confirm": confirm("replay_push_dead_letter")
            }),
            &["app_id", "dead_letter_id", "confirm"],
        ),
        ToolKind::Api(replay_push_dead_letter),
    ));
    table.push(entry(
        "sockudo_delete_push_scheduled_job", "Cancel scheduled push",
        "Cancel a scheduled (not_before_ms) push job before it runs. Requires admin scope and confirm=true.",
        Scope::Admin, DESTRUCTIVE,
        schema(json!({
            "app_id": app_id(),
            "job_id": string("Scheduled job id."),
            "confirm": confirm("delete_push_scheduled_job")
        }), &["app_id", "job_id", "confirm"]),
        ToolKind::Api(delete_push_scheduled_job),
    ));
    table.push(entry(
        "sockudo_list_push_credentials", "List push credentials",
        "List configured push provider credentials (metadata only; secrets are redacted by the server).",
        Scope::Read, READ,
        schema(json!({ "app_id": app_id(), "limit": integer("Page size."), "cursor": string("Pagination cursor.") }), &["app_id"]),
        ToolKind::Api(list_push_credentials),
    ));
    table.push(entry(
        "sockudo_list_push_templates", "List push templates",
        "List push payload templates.",
        Scope::Read, READ,
        schema(json!({ "app_id": app_id(), "limit": integer("Page size."), "cursor": string("Pagination cursor.") }), &["app_id"]),
        ToolKind::Api(list_push_templates),
    ));
    table.push(entry(
        "sockudo_get_push_template",
        "Get push template",
        "Read one push payload template.",
        Scope::Read,
        READ,
        schema(
            json!({ "app_id": app_id(), "template_id": string("Template id.") }),
            &["app_id", "template_id"],
        ),
        ToolKind::Api(get_push_template),
    ));

    // --- auth helpers ---------------------------------------------------------
    table.push(entry(
        "sockudo_sign_channel_auth", "Sign channel authorization",
        "Compute the Pusher-compatible `auth` string a client needs to subscribe to a private-* or presence-* channel (presence requires channel_data with user_id). Useful for debugging client auth endpoints. Requires write scope because it grants channel access.",
        Scope::Write, WRITE_IDEMPOTENT,
        schema(json!({
            "app_id": app_id(),
            "socket_id": string("Client socket id (e.g. `1234.5678`)."),
            "channel": channel(),
            "channel_data": any("Presence member data: object or JSON string with user_id and optional user_info.")
        }), &["app_id", "socket_id", "channel"]),
        ToolKind::Custom(sign_channel_auth),
    ));
    table.push(entry(
        "sockudo_sign_user_auth", "Sign user authentication",
        "Compute the `auth` string for pusher:signin user authentication from socket_id and user_data (must contain id).",
        Scope::Write, WRITE_IDEMPOTENT,
        schema(json!({
            "app_id": app_id(),
            "socket_id": string("Client socket id."),
            "user_data": any("User data: object or JSON string containing `id`.")
        }), &["app_id", "socket_id", "user_data"]),
        ToolKind::Custom(sign_user_auth),
    ));
    table.push(entry(
        "sockudo_verify_webhook_signature", "Verify webhook signature",
        "Verify an X-Pusher-Signature header against a raw webhook body for an app. Optionally also check X-Pusher-Key matches.",
        Scope::Read, READ,
        schema(json!({
            "app_id": app_id(),
            "body": any("Raw webhook body as received (string). Objects are re-serialized, which may not match byte-for-byte."),
            "signature": string("X-Pusher-Signature header value."),
            "key": string("X-Pusher-Key header value to compare with the app key.")
        }), &["app_id", "body", "signature"]),
        ToolKind::Custom(verify_webhook_signature),
    ));

    table
        .into_iter()
        .map(|entry| ToolSpec {
            name: entry.name,
            title: entry.title,
            description: entry.description,
            scope: entry.scope,
            read_only: entry.hints.0,
            destructive: entry.hints.1,
            idempotent: entry.hints.2,
            schema: entry.schema,
            kind: entry.kind,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn args(value: &Value) -> Args<'_> {
        Args::new(value.as_object().unwrap())
    }

    fn build(name: &str, value: Value) -> Result<ApiRequest, ToolError> {
        let spec = specs().into_iter().find(|spec| spec.name == name).unwrap();
        match spec.kind {
            ToolKind::Api(builder) => builder(&args(&value)),
            ToolKind::Custom(_) => panic!("{name} is custom"),
        }
    }

    #[test]
    fn trigger_event_requires_a_channel_target() {
        let error =
            build("sockudo_trigger_event", json!({"app_id": "a", "name": "e"})).unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments(_)));
        let both = build(
            "sockudo_trigger_event",
            json!({"app_id": "a", "name": "e", "channel": "c", "channels": ["d"]}),
        );
        assert!(both.is_err());
        let ok = build(
            "sockudo_trigger_event",
            json!({"app_id": "a", "name": "e", "channel": "c", "data": {"x": 1}, "socket_id": "1.2"}),
        )
        .unwrap();
        let body: Value = serde_json::from_slice(ok.body.as_ref().unwrap()).unwrap();
        assert_eq!(body["channel"], "c");
        assert_eq!(
            body["data"], "{\"x\":1}",
            "objects are sent as JSON strings"
        );
        assert_eq!(body["socket_id"], "1.2");
        assert_eq!(ok.endpoint.path(), "/apps/a/events");
    }

    #[test]
    fn destructive_tools_require_confirmation_and_reason() {
        let missing = build(
            "sockudo_reset_history",
            json!({"app_id": "a", "channel": "c", "reason": "r"}),
        );
        assert!(missing.is_err());
        let ok = build(
            "sockudo_reset_history",
            json!({"app_id": "a", "channel": "c", "reason": "corrupt stream", "confirm": true}),
        )
        .unwrap();
        let body: Value = serde_json::from_slice(ok.body.as_ref().unwrap()).unwrap();
        assert_eq!(body["confirm_channel"], "c");
        assert_eq!(body["confirm_operation"], "reset");
        let purge = build(
            "sockudo_purge_history",
            json!({"app_id": "a", "channel": "c", "reason": "r", "confirm": true, "mode": "before_serial"}),
        );
        assert!(purge.is_err(), "before_serial requires the bound");
    }

    #[test]
    fn history_query_maps_bounds_to_query_parameters() {
        let request = build(
            "sockudo_get_history",
            json!({"app_id": "a", "channel": "c", "limit": 5, "direction": "oldest_first", "start_serial": 3}),
        )
        .unwrap();
        let query: Vec<(String, String)> = request
            .query
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        assert!(query.contains(&("limit".to_string(), "5".to_string())));
        assert!(query.contains(&("direction".to_string(), "oldest_first".to_string())));
        assert!(query.contains(&("start_serial".to_string(), "3".to_string())));
    }

    #[test]
    fn presence_tools_reject_non_presence_channels() {
        assert!(
            build(
                "sockudo_get_presence_users",
                json!({"app_id": "a", "channel": "room"})
            )
            .is_err()
        );
        assert!(
            build(
                "sockudo_get_presence_users",
                json!({"app_id": "a", "channel": "presence-room"})
            )
            .is_ok()
        );
    }

    #[test]
    fn annotation_body_uses_camel_case_keys() {
        let request = build(
            "sockudo_publish_annotation",
            json!({"app_id": "a", "channel": "c", "message_serial": "m", "annotation_type": "reaction:distinct.v1", "name": "👍", "client_id": "u1"}),
        )
        .unwrap();
        let body: Value = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
        assert_eq!(body["type"], "reaction:distinct.v1");
        assert_eq!(body["clientId"], "u1");
        assert!(body.get("client_id").is_none());
    }

    #[test]
    fn batch_events_validates_each_item_and_sets_header() {
        let request = build(
            "sockudo_trigger_batch_events",
            json!({"app_id": "a", "idempotency_key": "k1", "events": [{"name": "e", "channel": "c", "data": "x"}]}),
        )
        .unwrap();
        assert_eq!(request.headers[0].0.as_str(), "x-idempotency-key");
        let bad = build(
            "sockudo_trigger_batch_events",
            json!({"app_id": "a", "events": [{"name": "e"}]}),
        );
        assert!(bad.is_err());
    }

    #[test]
    fn push_publish_body_uses_camel_case_top_level_keys() {
        let request = build(
            "sockudo_push_publish",
            json!({"app_id": "a", "publish_id": "p1", "recipients": [{"deviceId": "d"}], "payload": {"title": "t"}, "not_before_ms": 5}),
        )
        .unwrap();
        let body: Value = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
        assert_eq!(body["publishId"], "p1");
        assert_eq!(body["notBeforeMs"], 5);
        assert!(body.get("publish_id").is_none());
        assert_eq!(body["recipients"][0]["deviceId"], "d");
    }

    #[test]
    fn server_health_is_unsigned_and_optional_app() {
        let plain = build("sockudo_server_health", json!({})).unwrap();
        assert_eq!(plain.endpoint, Endpoint::Up);
        let scoped = build("sockudo_server_health", json!({"app_id": "a"})).unwrap();
        assert_eq!(scoped.endpoint.path(), "/up/a");
    }
}
