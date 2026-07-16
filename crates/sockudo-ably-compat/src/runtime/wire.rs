//! REST and realtime wire decoding, encoding, message projection, and errors.

use super::*;

pub(super) fn ably_rest_request_format(headers: &HeaderMap) -> AblyFormat {
    if header_contains(headers, header::CONTENT_TYPE, "msgpack") {
        AblyFormat::MsgPack
    } else {
        AblyFormat::Json
    }
}

pub(super) fn ably_rest_response_format(
    headers: &HeaderMap,
    query_format: Option<&str>,
    fallback: AblyFormat,
) -> AblyFormat {
    if let Ok(format) = parse_ably_format(query_format)
        && query_format.is_some()
    {
        format
    } else if header_contains(headers, header::ACCEPT, "msgpack") {
        AblyFormat::MsgPack
    } else if header_contains(headers, header::ACCEPT, "json") {
        AblyFormat::Json
    } else {
        fallback
    }
}

pub(super) fn header_contains(headers: &HeaderMap, name: header::HeaderName, needle: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains(needle))
}

pub(super) fn parse_ably_format(raw: Option<&str>) -> Result<AblyFormat, String> {
    match raw.unwrap_or("json").trim().to_ascii_lowercase().as_str() {
        "" | "json" => Ok(AblyFormat::Json),
        "msgpack" | "messagepack" => Ok(AblyFormat::MsgPack),
        other => Err(format!("Unsupported Ably protocol format '{other}'")),
    }
}

pub(super) fn decode_ably_protocol_message(
    body: &[u8],
    format: AblyFormat,
) -> Result<AblyProtocolMessage, String> {
    decode_protocol_bytes(body, format)
}

pub(super) fn decode_ably_publish_payload(
    body: &[u8],
    format: AblyFormat,
) -> Result<Vec<AblyMessage>, AppError> {
    if body.is_empty() {
        return Err(AppError::InvalidInput(
            "Ably REST publish body is required".to_string(),
        ));
    }

    let payload: AblyPublishPayload = decode_value(body, format).map_err(|error| {
        AppError::InvalidInput(format!(
            "Invalid Ably {} body: {error}",
            match format {
                AblyFormat::Json => "JSON",
                AblyFormat::MsgPack => "MsgPack",
            }
        ))
    })?;
    Ok(payload.into_messages())
}

pub(super) fn encode_ably_rest_response<T: Serialize>(
    status: StatusCode,
    format: AblyFormat,
    value: &T,
) -> Result<Response, AppError> {
    match format {
        AblyFormat::Json => {
            let body = sonic_rs::to_vec(value)
                .map_err(|error| AppError::InternalError(error.to_string()))?;
            Ok((status, [(header::CONTENT_TYPE, "application/json")], body).into_response())
        }
        AblyFormat::MsgPack => {
            let body = rmp_serde::to_vec_named(value)
                .map_err(|error| AppError::InternalError(error.to_string()))?;
            Ok((
                status,
                [(header::CONTENT_TYPE, "application/x-msgpack")],
                body,
            )
                .into_response())
        }
    }
}

pub(super) fn encode_ably_legacy_batch_publish_response(
    requested_format: AblyFormat,
    responses: Vec<AblyBatchPublishChannelResponse>,
) -> Result<Response, AppError> {
    if responses
        .iter()
        .all(|response| matches!(response, AblyBatchPublishChannelResponse::Success { .. }))
    {
        return encode_ably_rest_response(StatusCode::CREATED, requested_format, &responses);
    }

    let error = error_info(
        StatusCode::BAD_REQUEST,
        40020,
        "Batched response includes errors",
    );
    let mut response = encode_ably_rest_response(
        StatusCode::BAD_REQUEST,
        AblyFormat::Json,
        &AblyLegacyBatchResponse {
            error: error.clone(),
            batch_response: responses,
        },
    )?;
    insert_ably_error_headers(&mut response, &error);
    Ok(response)
}

pub(super) fn effective_ably_client_id(
    authenticated_client_id: Option<&str>,
    message: &AblyMessage,
) -> Result<Option<String>, AppError> {
    let effective = match (authenticated_client_id, message.client_id.as_deref()) {
        (Some(authenticated), Some(message_client_id)) if authenticated != message_client_id => {
            return Err(AppError::InvalidInput(
                "message.clientId must match authenticated clientId".to_string(),
            ));
        }
        (Some(authenticated), _) => Some(authenticated.to_string()),
        (None, Some(message_client_id)) => Some(message_client_id.to_string()),
        (None, None) => None,
    };
    let operation_client_id = message
        .version
        .as_ref()
        .and_then(|version| version.client_id.as_deref());
    match (effective, operation_client_id) {
        (Some(effective), Some(operation)) if effective != operation => {
            Err(AppError::InvalidInput(
                "message operation clientId must match authenticated clientId".to_string(),
            ))
        }
        (Some(effective), _) => Ok(Some(effective)),
        (None, Some(operation)) => Ok(Some(operation.to_string())),
        (None, None) => Ok(None),
    }
}

pub(super) fn rest_publish_identity(
    hub: &AblyCompatHub,
    app_id: &str,
    authenticated_client_id: Option<&str>,
    message: &AblyMessage,
) -> Result<(Option<String>, Option<String>), AppError> {
    if message.connection_id.is_some() {
        return Err(AppError::InvalidInput(
            "message.connectionId is server-assigned".to_string(),
        ));
    }
    let Some(connection_key) = message.connection_key.as_deref() else {
        return effective_ably_client_id(authenticated_client_id, message)
            .map(|client_id| (None, client_id));
    };
    let target = hub
        .resolve_live_connection(app_id, connection_key)
        .ok_or_else(|| {
            AppError::InvalidInput(
                "message.connectionKey must identify a live connection in the same app".to_string(),
            )
        })?;

    if let Some(authenticated) = authenticated_client_id
        && authenticated != "*"
        && target.client_id.as_deref() != Some(authenticated)
    {
        return Err(AppError::InvalidInput(
            "authenticated clientId cannot publish on behalf of this connection".to_string(),
        ));
    }
    if let Some(message_client_id) = message.client_id.as_deref()
        && target.client_id.as_deref() != Some(message_client_id)
    {
        return Err(AppError::InvalidInput(
            "message.clientId must match the connectionKey identity".to_string(),
        ));
    }
    if let Some(operation_client_id) = message
        .version
        .as_ref()
        .and_then(|version| version.client_id.as_deref())
        && target.client_id.as_deref() != Some(operation_client_id)
    {
        return Err(AppError::InvalidInput(
            "message operation clientId must match the connectionKey identity".to_string(),
        ));
    }

    Ok((Some(target.connection_id), target.client_id))
}

pub(super) fn pusher_to_ably_message(
    message: &PusherMessage,
    projection: AblyMessageProjection,
) -> Result<AblyMessage, String> {
    let action = extract_runtime_action(message);
    let exposed_action = match (projection, action) {
        (AblyMessageProjection::Aggregate, Some(ProtocolMessageAction::Append)) => {
            Some(ProtocolMessageAction::Update)
        }
        _ => action,
    };
    let serial = extract_runtime_message_serial(message)
        .map(str::to_string)
        .or_else(|| message.serial.map(|serial| serial.to_string()));
    let data = match (projection, action) {
        (AblyMessageProjection::Mutation, Some(ProtocolMessageAction::Append)) => {
            match extract_runtime_append_fragment(message) {
                Some(fragment) => Some(json!(fragment)),
                None => message
                    .data
                    .as_ref()
                    .map(message_data_to_ably_value)
                    .transpose()?,
            }
        }
        _ => message
            .data
            .as_ref()
            .map(message_data_to_ably_value)
            .transpose()?,
    };
    Ok(AblyMessage {
        id: message.message_id.clone(),
        name: message.name.clone().or_else(|| message.event.clone()),
        data,
        #[cfg(feature = "delta")]
        encoded_json: None,
        encoding: None,
        client_id: message.user_id.clone().or_else(|| ai_client_id(message)),
        connection_id: None,
        connection_key: None,
        timestamp: None,
        extras: message
            .extras
            .as_ref()
            .and_then(ably_extras_from_message_extras),
        annotations: None,
        serial,
        action: exposed_action.map(protocol_action_to_ably),
        version: message_version_from_runtime_headers(message),
    })
}

/// Deterministic compatibility projection of commit-time facts.  It does not
/// consult the clock, generate identifiers, or recover operation metadata from
/// runtime headers.
pub(super) fn envelope_to_ably_message(
    envelope: &MessageEnvelope,
    fallback: &PusherMessage,
    projection: AblyMessageProjection,
) -> Result<AblyMessage, String> {
    let action = envelope
        .action
        .map(core_action_to_protocol)
        .or_else(|| extract_runtime_action(fallback));
    let data = match (
        projection,
        envelope.version.as_ref().map(|version| version.projection),
    ) {
        (AblyMessageProjection::Mutation, Some(VersionProjection::AppendFragment)) => {
            extract_runtime_append_fragment(fallback)
                .map(|fragment| json!(fragment))
                .or_else(|| {
                    envelope
                        .data
                        .as_ref()
                        .map(message_content_to_ably_value)
                        .transpose()
                        .ok()
                        .flatten()
                })
        }
        _ => envelope_data_to_ably_value(envelope, projection)?,
    };
    let exposed_action = match (projection, action) {
        (AblyMessageProjection::Aggregate, Some(ProtocolMessageAction::Append)) => {
            Some(ProtocolMessageAction::Update)
        }
        _ => action,
    };
    #[cfg(feature = "delta")]
    let encoded_json = match (envelope.data.as_ref(), envelope.encoding.as_ref()) {
        (Some(MessageContent::Text(raw)), Some(encoding))
            if encoding.as_str().eq_ignore_ascii_case("json") =>
        {
            Some(Arc::<[u8]>::from(raw.as_bytes()))
        }
        _ => None,
    };
    Ok(AblyMessage {
        id: envelope.message_id.clone(),
        name: envelope.name.clone(),
        data,
        #[cfg(feature = "delta")]
        encoded_json,
        encoding: envelope
            .encoding
            .as_ref()
            .and_then(|encoding| ably_projected_encoding(encoding.as_str(), projection)),
        client_id: envelope.publisher_client_id.clone(),
        connection_id: envelope.publisher_connection_id.clone(),
        connection_key: None,
        timestamp: envelope.published_at_ms,
        extras: envelope
            .extras
            .as_ref()
            .and_then(ably_extras_from_message_extras),
        annotations: None,
        serial: envelope
            .message_serial
            .as_ref()
            .map(|serial| serial.as_str().to_string())
            .or_else(|| fallback.serial.map(|serial| serial.to_string())),
        action: exposed_action.map(protocol_action_to_ably),
        version: envelope.version.as_ref().map(|version| AblyMessageVersion {
            serial: version.serial.as_str().to_string(),
            timestamp: Some(version.timestamp_ms),
            client_id: version.client_id.clone(),
            description: version.description.clone(),
            metadata: version.metadata.clone(),
        }),
    })
}

pub(super) fn message_content_to_ably_value(content: &MessageContent) -> Result<Value, String> {
    match content {
        MessageContent::Text(value) => Ok(json!(value)),
        MessageContent::Structured(value) => Ok(value.clone()),
        MessageContent::Binary(value) => {
            sonic_rs::to_value(value).map_err(|error| error.to_string())
        }
    }
}

pub(super) fn envelope_data_to_ably_value(
    envelope: &MessageEnvelope,
    projection: AblyMessageProjection,
) -> Result<Option<Value>, String> {
    let Some(content) = envelope.data.as_ref() else {
        return Ok(None);
    };
    if let MessageContent::Text(raw) = content
        && let Some(encoding) = envelope.encoding.as_ref()
        && projection == AblyMessageProjection::Mutation
    {
        if encoding.as_str().eq_ignore_ascii_case("json")
            && let Ok(value) = sonic_rs::from_str::<Value>(raw)
        {
            return Ok(Some(value));
        }
        if encoding.as_str().eq_ignore_ascii_case("utf-8/base64")
            && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(raw)
            && let Ok(text) = String::from_utf8(bytes)
        {
            return Ok(Some(json!(text)));
        }
    }
    message_content_to_ably_value(content).map(Some)
}

pub(super) fn ably_projected_encoding(
    encoding: &str,
    projection: AblyMessageProjection,
) -> Option<String> {
    if projection == AblyMessageProjection::Aggregate {
        return Some(encoding.to_string());
    }
    match encoding {
        "json" | "utf-8/base64" => None,
        other => Some(other.to_string()),
    }
}

pub(super) fn core_action_to_protocol(
    action: sockudo_core::versioned_messages::MessageAction,
) -> ProtocolMessageAction {
    match action {
        sockudo_core::versioned_messages::MessageAction::Create => ProtocolMessageAction::Create,
        sockudo_core::versioned_messages::MessageAction::Update => ProtocolMessageAction::Update,
        sockudo_core::versioned_messages::MessageAction::Delete => ProtocolMessageAction::Delete,
        sockudo_core::versioned_messages::MessageAction::Append => ProtocolMessageAction::Append,
        sockudo_core::versioned_messages::MessageAction::Summary => ProtocolMessageAction::Summary,
    }
}

pub(super) fn protocol_action_to_ably(action: ProtocolMessageAction) -> u8 {
    match action {
        ProtocolMessageAction::Create => MESSAGE_CREATE,
        ProtocolMessageAction::Update => MESSAGE_UPDATE,
        ProtocolMessageAction::Delete => MESSAGE_DELETE,
        ProtocolMessageAction::Append => MESSAGE_APPEND,
        ProtocolMessageAction::Summary => MESSAGE_SUMMARY,
    }
}

pub(super) fn message_version_from_runtime_headers(
    message: &PusherMessage,
) -> Option<AblyMessageVersion> {
    let headers = message
        .extras
        .as_ref()
        .and_then(|extras| extras.headers.as_ref())?;
    let serial = match headers.get(HEADER_VERSION_SERIAL)? {
        sockudo_protocol::messages::ExtrasValue::String(value) => value.clone(),
        _ => return None,
    };
    let timestamp = match headers.get(HEADER_VERSION_TIMESTAMP_MS) {
        Some(sockudo_protocol::messages::ExtrasValue::Number(value)) => Some(*value as i64),
        _ => None,
    };
    Some(AblyMessageVersion {
        serial,
        timestamp,
        client_id: None,
        description: None,
        metadata: None,
    })
}

pub(super) fn ably_extras_from_message_extras(extras: &MessageExtras) -> Option<Value> {
    let mut visible = extras.clone();
    visible.ephemeral = None;
    visible.idempotency_key = None;
    visible.echo = None;
    if visible.headers.is_none()
        && visible.push.is_none()
        && visible.ai.is_none()
        && visible.opaque.is_empty()
    {
        return None;
    }
    sonic_rs::to_value(&visible).ok()
}

pub(super) fn ably_extras_to_message_extras(
    extras: Option<Value>,
) -> Result<Option<MessageExtras>, AppError> {
    let Some(extras) = extras else {
        return Ok(None);
    };
    let object = extras.as_object().ok_or_else(|| {
        AppError::InvalidInput("message.extras must be a JSON object".to_string())
    })?;
    for (key, _) in object {
        if !matches!(key, "ai" | "echo" | "headers" | "push" | "ref") {
            return Err(AppError::InvalidInput(format!(
                "Unsupported Ably message.extras field '{key}'"
            )));
        }
    }
    let push = object.get(&"push").cloned();
    let mut typed_extras = extras.clone();
    if let Some(object) = typed_extras.as_object_mut() {
        object.remove(&"push");
    }
    let mut decoded: MessageExtras = sonic_rs::from_value(&typed_extras)
        .map_err(|error| AppError::InvalidInput(format!("Invalid extras: {error}")))?;
    decoded.push = push;
    decoded.validate_opaque().map_err(AppError::InvalidInput)?;
    Ok(Some(decoded))
}

pub(super) fn validate_ably_publish_message(
    message: &AblyMessage,
    allow_connection_key: bool,
) -> Result<(), AppError> {
    if message.connection_id.is_some() {
        return Err(AppError::InvalidInput(
            "message.connectionId is server-assigned".to_string(),
        ));
    }
    if message.connection_key.is_some() && !allow_connection_key {
        return Err(AppError::InvalidInput(
            "message.connectionKey is only valid for REST publish".to_string(),
        ));
    }
    if let Some(id) = message.id.as_deref()
        && id.is_empty()
    {
        return Err(AppError::InvalidInput(
            "message.id must not be empty".to_string(),
        ));
    }
    if message
        .encoding
        .as_deref()
        .is_some_and(|encoding| encoding.len() > 256)
    {
        return Err(AppError::InvalidInput(
            "message.encoding exceeds 256 bytes".to_string(),
        ));
    }
    if message.action.unwrap_or(MESSAGE_CREATE) == MESSAGE_CREATE
        && (message.timestamp.is_some() || message.serial.is_some() || message.version.is_some())
    {
        return Err(AppError::InvalidInput(
            "message timestamp, serial, and version are server-assigned".to_string(),
        ));
    }
    if let Some(extras) = ably_extras_to_message_extras(message.extras.clone())?
        && let Err(error) = extras.validate_ai_headers()
    {
        return Err(AppError::InvalidInput(error.message));
    }
    let encoded = sonic_rs::to_vec(message)
        .map_err(|error| AppError::InvalidInput(format!("Invalid message: {error}")))?;
    if encoded.len() > usize::try_from(DEFAULT_MAX_MESSAGE_SIZE).unwrap_or(usize::MAX) {
        return Err(AppError::InvalidInput(format!(
            "message exceeds {DEFAULT_MAX_MESSAGE_SIZE} bytes"
        )));
    }
    Ok(())
}

pub(super) fn message_data_to_ably_value(data: &MessageData) -> Result<Value, String> {
    sonic_rs::to_value(data).map_err(|error| error.to_string())
}

pub(super) fn ably_value_to_message_data(value: Value) -> MessageData {
    value
        .as_str()
        .map(|value| MessageData::String(value.to_string()))
        .unwrap_or(MessageData::Json(value))
}

pub(super) fn ably_message_data_to_message_data(
    value: Value,
    encoding: Option<&str>,
) -> MessageData {
    if encoding.is_some_and(|encoding| encoding.eq_ignore_ascii_case("json"))
        && let Some(raw) = value.as_str()
        && let Ok(decoded) = sonic_rs::from_str::<Value>(raw)
    {
        return MessageData::Json(decoded);
    }
    ably_value_to_message_data(value)
}

pub(super) fn stamp_ai_identity(
    extras: &mut Option<MessageExtras>,
    event_name: &str,
    client_id: &str,
) -> Result<(), AppError> {
    let key = match event_name {
        AI_EVENT_INPUT => AI_HEADER_INPUT_CLIENT_ID,
        AI_EVENT_CANCEL => AI_HEADER_RUN_CLIENT_ID,
        _ => return Ok(()),
    };
    let extras = extras.get_or_insert_with(Default::default);
    let ai = extras.ai.get_or_insert_with(Default::default);
    let transport = ai.transport.get_or_insert_with(Default::default);
    match transport.get(key) {
        Some(existing) if existing != client_id => Err(AppError::InvalidInput(format!(
            "extras.ai.transport.{key} must match authenticated clientId"
        ))),
        Some(_) => Ok(()),
        None => {
            transport.insert(key.to_string(), client_id.to_string());
            Ok(())
        }
    }
}

pub(super) fn ai_client_id(message: &PusherMessage) -> Option<String> {
    let transport = message
        .extras
        .as_ref()
        .and_then(MessageExtras::ai_transport_headers)?;
    transport
        .input_client_id()
        .or_else(|| transport.run_client_identity())
        .or_else(|| transport.step_client_id())
        .map(str::to_string)
}

pub(super) fn send_protocol_error(sender: &AblySender, code: u32, message: impl Into<String>) {
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ERROR,
            error: Some(error_info(StatusCode::BAD_REQUEST, code, message)),
            ..empty_protocol_message(ACTION_ERROR)
        },
    );
}

pub(super) fn send_protocol_disconnected(
    sender: &AblySender,
    code: u32,
    message: impl Into<String>,
) {
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_DISCONNECTED,
            error: Some(error_info(StatusCode::UNAUTHORIZED, code, message)),
            ..empty_protocol_message(ACTION_DISCONNECTED)
        },
    );
}

pub(super) fn send_channel_error(
    sender: &AblySender,
    channel: &str,
    status: StatusCode,
    code: u32,
    message: impl Into<String>,
) {
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ERROR,
            channel: Some(channel.to_string()),
            error: Some(error_info(status, code, message)),
            ..empty_protocol_message(ACTION_ERROR)
        },
    );
}

pub(super) fn send_publish_nack(
    sender: &AblySender,
    inbound: &AblyProtocolMessage,
    code: u32,
    message: impl Into<String>,
) {
    let status = if (40100..40200).contains(&code) {
        StatusCode::UNAUTHORIZED
    } else {
        StatusCode::BAD_REQUEST
    };
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_NACK,
            msg_serial: inbound.msg_serial,
            count: inbound.count.or(Some(1)),
            error: Some(error_info(status, code, message)),
            ..empty_protocol_message(ACTION_NACK)
        },
    );
}

pub(super) fn error_info(
    status: StatusCode,
    code: u32,
    message: impl Into<String>,
) -> AblyErrorInfo {
    AblyErrorInfo {
        message: message.into(),
        code,
        status_code: status.as_u16(),
    }
}

pub(super) fn ably_error_info_from_app_error(error: AppError) -> AblyErrorInfo {
    let (status, code, message) = match error {
        AppError::AppNotFound(message) | AppError::NotFound(message) => {
            (StatusCode::NOT_FOUND, 40400, message)
        }
        AppError::InvalidInput(message) | AppError::FeatureDisabled(message) => {
            (StatusCode::BAD_REQUEST, 40000, message)
        }
        AppError::Protocol {
            status,
            code,
            message,
        }
        | AppError::AiTransport {
            status,
            code,
            message,
            ..
        } => (status, code, message),
        AppError::ApiAuthFailed(message) => (StatusCode::UNAUTHORIZED, 40140, message),
        AppError::Forbidden(message) => (StatusCode::FORBIDDEN, 40160, message),
        AppError::InternalError(message) => (StatusCode::INTERNAL_SERVER_ERROR, 50000, message),
        AppError::SerializationError(error) => {
            (StatusCode::INTERNAL_SERVER_ERROR, 50000, error.to_string())
        }
    };
    error_info(status, code, message)
}

pub(super) fn insert_ably_error_headers(response: &mut Response, error: &AblyErrorInfo) {
    if let Ok(value) = HeaderValue::from_str(&error.code.to_string()) {
        response.headers_mut().insert("x-ably-errorcode", value);
    }
    if let Ok(value) = HeaderValue::from_str(&error.message) {
        response.headers_mut().insert("x-ably-errormessage", value);
    }
}

pub(super) fn ably_error_response(
    status: StatusCode,
    code: u32,
    message: impl Into<String>,
) -> Response {
    ably_error_response_format(status, code, message, AblyFormat::Json)
}

pub(super) fn ably_error_response_format(
    status: StatusCode,
    code: u32,
    message: impl Into<String>,
    _requested_format: AblyFormat,
) -> Response {
    let error = error_info(status, code, message);
    let encoded = encode_ably_rest_response(
        status,
        AblyFormat::Json,
        &AblyErrorBody {
            error: error.clone(),
        },
    );
    match encoded {
        Ok(mut response) => {
            insert_ably_error_headers(&mut response, &error);
            response
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub(super) fn ably_app_error_response(error: AppError) -> Response {
    ably_app_error_response_format(error, AblyFormat::Json)
}

pub(super) fn ably_app_error_response_format(error: AppError, format: AblyFormat) -> Response {
    let error = ably_error_info_from_app_error(error);
    ably_error_response_format(
        StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        error.code,
        error.message,
        format,
    )
}
