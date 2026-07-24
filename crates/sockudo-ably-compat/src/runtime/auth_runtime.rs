//! Authentication, JWT/JWE validation, capabilities, and credential resolution.

use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) enum AblyCapabilityCheck {
    Publish,
    Subscribe,
    History,
    Presence,
    AnnotationPublish,
    AnnotationMutate,
    AnnotationSubscribe,
    AnnotationDeleteOwn,
    AnnotationDeleteAny,
    AnyChannelAccess,
}

impl AblyCapabilityCheck {
    fn label(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
            Self::History => "history",
            Self::Presence => "presence",
            Self::AnnotationPublish => "annotation publish",
            Self::AnnotationMutate => "annotation mutation",
            Self::AnnotationSubscribe => "annotation subscribe",
            Self::AnnotationDeleteOwn => "annotation delete own",
            Self::AnnotationDeleteAny => "annotation delete any",
            Self::AnyChannelAccess => "channel access",
        }
    }
}

pub(super) fn ensure_ably_capability(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &str,
    check: AblyCapabilityCheck,
) -> Result<(), AblyAuthError> {
    let Some(capabilities) = capabilities else {
        return Ok(());
    };

    let allowed = match check {
        AblyCapabilityCheck::Publish => capabilities.allows_publish(channel),
        AblyCapabilityCheck::Subscribe => capabilities.allows_subscribe(channel),
        AblyCapabilityCheck::History => capabilities.allows_history(channel),
        AblyCapabilityCheck::Presence => capabilities
            .presence
            .as_deref()
            .is_some_and(|patterns| ConnectionCapabilities::matches_any(patterns, channel)),
        AblyCapabilityCheck::AnnotationPublish => capabilities.allows_annotation_publish(channel),
        AblyCapabilityCheck::AnnotationMutate => {
            capabilities.allows_annotation_publish(channel)
                || capabilities.allows_annotation_delete_own(channel)
                || capabilities.allows_annotation_delete_any(channel)
        }
        AblyCapabilityCheck::AnnotationSubscribe => {
            capabilities.allows_annotation_subscribe(channel)
        }
        AblyCapabilityCheck::AnnotationDeleteOwn => {
            capabilities.allows_annotation_delete_own(channel)
        }
        AblyCapabilityCheck::AnnotationDeleteAny => {
            capabilities.allows_annotation_delete_any(channel)
        }
        AblyCapabilityCheck::AnyChannelAccess => {
            capabilities.allows_publish(channel)
                || capabilities.allows_subscribe(channel)
                || capabilities.allows_history(channel)
                || capabilities
                    .presence
                    .as_deref()
                    .is_some_and(|patterns| ConnectionCapabilities::matches_any(patterns, channel))
                || capabilities.allows_annotation_subscribe(channel)
                || capabilities.allows_annotation_publish(channel)
                || capabilities.allows_annotation_delete_own(channel)
                || capabilities.allows_annotation_delete_any(channel)
                || capabilities.allows_message_mutation_own(
                    sockudo_core::versioned_message_auth::MutationKind::Append,
                    channel,
                )
                || capabilities.allows_message_mutation_any(
                    sockudo_core::versioned_message_auth::MutationKind::Append,
                    channel,
                )
                || capabilities.allows_message_mutation_own(
                    sockudo_core::versioned_message_auth::MutationKind::Update,
                    channel,
                )
                || capabilities.allows_message_mutation_any(
                    sockudo_core::versioned_message_auth::MutationKind::Update,
                    channel,
                )
                || capabilities.allows_message_mutation_own(
                    sockudo_core::versioned_message_auth::MutationKind::Delete,
                    channel,
                )
                || capabilities.allows_message_mutation_any(
                    sockudo_core::versioned_message_auth::MutationKind::Delete,
                    channel,
                )
        }
    };

    if allowed {
        Ok(())
    } else {
        Err(AblyAuthError::forbidden(format!(
            "Ably token lacks {} capability for channel '{}'",
            check.label(),
            channel
        )))
    }
}

pub(super) fn ensure_ably_channel_capability(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &AblyChannelName,
    check: AblyCapabilityCheck,
) -> Result<(), AblyAuthError> {
    ensure_ably_channel_capability_parts(capabilities, channel.requested(), channel.base(), check)
}

pub(super) fn ensure_ably_channel_capability_parts(
    capabilities: Option<&ConnectionCapabilities>,
    requested_channel: &str,
    base_channel: &str,
    check: AblyCapabilityCheck,
) -> Result<(), AblyAuthError> {
    let result = ensure_ably_capability(capabilities, requested_channel, check);
    if result.is_ok() || requested_channel == base_channel {
        return result;
    }
    let Some(capabilities) = capabilities else {
        return result;
    };
    let matches = |patterns: Option<&[String]>| {
        patterns.is_some_and(|patterns| {
            patterns.iter().any(|pattern| {
                pattern.strip_prefix("[*]").is_some_and(|base_pattern| {
                    base_pattern == "*"
                        || base_pattern == base_channel
                        || sockudo_core::utils::wildcard_pattern_matches(base_channel, base_pattern)
                })
            })
        })
    };
    let allowed = match check {
        AblyCapabilityCheck::Publish => matches(capabilities.publish.as_deref()),
        AblyCapabilityCheck::Subscribe => matches(capabilities.subscribe.as_deref()),
        AblyCapabilityCheck::History => matches(capabilities.history.as_deref()),
        AblyCapabilityCheck::Presence => matches(capabilities.presence.as_deref()),
        AblyCapabilityCheck::AnnotationPublish => {
            matches(capabilities.annotation_publish.as_deref())
        }
        AblyCapabilityCheck::AnnotationMutate => {
            matches(capabilities.annotation_publish.as_deref())
                || matches(capabilities.annotation_delete_own.as_deref())
                || matches(capabilities.annotation_delete_any.as_deref())
        }
        AblyCapabilityCheck::AnnotationSubscribe => {
            matches(capabilities.annotation_subscribe.as_deref())
        }
        AblyCapabilityCheck::AnnotationDeleteOwn => {
            matches(capabilities.annotation_delete_own.as_deref())
        }
        AblyCapabilityCheck::AnnotationDeleteAny => {
            matches(capabilities.annotation_delete_any.as_deref())
        }
        AblyCapabilityCheck::AnyChannelAccess => [
            capabilities.publish.as_deref(),
            capabilities.subscribe.as_deref(),
            capabilities.history.as_deref(),
            capabilities.presence.as_deref(),
            capabilities.annotation_subscribe.as_deref(),
            capabilities.annotation_publish.as_deref(),
            capabilities.annotation_delete_own.as_deref(),
            capabilities.annotation_delete_any.as_deref(),
            capabilities.message_update_own.as_deref(),
            capabilities.message_update_any.as_deref(),
            capabilities.message_delete_own.as_deref(),
            capabilities.message_delete_any.as_deref(),
            capabilities.message_append_own.as_deref(),
            capabilities.message_append_any.as_deref(),
        ]
        .into_iter()
        .any(matches),
    };
    if allowed { Ok(()) } else { result }
}

pub(super) fn ensure_ably_channel_capability_app_error(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &AblyChannelName,
    check: AblyCapabilityCheck,
) -> Result<(), AppError> {
    ensure_ably_channel_capability(capabilities, channel, check)
        .map_err(|error| AppError::Forbidden(error.message))
}

pub(super) fn ably_auth_app_error(error: AblyAuthError) -> AppError {
    AppError::Protocol {
        status: error.status,
        code: error.code,
        message: error.message,
    }
}

#[derive(Debug)]
pub(super) enum CapabilityIntersectionError {
    Invalid(String),
    Empty,
}

pub(super) fn parse_token_request_integer(
    value: Option<&serde_json::Value>,
    field: &str,
) -> Result<Option<i64>, String> {
    value
        .map(|value| {
            value
                .as_i64()
                .ok_or_else(|| format!("TokenRequest {field} must be an integer"))
        })
        .transpose()
}

pub(super) fn token_request_signing_input(
    key_name: &str,
    ttl: Option<i64>,
    capability: Option<&str>,
    client_id: Option<&str>,
    timestamp: i64,
    nonce: &str,
) -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n",
        key_name,
        ttl.map(|value| value.to_string()).unwrap_or_default(),
        capability.unwrap_or_default(),
        client_id.unwrap_or_default(),
        timestamp,
        nonce
    )
}

pub(super) fn verify_token_request_mac(secret: &str, input: &str, encoded_mac: &str) -> bool {
    type HmacSha256 = Hmac<Sha256>;
    let Ok(signature) = base64::engine::general_purpose::STANDARD.decode(encoded_mac) else {
        return false;
    };
    let Ok(mut verifier) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    verifier.update(input.as_bytes());
    verifier.verify_slice(&signature).is_ok()
}

pub(super) fn token_cache_key(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    format!(
        "ably-compat:token:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    )
}

pub(super) fn is_well_formed_connection_key(connection_key: &str) -> bool {
    if connection_key.is_empty() || connection_key.len() > ABLY_CONNECTION_KEY_MAX_BYTES {
        return false;
    }
    let Some((connection_id, nonce)) = connection_key.rsplit_once(':') else {
        return false;
    };
    !connection_id.is_empty()
        && nonce.len() == 32
        && nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn session_cache_key(connection_key: &str) -> String {
    let digest = Sha256::digest(connection_key.as_bytes());
    format!(
        "ably-compat:session:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    )
}

pub(super) fn session_owner_cache_key(app_id: &str, connection_id: &str) -> String {
    let digest = Sha256::digest(connection_id.as_bytes());
    format!(
        "ably-compat:session-owner:{app_id}:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    )
}

pub(super) fn nonce_cache_key(key_name: &str, nonce: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(key_name.as_bytes());
    digest.update([0]);
    digest.update(nonce.as_bytes());
    format!(
        "ably-compat:nonce:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
    )
}

pub(super) fn revocation_cache_key(app_id: &str, target_type: &str, target_value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(target_type.as_bytes());
    digest.update([0]);
    digest.update(target_value.as_bytes());
    format!(
        "ably-compat:revocation:{app_id}:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
    )
}

pub(super) fn intersect_ably_capability(
    key_capability: &str,
    requested_capability: Option<&str>,
) -> Result<(String, Option<ConnectionCapabilities>), CapabilityIntersectionError> {
    let key = parse_ably_capability_object(key_capability)?;
    let Some(requested_raw) = requested_capability else {
        for (resource, operations) in &key {
            capability_operations(resource, operations)?;
        }
        let capabilities =
            ably_capability_value_to_sockudo(&serde_json::Value::Object(key.clone()))
                .map_err(|error| CapabilityIntersectionError::Invalid(error.to_string()))?;
        return Ok((key_capability.to_string(), Some(capabilities)));
    };
    let requested = parse_ably_capability_object(requested_raw)?;
    if capability_maps_equivalent(&requested, &key)? {
        for (resource, operations) in &requested {
            capability_operations(resource, operations)?;
        }
        let value = serde_json::Value::Object(requested);
        let capabilities = ably_capability_value_to_sockudo(&value)
            .map_err(|error| CapabilityIntersectionError::Invalid(error.to_string()))?;
        return Ok((requested_raw.to_string(), Some(capabilities)));
    }
    let mut intersection = serde_json::Map::new();

    for (requested_resource, requested_operations) in requested {
        let requested_operations =
            capability_operations(&requested_resource, &requested_operations)?;
        let mut allowed = Vec::<String>::new();
        for (key_resource, key_operations) in &key {
            if !capability_resource_matches(key_resource, &requested_resource) {
                continue;
            }
            let key_operations = capability_operations(key_resource, key_operations)?;
            for operation in &requested_operations {
                if operation == "*" {
                    if key_operations
                        .iter()
                        .any(|key_operation| key_operation == "*")
                    {
                        allowed.clear();
                        push_unique(&mut allowed, "*");
                        break;
                    }
                    for key_operation in &key_operations {
                        push_unique(&mut allowed, key_operation);
                    }
                } else if key_operations
                    .iter()
                    .any(|key_operation| key_operation == "*" || key_operation == operation)
                {
                    push_unique(&mut allowed, operation);
                }
            }
        }
        if !allowed.is_empty() {
            intersection.insert(
                requested_resource,
                serde_json::Value::Array(
                    allowed.into_iter().map(serde_json::Value::String).collect(),
                ),
            );
        }
    }

    if intersection.is_empty() {
        return Err(CapabilityIntersectionError::Empty);
    }
    let value = serde_json::Value::Object(intersection);
    let capabilities = ably_capability_value_to_sockudo(&value)
        .map_err(|error| CapabilityIntersectionError::Invalid(error.to_string()))?;
    let canonical = serde_json::to_string(&value)
        .map_err(|error| CapabilityIntersectionError::Invalid(error.to_string()))?;
    Ok((canonical, Some(capabilities)))
}

pub(super) fn capability_maps_equivalent(
    left: &serde_json::Map<String, serde_json::Value>,
    right: &serde_json::Map<String, serde_json::Value>,
) -> Result<bool, CapabilityIntersectionError> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (resource, left_operations) in left {
        let Some(right_operations) = right.get(resource) else {
            return Ok(false);
        };
        let mut left_operations = capability_operations(resource, left_operations)?;
        let mut right_operations = capability_operations(resource, right_operations)?;
        left_operations.sort_unstable();
        right_operations.sort_unstable();
        if left_operations != right_operations {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn parse_ably_capability_object(
    raw: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, CapabilityIntersectionError> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map_err(|error| {
            CapabilityIntersectionError::Invalid(format!("Invalid capability JSON: {error}"))
        })?
        .as_object()
        .cloned()
        .ok_or_else(|| {
            CapabilityIntersectionError::Invalid("Capability must be a JSON object".to_string())
        })
}

pub(super) fn capability_operations(
    resource: &str,
    value: &serde_json::Value,
) -> Result<Vec<String>, CapabilityIntersectionError> {
    let values = value.as_array().ok_or_else(|| {
        CapabilityIntersectionError::Invalid(format!(
            "Capability operations for '{resource}' must be an array"
        ))
    })?;
    if values.is_empty() {
        return Err(CapabilityIntersectionError::Invalid(format!(
            "Capability operations for '{resource}' must not be empty"
        )));
    }
    let mut operations = Vec::with_capacity(values.len());
    for value in values {
        let operation = value.as_str().ok_or_else(|| {
            CapabilityIntersectionError::Invalid(format!(
                "Capability operation for '{resource}' must be a string"
            ))
        })?;
        if !all_ably_operations().contains(&operation) && operation != "*" {
            return Err(CapabilityIntersectionError::Invalid(format!(
                "Unsupported capability operation '{operation}'"
            )));
        }
        push_unique(&mut operations, operation);
    }
    if operations.len() > 1 && operations.iter().any(|operation| operation == "*") {
        return Err(CapabilityIntersectionError::Invalid(format!(
            "Capability operations for '{resource}' cannot combine '*' with other operations"
        )));
    }
    Ok(operations)
}

pub(super) fn capability_resource_matches(key_pattern: &str, requested_resource: &str) -> bool {
    ConnectionCapabilities::matches_any(&[key_pattern.to_string()], requested_resource)
        || key_pattern == requested_resource
}

pub(super) fn all_ably_operations() -> &'static [&'static str] {
    &[
        "publish",
        "subscribe",
        "history",
        "presence",
        "annotation-subscribe",
        "annotation-publish",
        "annotation-delete-own",
        "annotation-delete-any",
        "message-update-own",
        "message-update-any",
        "message-delete-own",
        "message-delete-any",
        "object-subscribe",
        "object-publish",
        "stats",
        "channel-metadata",
        "push-subscribe",
        "push-admin",
        "privileged-headers",
    ]
}

pub(super) fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

#[cfg(test)]
pub(super) fn normalise_ably_token_capability(
    capability: Option<serde_json::Value>,
) -> Result<(Option<String>, Option<ConnectionCapabilities>), AppError> {
    let Some(capability) = capability else {
        return Ok((None, None));
    };

    let parsed = match &capability {
        serde_json::Value::String(raw) => {
            serde_json::from_str::<serde_json::Value>(raw).map_err(|error| {
                AppError::InvalidInput(format!("Invalid Ably token capability JSON: {error}"))
            })?
        }
        serde_json::Value::Object(_) => capability.clone(),
        _ => {
            return Err(AppError::InvalidInput(
                "Ably token capability must be a JSON object or JSON object string".to_string(),
            ));
        }
    };

    let capabilities = ably_capability_value_to_sockudo(&parsed)?;
    let capability = match capability {
        serde_json::Value::String(raw) => raw,
        _ => serde_json::to_string(&parsed).map_err(|error| {
            AppError::InvalidInput(format!("Invalid Ably token capability: {error}"))
        })?,
    };
    Ok((Some(capability), Some(capabilities)))
}

pub(super) fn ably_capability_value_to_sockudo(
    value: &serde_json::Value,
) -> Result<ConnectionCapabilities, AppError> {
    let object = value.as_object().ok_or_else(|| {
        AppError::InvalidInput("Ably token capability must be a JSON object".to_string())
    })?;
    let mut capabilities = restricted_ably_capabilities();

    for (resource, operations) in object {
        if resource.is_empty() {
            return Err(AppError::InvalidInput(
                "Ably token capability resource must not be empty".to_string(),
            ));
        }
        let operations = operations.as_array().ok_or_else(|| {
            AppError::InvalidInput(format!(
                "Ably token capability for '{resource}' must be an array"
            ))
        })?;
        for operation in operations {
            let operation = operation.as_str().ok_or_else(|| {
                AppError::InvalidInput(format!(
                    "Ably token capability operation for '{resource}' must be a string"
                ))
            })?;
            add_ably_capability_operation(&mut capabilities, resource, operation)?;
        }
    }

    Ok(capabilities)
}

pub(super) fn restricted_ably_capabilities() -> ConnectionCapabilities {
    ConnectionCapabilities {
        subscribe: Some(Vec::new()),
        publish: Some(Vec::new()),
        history: Some(Vec::new()),
        presence: Some(Vec::new()),
        annotation_subscribe: Some(Vec::new()),
        annotation_publish: Some(Vec::new()),
        annotation_delete_own: Some(Vec::new()),
        annotation_delete_any: Some(Vec::new()),
        message_update_own: Some(Vec::new()),
        message_update_any: Some(Vec::new()),
        message_delete_own: Some(Vec::new()),
        message_delete_any: Some(Vec::new()),
        message_append_own: Some(Vec::new()),
        message_append_any: Some(Vec::new()),
        push_admin: Some(Vec::new()),
        push_subscribe: Some(Vec::new()),
    }
}

pub(super) fn add_ably_capability_operation(
    capabilities: &mut ConnectionCapabilities,
    resource: &str,
    operation: &str,
) -> Result<(), AppError> {
    match operation {
        "*" => {
            add_all_supported_ably_capabilities(capabilities, resource);
            Ok(())
        }
        "publish" => {
            add_capability_pattern(&mut capabilities.publish, resource);
            Ok(())
        }
        "subscribe" => {
            add_capability_pattern(&mut capabilities.subscribe, resource);
            Ok(())
        }
        "history" => {
            add_capability_pattern(&mut capabilities.history, resource);
            Ok(())
        }
        "presence" => {
            add_capability_pattern(&mut capabilities.presence, resource);
            Ok(())
        }
        "annotation-subscribe" => {
            add_capability_pattern(&mut capabilities.annotation_subscribe, resource);
            Ok(())
        }
        "annotation-publish" => {
            add_capability_pattern(&mut capabilities.annotation_publish, resource);
            Ok(())
        }
        "annotation-delete-own" => {
            add_capability_pattern(&mut capabilities.annotation_delete_own, resource);
            Ok(())
        }
        "annotation-delete-any" => {
            add_capability_pattern(&mut capabilities.annotation_delete_any, resource);
            Ok(())
        }
        "message-update-own" => {
            add_capability_pattern(&mut capabilities.message_update_own, resource);
            Ok(())
        }
        "message-update-any" => {
            add_capability_pattern(&mut capabilities.message_update_any, resource);
            Ok(())
        }
        "message-delete-own" => {
            add_capability_pattern(&mut capabilities.message_delete_own, resource);
            Ok(())
        }
        "message-delete-any" => {
            add_capability_pattern(&mut capabilities.message_delete_any, resource);
            Ok(())
        }
        "push-admin" => {
            add_capability_pattern(&mut capabilities.push_admin, resource);
            Ok(())
        }
        "push-subscribe" => {
            add_capability_pattern(&mut capabilities.push_subscribe, resource);
            Ok(())
        }
        "object-subscribe" | "object-publish" | "stats" | "channel-metadata"
        | "privileged-headers" => Ok(()),
        other => Err(AppError::InvalidInput(format!(
            "Unsupported Ably token capability operation '{other}'"
        ))),
    }
}

pub(super) fn add_all_supported_ably_capabilities(
    capabilities: &mut ConnectionCapabilities,
    resource: &str,
) {
    add_capability_pattern(&mut capabilities.publish, resource);
    add_capability_pattern(&mut capabilities.subscribe, resource);
    add_capability_pattern(&mut capabilities.history, resource);
    add_capability_pattern(&mut capabilities.presence, resource);
    add_capability_pattern(&mut capabilities.annotation_subscribe, resource);
    add_capability_pattern(&mut capabilities.annotation_publish, resource);
    add_capability_pattern(&mut capabilities.annotation_delete_own, resource);
    add_capability_pattern(&mut capabilities.annotation_delete_any, resource);
    add_capability_pattern(&mut capabilities.message_update_own, resource);
    add_capability_pattern(&mut capabilities.message_update_any, resource);
    add_capability_pattern(&mut capabilities.message_delete_own, resource);
    add_capability_pattern(&mut capabilities.message_delete_any, resource);
    add_capability_pattern(&mut capabilities.message_append_own, resource);
    add_capability_pattern(&mut capabilities.message_append_any, resource);
    add_capability_pattern(&mut capabilities.push_admin, resource);
    add_capability_pattern(&mut capabilities.push_subscribe, resource);
}

pub(super) fn add_capability_pattern(patterns: &mut Option<Vec<String>>, pattern: &str) {
    // Ably's all-qualifiers resource is the broad wildcard used by the pinned
    // fixture for both derived and ordinary channel instances.
    let pattern = if pattern == "[*]*" { "*" } else { pattern };
    patterns
        .get_or_insert_with(Vec::new)
        .push(pattern.to_string());
}

pub(super) async fn resolve_ably_auth(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    headers: &HeaderMap,
    query_key: Option<&str>,
    access_token: Option<&str>,
    query_client_id: Option<&str>,
) -> Result<ResolvedAblyAuth, AblyAuthError> {
    resolve_ably_auth_with_expiry(
        hub,
        handler,
        headers,
        query_key,
        access_token,
        query_client_id,
        false,
    )
    .await
}

#[derive(Debug, Deserialize)]
pub(super) struct AblyJwtClaims {
    pub(super) iat: i64,
    pub(super) exp: i64,
    #[serde(default, rename = "x-ably-clientId", alias = "x-ably-client-id")]
    pub(super) client_id: Option<String>,
    #[serde(default, rename = "x-ably-capability")]
    pub(super) capability: Option<String>,
    #[serde(default, rename = "x-ably-revocation-key")]
    pub(super) revocation_key: Option<String>,
    #[serde(default, rename = "x-ably-token")]
    pub(super) embedded_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct AblyJoseHeader {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    enc: Option<String>,
    #[serde(default)]
    cty: Option<String>,
    #[serde(default, rename = "x-ably-token")]
    embedded_token: Option<String>,
}

pub(super) struct VerifiedAblyJwt {
    claims: AblyJwtClaims,
    pub(super) embedded_token: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn resolve_ably_auth_with_expiry(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    headers: &HeaderMap,
    query_key: Option<&str>,
    access_token: Option<&str>,
    query_client_id: Option<&str>,
    allow_expired: bool,
) -> Result<ResolvedAblyAuth, AblyAuthError> {
    let header_client_id = decode_ably_client_id_header(headers)?;
    if let (Some(query_client_id), Some(header_client_id)) =
        (query_client_id, header_client_id.as_deref())
        && query_client_id != header_client_id
    {
        return Err(AblyAuthError::unauthorized(
            "client_id and X-Ably-ClientId identify different clients",
        ));
    }
    let requested_client_id = query_client_id.or(header_client_id.as_deref());
    let access_token = access_token
        .map(str::to_string)
        .or_else(|| bearer_token(headers));
    if let Some(access_token) = access_token {
        if let Some(record) = hub.resolve_token(&access_token).await {
            if record.revocable && !token_key_is_current(hub, &record, now_ms()) {
                return Err(AblyAuthError::unauthorized("Token has been revoked"));
            }
            if !allow_expired && record.expires_ms <= now_ms() {
                return Err(AblyAuthError::expired());
            }
            let app = find_enabled_app_by_id(handler, &record.app_id).await?;
            let token_client_id = record.client_id.clone();
            let client_id = resolve_ably_token_client_id(record.client_id, requested_client_id)?;
            let connection_client_id =
                if token_client_id.as_deref() == Some("*") && requested_client_id.is_none() {
                    Some("*".to_string())
                } else {
                    client_id.clone()
                };
            return Ok(ResolvedAblyAuth {
                app,
                client_id,
                connection_client_id,
                capabilities: record.capabilities,
                issued_ms: record.issued_ms,
                expires_ms: Some(record.expires_ms),
                credential_id: credential_id(&access_token),
                revocable: record.revocable,
                revocation_key: record.revocation_key,
                #[cfg(feature = "push")]
                push_device_id: None,
            });
        }
        return resolve_ably_jwt(
            hub,
            handler,
            &access_token,
            requested_client_id,
            allow_expired,
        )
        .await;
    }

    let credential = query_key
        .map(str::to_string)
        .or_else(|| basic_credential(headers))
        .ok_or_else(|| AblyAuthError::unauthorized("Missing Ably key credentials"))?;
    let (app_key, app_secret) = parse_ably_key(&credential);
    let app_secret = app_secret
        .filter(|secret| !secret.is_empty())
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let key = resolve_ably_key(hub, handler, app_key).await?;
    if !secure_compare(app_secret, &key.secret) {
        return Err(AblyAuthError::invalid_credentials());
    }
    Ok(ResolvedAblyAuth {
        app: key.app,
        client_id: requested_client_id.map(str::to_string),
        connection_client_id: requested_client_id.map(str::to_string),
        capabilities: key.capabilities,
        issued_ms: now_ms(),
        expires_ms: None,
        credential_id: format!("key:{}", key.key_name),
        revocable: false,
        revocation_key: None,
        #[cfg(feature = "push")]
        push_device_id: None,
    })
}

pub(super) fn decode_ably_client_id_header(
    headers: &HeaderMap,
) -> Result<Option<String>, AblyAuthError> {
    let Some(value) = headers.get("x-ably-clientid") else {
        return Ok(None);
    };
    let encoded = value
        .to_str()
        .map_err(|_| AblyAuthError::unauthorized("X-Ably-ClientId is not valid ASCII"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| AblyAuthError::unauthorized("X-Ably-ClientId is not valid base64"))?;
    let client_id = String::from_utf8(bytes)
        .map_err(|_| AblyAuthError::unauthorized("X-Ably-ClientId is not valid UTF-8"))?;
    if client_id.is_empty() {
        return Err(AblyAuthError::unauthorized(
            "X-Ably-ClientId must not be empty",
        ));
    }
    Ok(Some(client_id))
}

pub(super) async fn resolve_ably_jwt(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    token: &str,
    requested_client_id: Option<&str>,
    allow_expired: bool,
) -> Result<ResolvedAblyAuth, AblyAuthError> {
    if token.len() > 8 * 1024 {
        return Err(AblyAuthError::unauthorized("JWT exceeds 8 KiB"));
    }
    let original_token = token;
    let mut jwe_key = None;
    let signed_token = if token.split('.').count() == 5 {
        let header = parse_ably_jose_header(token)?;
        if header.alg != "dir" || header.enc.as_deref() != Some("A256GCM") {
            return Err(AblyAuthError::unauthorized("JWE must use dir with A256GCM"));
        }
        if header.cty.as_deref() != Some("JWT") {
            return Err(AblyAuthError::unauthorized("JWE content type must be JWT"));
        }
        let key_name = header
            .kid
            .as_deref()
            .ok_or_else(AblyAuthError::invalid_credentials)?;
        let key = resolve_ably_jwt_key(hub, handler, key_name).await?;
        let nested = decrypt_ably_compact_jwe(token, &key.key_name, &key.secret)?;
        jwe_key = Some(key);
        std::borrow::Cow::Owned(nested)
    } else {
        std::borrow::Cow::Borrowed(token)
    };

    let outer_header = parse_ably_jose_header(&signed_token)?;
    let outer_key_name = outer_header
        .kid
        .as_deref()
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let outer_key = resolve_ably_jwt_key(hub, handler, outer_key_name).await?;
    if let Some(jwe_key) = jwe_key.as_ref()
        && (jwe_key.app.id != outer_key.app.id || jwe_key.key_name != outer_key.key_name)
    {
        return Err(AblyAuthError::invalid_credentials());
    }
    let outer = verify_ably_signed_jwt(&signed_token, &outer_key.key_name, &outer_key.secret)?;
    let (_, outer_expires_ms) = validate_ably_jwt_times(
        &outer.claims,
        now_ms().div_euclid(1_000),
        hub.config.max_token_ttl_ms,
        allow_expired,
    )?;

    let (key, verified, issued_ms, expires_ms) = if let Some(embedded_token) =
        outer.embedded_token.as_deref()
    {
        let inner_header = parse_ably_jose_header(embedded_token)?;
        let inner_key_name = inner_header
            .kid
            .as_deref()
            .ok_or_else(AblyAuthError::invalid_credentials)?;
        let inner_key = resolve_ably_jwt_key(hub, handler, inner_key_name).await?;
        if inner_key.app.id != outer_key.app.id {
            return Err(AblyAuthError::invalid_credentials());
        }
        let inner = verify_ably_signed_jwt(embedded_token, &inner_key.key_name, &inner_key.secret)?;
        if inner.embedded_token.is_some() {
            return Err(AblyAuthError::unauthorized(
                "Nested embedded JWTs are not supported",
            ));
        }
        let (inner_issued_ms, inner_expires_ms) = validate_ably_jwt_times(
            &inner.claims,
            now_ms().div_euclid(1_000),
            hub.config.max_token_ttl_ms,
            allow_expired,
        )?;
        if outer_expires_ms > inner_expires_ms {
            return Err(AblyAuthError::unauthorized(
                "Embedded JWT expires before its outer JWT",
            ));
        }
        (inner_key, inner, inner_issued_ms, inner_expires_ms)
    } else {
        if jwe_key.is_some() {
            return Err(AblyAuthError::unauthorized(
                "Encrypted JWT must contain an embedded Ably token",
            ));
        }
        let (issued_ms, expires_ms) = validate_ably_jwt_times(
            &outer.claims,
            now_ms().div_euclid(1_000),
            hub.config.max_token_ttl_ms,
            allow_expired,
        )?;
        (outer_key, outer, issued_ms, expires_ms)
    };

    resolved_ably_jwt_auth(
        key,
        verified.claims,
        requested_client_id,
        issued_ms,
        expires_ms,
        original_token,
    )
}

pub(super) async fn resolve_ably_jwt_key(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    key_name: &str,
) -> Result<ResolvedAblyKey, AblyAuthError> {
    resolve_ably_key(hub, handler, key_name)
        .await
        .map_err(|error| {
            if error.code == 40101 {
                AblyAuthError {
                    status: StatusCode::NOT_FOUND,
                    code: 40400,
                    message: "JWT key does not identify an app".to_string(),
                }
            } else {
                error
            }
        })
}

pub(super) fn parse_ably_jose_header(token: &str) -> Result<AblyJoseHeader, AblyAuthError> {
    if token.len() > 8 * 1024 {
        return Err(AblyAuthError::unauthorized("JWT exceeds 8 KiB"));
    }
    let protected = token
        .split_once('.')
        .map(|(protected, _)| protected)
        .filter(|protected| !protected.is_empty())
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    if protected.len() > 4 * 1024 {
        return Err(AblyAuthError::unauthorized("JOSE header exceeds 4 KiB"));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(protected)
        .map_err(|_| AblyAuthError::invalid_credentials())?;
    if bytes.len() > 4 * 1024 {
        return Err(AblyAuthError::unauthorized("JOSE header exceeds 4 KiB"));
    }
    serde_json::from_slice(&bytes).map_err(|_| AblyAuthError::invalid_credentials())
}

pub(super) fn verify_ably_signed_jwt(
    token: &str,
    expected_key_name: &str,
    secret: &str,
) -> Result<VerifiedAblyJwt, AblyAuthError> {
    if token.len() > 8 * 1024 {
        return Err(AblyAuthError::unauthorized("JWT exceeds 8 KiB"));
    }
    let mut segments = token.split('.');
    if segments.next().is_none()
        || segments.next().is_none()
        || segments.next().is_none()
        || segments.next().is_some()
    {
        return Err(AblyAuthError::invalid_credentials());
    }
    let header = parse_ably_jose_header(token)?;
    if header.alg != "HS256" {
        return Err(AblyAuthError::unauthorized("JWT must use HS256"));
    }
    if header.kid.as_deref() != Some(expected_key_name) {
        return Err(AblyAuthError::invalid_credentials());
    }
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.required_spec_claims.clear();
    let decoded = decode::<AblyJwtClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|_| AblyAuthError::invalid_credentials())?;
    let mut claims = decoded.claims;
    let embedded_token = header
        .embedded_token
        .or_else(|| claims.embedded_token.take());
    if embedded_token
        .as_deref()
        .is_some_and(|token| token.is_empty() || token.len() > 8 * 1024)
    {
        return Err(AblyAuthError::unauthorized(
            "Embedded Ably token is invalid",
        ));
    }
    Ok(VerifiedAblyJwt {
        claims,
        embedded_token,
    })
}

pub(super) fn decrypt_ably_compact_jwe(
    token: &str,
    expected_key_name: &str,
    secret: &str,
) -> Result<String, AblyAuthError> {
    if token.len() > 8 * 1024 {
        return Err(AblyAuthError::unauthorized("JWT exceeds 8 KiB"));
    }
    let mut segments = token.split('.');
    let protected = segments
        .next()
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let encrypted_key = segments
        .next()
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let iv = segments
        .next()
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let ciphertext = segments
        .next()
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let tag = segments
        .next()
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    if segments.next().is_some() || protected.is_empty() || !encrypted_key.is_empty() {
        return Err(AblyAuthError::invalid_credentials());
    }
    let header = parse_ably_jose_header(token)?;
    if header.alg != "dir"
        || header.enc.as_deref() != Some("A256GCM")
        || header.cty.as_deref() != Some("JWT")
        || header.kid.as_deref() != Some(expected_key_name)
    {
        return Err(AblyAuthError::invalid_credentials());
    }
    let iv = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(iv)
        .map_err(|_| AblyAuthError::invalid_credentials())?;
    let mut ciphertext = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(ciphertext)
        .map_err(|_| AblyAuthError::invalid_credentials())?;
    let tag = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(tag)
        .map_err(|_| AblyAuthError::invalid_credentials())?;
    if iv.len() != 12 || tag.len() != 16 {
        return Err(AblyAuthError::invalid_credentials());
    }
    ciphertext
        .try_reserve_exact(tag.len())
        .map_err(|_| AblyAuthError::internal())?;
    ciphertext.extend_from_slice(&tag);
    let key = Sha256::digest(secret.as_bytes());
    let cipher = <Aes256Gcm as AeadKeyInit>::new_from_slice(&key)
        .map_err(|_| AblyAuthError::invalid_credentials())?;
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(&iv),
            Payload {
                msg: &ciphertext,
                aad: protected.as_bytes(),
            },
        )
        .map_err(|_| AblyAuthError::invalid_credentials())?;
    if plaintext.len() > 8 * 1024 {
        return Err(AblyAuthError::unauthorized("Nested JWT exceeds 8 KiB"));
    }
    String::from_utf8(plaintext).map_err(|_| AblyAuthError::invalid_credentials())
}

pub(super) fn validate_ably_jwt_times(
    claims: &AblyJwtClaims,
    now_seconds: i64,
    max_token_ttl_ms: i64,
    allow_expired: bool,
) -> Result<(i64, i64), AblyAuthError> {
    let issued_ms = jwt_seconds_to_millis(claims.iat)?;
    let expires_ms = jwt_seconds_to_millis(claims.exp)?;
    validate_jwt_lifetime_ms(issued_ms, expires_ms, max_token_ttl_ms)?;
    if claims.iat > now_seconds.saturating_add(30) {
        return Err(AblyAuthError::unauthorized("JWT claims are invalid"));
    }
    if !allow_expired && claims.exp <= now_seconds {
        return Err(AblyAuthError::expired());
    }
    Ok((issued_ms, expires_ms))
}

pub(super) fn resolved_ably_jwt_auth(
    key: ResolvedAblyKey,
    claims: AblyJwtClaims,
    requested_client_id: Option<&str>,
    issued_ms: i64,
    expires_ms: i64,
    credential: &str,
) -> Result<ResolvedAblyAuth, AblyAuthError> {
    let token_client_id = claims.client_id.clone();
    let client_id = resolve_ably_token_client_id(claims.client_id, requested_client_id)?;
    let connection_client_id =
        if token_client_id.as_deref() == Some("*") && requested_client_id.is_none() {
            Some("*".to_string())
        } else {
            client_id.clone()
        };
    let (capability, capabilities) = intersect_ably_capability(
        &key.capability,
        claims.capability.as_deref(),
    )
    .map_err(|error| match error {
        CapabilityIntersectionError::Invalid(message) => AblyAuthError::unauthorized(message),
        CapabilityIntersectionError::Empty => {
            AblyAuthError::forbidden("JWT capability exceeds key capability")
        }
    })?;
    let _ = capability;
    Ok(ResolvedAblyAuth {
        app: key.app,
        client_id,
        connection_client_id,
        capabilities,
        issued_ms,
        expires_ms: Some(expires_ms),
        credential_id: credential_id(credential),
        revocable: key.revocable_tokens,
        revocation_key: claims.revocation_key,
        #[cfg(feature = "push")]
        push_device_id: None,
    })
}

pub(super) fn jwt_seconds_to_millis(seconds: i64) -> Result<i64, AblyAuthError> {
    seconds
        .checked_mul(1_000)
        .ok_or_else(|| AblyAuthError::unauthorized("JWT claims are invalid"))
}

pub(super) fn validate_jwt_lifetime_ms(
    issued_ms: i64,
    expires_ms: i64,
    max_token_ttl_ms: i64,
) -> Result<(), AblyAuthError> {
    expires_ms
        .checked_sub(issued_ms)
        .filter(|lifetime_ms| *lifetime_ms > 0 && *lifetime_ms <= max_token_ttl_ms)
        .map(|_| ())
        .ok_or_else(|| AblyAuthError::unauthorized("JWT claims are invalid"))
}

pub(super) fn credential_id(token: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes()))
}

pub(super) async fn resolve_ably_key(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    key_name: &str,
) -> Result<ResolvedAblyKey, AblyAuthError> {
    if let Some(key) = hub.key_registry.get(key_name) {
        if !key_is_active(key, now_ms()) {
            return Err(AblyAuthError::invalid_credentials());
        }
        let app = find_enabled_app_by_id(handler, &key.app_id).await?;
        let capability = key
            .capability
            .clone()
            .unwrap_or_else(default_ably_capability);
        let (_, capabilities) = intersect_ably_capability(&capability, None)
            .map_err(|_| auth_internal_failure("configured Ably capability validation"))?;
        return Ok(ResolvedAblyKey {
            app,
            key_name: key.key_name.clone(),
            secret: key.secret.clone(),
            capability,
            capabilities,
            revocable_tokens: key.revocable_tokens,
            rotation_id: key.rotation_id.clone(),
        });
    }

    let app = find_enabled_app_by_key(handler, key_name).await?;
    Ok(ResolvedAblyKey {
        key_name: app.key.clone(),
        secret: app.secret.clone(),
        app,
        capability: default_ably_capability(),
        capabilities: None,
        revocable_tokens: false,
        rotation_id: None,
    })
}

pub(super) fn key_is_active(key: &AblyCompatKeyConfig, now: i64) -> bool {
    key.enabled
        && key.revoked_at_ms.is_none_or(|revoked_at| revoked_at > now)
        && key.created_at_ms.is_none_or(|created_at| created_at <= now)
        && key.expires_at_ms.is_none_or(|expires_at| expires_at > now)
}

pub(super) fn token_key_is_current(
    hub: &AblyCompatHub,
    record: &AblyTokenRecord,
    now: i64,
) -> bool {
    hub.key_registry.get(&record.key_name).is_some_and(|key| {
        key_is_active(key, now)
            && key.app_id == record.app_id
            && key.rotation_id == record.rotation_id
    })
}

pub(super) fn default_ably_capability() -> String {
    r#"{"*":["*"]}"#.to_string()
}

pub(super) fn resolve_ably_token_client_id(
    token_client_id: Option<String>,
    query_client_id: Option<&str>,
) -> Result<Option<String>, AblyAuthError> {
    match (token_client_id, query_client_id) {
        (Some(token_client_id), Some(query_client_id)) if token_client_id == "*" => {
            Ok(Some(query_client_id.to_string()))
        }
        (Some(token_client_id), None) if token_client_id == "*" => Ok(None),
        (Some(token_client_id), Some(query_client_id)) if token_client_id != query_client_id => {
            Err(AblyAuthError {
                status: StatusCode::UNAUTHORIZED,
                code: 40102,
                message: "Token clientId does not match requested clientId".to_string(),
            })
        }
        (Some(token_client_id), _) => Ok(Some(token_client_id)),
        (None, Some(query_client_id)) => Ok(Some(query_client_id.to_string())),
        (None, None) => Ok(None),
    }
}

pub(super) async fn find_enabled_app_by_key(
    handler: &Arc<ConnectionHandler>,
    app_key: &str,
) -> Result<App, AblyAuthError> {
    match handler.app_manager().find_by_key(app_key).await {
        Ok(Some(app)) if app.enabled => Ok(app),
        Ok(Some(_)) => Err(AblyAuthError::invalid_credentials()),
        Ok(None) => Err(AblyAuthError::invalid_credentials()),
        Err(_) => Err(auth_backend_failure("application lookup by key")),
    }
}

pub(super) async fn find_enabled_app_by_id(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
) -> Result<App, AblyAuthError> {
    match handler.app_manager().find_by_id(app_id).await {
        Ok(Some(app)) if app.enabled => Ok(app),
        Ok(Some(_)) => Err(AblyAuthError::invalid_credentials()),
        Ok(None) => Err(AblyAuthError::invalid_credentials()),
        Err(_) => Err(auth_backend_failure("application lookup by id")),
    }
}
