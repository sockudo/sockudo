use std::collections::BTreeMap;
use std::env;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use axum::{
    body::Bytes,
    extract::{Extension, FromRequest, Path, Query, Request},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use sockudo_core::app::App;
use sockudo_core::options::{PushRuleConfig, PushRulePayloadMappingConfig};
use sockudo_core::rate_limiter::RateLimiter;
use sockudo_protocol::messages::ApiMessageData;
use sockudo_push::{
    ChannelPushRule, ChannelSubscription, DeliveryEvent, DeviceDetails, DeviceRegistrationChange,
    DynPushQueue, DynPushStore, EncryptedSecret, FanoutRegime, IdempotencyRecord,
    NotificationTemplate, OperatorInvalidationEvent, ProviderCredential,
    ProviderCredentialMaterial, ProviderOverridePayload, PublishCounters, PublishIntent,
    PublishLifecycleState, PublishLogEvent, PublishStatus, PublishTarget, PushCursor,
    PushMetaEvent, PushMetrics, PushPayload, PushProviderKind, PushQueuePayload, PushQueueStage,
    PushRecipient, PushRulePayloadMapping, RenderedProviderPayload, SecretString,
    emit_push_meta_event, generate_device_identity_token, hash_device_identity_token,
    render_all_provider_payloads, resolve_template_payload, verify_device_identity_token,
};
use sonic_rs::{Value, json};
use tracing::{info, warn};

use crate::bootstrap::push::{PushAdmissionRejection, PushAdmissionSnapshot};
use crate::http_handler::AppError;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const DEFAULT_PUSH_FANOUT_FAST_THRESHOLD: u64 = 10_000;
const DEFAULT_PUSH_FANOUT_SHARD_SIZE: u64 = 100_000;
const DEFAULT_PUSH_BACKPRESSURE_RETRY_AFTER_SECONDS: u64 = 5;
const DEFAULT_PUSH_CRITICAL_QUEUE_MAX_LAG: u64 = 100_000;
const PUSH_HTTP_DEFAULT_IDEMPOTENCY_TTL_MS: u64 = 24 * 60 * 60 * 1000;
const QUOTA_OVERRIDE_HEADER: &str = "x-sockudo-push-quota-override";
const PUSH_CAPABILITY_HEADER: &str = "x-sockudo-push-capability";
const DEVICE_TOKEN_HEADER: &str = "x-sockudo-device-identity-token";
const CREDENTIAL_SECRET_LOCAL_PREFIX: &str = "envelope:v1:local:";
const CREDENTIAL_SECRET_AES_PREFIX: &str = "envelope:v1:aes256gcm:";
const CREDENTIAL_SECRET_LEGACY_HASH_PREFIX: &str = "envelope:v1:sha256:";

static PUSH_DEVICE_RATE_WINDOWS: LazyLock<Mutex<BTreeMap<String, RateWindow>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
static PUSH_RULE_RATE_WINDOWS: LazyLock<Mutex<BTreeMap<String, RateWindow>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
static PUSH_HTTP_METRICS: LazyLock<PushMetrics> = LazyLock::new(PushMetrics::default);
#[cfg(test)]
pub(crate) static PUSH_TEST_ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[derive(Debug)]
pub struct Json<T>(pub T);

impl<S, T> FromRequest<S> for Json<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        if !is_json_content_type(req.headers()) {
            return Err(AppError::InvalidInput(
                "expected application/json content type".to_owned(),
            ));
        }
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|error| AppError::InvalidInput(format!("invalid JSON body: {error}")))?;
        let value = sonic_rs::from_slice(&bytes)
            .map_err(|error| AppError::InvalidInput(format!("invalid JSON body: {error}")))?;
        Ok(Self(value))
    }
}

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        match sonic_rs::to_vec(&self.0) {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                format!("failed to serialize JSON response: {error}"),
            )
                .into_response(),
        }
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let media_type = content_type.split(';').next().unwrap_or("").trim();
    media_type.eq_ignore_ascii_case("application/json")
        || media_type.rsplit_once('/').is_some_and(|(_, subtype)| {
            subtype
                .get(subtype.len().saturating_sub(5)..)
                .is_some_and(|suffix| suffix.eq_ignore_ascii_case("+json"))
        })
}

fn publish_state_label(state: PublishLifecycleState) -> &'static str {
    match state {
        PublishLifecycleState::Queued => "accepted",
        PublishLifecycleState::Planning => "planning",
        PublishLifecycleState::Throttled => "throttled",
        PublishLifecycleState::Dispatching => "dispatching",
        PublishLifecycleState::Cancelled => "cancelled",
        PublishLifecycleState::Succeeded => "succeeded",
        PublishLifecycleState::PartiallySucceeded => "partially_succeeded",
        PublishLifecycleState::Failed => "failed",
        PublishLifecycleState::QuotaExceeded => "quota_exceeded",
        PublishLifecycleState::Expired => "expired",
        PublishLifecycleState::DeadLettered => "dead_lettered",
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct RateWindow {
    second: u64,
    count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PushCapability {
    Admin,
    Subscribe,
}

#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Serialize)]
pub struct CredentialResponse {
    pub app_id: String,
    pub credential_id: String,
    pub provider: PushProviderKind,
    pub version: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceRecipientResponse {
    pub transport_type: &'static str,
    pub provider: PushProviderKind,
    pub token_hash: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceResponse {
    pub app_id: String,
    pub id: String,
    pub client_id: Option<String>,
    pub form_factor: sockudo_push::FormFactor,
    pub platform: sockudo_push::Platform,
    pub timezone: String,
    pub locale: String,
    pub last_active_at_ms: u64,
    pub push_state: sockudo_push::DevicePushState,
    pub push_failure_count: u32,
    pub recipient: DeviceRecipientResponse,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceRegistrationResponse {
    pub change: DeviceRegistrationChange,
    pub token_hash: String,
    pub device: DeviceResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_identity_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FcmCredentialRequest {
    pub credential_id: Option<String>,
    pub version: Option<u64>,
    pub service_account_json: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApnsCredentialRequest {
    pub credential_id: Option<String>,
    pub version: Option<u64>,
    pub p12: Option<String>,
    pub p12_password: Option<String>,
    pub pem: Option<String>,
    pub team_id: Option<String>,
    pub key_id: Option<String>,
    pub private_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebPushCredentialRequest {
    pub credential_id: Option<String>,
    pub version: Option<u64>,
    pub public_key: String,
    pub private_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HmsCredentialRequest {
    pub credential_id: Option<String>,
    pub version: Option<u64>,
    pub hms_app_id: String,
    pub client_secret: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WnsCredentialRequest {
    pub credential_id: Option<String>,
    pub version: Option<u64>,
    pub package_sid: String,
    pub client_secret: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveDevicesQuery {
    pub client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionQuery {
    pub channel: Option<String>,
    pub device_id: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishRequest {
    pub publish_id: Option<String>,
    #[serde(default)]
    pub recipients: Vec<PublishTarget>,
    pub payload: PushPayload,
    #[serde(default)]
    pub provider_overrides: Vec<ProviderOverridePayload>,
    #[serde(default)]
    pub sync: bool,
    pub not_before_ms: Option<u64>,
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishAcceptedResponse {
    pub publish_id: String,
    pub status: &'static str,
    pub expected_recipients: u64,
    pub fanout_regime: FanoutRegime,
    pub rendered_payloads: Vec<RenderedProviderPayload>,
}

pub async fn post_fcm_credential(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(request): Json<FcmCredentialRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    put_credential(
        store,
        ProviderCredential {
            app_id,
            credential_id: request.credential_id.unwrap_or_else(|| "fcm".to_owned()),
            provider: PushProviderKind::Fcm,
            version: request.version.unwrap_or(1),
            material: ProviderCredentialMaterial::Fcm {
                service_account_json: encrypted_json(&request.service_account_json)?,
            },
        },
    )
    .await
}

pub async fn post_apns_credential(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(request): Json<ApnsCredentialRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    put_credential(
        store,
        ProviderCredential {
            app_id,
            credential_id: request.credential_id.unwrap_or_else(|| "apns".to_owned()),
            provider: PushProviderKind::Apns,
            version: request.version.unwrap_or(1),
            material: ProviderCredentialMaterial::Apns {
                p12: encrypt_optional(request.p12)?,
                p12_password: encrypt_optional(request.p12_password)?,
                pem: encrypt_optional(request.pem)?,
                team_id: request.team_id,
                key_id: request.key_id,
                private_key: encrypt_optional(request.private_key)?,
            },
        },
    )
    .await
}

pub async fn post_webpush_credential(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(request): Json<WebPushCredentialRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    put_credential(
        store,
        ProviderCredential {
            app_id,
            credential_id: request
                .credential_id
                .unwrap_or_else(|| "webpush".to_owned()),
            provider: PushProviderKind::WebPush,
            version: request.version.unwrap_or(1),
            material: ProviderCredentialMaterial::WebPush {
                public_key: request.public_key,
                private_key: encrypted_secret(&request.private_key)?,
            },
        },
    )
    .await
}

pub async fn post_hms_credential(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(request): Json<HmsCredentialRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    put_credential(
        store,
        ProviderCredential {
            app_id,
            credential_id: request.credential_id.unwrap_or_else(|| "hms".to_owned()),
            provider: PushProviderKind::Hms,
            version: request.version.unwrap_or(1),
            material: ProviderCredentialMaterial::Hms {
                hms_app_id: request.hms_app_id,
                client_secret: encrypted_secret(&request.client_secret)?,
            },
        },
    )
    .await
}

pub async fn post_wns_credential(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(request): Json<WnsCredentialRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    put_credential(
        store,
        ProviderCredential {
            app_id,
            credential_id: request.credential_id.unwrap_or_else(|| "wns".to_owned()),
            provider: PushProviderKind::Wns,
            version: request.version.unwrap_or(1),
            material: ProviderCredentialMaterial::Wns {
                package_sid: request.package_sid,
                client_secret: encrypted_secret(&request.client_secret)?,
            },
        },
    )
    .await
}

pub async fn list_credentials(
    Path(app_id): Path<String>,
    Query(query): Query<PaginationQuery>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    let page = store
        .list_credentials(
            &app_id,
            limit(query.limit)?,
            decode_cursor(query.cursor, &app_id)?,
        )
        .await
        .map_err(push_error)?;
    list_response(
        page.items.into_iter().map(credential_response).collect(),
        page.next_cursor,
    )
}

pub async fn post_template(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(mut template): Json<NotificationTemplate>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    template.app_id = app_id;
    store
        .put_template(template.clone())
        .await
        .map_err(push_error)?;
    Ok((StatusCode::CREATED, Json(template)))
}

pub async fn get_template(
    Path((app_id, template_id)): Path<(String, String)>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    let template = store
        .get_template(&app_id, &template_id)
        .await
        .map_err(push_error)?
        .ok_or_else(|| AppError::NotFound("template not found".to_owned()))?;
    Ok(Json(template))
}

pub async fn list_templates(
    Path(app_id): Path<String>,
    Query(query): Query<PaginationQuery>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    let page = store
        .list_templates(
            &app_id,
            limit(query.limit)?,
            decode_cursor(query.cursor, &app_id)?,
        )
        .await
        .map_err(push_error)?;
    list_response(page.items, page.next_cursor)
}

pub async fn delete_template(
    Path((app_id, template_id)): Path<(String, String)>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    store
        .delete_template(&app_id, &template_id)
        .await
        .map_err(push_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn register_device(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(mut device): Json<DeviceDetails>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_subscribe_or_admin(&headers)?;
    device.app_id = app_id.clone();

    let existing = store
        .get_device(&app_id, &device.id)
        .await
        .map_err(push_error)?;
    let capability = push_capability(&headers)?;
    let rotate = header_bool(&headers, "x-sockudo-rotate-device-identity-token");
    let mut issued_token = None;

    if capability == PushCapability::Admin {
        if existing.is_none() || rotate {
            let token = generate_device_identity_token();
            device.device_secret = hash_device_identity_token(&token);
            issued_token = Some(token.expose_secret().to_owned());
        } else if let Some(existing) = existing.as_ref() {
            device.device_secret = existing.device_secret.clone();
        }
    } else {
        let existing = existing.ok_or_else(|| {
            AppError::Forbidden("device token registration requires an existing device".to_owned())
        })?;
        ensure_device_identity(&headers, &existing)?;
        device.device_secret = existing.device_secret;
    }

    let result = store
        .upsert_device(device.clone())
        .await
        .map_err(push_error)?;
    Ok((
        StatusCode::CREATED,
        Json(DeviceRegistrationResponse {
            change: result.change,
            token_hash: result.token_hash,
            device: device_response(device),
            device_identity_token: issued_token,
        }),
    ))
}

pub async fn get_device(
    Path((app_id, device_id)): Path<(String, String)>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    let device = store
        .get_device(&app_id, &device_id)
        .await
        .map_err(push_error)?
        .ok_or_else(|| AppError::NotFound("device not found".to_owned()))?;
    if push_capability(&headers)? != PushCapability::Admin {
        ensure_device_identity(&headers, &device)?;
    }
    Ok(Json(device_response(device)))
}

pub async fn list_devices(
    Path(app_id): Path<String>,
    Query(query): Query<PaginationQuery>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    let page = store
        .list_devices(
            &app_id,
            limit(query.limit)?,
            decode_cursor(query.cursor, &app_id)?,
        )
        .await
        .map_err(push_error)?;
    list_response(
        page.items.into_iter().map(device_response).collect(),
        page.next_cursor,
    )
}

pub async fn delete_device(
    Path((app_id, device_id)): Path<(String, String)>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    if push_capability(&headers)? != PushCapability::Admin {
        let device = store
            .get_device(&app_id, &device_id)
            .await
            .map_err(push_error)?
            .ok_or_else(|| AppError::NotFound("device not found".to_owned()))?;
        ensure_device_identity(&headers, &device)?;
    }
    store
        .delete_device(&app_id, &device_id)
        .await
        .map_err(push_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_devices_where(
    Path(app_id): Path<String>,
    Query(query): Query<RemoveDevicesQuery>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    let Some(client_id) = query.client_id else {
        return Err(AppError::InvalidInput(
            "removeWhere requires clientId".to_owned(),
        ));
    };
    audit_log(&app_id, "removeWhere", Some("clientId"));
    let deleted = delete_devices_by_client_paged(&app_id, &client_id, &store).await?;
    Ok(Json(json!({
        "deleted": deleted,
        "consistency": "eventual"
    })))
}

pub async fn upsert_channel_subscription(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(mut subscription): Json<ChannelSubscription>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_subscribe_or_admin(&headers)?;
    subscription.app_id = app_id;
    if push_capability(&headers)? != PushCapability::Admin {
        let device = store
            .get_device(&subscription.app_id, &subscription.device_id)
            .await
            .map_err(push_error)?
            .ok_or_else(|| AppError::NotFound("device not found".to_owned()))?;
        ensure_device_identity(&headers, &device)?;
    }
    store
        .upsert_subscription(subscription.clone())
        .await
        .map_err(push_error)?;
    Ok((StatusCode::CREATED, Json(subscription)))
}

pub async fn list_channel_subscriptions(
    Path(app_id): Path<String>,
    Query(query): Query<SubscriptionQuery>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    if push_capability(&headers)? != PushCapability::Admin {
        let Some(device_id) = query.device_id.as_deref() else {
            return Err(AppError::Forbidden(
                "push-subscribe can only list its own device subscriptions".to_owned(),
            ));
        };
        let device = store
            .get_device(&app_id, device_id)
            .await
            .map_err(push_error)?
            .ok_or_else(|| AppError::NotFound("device not found".to_owned()))?;
        ensure_device_identity(&headers, &device)?;
    }
    let cursor = decode_cursor(query.cursor, &app_id)?;
    let page = if let Some(channel) = query.channel {
        store
            .list_channel_subscribers(&app_id, &channel, limit(query.limit)?, cursor)
            .await
    } else if let Some(device_id) = query.device_id {
        store
            .list_device_channels(&app_id, &device_id, limit(query.limit)?, cursor)
            .await
    } else {
        store
            .list_subscriptions(&app_id, limit(query.limit)?, cursor)
            .await
    }
    .map_err(push_error)?;
    list_response(page.items, page.next_cursor)
}

pub async fn list_subscription_channels(
    Path(app_id): Path<String>,
    Query(query): Query<PaginationQuery>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    let page = store
        .list_subscription_channels(
            &app_id,
            limit(query.limit)?,
            decode_cursor(query.cursor, &app_id)?,
        )
        .await
        .map_err(push_error)?;
    list_response(page.items, page.next_cursor)
}

pub async fn delete_channel_subscriptions(
    Path(app_id): Path<String>,
    Query(query): Query<SubscriptionQuery>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    if push_capability(&headers)? != PushCapability::Admin {
        let Some(device_id) = query.device_id.as_deref() else {
            return Err(AppError::Forbidden(
                "push-subscribe can only remove its own device subscriptions".to_owned(),
            ));
        };
        let device = store
            .get_device(&app_id, device_id)
            .await
            .map_err(push_error)?
            .ok_or_else(|| AppError::NotFound("device not found".to_owned()))?;
        ensure_device_identity(&headers, &device)?;
    }
    let deleted = if let Some(channel) = query.channel {
        store
            .delete_subscriptions_by_channel(&app_id, &channel)
            .await
    } else if let Some(device_id) = query.device_id {
        store
            .delete_subscriptions_by_device(&app_id, &device_id)
            .await
    } else {
        return Err(AppError::InvalidInput(
            "delete requires channel or deviceId".to_owned(),
        ));
    }
    .map_err(push_error)?;
    Ok(Json(json!({ "deleted": deleted })))
}

struct PushPublishDeps {
    store: DynPushStore,
    queue: DynPushQueue,
    admission: Arc<PushAdmissionSnapshot>,
    admission_limiter: Arc<dyn RateLimiter + Send + Sync>,
}

impl PushPublishDeps {
    fn context(&self) -> PushPublishContext<'_> {
        PushPublishContext {
            store: &self.store,
            queue: &self.queue,
            admission: &self.admission,
            admission_limiter: Some(&self.admission_limiter),
        }
    }
}

#[derive(Clone, Copy)]
struct PushPublishContext<'a> {
    store: &'a DynPushStore,
    queue: &'a DynPushQueue,
    admission: &'a PushAdmissionSnapshot,
    admission_limiter: Option<&'a Arc<dyn RateLimiter + Send + Sync>>,
}

#[allow(clippy::too_many_arguments)]
pub async fn publish(
    Path(app_id): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Extension(queue): Extension<DynPushQueue>,
    Extension(admission): Extension<Arc<PushAdmissionSnapshot>>,
    Extension(admission_limiter): Extension<Arc<dyn RateLimiter + Send + Sync>>,
    Json(request): Json<PublishRequest>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    accept_publish(
        app_id,
        request,
        query.get("mode").is_some_and(|mode| mode == "sync"),
        headers,
        PushPublishDeps {
            store,
            queue,
            admission,
            admission_limiter,
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn batch_publish(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Extension(queue): Extension<DynPushQueue>,
    Extension(admission): Extension<Arc<PushAdmissionSnapshot>>,
    Extension(admission_limiter): Extension<Arc<dyn RateLimiter + Send + Sync>>,
    Json(requests): Json<Vec<PublishRequest>>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    let mut responses = Vec::with_capacity(requests.len());
    let context = PushPublishContext {
        store: &store,
        queue: &queue,
        admission: &admission,
        admission_limiter: Some(&admission_limiter),
    };
    for request in requests {
        let (_, Json(body), _) =
            accept_publish_inner(&app_id, request, false, &headers, context).await?;
        responses.push(body);
    }
    Ok((StatusCode::ACCEPTED, Json(json!({ "items": responses }))))
}

pub async fn get_publish_status(
    Path((app_id, publish_id)): Path<(String, String)>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    let status = store
        .get_publish_status(&app_id, &publish_id)
        .await
        .map_err(push_error)?
        .ok_or_else(|| AppError::NotFound("publish status not found".to_owned()))?;
    Ok(Json(status))
}

pub async fn delete_scheduled_job(
    Path((app_id, job_id)): Path<(String, String)>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    store
        .delete_scheduled_job(&app_id, &job_id)
        .await
        .map_err(push_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn post_delivery_status(
    Path(app_id): Path<String>,
    headers: HeaderMap,
    Extension(app): Extension<App>,
    Extension(store): Extension<DynPushStore>,
    Json(mut event): Json<DeliveryEvent>,
) -> Result<impl IntoResponse, AppError> {
    ensure_app_scope(&app_id, &app)?;
    ensure_push_admin(&headers)?;
    event.app_id = app_id;
    store
        .append_delivery_event(event.clone())
        .await
        .map_err(push_error)?;
    Ok((StatusCode::ACCEPTED, Json(event)))
}

async fn accept_publish(
    app_id: String,
    request: PublishRequest,
    sync_query: bool,
    headers: HeaderMap,
    deps: PushPublishDeps,
) -> Result<impl IntoResponse, AppError> {
    let (status, body, forced_async) =
        accept_publish_inner(&app_id, request, sync_query, &headers, deps.context()).await?;
    let mut headers = HeaderMap::new();
    if forced_async {
        headers.insert(
            "X-Sockudo-Forced-Async",
            HeaderValue::from_static("publish-api-is-async"),
        );
    }
    Ok((status, headers, body))
}

async fn accept_publish_inner(
    app_id: &str,
    request: PublishRequest,
    sync_query: bool,
    headers: &HeaderMap,
    context: PushPublishContext<'_>,
) -> Result<(StatusCode, Json<PublishAcceptedResponse>, bool), AppError> {
    let started = std::time::Instant::now();
    enforce_raw_recipient_auth(&request.recipients, headers)?;

    let publish_id = request
        .publish_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let sync_requested = request.sync || sync_query;
    let expected_recipients =
        expected_recipients(app_id, &request.recipients, context.store).await?;
    let fast_threshold = fanout_fast_threshold();
    let shard_size = fanout_shard_size();
    let fanout_regime = if expected_recipients < fast_threshold {
        FanoutRegime::FastPath
    } else {
        FanoutRegime::ShardPath
    };
    let forced_async = sync_requested;

    let mut intent = PublishIntent {
        app_id: app_id.to_owned(),
        publish_id: publish_id.clone(),
        targets: request.recipients.clone(),
        payload: request.payload.clone(),
        provider_overrides: request.provider_overrides.clone(),
        not_before_ms: request.not_before_ms,
        expires_at_ms: request.expires_at_ms,
    };
    intent
        .validate()
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    intent = resolve_publish_templates(context.store, intent).await?;
    intent
        .validate()
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;

    enforce_push_admission_capability(
        context.admission,
        &request.recipients,
        fanout_regime,
        context.queue,
    )
    .await?;
    enforce_publish_admission_rate(app_id, context.admission_limiter).await?;
    enforce_backpressure(context.queue, context.admission).await?;

    let quota_override = quota_override_requested(headers)?;
    if quota_override {
        audit_log(app_id, "quotaOverride", Some(&publish_id));
    }

    let rendered_payloads = render_all_payloads(&intent.payload, &intent.provider_overrides)?;
    let idempotency_key = intent.idempotency_key();
    let existing_idempotency = context
        .store
        .get_idempotency_record(app_id, &idempotency_key)
        .await
        .map_err(push_error)?;
    let existing_status = if let Some(existing) = existing_idempotency.as_ref() {
        context
            .store
            .get_publish_status(app_id, &existing.publish_id)
            .await
            .map_err(push_error)?
    } else {
        None
    };
    if let (Some(existing), Some(status)) = (existing_idempotency, existing_status) {
        return Ok((
            StatusCode::ACCEPTED,
            Json(PublishAcceptedResponse {
                publish_id: existing.publish_id,
                status: publish_state_label(status.state),
                expected_recipients: status.counters.planned,
                fanout_regime: status.fanout_regime.unwrap_or(fanout_regime),
                rendered_payloads,
            }),
            forced_async,
        ));
    }

    let quota_failure = if quota_override {
        None
    } else {
        quota_failure(
            app_id,
            expected_recipients,
            &request.recipients,
            context.store,
        )
        .await?
    };
    if let Some(reason) = quota_failure.as_deref() {
        PUSH_HTTP_METRICS.quota_acceptance_rejected(app_id);
        emit_push_meta_event(PushMetaEvent::quota_event(
            app_id,
            Some(&publish_id),
            reason,
        ));
        warn!(
            app_id = %app_id,
            publish_id = %publish_id,
            expected_recipients = expected_recipients,
            reason = reason,
            "push quota rejection"
        );
    }
    let status_state = if quota_failure.is_some() {
        PublishLifecycleState::QuotaExceeded
    } else {
        PublishLifecycleState::Queued
    };
    context
        .store
        .put_publish_status(PublishStatus {
            app_id: app_id.to_owned(),
            publish_id: publish_id.clone(),
            state: status_state,
            counters: PublishCounters {
                planned: expected_recipients,
                dispatched: 0,
                succeeded: 0,
                failed: 0,
                expired: 0,
                retry_scheduled: 0,
                retry_attempted: 0,
                dead_lettered: 0,
            },
            fanout_regime: Some(fanout_regime),
            retry_after_ms: None,
            error_reason: quota_failure.clone(),
        })
        .await
        .map_err(push_error)?;

    let idempotency = IdempotencyRecord {
        app_id: app_id.to_owned(),
        key: idempotency_key,
        publish_id: publish_id.clone(),
        expires_at_ms: request
            .expires_at_ms
            .unwrap_or_else(|| now_ms().saturating_add(PUSH_HTTP_DEFAULT_IDEMPOTENCY_TTL_MS)),
    };
    if !context
        .store
        .put_idempotency_record_if_absent(idempotency)
        .await
        .map_err(push_error)?
    {
        let existing = context
            .store
            .get_idempotency_record(app_id, &intent.idempotency_key())
            .await
            .map_err(push_error)?
            .ok_or_else(|| {
                AppError::InternalError(
                    "duplicate publish idempotency record disappeared".to_owned(),
                )
            })?;
        if let Some(status) = context
            .store
            .get_publish_status(app_id, &existing.publish_id)
            .await
            .map_err(push_error)?
        {
            return Ok((
                StatusCode::ACCEPTED,
                Json(PublishAcceptedResponse {
                    publish_id: existing.publish_id,
                    status: publish_state_label(status.state),
                    expected_recipients: status.counters.planned,
                    fanout_regime: status.fanout_regime.unwrap_or(fanout_regime),
                    rendered_payloads,
                }),
                forced_async,
            ));
        }
        return Err(AppError::InternalError(
            "duplicate publish is missing persisted status".to_owned(),
        ));
    }

    if status_state == PublishLifecycleState::QuotaExceeded {
        PUSH_HTTP_METRICS.publish_accepted(app_id, "quota_exceeded", started.elapsed());
        return Ok((
            StatusCode::ACCEPTED,
            Json(PublishAcceptedResponse {
                publish_id,
                status: "quota_exceeded",
                expected_recipients,
                fanout_regime,
                rendered_payloads,
            }),
            forced_async,
        ));
    }

    let publish_event = PublishLogEvent {
        app_id: app_id.to_owned(),
        publish_id: publish_id.clone(),
        event_id: uuid::Uuid::new_v4().to_string(),
        occurred_at_ms: now_ms(),
        intent,
        fanout_regime,
        expected_recipients,
        fast_threshold,
        shard_size,
    };
    context
        .store
        .append_publish_log_event(publish_event.clone())
        .await
        .map_err(push_error)?;
    context
        .queue
        .produce(
            PushQueueStage::PublishLog,
            publish_event.queue_key(),
            PushQueuePayload::PublishLog(Box::new(publish_event)),
        )
        .await
        .map_err(|error| AppError::InternalError(error.to_string()))?;
    PUSH_HTTP_METRICS.publish_accepted(app_id, "accepted", started.elapsed());
    PUSH_HTTP_METRICS.fanout_size(app_id, expected_recipients);
    emit_push_meta_event(PushMetaEvent::accepted(
        app_id,
        &publish_id,
        expected_recipients,
    ));
    info!(
        app_id = %app_id,
        publish_id = %publish_id,
        expected_recipients = expected_recipients,
        fanout_regime = ?fanout_regime,
        "push publish accepted"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(PublishAcceptedResponse {
            publish_id,
            status: "accepted",
            expected_recipients,
            fanout_regime,
            rendered_payloads,
        }),
        forced_async,
    ))
}

async fn put_credential(
    store: DynPushStore,
    credential: ProviderCredential,
) -> Result<impl IntoResponse, AppError> {
    audit_log(
        &credential.app_id,
        "credentialWrite",
        Some(&credential.credential_id),
    );
    store
        .put_credential(credential.clone())
        .await
        .map_err(push_error)?;
    store
        .append_operator_invalidation(OperatorInvalidationEvent {
            app_id: credential.app_id.clone(),
            event_id: uuid::Uuid::new_v4().to_string(),
            subject: format!(
                "credential:{:?}:{}:{}",
                credential.provider, credential.credential_id, credential.version
            )
            .to_ascii_lowercase(),
            occurred_at_ms: now_ms(),
        })
        .await
        .map_err(push_error)?;
    Ok((StatusCode::CREATED, Json(credential_response(credential))))
}

async fn expected_recipients(
    app_id: &str,
    recipients: &[PublishTarget],
    store: &DynPushStore,
) -> Result<u64, AppError> {
    let mut total = 0_u64;
    for recipient in recipients {
        total = total.saturating_add(match recipient {
            PublishTarget::Device { device_id } => store
                .get_device(app_id, device_id)
                .await
                .map_err(push_error)?
                .map(|_| 1)
                .unwrap_or(0),
            PublishTarget::Client { client_id } => {
                count_devices_by_client(app_id, client_id, store).await?
            }
            PublishTarget::Channel { channel } => count_channel(app_id, channel, store).await?,
            PublishTarget::Recipient { .. }
            | PublishTarget::ProviderTopic { .. }
            | PublishTarget::ProviderCondition { .. }
            | PublishTarget::RegisteredTopic { .. }
            | PublishTarget::UserTopic { .. }
            | PublishTarget::IndexedFilter { .. } => 1,
        });
    }
    Ok(total)
}

async fn delete_devices_by_client_paged(
    app_id: &str,
    client_id: &str,
    store: &DynPushStore,
) -> Result<u64, AppError> {
    let mut cursor = None;
    let mut deleted = 0_u64;
    loop {
        let page = store
            .list_devices(app_id, MAX_LIMIT, cursor)
            .await
            .map_err(push_error)?;
        for device in page
            .items
            .into_iter()
            .filter(|device| device.client_id.as_deref() == Some(client_id))
        {
            if store
                .delete_device(app_id, &device.id)
                .await
                .map_err(push_error)?
                .is_success()
            {
                deleted += 1;
            }
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(deleted);
        }
    }
}

async fn count_channel(app_id: &str, channel: &str, store: &DynPushStore) -> Result<u64, AppError> {
    let mut cursor = None;
    let mut total = 0_u64;
    loop {
        let page = store
            .list_channel_subscribers(app_id, channel, MAX_LIMIT, cursor)
            .await
            .map_err(push_error)?;
        total = total.saturating_add(page.items.len() as u64);
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(total);
        }
    }
}

async fn count_devices_by_client(
    app_id: &str,
    client_id: &str,
    store: &DynPushStore,
) -> Result<u64, AppError> {
    let mut cursor = None;
    let mut total = 0_u64;
    loop {
        let page = store
            .list_devices(app_id, MAX_LIMIT, cursor)
            .await
            .map_err(push_error)?;
        total = total.saturating_add(
            page.items
                .iter()
                .filter(|device| device.client_id.as_deref() == Some(client_id))
                .count() as u64,
        );
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(total);
        }
    }
}

fn render_all_payloads(
    payload: &PushPayload,
    overrides: &[ProviderOverridePayload],
) -> Result<Vec<RenderedProviderPayload>, AppError> {
    render_all_provider_payloads(payload, overrides)
        .map_err(|error| AppError::InvalidInput(error.to_string()))
}

async fn resolve_publish_templates(
    store: &DynPushStore,
    mut intent: PublishIntent,
) -> Result<PublishIntent, AppError> {
    let Some(template_id) = intent.payload.template_id.clone() else {
        return Ok(intent);
    };
    let template = store
        .get_template(&intent.app_id, &template_id)
        .await
        .map_err(push_error)?
        .ok_or_else(|| AppError::InvalidInput(format!("push template {template_id} not found")))?;
    let effective =
        resolve_template_payload(&template, &intent.payload, &intent.provider_overrides)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    intent.payload = effective.payload;
    intent.provider_overrides = effective.provider_overrides;
    Ok(intent)
}

fn device_response(device: DeviceDetails) -> DeviceResponse {
    let recipient = recipient_response(&device.push.recipient);
    DeviceResponse {
        app_id: device.app_id,
        id: device.id,
        client_id: device.client_id,
        form_factor: device.form_factor,
        platform: device.platform,
        timezone: device.timezone,
        locale: device.locale,
        last_active_at_ms: device.last_active_at_ms,
        push_state: device.push.state,
        push_failure_count: device.push.failure_count,
        recipient,
    }
}

fn recipient_response(recipient: &PushRecipient) -> DeviceRecipientResponse {
    let transport_type = match recipient {
        PushRecipient::Fcm { .. } => "gcm",
        PushRecipient::Apns { .. } => "apns",
        PushRecipient::Web { .. } => "web",
        PushRecipient::Hms { .. } => "hms",
        PushRecipient::Wns { .. } => "wns",
    };
    DeviceRecipientResponse {
        transport_type,
        provider: recipient.provider(),
        token_hash: recipient.token_hash(),
    }
}

fn credential_response(credential: ProviderCredential) -> CredentialResponse {
    CredentialResponse {
        app_id: credential.app_id,
        credential_id: credential.credential_id,
        provider: credential.provider,
        version: credential.version,
    }
}

fn ensure_app_scope(app_id: &str, app: &App) -> Result<(), AppError> {
    if app.id == app_id {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "authenticated app does not match path".to_owned(),
        ))
    }
}

fn ensure_push_admin(headers: &HeaderMap) -> Result<(), AppError> {
    if push_capability(headers)? == PushCapability::Admin {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "push-admin capability is required".to_owned(),
        ))
    }
}

fn ensure_push_subscribe_or_admin(headers: &HeaderMap) -> Result<(), AppError> {
    match push_capability(headers)? {
        PushCapability::Admin | PushCapability::Subscribe => Ok(()),
    }
}

fn push_capability(headers: &HeaderMap) -> Result<PushCapability, AppError> {
    let Some(raw) = headers
        .get(PUSH_CAPABILITY_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(PushCapability::Admin);
    };

    match raw {
        "push-admin" => Ok(PushCapability::Admin),
        "push-subscribe" => Ok(PushCapability::Subscribe),
        _ => Err(AppError::Forbidden(
            "unknown push capability requested".to_owned(),
        )),
    }
}

fn ensure_device_identity(headers: &HeaderMap, device: &DeviceDetails) -> Result<(), AppError> {
    let token = device_identity_token(headers)?;
    if verify_device_identity_token(token, &device.device_secret) {
        Ok(())
    } else {
        Err(AppError::ApiAuthFailed(
            "invalid deviceIdentityToken".to_owned(),
        ))
    }
}

fn device_identity_token(headers: &HeaderMap) -> Result<&str, AppError> {
    if let Some(value) = headers
        .get(DEVICE_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        return Ok(value);
    }

    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(AppError::ApiAuthFailed(
            "deviceIdentityToken is required".to_owned(),
        ));
    };

    value.strip_prefix("Bearer ").ok_or_else(|| {
        AppError::ApiAuthFailed("deviceIdentityToken bearer auth is required".to_owned())
    })
}

fn enforce_raw_recipient_auth(
    recipients: &[PublishTarget],
    headers: &HeaderMap,
) -> Result<(), AppError> {
    if recipients
        .iter()
        .any(|target| matches!(target, PublishTarget::Recipient { .. }))
    {
        ensure_push_admin(headers)?;
    }
    Ok(())
}

fn quota_override_requested(headers: &HeaderMap) -> Result<bool, AppError> {
    if headers.get(QUOTA_OVERRIDE_HEADER).is_none() {
        return Ok(false);
    }
    ensure_push_admin(headers)?;
    Ok(true)
}

async fn quota_failure(
    app_id: &str,
    expected_recipients: u64,
    recipients: &[PublishTarget],
    store: &DynPushStore,
) -> Result<Option<String>, AppError> {
    if let Some(max) = env_u64("PUSH_FANOUT_MAX")
        && expected_recipients > max
    {
        return Ok(Some(format!("fanout_max exceeded for app {app_id}")));
    }

    if let Some(max) = env_u64("PUSH_DELIVERY_QUOTA_DAILY")
        && expected_recipients > max
    {
        return Ok(Some(format!(
            "delivery_quota_daily exceeded for app {app_id}"
        )));
    }

    let provider_counts = provider_counts(app_id, recipients, store).await?;
    for (provider, count) in provider_counts {
        if let Some(max) = provider_ceiling(provider)
            && count > max
        {
            return Ok(Some(format!(
                "provider ceiling exceeded for {}",
                provider_env_key(provider)
            )));
        }
    }

    if let Some(reason) = enforce_per_device_rate_limits(app_id, recipients, store).await? {
        return Ok(Some(reason));
    }

    Ok(None)
}

async fn provider_counts(
    app_id: &str,
    recipients: &[PublishTarget],
    store: &DynPushStore,
) -> Result<BTreeMap<PushProviderKind, u64>, AppError> {
    let mut counts = BTreeMap::new();
    for recipient in recipients {
        match recipient {
            PublishTarget::Device { device_id } => {
                if let Some(device) = store
                    .get_device(app_id, device_id)
                    .await
                    .map_err(push_error)?
                {
                    increment_provider(&mut counts, device.push.recipient.provider(), 1);
                }
            }
            PublishTarget::Client { client_id } => {
                let mut cursor = None;
                loop {
                    let page = store
                        .list_devices(app_id, MAX_LIMIT, cursor)
                        .await
                        .map_err(push_error)?;
                    for device in &page.items {
                        if device.client_id.as_deref() == Some(client_id) {
                            increment_provider(&mut counts, device.push.recipient.provider(), 1);
                        }
                    }
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
            PublishTarget::Channel { channel } => {
                let mut cursor = None;
                loop {
                    let page = store
                        .list_channel_subscribers(app_id, channel, MAX_LIMIT, cursor)
                        .await
                        .map_err(push_error)?;
                    for subscription in &page.items {
                        increment_provider(&mut counts, subscription.provider, 1);
                    }
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
            }
            PublishTarget::Recipient { recipient } => {
                increment_provider(&mut counts, recipient.provider(), 1);
            }
            PublishTarget::ProviderTopic { provider, .. }
            | PublishTarget::ProviderCondition { provider, .. } => {
                increment_provider(&mut counts, *provider, 1);
            }
            PublishTarget::RegisteredTopic { .. }
            | PublishTarget::UserTopic { .. }
            | PublishTarget::IndexedFilter { .. } => {}
        }
    }
    Ok(counts)
}

async fn enforce_per_device_rate_limits(
    app_id: &str,
    recipients: &[PublishTarget],
    store: &DynPushStore,
) -> Result<Option<String>, AppError> {
    for recipient in recipients {
        let PublishTarget::Device { device_id } = recipient else {
            continue;
        };
        let Some(device) = store
            .get_device(app_id, device_id)
            .await
            .map_err(push_error)?
        else {
            continue;
        };
        let Some(policy) = device.push_rate_policy else {
            continue;
        };
        let key = format!("{app_id}:{device_id}");
        let second = now_ms() / 1000;
        let mut windows = PUSH_DEVICE_RATE_WINDOWS.lock().map_err(|_| {
            AppError::InternalError("push device quota state lock poisoned".to_owned())
        })?;
        let window = windows.entry(key).or_default();
        if window.second != second {
            *window = RateWindow { second, count: 0 };
        }
        if window.count >= u64::from(policy.capacity) {
            return Ok(Some(format!(
                "per-device rate limit exceeded for device {device_id}"
            )));
        }
        window.count += 1;
    }
    Ok(None)
}

fn increment_provider(
    counts: &mut BTreeMap<PushProviderKind, u64>,
    provider: PushProviderKind,
    count: u64,
) {
    *counts.entry(provider).or_default() += count;
}

fn provider_ceiling(provider: PushProviderKind) -> Option<u64> {
    env_u64(&format!(
        "PUSH_PROVIDER_{}_CEILING",
        provider_env_key(provider)
    ))
    .or_else(|| {
        env_u64(&format!(
            "PUSH_PROVIDER_{}_DAILY_QUOTA",
            provider_env_key(provider)
        ))
    })
}

fn provider_env_key(provider: PushProviderKind) -> &'static str {
    match provider {
        PushProviderKind::Fcm => "FCM",
        PushProviderKind::Apns => "APNS",
        PushProviderKind::WebPush => "WEBPUSH",
        PushProviderKind::Hms => "HMS",
        PushProviderKind::Wns => "WNS",
    }
}

async fn enforce_publish_admission_rate(
    app_id: &str,
    admission_limiter: Option<&Arc<dyn RateLimiter + Send + Sync>>,
) -> Result<(), AppError> {
    let Some(admission_limiter) = admission_limiter else {
        return Ok(());
    };
    let key = format!("push:acceptance:{app_id}");
    let result = admission_limiter.increment(&key).await.map_err(|error| {
        warn!(
            app_id = %app_id,
            error = %error,
            "push acceptance rate limiter backend unavailable"
        );
        AppError::Backpressure {
            message: "push acceptance rate limiter unavailable".to_owned(),
            retry_after_seconds: env_u64("PUSH_BACKPRESSURE_RETRY_AFTER_SECONDS")
                .unwrap_or(DEFAULT_PUSH_BACKPRESSURE_RETRY_AFTER_SECONDS),
        }
    })?;
    if !result.allowed {
        PUSH_HTTP_METRICS.rate_dropped(app_id);
        emit_push_meta_event(PushMetaEvent::quota_event(
            app_id,
            None,
            "push.acceptance_rate_limit exceeded",
        ));
        warn!(
            app_id = %app_id,
            limit = result.limit,
            reset_after = result.reset_after,
            "push acceptance quota rejection"
        );
        return Err(AppError::TooManyRequests {
            message: "push.acceptance_rate_limit exceeded".to_owned(),
            retry_after_seconds: result.reset_after.max(1),
        });
    }
    PUSH_HTTP_METRICS.quota_consumed(
        app_id,
        u64::from(result.limit.saturating_sub(result.remaining)),
        0,
    );
    PUSH_HTTP_METRICS.rate_queued(app_id);
    Ok(())
}

async fn enforce_push_admission_capability(
    admission: &PushAdmissionSnapshot,
    recipients: &[PublishTarget],
    fanout_regime: FanoutRegime,
    queue: &DynPushQueue,
) -> Result<(), AppError> {
    if let Some(rejection) = admission.rejection_for_targets(recipients, fanout_regime) {
        warn!(
            storage_driver = ?admission.storage_driver(),
            queue_driver = ?admission.queue_driver(),
            safe_for_admission = admission.safe_for_admission(),
            reason = %rejection.message(),
            "push publish rejected by admission capability guard"
        );
        return Err(AppError::ServiceUnavailable(rejection.message()));
    }

    let health = queue.health().await.map_err(|error| {
        let rejection = PushAdmissionRejection::QueueUnavailable {
            reason: error.to_string(),
        };
        warn!(
            queue_driver = ?admission.queue_driver(),
            reason = %rejection.message(),
            "push publish rejected because push queue health check failed"
        );
        AppError::ServiceUnavailable(rejection.message())
    })?;
    if !health.healthy {
        let rejection = PushAdmissionRejection::QueueUnavailable {
            reason: health.details,
        };
        warn!(
            backend = ?health.backend,
            reason = %rejection.message(),
            "push publish rejected because push queue is unhealthy"
        );
        return Err(AppError::ServiceUnavailable(rejection.message()));
    }

    Ok(())
}

async fn enforce_backpressure(
    queue: &DynPushQueue,
    admission: &PushAdmissionSnapshot,
) -> Result<(), AppError> {
    if env_bool("PUSH_BACKPRESSURE") {
        warn!("push publish rejected by configured backpressure");
        return Err(AppError::TooManyRequests {
            message: "push pipeline is applying backpressure".to_owned(),
            retry_after_seconds: env_u64("PUSH_BACKPRESSURE_RETRY_AFTER_SECONDS")
                .unwrap_or(DEFAULT_PUSH_BACKPRESSURE_RETRY_AFTER_SECONDS),
        });
    }
    let max_publish_log_lag =
        env_u64("PUSH_PUBLISH_LOG_MAX_LAG").unwrap_or(DEFAULT_PUSH_CRITICAL_QUEUE_MAX_LAG);
    reject_on_stage_lag(queue, PushQueueStage::PublishLog, max_publish_log_lag).await?;

    let max_critical_lag =
        env_u64("PUSH_CRITICAL_QUEUE_MAX_LAG").unwrap_or(DEFAULT_PUSH_CRITICAL_QUEUE_MAX_LAG);
    for stage in critical_backpressure_stages(admission) {
        reject_on_stage_lag(queue, stage, max_critical_lag).await?;
    }

    Ok(())
}

async fn reject_on_stage_lag(
    queue: &DynPushQueue,
    stage: PushQueueStage,
    max_lag: u64,
) -> Result<(), AppError> {
    let lag = queue
        .lag(stage)
        .await
        .map_err(|error| AppError::InternalError(error.to_string()))?;
    if stage == PushQueueStage::PublishLog {
        PUSH_HTTP_METRICS.publish_log_lag_seconds(lag.ready_depth as f64);
    }
    if lag.ready_depth.saturating_add(lag.delayed_depth) >= max_lag {
        warn!(
            stage = %queue_stage_label(stage),
            ready_depth = lag.ready_depth,
            delayed_depth = lag.delayed_depth,
            max_lag = max_lag,
            "push publish rejected by queue-stage backpressure"
        );
        return Err(AppError::TooManyRequests {
            message: format!("push {} lag exceeded", queue_stage_label(stage)),
            retry_after_seconds: env_u64("PUSH_BACKPRESSURE_RETRY_AFTER_SECONDS")
                .unwrap_or(DEFAULT_PUSH_BACKPRESSURE_RETRY_AFTER_SECONDS),
        });
    }
    Ok(())
}

fn critical_backpressure_stages(admission: &PushAdmissionSnapshot) -> Vec<PushQueueStage> {
    let mut stages = Vec::with_capacity(4 + admission.active_providers().len());
    stages.push(PushQueueStage::ShardJobs);
    stages.push(PushQueueStage::DeliveryResults);
    stages.push(PushQueueStage::RetrySchedule);
    stages.push(PushQueueStage::DeadLetters);
    stages.extend(
        admission
            .active_providers()
            .into_iter()
            .map(PushQueueStage::DeliveryJobs),
    );
    stages
}

fn queue_stage_label(stage: PushQueueStage) -> String {
    match stage {
        PushQueueStage::PublishLog => "publish_log".to_owned(),
        PushQueueStage::ShardJobs => "shard_jobs".to_owned(),
        PushQueueStage::DeliveryJobs(provider) => {
            format!(
                "delivery_jobs_{}",
                provider_env_key(provider).to_ascii_lowercase()
            )
        }
        PushQueueStage::DeliveryResults => "delivery_results".to_owned(),
        PushQueueStage::DeadLetters => "dead_letters".to_owned(),
        PushQueueStage::RetrySchedule => "retry_schedule".to_owned(),
    }
}

fn audit_log(app_id: &str, action: &'static str, subject: Option<&str>) {
    info!(
        target: "sockudo_push_audit",
        app_id = %app_id,
        action = action,
        subject = subject.unwrap_or("[none]"),
        "push audit event"
    );
}

fn header_bool(headers: &HeaderMap, name: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|raw| matches!(raw, "1" | "true" | "TRUE" | "yes"))
}

fn limit(raw: Option<usize>) -> Result<usize, AppError> {
    let limit = raw.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 || limit > MAX_LIMIT {
        return Err(AppError::InvalidInput(
            "limit must be between 1 and 1000".to_owned(),
        ));
    }
    Ok(limit)
}

fn decode_cursor(raw: Option<String>, app_id: &str) -> Result<Option<PushCursor>, AppError> {
    raw.map(|cursor| PushCursor::decode(&cursor, app_id))
        .transpose()
        .map_err(|error| AppError::InvalidInput(error.to_string()))
}

fn list_response<T: Serialize>(
    items: Vec<T>,
    next_cursor: Option<PushCursor>,
) -> Result<impl IntoResponse, AppError> {
    let encoded = next_cursor
        .as_ref()
        .map(PushCursor::encode)
        .transpose()
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(Json(ListResponse {
        items,
        has_more: encoded.is_some(),
        next_cursor: encoded,
    }))
}

fn encrypted_json(value: &Value) -> Result<EncryptedSecret, AppError> {
    encrypted_secret(&sonic_rs::to_string(value).map_err(|error| {
        AppError::InvalidInput(format!("credential material must be valid JSON: {error}"))
    })?)
}

fn encrypt_optional(raw: Option<String>) -> Result<Option<EncryptedSecret>, AppError> {
    raw.as_deref().map(encrypted_secret).transpose()
}

fn encrypted_secret(raw: &str) -> Result<EncryptedSecret, AppError> {
    SecretString::new(raw).map_err(|error| AppError::InvalidInput(error.to_string()))?;
    let key = credential_encryption_key().ok_or_else(|| {
        AppError::InvalidInput(
            "PUSH_CREDENTIAL_ENCRYPTION_KEY must be configured before storing push credentials"
                .to_string(),
        )
    })?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|error| AppError::InternalError(format!("invalid credential key: {error}")))?;
    let nonce_bytes = credential_nonce_bytes();
    let encrypted = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), raw.as_bytes())
        .map_err(|error| {
            AppError::InternalError(format!("failed to encrypt credential material: {error}"))
        })?;
    let mut envelope = Vec::with_capacity(nonce_bytes.len() + encrypted.len());
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend_from_slice(&encrypted);
    let ciphertext = format!(
        "{CREDENTIAL_SECRET_AES_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(envelope)
    );
    EncryptedSecret::new(ciphertext).map_err(|error| AppError::InvalidInput(error.to_string()))
}

pub(crate) fn decrypt_credential_secret(secret: &EncryptedSecret) -> Result<String, String> {
    let envelope = secret.ciphertext();
    if let Some(encoded) = envelope.strip_prefix(CREDENTIAL_SECRET_LOCAL_PREFIX) {
        let plaintext = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|error| format!("invalid local credential envelope: {error}"))?;
        return String::from_utf8(plaintext)
            .map_err(|error| format!("credential material is not valid UTF-8: {error}"));
    }

    if let Some(encoded) = envelope.strip_prefix(CREDENTIAL_SECRET_AES_PREFIX) {
        let key = credential_encryption_key().ok_or_else(|| {
            "PUSH_CREDENTIAL_ENCRYPTION_KEY is required to decrypt stored credential material"
                .to_owned()
        })?;
        let sealed = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|error| format!("invalid encrypted credential envelope: {error}"))?;
        let (nonce_bytes, ciphertext) = sealed
            .split_at_checked(12)
            .ok_or_else(|| "encrypted credential envelope is too short".to_owned())?;
        let cipher =
            Aes256Gcm::new_from_slice(&key).map_err(|error| format!("invalid key: {error}"))?;
        let plaintext = cipher
            .decrypt(Nonce::from_slice(nonce_bytes), ciphertext)
            .map_err(|error| format!("failed to decrypt credential material: {error}"))?;
        return String::from_utf8(plaintext)
            .map_err(|error| format!("credential material is not valid UTF-8: {error}"));
    }

    if envelope.starts_with(CREDENTIAL_SECRET_LEGACY_HASH_PREFIX) {
        return Err(
            "credential was stored with a legacy non-decryptable hash envelope; re-upload it"
                .to_owned(),
        );
    }

    Err("unsupported credential envelope".to_owned())
}

fn credential_encryption_key() -> Option<[u8; 32]> {
    env::var("PUSH_CREDENTIAL_ENCRYPTION_KEY")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(|value| Sha256::digest(value.as_bytes()).into())
}

fn credential_nonce_bytes() -> [u8; 12] {
    let mut nonce = [0_u8; 12];
    nonce.copy_from_slice(&uuid::Uuid::new_v4().as_bytes()[..12]);
    nonce
}

fn push_error(error: sockudo_push::PushStorageError) -> AppError {
    AppError::InternalError(error.to_string())
}

fn fanout_fast_threshold() -> u64 {
    env_u64("PUSH_FANOUT_FAST_THRESHOLD").unwrap_or(DEFAULT_PUSH_FANOUT_FAST_THRESHOLD)
}

fn fanout_shard_size() -> u64 {
    env_u64("PUSH_FANOUT_SHARD_SIZE").unwrap_or(DEFAULT_PUSH_FANOUT_SHARD_SIZE)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .is_some_and(|raw| matches!(raw.as_str(), "1" | "true" | "TRUE" | "yes"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub async fn enqueue_v2_channel_push_from_extras(
    app_id: &str,
    channel: &str,
    extras_push: &sonic_rs::Value,
    store: &DynPushStore,
    queue: &DynPushQueue,
    admission: &PushAdmissionSnapshot,
) -> Result<Option<String>, AppError> {
    let payload: PushPayload = sonic_rs::from_slice(
        &sonic_rs::to_vec(extras_push)
            .map_err(|error| AppError::InvalidInput(format!("invalid extras.push: {error}")))?,
    )
    .map_err(|error| AppError::InvalidInput(format!("invalid extras.push: {error}")))?;
    let publish_id = uuid::Uuid::new_v4().to_string();
    let request = PublishRequest {
        publish_id: Some(publish_id.clone()),
        recipients: vec![PublishTarget::Channel {
            channel: channel.to_owned(),
        }],
        payload,
        provider_overrides: vec![],
        sync: false,
        not_before_ms: None,
        expires_at_ms: None,
    };
    let headers = HeaderMap::new();
    let _ = accept_publish_inner(
        app_id,
        request,
        false,
        &headers,
        PushPublishContext {
            store,
            queue,
            admission,
            admission_limiter: None,
        },
    )
    .await?;
    PUSH_HTTP_METRICS.channel_publish(channel);
    Ok(Some(publish_id))
}

pub fn build_channel_push_rule_requests(
    app_id: &str,
    event_name: Option<&str>,
    message_data: Option<&ApiMessageData>,
    channels: &[String],
    rules: &[PushRuleConfig],
) -> Result<Vec<PublishRequest>, AppError> {
    if rules.is_empty() || channels.is_empty() {
        return Ok(Vec::new());
    }
    let Some(event_name) = event_name else {
        return Ok(Vec::new());
    };

    let mut requests = Vec::new();
    for (rule_index, rule_config) in rules.iter().enumerate() {
        if !rule_config.enabled {
            continue;
        }
        for channel in channels {
            if !rule_config_matches(rule_config, channel, event_name) {
                continue;
            }
            enforce_push_rule_rate(app_id, rule_index, rule_config.rate_limit_per_second)?;
            let data = message_data_to_json(message_data)?;
            let rule = channel_push_rule_from_config(rule_config);
            let payload = rule
                .map_payload(&data)
                .map_err(|error| AppError::InvalidInput(error.to_string()))?;
            requests.push(PublishRequest {
                publish_id: Some(uuid::Uuid::new_v4().to_string()),
                recipients: vec![PublishTarget::Channel {
                    channel: channel.clone(),
                }],
                payload,
                provider_overrides: vec![],
                sync: false,
                not_before_ms: None,
                expires_at_ms: None,
            });
        }
    }
    Ok(requests)
}

pub fn spawn_channel_push_rule_requests(
    app_id: String,
    requests: Vec<PublishRequest>,
    store: DynPushStore,
    queue: DynPushQueue,
    admission: Arc<PushAdmissionSnapshot>,
) {
    for request in requests {
        let app_id = app_id.clone();
        let store = store.clone();
        let queue = queue.clone();
        let admission = admission.clone();
        tokio::spawn(async move {
            let headers = HeaderMap::new();
            let publish_id = request.publish_id.clone().unwrap_or_default();
            let channel = request
                .recipients
                .iter()
                .find_map(|target| match target {
                    PublishTarget::Channel { channel } => Some(channel.as_str()),
                    _ => None,
                })
                .unwrap_or("<unknown>")
                .to_owned();
            match accept_publish_inner(
                &app_id,
                request,
                false,
                &headers,
                PushPublishContext {
                    store: &store,
                    queue: &queue,
                    admission: &admission,
                    admission_limiter: None,
                },
            )
            .await
            {
                Ok(_) => {
                    PUSH_HTTP_METRICS.channel_publish(&channel);
                }
                Err(error) => {
                    warn!(
                        app_id = %app_id,
                        publish_id = %publish_id,
                        channel = %channel,
                        error = %error,
                        "channel push rule enqueue failed"
                    );
                }
            }
        });
    }
}

fn rule_config_matches(rule: &PushRuleConfig, channel: &str, event: &str) -> bool {
    channel_pattern_matches(&rule.channel_pattern, channel)
        && rule
            .event_filter
            .iter()
            .any(|candidate| candidate.as_str() == event)
}

fn channel_pattern_matches(pattern: &str, channel: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return channel.starts_with(prefix);
    }
    pattern == channel
}

fn channel_push_rule_from_config(config: &PushRuleConfig) -> ChannelPushRule {
    ChannelPushRule {
        enabled: config.enabled,
        channel_pattern: config.channel_pattern.clone(),
        event_filter: config.event_filter.clone(),
        payload_mapping: payload_mapping_from_config(&config.payload_mapping),
        rate_limit_per_second: config.rate_limit_per_second,
    }
}

fn payload_mapping_from_config(config: &PushRulePayloadMappingConfig) -> PushRulePayloadMapping {
    PushRulePayloadMapping {
        title_field: config.title_field.clone(),
        body_field: config.body_field.clone(),
        template_data_field: config.template_data_field.clone(),
        include_remaining_fields: config.include_remaining_fields,
    }
}

fn message_data_to_json(message_data: Option<&ApiMessageData>) -> Result<Value, AppError> {
    match message_data {
        Some(ApiMessageData::Json(value)) => Ok(value.clone()),
        Some(ApiMessageData::String(raw)) => sonic_rs::from_str(raw)
            .map_err(|error| AppError::InvalidInput(format!("invalid push rule data: {error}"))),
        None => Err(AppError::InvalidInput(
            "push rule message data is required".to_owned(),
        )),
    }
}

fn enforce_push_rule_rate(
    app_id: &str,
    rule_index: usize,
    limit_per_second: u64,
) -> Result<(), AppError> {
    let second = now_ms() / 1_000;
    let key = format!("{app_id}:{rule_index}");
    let mut windows = PUSH_RULE_RATE_WINDOWS.lock().map_err(|_| {
        AppError::InternalError("push rule rate limiter lock is poisoned".to_owned())
    })?;
    let window = windows.entry(key).or_default();
    if window.second != second {
        window.second = second;
        window.count = 0;
    }
    if window.count >= limit_per_second {
        return Err(AppError::TooManyRequests {
            message: "push rule rate limit exceeded".to_owned(),
            retry_after_seconds: 1,
        });
    }
    window.count = window.count.saturating_add(1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use sockudo_push::{
        DevicePushDetails, DevicePushState, FormFactor, IdempotencyRecord, MemoryPushQueue,
        MemoryPushStore, NotificationTemplate, Platform, PublishIntent, PublishLifecycleState,
        PushDeviceStore, PushIdempotencyStore, PushPublishLogStore, PushPublishStatusStore,
        PushQueue, PushRecipient, PushTemplateStore, TemplateContent,
    };
    use sonic_rs::{JsonValueTrait, json};
    use std::sync::{Arc, Mutex};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn test_publish_context<'a>(
        store: &'a DynPushStore,
        queue: &'a DynPushQueue,
        admission: &'a PushAdmissionSnapshot,
    ) -> PushPublishContext<'a> {
        PushPublishContext {
            store,
            queue,
            admission,
            admission_limiter: None,
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &'static str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(previous) = self.previous.as_ref() {
                    std::env::set_var(self.key, previous);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    fn hashed_token(raw: &str) -> SecretString {
        hash_device_identity_token(&SecretString::new(raw).unwrap())
    }

    fn sample_device() -> DeviceDetails {
        DeviceDetails {
            app_id: "app-1".to_owned(),
            id: "device-1".to_owned(),
            client_id: Some("client-1".to_owned()),
            form_factor: FormFactor::Phone,
            platform: Platform::Android,
            metadata: json!({"token": "must-not-leak"}),
            device_secret: hashed_token("device-token"),
            timezone: "UTC".to_owned(),
            locale: "en".to_owned(),
            last_active_at_ms: 1,
            push: DevicePushDetails {
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new("provider-token").unwrap(),
                },
                state: DevicePushState::Active,
                failure_count: 0,
                error_reason: None,
            },
            push_rate_policy: None,
        }
    }

    fn sample_payload() -> PushPayload {
        PushPayload {
            template_id: None,
            template_data: json!({"kind": "test"}),
            title: Some("Hello".to_owned()),
            body: Some("World".to_owned()),
            icon: None,
            sound: None,
            collapse_key: None,
        }
    }

    fn sample_publish_request(publish_id: &str) -> PublishRequest {
        PublishRequest {
            publish_id: Some(publish_id.to_owned()),
            recipients: vec![PublishTarget::Device {
                device_id: "device-1".to_owned(),
            }],
            payload: sample_payload(),
            provider_overrides: vec![],
            sync: false,
            not_before_ms: None,
            expires_at_ms: None,
        }
    }

    fn idempotency_record_for_request(
        app_id: &str,
        publish_id: &str,
        request: &PublishRequest,
    ) -> IdempotencyRecord {
        let intent = PublishIntent {
            app_id: app_id.to_owned(),
            publish_id: publish_id.to_owned(),
            targets: request.recipients.clone(),
            payload: request.payload.clone(),
            provider_overrides: request.provider_overrides.clone(),
            not_before_ms: request.not_before_ms,
            expires_at_ms: request.expires_at_ms,
        };
        IdempotencyRecord {
            app_id: app_id.to_owned(),
            key: intent.idempotency_key(),
            publish_id: publish_id.to_owned(),
            expires_at_ms: request.expires_at_ms.unwrap_or(u64::MAX),
        }
    }

    fn sample_queue_payload(publish_id: &str) -> PushQueuePayload {
        let request = sample_publish_request(publish_id);
        let intent = PublishIntent {
            app_id: "app-1".to_owned(),
            publish_id: publish_id.to_owned(),
            targets: request.recipients,
            payload: request.payload,
            provider_overrides: request.provider_overrides,
            not_before_ms: request.not_before_ms,
            expires_at_ms: request.expires_at_ms,
        };
        PushQueuePayload::PublishLog(Box::new(PublishLogEvent {
            app_id: "app-1".to_owned(),
            publish_id: publish_id.to_owned(),
            event_id: format!("event-{publish_id}"),
            occurred_at_ms: 1,
            intent,
            fanout_regime: FanoutRegime::FastPath,
            expected_recipients: 1,
            fast_threshold: 10,
            shard_size: 100,
        }))
    }

    async fn assert_stage_backpressure(stage: PushQueueStage, env_name: &'static str) {
        let _guard = PUSH_TEST_ENV_LOCK.lock().await;
        let _threshold = EnvVarGuard::set(env_name, "1");
        let queue = Arc::new(MemoryPushQueue::new());
        queue
            .produce(
                stage,
                format!("key-{stage:?}"),
                sample_queue_payload(&format!("publish-{stage:?}")),
            )
            .await
            .unwrap();
        let dyn_queue: DynPushQueue = queue;
        let admission = PushAdmissionSnapshot::testing_active([PushProviderKind::Fcm]);

        let error = enforce_backpressure(&dyn_queue, &admission)
            .await
            .unwrap_err();

        assert!(matches!(error, AppError::TooManyRequests { .. }));
    }

    #[test]
    fn push_capability_header_distinguishes_admin_and_subscribe() {
        let mut headers = HeaderMap::new();
        assert_eq!(push_capability(&headers).unwrap(), PushCapability::Admin);

        headers.insert(
            PUSH_CAPABILITY_HEADER,
            HeaderValue::from_static("push-subscribe"),
        );
        assert_eq!(
            push_capability(&headers).unwrap(),
            PushCapability::Subscribe
        );
        assert!(ensure_push_admin(&headers).is_err());
    }

    #[test]
    fn sonic_json_extractor_accepts_json_media_types_only() {
        let mut headers = HeaderMap::new();
        assert!(!is_json_content_type(&headers));

        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert!(!is_json_content_type(&headers));

        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        assert!(is_json_content_type(&headers));

        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.sockudo+json"),
        );
        assert!(is_json_content_type(&headers));
    }

    #[test]
    fn device_identity_header_verifies_hashed_device_secret() {
        let device = sample_device();
        let mut headers = HeaderMap::new();
        headers.insert(
            DEVICE_TOKEN_HEADER,
            HeaderValue::from_static("device-token"),
        );

        assert!(ensure_device_identity(&headers, &device).is_ok());

        headers.insert(DEVICE_TOKEN_HEADER, HeaderValue::from_static("wrong"));
        assert!(ensure_device_identity(&headers, &device).is_err());
    }

    #[test]
    fn device_response_redacts_secrets_and_endpoints() {
        let response: Value =
            sonic_rs::from_slice(&sonic_rs::to_vec(&device_response(sample_device())).unwrap())
                .unwrap();

        assert_eq!(
            response["recipient"]["tokenHash"].as_str().unwrap().len(),
            64
        );
        let encoded = response.to_string();
        assert!(!encoded.contains("provider-token"));
        assert!(!encoded.contains("device-token"));
        assert!(!encoded.contains("must-not-leak"));
    }

    #[test]
    fn encrypted_secret_requires_encryption_key_for_new_writes() {
        let _guard = ENV_LOCK.lock().unwrap();
        let previous = std::env::var("PUSH_CREDENTIAL_ENCRYPTION_KEY").ok();
        unsafe { std::env::remove_var("PUSH_CREDENTIAL_ENCRYPTION_KEY") };

        let error = encrypted_secret("provider-secret").unwrap_err();

        if let Some(previous) = previous {
            unsafe { std::env::set_var("PUSH_CREDENTIAL_ENCRYPTION_KEY", previous) };
        } else {
            unsafe { std::env::remove_var("PUSH_CREDENTIAL_ENCRYPTION_KEY") };
        }
        assert!(
            error
                .to_string()
                .contains("PUSH_CREDENTIAL_ENCRYPTION_KEY must be configured")
        );
    }

    #[test]
    fn encrypted_secret_writes_aes_envelope_when_key_is_configured() {
        let _guard = ENV_LOCK.lock().unwrap();
        let previous = std::env::var("PUSH_CREDENTIAL_ENCRYPTION_KEY").ok();
        unsafe { std::env::set_var("PUSH_CREDENTIAL_ENCRYPTION_KEY", "test-key") };

        let secret = encrypted_secret("provider-secret").unwrap();
        let decrypted = decrypt_credential_secret(&secret).unwrap();

        if let Some(previous) = previous {
            unsafe { std::env::set_var("PUSH_CREDENTIAL_ENCRYPTION_KEY", previous) };
        } else {
            unsafe { std::env::remove_var("PUSH_CREDENTIAL_ENCRYPTION_KEY") };
        }
        assert!(
            secret
                .ciphertext()
                .starts_with(CREDENTIAL_SECRET_AES_PREFIX)
        );
        assert_eq!(decrypted, "provider-secret");
    }

    #[test]
    fn retry_after_errors_set_retry_after_header() {
        let response = AppError::TooManyRequests {
            message: "push.acceptance_rate_limit exceeded".to_owned(),
            retry_after_seconds: 7,
        }
        .into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).unwrap(),
            HeaderValue::from_static("7")
        );
    }

    #[tokio::test]
    async fn remove_where_deletes_matching_devices_through_paged_registry_scan() {
        let store = Arc::new(MemoryPushStore::new());
        for index in 0..3 {
            let mut device = sample_device();
            device.id = format!("target-{index}");
            store.upsert_device(device).await.unwrap();
        }
        let mut other = sample_device();
        other.id = "other-1".to_owned();
        other.client_id = Some("other-client".to_owned());
        store.upsert_device(other).await.unwrap();

        let dyn_store: DynPushStore = store.clone();
        let deleted = delete_devices_by_client_paged("app-1", "client-1", &dyn_store)
            .await
            .unwrap();

        assert_eq!(deleted, 3);
        assert!(
            store
                .get_device("app-1", "other-1")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn publish_admission_uses_injected_rate_limiter() {
        let limiter: Arc<dyn RateLimiter + Send + Sync> =
            Arc::new(sockudo_rate_limiter::memory_limiter::MemoryRateLimiter::new(1, 60));

        enforce_publish_admission_rate("app-1", Some(&limiter))
            .await
            .unwrap();

        let error = enforce_publish_admission_rate("app-1", Some(&limiter))
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::TooManyRequests { .. }));
    }

    #[tokio::test]
    async fn publish_admission_skips_internal_without_limiter() {
        enforce_publish_admission_rate("app-1", None).await.unwrap();
    }

    #[tokio::test]
    async fn quota_rejection_persists_terminal_status_without_pipeline_work() {
        let _guard = PUSH_TEST_ENV_LOCK.lock().await;
        let _quota = EnvVarGuard::set("PUSH_FANOUT_MAX", "0");
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.upsert_device(sample_device()).await.unwrap();
        let dyn_store: DynPushStore = store.clone();
        let dyn_queue: DynPushQueue = queue.clone();
        let admission = PushAdmissionSnapshot::testing_active([PushProviderKind::Fcm]);

        let request = sample_publish_request("quota-publish-1");
        let (status, Json(body), _) = accept_publish_inner(
            "app-1",
            request,
            false,
            &HeaderMap::new(),
            test_publish_context(&dyn_store, &dyn_queue, &admission),
        )
        .await
        .unwrap();

        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body.status, "quota_exceeded");
        let status = store
            .get_publish_status("app-1", "quota-publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.state, PublishLifecycleState::QuotaExceeded);
        assert_eq!(status.counters.planned, 1);
        let idempotency = idempotency_record_for_request(
            "app-1",
            "quota-publish-1",
            &sample_publish_request("quota-publish-1"),
        );
        assert!(
            store
                .get_idempotency_record("app-1", &idempotency.key)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .list_publish_log_events("app-1", 10, None)
                .await
                .unwrap()
                .items
                .is_empty()
        );
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .ready_depth,
            0
        );
    }

    #[tokio::test]
    async fn quota_rejection_idempotent_replay_does_not_enqueue_pipeline_work() {
        let _guard = PUSH_TEST_ENV_LOCK.lock().await;
        let _quota = EnvVarGuard::set("PUSH_FANOUT_MAX", "0");
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.upsert_device(sample_device()).await.unwrap();
        let dyn_store: DynPushStore = store.clone();
        let dyn_queue: DynPushQueue = queue.clone();
        let admission = PushAdmissionSnapshot::testing_active([PushProviderKind::Fcm]);

        for _ in 0..2 {
            let (_, Json(body), _) = accept_publish_inner(
                "app-1",
                sample_publish_request("quota-publish-replay"),
                false,
                &HeaderMap::new(),
                test_publish_context(&dyn_store, &dyn_queue, &admission),
            )
            .await
            .unwrap();
            assert_eq!(body.publish_id, "quota-publish-replay");
            assert_eq!(body.status, "quota_exceeded");
        }

        assert!(
            store
                .list_publish_log_events("app-1", 10, None)
                .await
                .unwrap()
                .items
                .is_empty()
        );
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .ready_depth,
            0
        );
    }

    #[tokio::test]
    async fn publish_preview_and_queued_intent_use_resolved_template() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.upsert_device(sample_device()).await.unwrap();
        store
            .put_template(NotificationTemplate {
                app_id: "app-1".to_owned(),
                template_id: "welcome".to_owned(),
                default_locale: "en".to_owned(),
                locales: BTreeMap::from([(
                    "en".to_owned(),
                    TemplateContent {
                        title: "Hello {{ data.name }}".to_owned(),
                        body: "Ready".to_owned(),
                        icon: None,
                        sound: None,
                        collapse_key: Some("welcome".to_owned()),
                    },
                )]),
                provider_overrides: BTreeMap::new(),
            })
            .await
            .unwrap();
        let dyn_store: DynPushStore = store;
        let dyn_queue: DynPushQueue = queue.clone();
        let admission = PushAdmissionSnapshot::testing_active([PushProviderKind::Fcm]);
        let mut request = sample_publish_request("template-publish");
        request.payload.template_id = Some("welcome".to_owned());
        request.payload.template_data = json!({"name": "Ada"});
        request.payload.title = None;
        request.payload.body = None;

        let (_, Json(body), _) = accept_publish_inner(
            "app-1",
            request,
            false,
            &HeaderMap::new(),
            test_publish_context(&dyn_store, &dyn_queue, &admission),
        )
        .await
        .unwrap();

        let fcm = body
            .rendered_payloads
            .iter()
            .find(|rendered| rendered.provider == PushProviderKind::Fcm)
            .unwrap();
        assert_eq!(fcm.payload["message"]["notification"]["title"], "Hello Ada");
        let message = queue
            .consume(PushQueueStage::PublishLog, "planner", 1, 30_000)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let PushQueuePayload::PublishLog(event) = message.payload else {
            panic!("expected publish log");
        };
        assert_eq!(
            event.intent.payload.title.as_deref(),
            Some("Hello {{ data.name }}")
        );
        assert_eq!(event.intent.payload.body.as_deref(), Some("Ready"));
    }

    #[tokio::test]
    async fn publish_rejects_when_no_provider_worker_is_available() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.upsert_device(sample_device()).await.unwrap();
        let dyn_store: DynPushStore = store;
        let dyn_queue: DynPushQueue = queue;
        let admission =
            PushAdmissionSnapshot::testing_active(std::iter::empty::<PushProviderKind>());

        let error = accept_publish_inner(
            "app-1",
            sample_publish_request("providerless-publish"),
            false,
            &HeaderMap::new(),
            test_publish_context(&dyn_store, &dyn_queue, &admission),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, AppError::ServiceUnavailable(_)));
    }

    #[tokio::test]
    async fn raw_fcm_publish_requires_fcm_worker_capability() {
        let store: DynPushStore = Arc::new(MemoryPushStore::new());
        let queue: DynPushQueue = Arc::new(MemoryPushQueue::new());
        let admission = PushAdmissionSnapshot::testing_active([PushProviderKind::Apns]);
        let mut request = sample_publish_request("raw-fcm-publish");
        request.recipients = vec![PublishTarget::Recipient {
            recipient: PushRecipient::Fcm {
                registration_token: SecretString::new("raw-fcm-token").unwrap(),
            },
        }];

        let error = accept_publish_inner(
            "app-1",
            request,
            false,
            &HeaderMap::new(),
            test_publish_context(&store, &queue, &admission),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, AppError::ServiceUnavailable(_)));
    }

    #[tokio::test]
    async fn raw_apns_publish_requires_apns_worker_capability() {
        let store: DynPushStore = Arc::new(MemoryPushStore::new());
        let queue: DynPushQueue = Arc::new(MemoryPushQueue::new());
        let admission = PushAdmissionSnapshot::testing_active([PushProviderKind::Fcm]);
        let mut request = sample_publish_request("raw-apns-publish");
        request.recipients = vec![PublishTarget::Recipient {
            recipient: PushRecipient::Apns {
                device_token: SecretString::new("raw-apns-token").unwrap(),
            },
        }];

        let error = accept_publish_inner(
            "app-1",
            request,
            false,
            &HeaderMap::new(),
            test_publish_context(&store, &queue, &admission),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, AppError::ServiceUnavailable(_)));
    }

    #[tokio::test]
    async fn critical_stage_backpressure_rejects_admission() {
        for (stage, env_name) in [
            (PushQueueStage::PublishLog, "PUSH_PUBLISH_LOG_MAX_LAG"),
            (PushQueueStage::ShardJobs, "PUSH_CRITICAL_QUEUE_MAX_LAG"),
            (
                PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
                "PUSH_CRITICAL_QUEUE_MAX_LAG",
            ),
            (
                PushQueueStage::DeliveryResults,
                "PUSH_CRITICAL_QUEUE_MAX_LAG",
            ),
            (PushQueueStage::RetrySchedule, "PUSH_CRITICAL_QUEUE_MAX_LAG"),
            (PushQueueStage::DeadLetters, "PUSH_CRITICAL_QUEUE_MAX_LAG"),
        ] {
            assert_stage_backpressure(stage, env_name).await;
        }
    }

    #[tokio::test]
    async fn healthy_backpressure_lag_admits() {
        let _guard = PUSH_TEST_ENV_LOCK.lock().await;
        let _publish_log_threshold = EnvVarGuard::set("PUSH_PUBLISH_LOG_MAX_LAG", "1");
        let _critical_threshold = EnvVarGuard::set("PUSH_CRITICAL_QUEUE_MAX_LAG", "1");
        let queue: DynPushQueue = Arc::new(MemoryPushQueue::new());
        let admission = PushAdmissionSnapshot::testing_active([PushProviderKind::Fcm]);

        enforce_backpressure(&queue, &admission).await.unwrap();
    }

    #[test]
    fn channel_push_rule_builder_maps_matching_event_payload() {
        let requests = build_channel_push_rule_requests(
            "app-rule-builder",
            Some("agent-complete"),
            Some(&ApiMessageData::Json(sonic_rs::json!({
                "title": "Done",
                "body": "Ready",
                "sessionId": "sess-1"
            }))),
            &["notifications:user-1".to_string()],
            &[PushRuleConfig {
                channel_pattern: "notifications:*".to_string(),
                event_filter: vec!["agent-complete".to_string()],
                ..PushRuleConfig::default()
            }],
        )
        .unwrap();

        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].recipients,
            vec![PublishTarget::Channel {
                channel: "notifications:user-1".to_string()
            }]
        );
        assert_eq!(requests[0].payload.title.as_deref(), Some("Done"));
        assert_eq!(
            requests[0].payload.template_data["data"]["sessionId"],
            "sess-1"
        );
    }

    #[test]
    fn channel_push_rule_builder_skips_non_matching_event() {
        let requests = build_channel_push_rule_requests(
            "app-rule-builder-skip",
            Some("agent-start"),
            Some(&ApiMessageData::Json(sonic_rs::json!({
                "title": "Started",
                "body": "Working"
            }))),
            &["notifications:user-1".to_string()],
            &[PushRuleConfig {
                channel_pattern: "notifications:*".to_string(),
                event_filter: vec!["agent-complete".to_string()],
                ..PushRuleConfig::default()
            }],
        )
        .unwrap();

        assert!(requests.is_empty());
    }

    #[test]
    fn channel_push_rule_builder_rejects_malformed_matching_payload() {
        let error = build_channel_push_rule_requests(
            "app-rule-builder-invalid",
            Some("agent-complete"),
            Some(&ApiMessageData::Json(sonic_rs::json!({
                "title": "Done"
            }))),
            &["notifications:user-1".to_string()],
            &[PushRuleConfig {
                channel_pattern: "notifications:*".to_string(),
                event_filter: vec!["agent-complete".to_string()],
                ..PushRuleConfig::default()
            }],
        )
        .unwrap_err();

        assert!(matches!(error, AppError::InvalidInput(_)));
    }
}
