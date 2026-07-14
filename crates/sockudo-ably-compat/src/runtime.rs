//! Reduced Ably REST and Realtime compatibility for Ably AI Transport tests.
//!
//! This is an additive compatibility surface. Sockudo and Pusher clients still
//! use their native routes and protocol frames; Ably ProtocolMessages are
//! translated at the edge into Sockudo's existing publish, history, and version
//! stores.

use axum::{
    body::{Bytes, HttpBody as _},
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::Engine as _;
use dashmap::DashMap;
use hmac::{Hmac, KeyInit, Mac};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sockudo_adapter::{
    ConnectionHandler, RealtimeEgressTap,
    handler::{
        annotations::{DeleteAnnotationRuntimeRequest, PublishAnnotationRuntimeRequest},
        types::SubscriptionRewind,
    },
    services::{
        MessageService, MutableMessageActor, PresenceOperation, PresenceService, PublishContext,
    },
};
use sockudo_core::{
    annotations::{
        AnnotationAction, AnnotationEventLookupRequest, AnnotationEventsRequest, AnnotationId,
        AnnotationSerial, AnnotationType, RawAnnotationReplayRequest, StoredAnnotationEvent,
    },
    app::App,
    cache::CacheManager,
    error::Result as SockudoResult,
    history::{HistoryCursor, HistoryDirection, HistoryQueryBounds, HistoryReadRequest, now_ms},
    message_envelope::{
        MessageContent, MessageEnvelope, VersionProjection, decode_stored_message_payload,
    },
    options::{AblyCompatConfig, AblyCompatKeyConfig, AblyRealtimeAdmission},
    origin_validation::OriginValidator,
    presence_history::{
        PresenceHistoryCursor, PresenceHistoryDirection, PresenceHistoryEventCause,
        PresenceHistoryEventKind, PresenceHistoryFilter, PresenceHistoryQueryBounds,
        PresenceHistoryReadRequest,
    },
    presence_registry::{
        PresenceChange, PresenceChangeAction, PresenceRecord, PresenceRegistry, PresenceReplication,
    },
    token::secure_compare,
    versioned_messages::{MessageSerial, VersionSerial},
    websocket::ConnectionCapabilities,
};
use sockudo_protocol::{
    messages::{
        AI_EVENT_CANCEL, AI_EVENT_INPUT, AI_HEADER_INPUT_CLIENT_ID, AI_HEADER_RUN_CLIENT_ID,
        ANNOTATION_EVENT_NAME, AnnotationEventAction, AnnotationEventData,
        MESSAGE_SUMMARY_EVENT_NAME, MessageData, MessageExtras, PusherMessage, is_ai_event,
    },
    versioned_messages::{
        AppendMessageRequest, HEADER_VERSION_SERIAL, HEADER_VERSION_TIMESTAMP_MS,
        MessageAction as ProtocolMessageAction, UpdateMessageRequest, extract_runtime_action,
        extract_runtime_append_fragment, extract_runtime_message_serial,
    },
};
#[cfg(feature = "push")]
use sockudo_push::{
    ChannelSubscription, DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult,
    DeviceDetails, DevicePushDetails, DevicePushState, DynPushQueue, DynPushStore, FanoutConfig,
    FormFactor, HealthStatus, Platform, ProviderDispatchWorker, ProviderError,
    ProviderFailureClass, PublishIntent, PublishTarget, PushAcceptOutcome, PushAcceptRequest,
    PushDispatcher, PushPayload, PushPipeline, PushPipelineError, PushProviderKind, PushRecipient,
    SecretString, generate_device_identity_token, hash_device_identity_token, stable_hash,
    verify_device_identity_token,
};
use sockudo_ws::{Message, axum_integration::WebSocketUpgrade};
use sonic_rs::JsonValueMutTrait;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value, json};
#[cfg(feature = "delta")]
use std::time::Instant;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    sync::{Arc, Mutex, OnceLock, RwLock, Weak, atomic::Ordering},
    time::Duration,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::auth::{basic_credential, bearer_token, parse_ably_key};
#[cfg(feature = "push")]
use crate::codec::WireValue;
use crate::codec::{decode_protocol_bytes, decode_value, encode_protocol_bytes};
use crate::outbound::{
    AblyOutbound, AblySender, OutboundLimits, OutboundMetrics, OutboundMetricsSnapshot,
    OutboundPriority,
};
use crate::protocol::*;
use crate::services::{
    VersionMutationPath, apply_append_message, apply_delete_message, apply_update_message,
    parse_message_serial,
};
use crate::stats::{StatsAggregator, StatsError, StatsObservation, StatsQuery, StatsRuntimeConfig};
use crate::{AblyCompatError, channel_name::AblyChannelName, filter::AblyMessageFilter};

type AppError = AblyCompatError;
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AblyErrorBody {
    error: AblyErrorInfo,
}

const ABLY_MAX_BATCH_SPECS: usize = 100;
const ABLY_MAX_BATCH_CHANNELS: usize = 100;
const ABLY_MAX_BATCH_MESSAGES: usize = 1_000;
const ABLY_MAX_BATCH_RESULTS: usize = 1_000;
const ABLY_MAX_BATCH_OPERATIONS: usize = 10_000;
const ABLY_MAX_BATCH_BYTES: usize = 10 * 1024 * 1024;
const ABLY_BATCH_CONCURRENCY: usize = 8;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AblyPublishPayload {
    Many(Vec<AblyMessage>),
    One(Box<AblyMessage>),
}

impl AblyPublishPayload {
    fn len(&self) -> usize {
        match self {
            Self::Many(messages) => messages.len(),
            Self::One(_) => 1,
        }
    }

    fn into_messages(self) -> Vec<AblyMessage> {
        match self {
            Self::Many(messages) => messages,
            Self::One(message) => vec![*message],
        }
    }
}

#[derive(Debug, Deserialize)]
struct AblyBatchPublishRequest {
    channels: Vec<String>,
    messages: AblyPublishPayload,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AblyBatchPublishBody {
    One(AblyBatchPublishRequest),
    Many(Vec<AblyBatchPublishRequest>),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AblyBatchResult<T> {
    success_count: usize,
    failure_count: usize,
    results: Vec<T>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AblyLegacyBatchResponse<T> {
    error: AblyErrorInfo,
    batch_response: Vec<T>,
}

async fn run_bounded_ordered<T, R, F, Fut>(
    items: Vec<T>,
    concurrency: usize,
    mut work: F,
) -> Result<Vec<R>, AppError>
where
    T: Send + 'static,
    R: Send + 'static,
    F: FnMut(T) -> Fut,
    Fut: Future<Output = R> + Send + 'static,
{
    let item_count = items.len();
    if item_count == 0 {
        return Ok(Vec::new());
    }

    let concurrency = concurrency.max(1).min(item_count);
    let mut pending = items.into_iter().enumerate();
    let mut tasks = tokio::task::JoinSet::new();
    let mut results = std::iter::repeat_with(|| None)
        .take(item_count)
        .collect::<Vec<_>>();

    while tasks.len() < concurrency {
        let Some((index, item)) = pending.next() else {
            break;
        };
        let future = work(item);
        tasks.spawn(async move { (index, future.await) });
    }

    while let Some(joined) = tasks.join_next().await {
        let (index, result) = joined.map_err(|error| {
            AppError::InternalError(format!("bounded Ably batch task failed: {error}"))
        })?;
        results[index] = Some(result);

        if let Some((next_index, item)) = pending.next() {
            let future = work(item);
            tasks.spawn(async move { (next_index, future.await) });
        }
    }

    results
        .into_iter()
        .map(|result| {
            result.ok_or_else(|| {
                AppError::InternalError("bounded Ably batch task returned no result".to_string())
            })
        })
        .collect()
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum AblyBatchPublishChannelResponse {
    Success {
        channel: String,
        #[serde(rename = "messageId")]
        message_id: String,
        serials: Vec<String>,
    },
    Failure {
        channel: String,
        error: AblyErrorInfo,
    },
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum AblyBatchPresenceChannelResponse {
    Success {
        channel: String,
        presence: Vec<AblyPresenceMessage>,
    },
    Failure {
        channel: String,
        error: AblyErrorInfo,
    },
}

enum AblyBatchPresenceWork {
    Ready(AblyBatchPresenceChannelResponse),
    Snapshot(AblyChannelName),
}

#[derive(Clone)]
struct PreparedAblyBatchMessage {
    message: AblyMessage,
    connection_id: Option<String>,
    client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AblyBatchPresenceQuery {
    key: Option<String>,
    #[serde(rename = "access_token", alias = "accessToken")]
    access_token: Option<String>,
    #[serde(rename = "client_id", alias = "clientId")]
    client_id: Option<String>,
    format: Option<String>,
    channels: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AblyStoredPresenceData {
    #[serde(rename = "__sockudoAblyPresence")]
    marker: bool,
    data: Option<Value>,
    encoding: Option<String>,
    extras: Option<Value>,
    id: Option<String>,
}

fn decode_stored_presence_data(value: &Value) -> Option<AblyStoredPresenceData> {
    let marked = value.get("__sockudoAblyPresence").and_then(Value::as_bool) == Some(true);
    let legacy_envelope = value.as_object().is_some_and(|object| {
        ["data", "encoding", "extras", "id"]
            .into_iter()
            .all(|key| object.contains_key(&key))
    });
    if !marked && !legacy_envelope {
        return None;
    }
    let optional_value = |key: &str| value.get(key).filter(|value| !value.is_null()).cloned();
    Some(AblyStoredPresenceData {
        marker: true,
        data: optional_value("data"),
        encoding: value
            .get("encoding")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        extras: optional_value("extras"),
        id: value
            .get("id")
            .and_then(Value::as_str)
            .map(ToString::to_string),
    })
}

fn presence_record_from_ably(member: &AblyPresenceMessage) -> Result<PresenceRecord, &'static str> {
    let connection_id = member
        .connection_id
        .clone()
        .ok_or("presence member requires connectionId")?;
    let client_id = member
        .client_id
        .clone()
        .ok_or("presence member requires clientId")?;
    let id = member
        .id
        .clone()
        .ok_or("presence member requires server id")?;
    Ok(PresenceRecord {
        connection_id,
        client_id,
        id,
        data: member.data.clone(),
        encoding: member.encoding.clone(),
        extras: member.extras.clone(),
        timestamp_ms: member.timestamp.unwrap_or_else(now_ms),
    })
}

fn ably_presence_from_record(record: PresenceRecord, action: u8) -> AblyPresenceMessage {
    AblyPresenceMessage {
        id: Some(record.id),
        action: Some(action),
        client_id: Some(record.client_id),
        connection_id: Some(record.connection_id),
        data: record.data,
        encoding: record.encoding,
        timestamp: Some(record.timestamp_ms),
        extras: record.extras,
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct AblyPresenceQuery {
    key: Option<String>,
    #[serde(rename = "access_token", alias = "accessToken")]
    access_token: Option<String>,
    #[serde(rename = "client_id")]
    auth_client_id: Option<String>,
    client_id: Option<String>,
    connection_id: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
    start: Option<i64>,
    end: Option<i64>,
    format: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AblyTokenRecord {
    app_id: String,
    key_name: String,
    client_id: Option<String>,
    #[serde(default)]
    issued_ms: i64,
    expires_ms: i64,
    capabilities: Option<ConnectionCapabilities>,
    revocable: bool,
    rotation_id: Option<String>,
    #[serde(default)]
    revocation_key: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedAblyAuth {
    app: App,
    client_id: Option<String>,
    connection_client_id: Option<String>,
    capabilities: Option<ConnectionCapabilities>,
    issued_ms: i64,
    expires_ms: Option<i64>,
    credential_id: String,
    revocable: bool,
    revocation_key: Option<String>,
    #[cfg(feature = "push")]
    push_device_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ConnectionAuthorization {
    generation: u64,
    client_id: Option<String>,
    connection_client_id: Option<String>,
    capabilities: Option<ConnectionCapabilities>,
    issued_ms: i64,
    expires_ms: Option<i64>,
    credential_id: String,
    revocable: bool,
    revocation_key: Option<String>,
}

impl ConnectionAuthorization {
    fn from_resolved(resolved: &ResolvedAblyAuth) -> Self {
        Self {
            generation: 1,
            client_id: resolved.client_id.clone(),
            connection_client_id: resolved.connection_client_id.clone(),
            capabilities: resolved.capabilities.clone(),
            issued_ms: resolved.issued_ms,
            expires_ms: resolved.expires_ms,
            credential_id: resolved.credential_id.clone(),
            revocable: resolved.revocable,
            revocation_key: resolved.revocation_key.clone(),
        }
    }

    fn replace_from(&mut self, resolved: &ResolvedAblyAuth) {
        self.generation = self.generation.wrapping_add(1).max(1);
        self.client_id.clone_from(&resolved.client_id);
        self.connection_client_id
            .clone_from(&resolved.connection_client_id);
        self.capabilities.clone_from(&resolved.capabilities);
        self.issued_ms = resolved.issued_ms;
        self.expires_ms = resolved.expires_ms;
        self.credential_id.clone_from(&resolved.credential_id);
        self.revocable = resolved.revocable;
        self.revocation_key.clone_from(&resolved.revocation_key);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AblyRevocationRecord {
    target_type: String,
    target_value: String,
    issued_before: i64,
    applies_at: i64,
}

#[derive(Debug, Clone)]
enum AblySessionCommand {
    ReauthHint { generation: u64 },
    RevocationChanged { generation: u64 },
    Superseded,
}

#[derive(Clone)]
struct AblyLiveSession {
    session_id: String,
    app_id: String,
    authorization: Arc<RwLock<ConnectionAuthorization>>,
    command_tx: tokio::sync::mpsc::Sender<AblySessionCommand>,
}

#[derive(Clone)]
struct ResolvedAblyKey {
    app: App,
    key_name: String,
    secret: String,
    capability: String,
    capabilities: Option<ConnectionCapabilities>,
    revocable_tokens: bool,
    rotation_id: Option<String>,
}

#[derive(Debug)]
struct AblyAuthError {
    status: StatusCode,
    code: u32,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AblySessionRecord {
    app_id: String,
    connection_id: String,
    client_id: Option<String>,
    expires_at_ms: i64,
}

#[derive(Debug, Clone)]
struct AblyChannelPosition {
    stream_id: String,
    serial: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AblyChannelKey {
    app_id: String,
    channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AblySubscriberKey {
    session_id: Arc<str>,
    requested_channel: Arc<str>,
}

#[derive(Clone)]
struct AblySubscriber {
    connection_id: Arc<str>,
    sender: AblySender,
    filter: Option<Arc<AblyMessageFilter>>,
    mode_flags: u64,
    echo: bool,
    #[cfg(feature = "delta")]
    delta: Option<AblyDeltaState>,
    attach_gate: Option<AblyAttachGate>,
}

#[derive(Clone, Default)]
struct AblyDeltaState {
    #[cfg(feature = "delta")]
    previous_id: Option<Arc<str>>,
    #[cfg(feature = "delta")]
    previous_payload: Option<Arc<[u8]>>,
    #[cfg(feature = "delta")]
    previous_at: Option<Instant>,
}

#[cfg(feature = "delta")]
impl AblyDeltaState {
    fn with_base(message_id: Arc<str>, payload: Arc<[u8]>, now: Instant) -> Self {
        Self {
            previous_id: Some(message_id),
            previous_payload: Some(payload),
            previous_at: Some(now),
        }
    }

    fn fresh_base(&self, now: Instant) -> Option<(&Arc<str>, &Arc<[u8]>)> {
        let stored_at = self.previous_at?;
        if now.saturating_duration_since(stored_at) > ABLY_DELTA_BASE_MAX_AGE {
            return None;
        }
        Some((self.previous_id.as_ref()?, self.previous_payload.as_ref()?))
    }

    fn expire(&mut self, now: Instant) {
        if self.previous_at.is_some_and(|stored_at| {
            now.saturating_duration_since(stored_at) > ABLY_DELTA_BASE_MAX_AGE
        }) {
            *self = Self::default();
        }
    }
}

#[cfg(feature = "delta")]
fn ably_delta_state(params: &HashMap<String, String>) -> Option<AblyDeltaState> {
    params
        .get("delta")
        .is_some_and(|value| value.eq_ignore_ascii_case("vcdiff"))
        .then(AblyDeltaState::default)
}

#[derive(Clone, Default)]
struct AblyAttachGate {
    messages: Vec<AblyProtocolMessage>,
    bytes: usize,
    overflowed: bool,
}

struct AblyAttachment<'a> {
    connection_id: &'a str,
    session_id: &'a str,
    sender: AblySender,
    filter: Option<Arc<AblyMessageFilter>>,
    params: HashMap<String, String>,
    mode_flags: u64,
    echo: bool,
    presence: Vec<AblyPresenceMessage>,
}

#[derive(Clone)]
struct AblyConnectionAttachment {
    channel: AblyChannelName,
    params: HashMap<String, String>,
    mode_flags: u64,
    explicit_modes: bool,
    filter: Option<Arc<AblyMessageFilter>>,
    attach_position: Option<String>,
    has_presence: bool,
}

const ABLY_MODE_PRESENCE: u64 = 1 << 16;
const ABLY_MODE_PUBLISH: u64 = 1 << 17;
const ABLY_MODE_SUBSCRIBE: u64 = 1 << 18;
const ABLY_MODE_PRESENCE_SUBSCRIBE: u64 = 1 << 19;
const ABLY_MODE_ANNOTATION_PUBLISH: u64 = 1 << 21;
const ABLY_MODE_ANNOTATION_SUBSCRIBE: u64 = 1 << 22;
const ABLY_MODE_OBJECT_SUBSCRIBE: u64 = 1 << 24;
const ABLY_MODE_OBJECT_PUBLISH: u64 = 1 << 25;
const ABLY_MODE_MASK: u64 = ABLY_MODE_PRESENCE
    | ABLY_MODE_PUBLISH
    | ABLY_MODE_SUBSCRIBE
    | ABLY_MODE_PRESENCE_SUBSCRIBE
    | ABLY_MODE_ANNOTATION_PUBLISH
    | ABLY_MODE_ANNOTATION_SUBSCRIBE
    | ABLY_MODE_OBJECT_SUBSCRIBE
    | ABLY_MODE_OBJECT_PUBLISH;
const ABLY_DEFAULT_MODE_FLAGS: u64 = ABLY_MODE_PRESENCE
    | ABLY_MODE_PUBLISH
    | ABLY_MODE_SUBSCRIBE
    | ABLY_MODE_PRESENCE_SUBSCRIBE
    | ABLY_MODE_ANNOTATION_PUBLISH;
const ABLY_PRESENCE_SYNC_CHUNK_SIZE: usize = 100;
const ABLY_FILTER_CACHE_MAX_ENTRIES: usize = 1_024;
const ABLY_FILTER_CACHE_MAX_BYTES: usize = 1024 * 1024;
const ABLY_ATTACH_GATE_MAX_MESSAGES: usize = ABLY_COMPAT_MAX_REPLAY_MESSAGES;
const ABLY_ATTACH_GATE_MAX_BYTES: usize = 64 * 1024 * 1024;
#[cfg(feature = "delta")]
const ABLY_DELTA_BASE_MAX_BYTES: usize = DEFAULT_MAX_MESSAGE_SIZE as usize;
#[cfg(feature = "delta")]
const ABLY_DELTA_BASE_MAX_AGE: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, PartialEq, Eq)]
struct AblyAttachOptions {
    params: HashMap<String, String>,
    mode_flags: u64,
    explicit_modes: bool,
}

impl AblyAttachOptions {
    fn from_wire(flags: Option<u64>, params: Option<HashMap<String, String>>) -> Self {
        let mut recognized = HashMap::new();
        for (key, value) in params.unwrap_or_default() {
            if key == "delta" {
                if value.eq_ignore_ascii_case("vcdiff") {
                    recognized.insert(key, "vcdiff".to_owned());
                }
                continue;
            }
            if matches!(key.as_str(), "echo" | "modes" | "rewind" | "rewindCount") {
                recognized.insert(key, value);
            }
        }

        let explicit_modes = recognized.contains_key("modes")
            || flags.is_some_and(|flags| flags & ABLY_MODE_MASK != 0);
        let mode_flags = recognized
            .get("modes")
            .map(|modes| {
                modes
                    .split(',')
                    .filter_map(|mode| ably_mode_flag(mode.trim()))
                    .fold(0, |flags, flag| flags | flag)
            })
            .filter(|flags| *flags != 0)
            .or_else(|| {
                flags
                    .map(|flags| flags & ABLY_MODE_MASK)
                    .filter(|flags| *flags != 0)
            })
            .unwrap_or(ABLY_DEFAULT_MODE_FLAGS);

        Self {
            params: recognized,
            mode_flags,
            explicit_modes,
        }
    }
}

fn parse_ably_rewind_param(raw: &str) -> Option<SubscriptionRewind> {
    let raw = raw.trim();
    if let Ok(count) = raw.parse::<usize>() {
        return (count > 0).then_some(SubscriptionRewind::Count(count));
    }

    let (unit_index, unit) = raw.char_indices().next_back()?;
    let amount = &raw[..unit_index];
    let amount = amount.parse::<u64>().ok()?;
    if amount == 0 {
        return None;
    }
    let multiplier = match unit.to_ascii_lowercase() {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        _ => return None,
    };
    amount
        .checked_mul(multiplier)
        .filter(|seconds| *seconds <= (i64::MAX as u64) / 1000)
        .map(SubscriptionRewind::Seconds)
}

fn resolve_ably_rewind(
    params: &HashMap<String, String>,
    attach_resume: bool,
) -> Option<SubscriptionRewind> {
    if attach_resume {
        return None;
    }
    params
        .get("rewindCount")
        .or_else(|| params.get("rewind"))
        .and_then(|raw| parse_ably_rewind_param(raw))
}

fn build_ably_rewind_history_request(
    app_id: &str,
    channel: &str,
    rewind: &SubscriptionRewind,
    max_page_size: usize,
    high_water: Option<&AblyChannelPosition>,
    now: i64,
) -> HistoryReadRequest {
    HistoryReadRequest {
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        direction: match rewind {
            SubscriptionRewind::Count(_) => HistoryDirection::NewestFirst,
            SubscriptionRewind::Seconds(_) => HistoryDirection::OldestFirst,
        },
        limit: rewind.limit().min(max_page_size).max(1),
        cursor: None,
        bounds: HistoryQueryBounds {
            end_serial: high_water.map(|position| position.serial),
            ..rewind.to_history_bounds(now)
        },
    }
}

fn ably_mode_flag(mode: &str) -> Option<u64> {
    match mode.to_ascii_lowercase().as_str() {
        "presence" => Some(ABLY_MODE_PRESENCE),
        "publish" => Some(ABLY_MODE_PUBLISH),
        "subscribe" => Some(ABLY_MODE_SUBSCRIBE),
        "presence_subscribe" => Some(ABLY_MODE_PRESENCE_SUBSCRIBE),
        "annotation_publish" => Some(ABLY_MODE_ANNOTATION_PUBLISH),
        "annotation_subscribe" => Some(ABLY_MODE_ANNOTATION_SUBSCRIBE),
        "object_subscribe" => Some(ABLY_MODE_OBJECT_SUBSCRIBE),
        "object_publish" => Some(ABLY_MODE_OBJECT_PUBLISH),
        _ => None,
    }
}

fn attached_channel_mode_denies(
    attachments: &HashMap<String, AblyConnectionAttachment>,
    channel: Option<&str>,
    required_mode: u64,
) -> bool {
    channel.is_some_and(|channel| {
        attachments
            .get(channel)
            .is_some_and(|attachment| attachment.mode_flags & required_mode == 0)
    })
}

#[derive(Debug, Clone)]
struct AblyRecoveryFailure {
    code: u32,
    status: StatusCode,
    message: String,
}

#[derive(Default)]
struct AblyChannelState {
    subscribers: HashMap<AblySubscriberKey, AblySubscriber>,
    current_stream_id: Option<String>,
    attach_position: Option<AblyChannelPosition>,
    last_touched_ms: i64,
}

struct CachedAblyFilter {
    filter: Arc<AblyMessageFilter>,
    bytes: usize,
    last_used: u64,
}

#[derive(Default)]
struct AblyFilterCache {
    entries: HashMap<String, CachedAblyFilter>,
    bytes: usize,
    clock: u64,
}

enum AblyConnectionStart {
    Fresh,
    Resumed { connection_id: String },
    Failed { error: AblyErrorInfo },
}

impl AblyAuthError {
    fn invalid_credentials() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: 40101,
            message: "Invalid credentials".to_string(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: 40140,
            message: message.into(),
        }
    }

    fn expired() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: 40142,
            message: "Token expired".to_string(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: 40160,
            message: message.into(),
        }
    }
}

#[derive(Default)]
pub struct AblyCompatHub {
    channels: DashMap<AblyChannelKey, Arc<Mutex<AblyChannelState>>>,
    sessions: DashMap<String, AblySessionRecord>,
    tokens: DashMap<String, AblyTokenRecord>,
    revocations: DashMap<String, AblyRevocationRecord>,
    live_sessions: DashMap<String, AblyLiveSession>,
    session_echo: DashMap<String, bool>,
    presence_registry: Arc<PresenceRegistry>,
    handler: OnceLock<Weak<ConnectionHandler>>,
    nonces: DashMap<String, i64>,
    key_registry: HashMap<String, AblyCompatKeyConfig>,
    config: AblyCompatConfig,
    cache: Option<Arc<dyn CacheManager>>,
    metrics: Arc<OutboundMetrics>,
    filter_cache: Mutex<AblyFilterCache>,
    stats: Arc<StatsAggregator>,
    #[cfg(feature = "push")]
    push_store: Option<sockudo_push::DynPushStore>,
    #[cfg(feature = "push")]
    push_queue: Option<sockudo_push::DynPushQueue>,
    #[cfg(feature = "push")]
    push_admission: Option<Arc<dyn AblyPushAdmissionGuard>>,
}

#[cfg(feature = "push")]
pub trait AblyPushAdmissionGuard: Send + Sync {
    fn rejection_for(
        &self,
        targets: &[PublishTarget],
        required_providers: &BTreeSet<PushProviderKind>,
        expected_recipients: u64,
    ) -> Option<String>;
}

/// Dependencies used to construct an isolated compatibility runtime.
#[derive(Clone, Default)]
pub struct AblyCompatDependencies {
    pub config: AblyCompatConfig,
    pub cache: Option<Arc<dyn CacheManager>>,
    pub presence_registry: Arc<PresenceRegistry>,
    #[cfg(feature = "push")]
    pub push_store: Option<sockudo_push::DynPushStore>,
    #[cfg(feature = "push")]
    pub push_queue: Option<sockudo_push::DynPushQueue>,
    #[cfg(feature = "push")]
    pub push_admission: Option<Arc<dyn AblyPushAdmissionGuard>>,
}

/// Explicitly constructed Ably compatibility runtime.
pub struct AblyCompatRuntime {
    hub: Arc<AblyCompatHub>,
}

impl AblyCompatRuntime {
    #[must_use]
    pub fn new(dependencies: AblyCompatDependencies) -> Self {
        let stats = StatsAggregator::new(
            dependencies.cache.clone(),
            StatsRuntimeConfig {
                queue_capacity: dependencies.config.stats_queue_capacity,
                flush_interval: Duration::from_millis(dependencies.config.stats_flush_interval_ms),
                retention_seconds: dependencies.config.stats_retention_seconds,
                max_scan_entries: dependencies.config.stats_max_scan_entries,
                cas_retries: dependencies.config.stats_cas_retries,
            },
        );
        let key_registry = if dependencies.config.enabled {
            dependencies
                .config
                .keys
                .iter()
                .cloned()
                .map(|key| (key.key_name.clone(), key))
                .collect()
        } else {
            HashMap::new()
        };
        let hub = Arc::new(AblyCompatHub {
            key_registry,
            config: dependencies.config,
            cache: dependencies.cache,
            stats,
            presence_registry: dependencies.presence_registry,
            #[cfg(feature = "push")]
            push_store: dependencies.push_store,
            #[cfg(feature = "push")]
            push_queue: dependencies.push_queue,
            #[cfg(feature = "push")]
            push_admission: dependencies.push_admission,
            ..AblyCompatHub::default()
        });
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let weak_hub = Arc::downgrade(&hub);
            handle.spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_millis(ABLY_COMPAT_EXPIRY_SWEEP_MS));
                while let Some(hub) = weak_hub.upgrade() {
                    interval.tick().await;
                    hub.expire(now_ms()).await;
                }
            });
        }
        Self { hub }
    }

    /// Return a lock-free snapshot of compatibility delivery counters.
    pub fn metrics(&self) -> OutboundMetricsSnapshot {
        self.hub.metrics.snapshot()
    }

    /// Return a lock-free snapshot of the bounded stats aggregation worker.
    pub fn stats_metrics(&self) -> crate::StatsRuntimeSnapshot {
        self.hub.stats.snapshot()
    }

    pub fn presence_registry(&self) -> Arc<PresenceRegistry> {
        Arc::clone(&self.hub.presence_registry)
    }

    /// Bind native services after the server finishes constructing its handler.
    /// The weak reference prevents the compatibility runtime from extending the
    /// handler lifetime or creating a reference cycle.
    pub fn bind_handler(&self, handler: &Arc<ConnectionHandler>) {
        if self.hub.handler.set(Arc::downgrade(handler)).is_ok() {
            #[cfg(feature = "push")]
            if let Some(queue) = self.hub.push_queue.clone()
                && let Ok(runtime) = tokio::runtime::Handle::try_current()
            {
                let dispatcher = Arc::new(AblyRealtimePushDispatcher::new(Arc::downgrade(handler)));
                runtime.spawn(async move {
                    let mut worker = ProviderDispatchWorker::new(
                        PushProviderKind::Realtime,
                        queue,
                        dispatcher.clone(),
                    );
                    while dispatcher.is_bound() {
                        if let Err(error) = worker.run_once("sockudo-ably-realtime").await {
                            warn!(error = %error, "Ably realtime push worker tick failed");
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                });
            }
        }
    }

    /// Build the compatibility router with this runtime installed as an
    /// extension. Native connection-handler state is supplied by the server.
    pub fn realtime_router(self: &Arc<Self>) -> axum::Router<Arc<ConnectionHandler>> {
        use axum::{Extension, Router, routing::get};

        Router::new()
            .route("/", get(handle_ably_realtime_upgrade))
            .layer(Extension(Arc::clone(self)))
    }

    pub fn rest_router(self: &Arc<Self>) -> axum::Router<Arc<ConnectionHandler>> {
        use axum::{
            Extension, Router,
            routing::{get, post},
        };

        let router = Router::new()
            .route("/time", get(ably_time))
            .route("/stats", post(ably_ingest_stats))
            .route(
                "/keys/{keyName}/requestToken",
                get(ably_not_found).post(ably_request_token),
            )
            .route("/keys/{keyName}/revokeTokens", post(ably_revoke_tokens))
            .route("/messages", post(ably_batch_publish))
            .route("/presence", get(ably_batch_presence))
            .route("/channels/{channelName}", get(ably_channel_status))
            .route(
                "/channels/{channelName}/messages",
                get(ably_channel_history).post(ably_channel_publish),
            )
            .route(
                "/channels/{channelName}/presence",
                get(ably_channel_presence),
            )
            .route(
                "/channels/{channelName}/presence/history",
                get(ably_channel_presence_history),
            )
            .route(
                "/channels/{channelName}/messages/{messageSerial}",
                get(ably_channel_message).patch(ably_channel_message_mutation),
            )
            .route(
                "/channels/{channelName}/messages/{messageSerial}/versions",
                get(ably_channel_message_versions),
            )
            .route(
                "/channels/{channelName}/messages/{messageSerial}/annotations",
                get(ably_channel_annotations).post(ably_publish_annotations),
            );
        #[cfg(feature = "push")]
        let router = router
            .route("/push/publish", post(ably_push_publish))
            .route(
                "/push/deviceRegistrations",
                post(ably_push_save_device)
                    .patch(ably_push_save_device)
                    .get(ably_push_list_devices)
                    .delete(ably_push_delete_devices),
            )
            .route(
                "/push/deviceRegistrations/{deviceId}",
                get(ably_push_get_device)
                    .put(ably_push_put_device)
                    .delete(ably_push_delete_device),
            )
            .route(
                "/push/channelSubscriptions",
                post(ably_push_save_subscription)
                    .get(ably_push_list_subscriptions)
                    .delete(ably_push_delete_subscriptions),
            )
            .route("/push/channels", get(ably_push_list_channels));
        router
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(self),
                ably_stats_api_middleware,
            ))
            .layer(Extension(Arc::clone(self)))
    }

    pub fn router(self: &Arc<Self>) -> axum::Router<Arc<ConnectionHandler>> {
        self.realtime_router().merge(self.rest_router())
    }

    pub fn egress_tap(self: &Arc<Self>) -> Arc<dyn RealtimeEgressTap> {
        Arc::clone(self) as Arc<dyn RealtimeEgressTap>
    }
}

async fn ably_stats_api_middleware(
    State(runtime): State<Arc<AblyCompatRuntime>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let app_id = stats_request_app_id(&runtime.hub, request.headers(), request.uri()).await;
    let request_bytes = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| request.body().size_hint().exact())
        .unwrap_or_default();
    let path = request.uri().path().to_string();
    let response = next.run(request).await;
    let succeeded = response.status().is_success();
    let response_bytes = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| response.body().size_hint().exact())
        .unwrap_or_default();

    if let Some(app_id) = app_id {
        let now = now_ms();
        let mut observations = Vec::with_capacity(3);
        if let Ok(observation) =
            StatsObservation::api_request(&app_id, now, succeeded, request_bytes, response_bytes)
        {
            observations.push(observation);
        }
        if path.ends_with("/requestToken")
            && let Ok(observation) = StatsObservation::token_request(&app_id, now, succeeded)
        {
            observations.push(observation);
        }
        #[cfg(feature = "push")]
        if path == "/push/publish"
            && !succeeded
            && let Ok(observation) =
                StatsObservation::push(&app_id, now, succeeded, 0, request_bytes)
        {
            observations.push(observation);
        }
        for observation in observations {
            if let Err(error) = runtime.hub.stats.record(observation).await {
                warn!(app_id = %app_id, error = %error, "failed to persist Ably API stats");
            }
        }
    }
    response
}

async fn stats_request_app_id(
    hub: &AblyCompatHub,
    headers: &HeaderMap,
    uri: &axum::http::Uri,
) -> Option<String> {
    let credential = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if let Some(encoded) = credential.and_then(|value| value.strip_prefix("Basic "))
        && let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded)
        && let Ok(decoded) = std::str::from_utf8(&decoded)
        && let Some((key_name, _)) = decoded.split_once(':')
    {
        return stats_key_app_id(hub, key_name).await;
    }
    if let Some(token) = credential.and_then(|value| value.strip_prefix("Bearer ")) {
        if let Some(record) = hub.resolve_token(token).await {
            return Some(record.app_id);
        }
        if let Ok(header) = decode_header(token)
            && let Some(key_name) = header.kid
        {
            return stats_key_app_id(hub, &key_name).await;
        }
    }
    let query_value = |target: &str| {
        uri.query()?.split('&').find_map(|pair| {
            let (name, value) = pair.split_once('=')?;
            (name == target)
                .then(|| {
                    urlencoding::decode(value)
                        .ok()
                        .map(|value| value.into_owned())
                })
                .flatten()
        })
    };
    if let Some(key_value) = query_value("key")
        && let Some((key_name, _)) = key_value.split_once(':')
    {
        return stats_key_app_id(hub, key_name).await;
    }
    if let Some(token) = query_value("access_token") {
        if let Some(record) = hub.resolve_token(&token).await {
            return Some(record.app_id);
        }
        if let Ok(header) = decode_header(&token)
            && let Some(key_name) = header.kid
        {
            return stats_key_app_id(hub, &key_name).await;
        }
    }
    let key_name = uri
        .path()
        .strip_prefix("/keys/")
        .and_then(|path| path.split('/').next())?;
    stats_key_app_id(hub, key_name).await
}

async fn stats_key_app_id(hub: &AblyCompatHub, key_name: &str) -> Option<String> {
    if let Some(key) = hub.key_registry.get(key_name) {
        return Some(key.app_id.clone());
    }
    let handler = hub.handler.get()?.upgrade()?;
    handler
        .app_manager()
        .find_by_key(key_name)
        .await
        .ok()
        .flatten()
        .filter(|app| app.enabled)
        .map(|app| app.id)
}

impl RealtimeEgressTap for AblyCompatRuntime {
    fn has_subscribers(&self, app_id: &str, channel: &str) -> bool {
        self.hub.has_subscribers(app_id, channel)
    }

    fn deliver(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        envelope: &MessageEnvelope,
    ) -> SockudoResult<()> {
        self.hub.deliver(app_id, channel, message, envelope)
    }

    fn deliver_presence(
        &self,
        app_id: &str,
        channel: &str,
        replication: &PresenceReplication,
    ) -> SockudoResult<()> {
        self.hub.deliver_presence(app_id, channel, replication)
    }

    fn replicate_presence(
        &self,
        app_id: &str,
        channel: &str,
        replication: &PresenceReplication,
    ) -> SockudoResult<()> {
        self.hub.replicate_presence(app_id, channel, replication)
    }
}

impl RealtimeEgressTap for AblyCompatHub {
    fn has_subscribers(&self, app_id: &str, channel: &str) -> bool {
        self.has_subscribers(app_id, channel)
    }

    fn deliver(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        envelope: &MessageEnvelope,
    ) -> SockudoResult<()> {
        let channel_serial = envelope
            .stream_id
            .as_deref()
            .zip(envelope.delivery_serial)
            .map(|(stream_id, serial)| encode_ably_channel_serial(stream_id, serial));
        if message.event.as_deref() == Some(ANNOTATION_EVENT_NAME) {
            let event: AnnotationEventData = match decode_native_message_data(message.data.as_ref())
            {
                Ok(event) => event,
                Err(error) => {
                    warn!(app_id = %app_id, channel = %channel, error, "failed to decode native annotation delivery");
                    return Ok(());
                }
            };
            self.broadcast(
                app_id,
                channel,
                AblyProtocolMessage {
                    action: ACTION_ANNOTATION,
                    timestamp: Some(event.timestamp),
                    channel: Some(channel.to_string()),
                    channel_serial: channel_serial.clone(),
                    annotations: Some(vec![ably_annotation_from_native_event(event)]),
                    ..empty_protocol_message(ACTION_ANNOTATION)
                },
                envelope.publisher_connection_id.as_deref(),
                envelope.extras.as_ref().and_then(|extras| extras.echo),
            );
            return Ok(());
        }
        if message.event.as_deref() == Some(MESSAGE_SUMMARY_EVENT_NAME) {
            let value: Value = match decode_native_message_data(message.data.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    warn!(app_id = %app_id, channel = %channel, error, "failed to decode native annotation summary delivery");
                    return Ok(());
                }
            };
            let Some(serial) = value.get("serial").and_then(Value::as_str) else {
                return Ok(());
            };
            let annotations = ably_summary_annotations(&value);
            self.broadcast(
                app_id,
                channel,
                AblyProtocolMessage {
                    action: ACTION_MESSAGE,
                    timestamp: Some(now_ms()),
                    channel: Some(channel.to_string()),
                    channel_serial: channel_serial.clone(),
                    messages: Some(vec![AblyMessage {
                        serial: Some(serial.to_string()),
                        action: Some(MESSAGE_SUMMARY),
                        annotations,
                        ..AblyMessage::default()
                    }]),
                    ..empty_protocol_message(ACTION_MESSAGE)
                },
                envelope.publisher_connection_id.as_deref(),
                envelope.extras.as_ref().and_then(|extras| extras.echo),
            );
            return Ok(());
        }
        let ably_message =
            match envelope_to_ably_message(envelope, message, AblyMessageProjection::Mutation) {
                Ok(message) => message,
                Err(error) => {
                    warn!(
                        app_id = %app_id,
                        channel = %channel,
                        error = %error,
                        "failed to translate Sockudo message for Ably compatibility"
                    );
                    return Ok(());
                }
            };
        self.broadcast(
            app_id,
            channel,
            AblyProtocolMessage {
                action: ACTION_MESSAGE,
                timestamp: Some(now_ms()),
                channel: Some(channel.to_string()),
                channel_serial: channel_serial.clone(),
                messages: Some(vec![ably_message]),
                ..empty_protocol_message(ACTION_MESSAGE)
            },
            envelope.publisher_connection_id.as_deref(),
            envelope.extras.as_ref().and_then(|extras| extras.echo),
        );
        if envelope.action == Some(sockudo_core::versioned_messages::MessageAction::Append)
            && let Ok(aggregate) =
                envelope_to_ably_message(envelope, message, AblyMessageProjection::Aggregate)
        {
            self.broadcast(
                app_id,
                channel,
                AblyProtocolMessage {
                    action: ACTION_MESSAGE,
                    timestamp: Some(now_ms()),
                    channel: Some(channel.to_string()),
                    channel_serial,
                    messages: Some(vec![aggregate]),
                    ..empty_protocol_message(ACTION_MESSAGE)
                },
                envelope.publisher_connection_id.as_deref(),
                envelope.extras.as_ref().and_then(|extras| extras.echo),
            );
        }
        Ok(())
    }

    fn deliver_presence(
        &self,
        app_id: &str,
        channel: &str,
        replication: &PresenceReplication,
    ) -> SockudoResult<()> {
        let presence = replication
            .changes
            .iter()
            .map(ably_presence_from_change)
            .collect::<Vec<_>>();
        if !presence.is_empty() {
            self.broadcast(
                app_id,
                channel,
                AblyProtocolMessage {
                    action: ACTION_PRESENCE,
                    channel: Some(channel.to_string()),
                    presence: Some(presence),
                    timestamp: Some(now_ms()),
                    ..empty_protocol_message(ACTION_PRESENCE)
                },
                None,
                None,
            );
        }
        Ok(())
    }

    fn replicate_presence(
        &self,
        app_id: &str,
        channel: &str,
        replication: &PresenceReplication,
    ) -> SockudoResult<()> {
        let mut applied = Vec::with_capacity(replication.changes.len());
        for change in &replication.changes {
            let changed = match change.action {
                PresenceChangeAction::Enter | PresenceChangeAction::Update => {
                    self.presence_registry
                        .register_connection(app_id, &change.member.connection_id);
                    let transition = match change.action {
                        PresenceChangeAction::Enter => {
                            self.presence_registry
                                .enter(app_id, channel, change.member.clone())?
                        }
                        PresenceChangeAction::Update => {
                            self.presence_registry
                                .update(app_id, channel, change.member.clone())?
                        }
                        PresenceChangeAction::Leave => unreachable!(),
                    };
                    transition
                        .previous
                        .as_ref()
                        .is_none_or(|previous| previous != &change.member)
                }
                PresenceChangeAction::Leave => self
                    .presence_registry
                    .leave(
                        app_id,
                        channel,
                        &change.member.connection_id,
                        &change.member.client_id,
                    )?
                    .is_some(),
            };
            if changed {
                applied.push(change.clone());
            }
        }
        if let Some(connection_id) = replication.unregister_connection.as_deref() {
            self.presence_registry
                .unregister_connection(app_id, connection_id);
        }
        self.deliver_presence(
            app_id,
            channel,
            &PresenceReplication {
                changes: applied,
                unregister_connection: None,
            },
        )
    }
}

fn ably_presence_from_change(change: &PresenceChange) -> AblyPresenceMessage {
    let action = match change.action {
        PresenceChangeAction::Enter => 2,
        PresenceChangeAction::Update => 4,
        PresenceChangeAction::Leave => 3,
    };
    let mut message = ably_presence_from_record(change.member.clone(), action);
    message.id.clone_from(&change.wire_id);
    message
}

impl AblyCompatHub {
    fn message_filter(
        &self,
        channel: &AblyChannelName,
    ) -> Result<Option<Arc<AblyMessageFilter>>, String> {
        let Some(source) = AblyMessageFilter::source_from_channel(channel)? else {
            return Ok(None);
        };
        {
            let mut cache = self
                .filter_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.clock = cache.clock.wrapping_add(1);
            let clock = cache.clock;
            if let Some(entry) = cache.entries.get_mut(&source) {
                entry.last_used = clock;
                self.metrics
                    .filter_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Some(Arc::clone(&entry.filter)));
            }
        }
        self.metrics
            .filter_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        let compiled = Arc::new(AblyMessageFilter::compile(&source)?);
        let bytes = source
            .len()
            .saturating_add(std::mem::size_of::<AblyMessageFilter>());
        let mut cache = self
            .filter_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.clock = cache.clock.wrapping_add(1);
        let clock = cache.clock;
        if let Some(entry) = cache.entries.get_mut(&source) {
            entry.last_used = clock;
            self.metrics
                .filter_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Some(Arc::clone(&entry.filter)));
        }
        while !cache.entries.is_empty()
            && (cache.entries.len() >= ABLY_FILTER_CACHE_MAX_ENTRIES
                || cache.bytes.saturating_add(bytes) > ABLY_FILTER_CACHE_MAX_BYTES)
        {
            let oldest = cache
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else { break };
            if let Some(removed) = cache.entries.remove(&oldest) {
                cache.bytes = cache.bytes.saturating_sub(removed.bytes);
                self.metrics
                    .filter_cache_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        if bytes <= ABLY_FILTER_CACHE_MAX_BYTES {
            cache.bytes = cache.bytes.saturating_add(bytes);
            cache.entries.insert(
                source,
                CachedAblyFilter {
                    filter: Arc::clone(&compiled),
                    bytes,
                    last_used: clock,
                },
            );
        }
        self.metrics
            .filter_cache_entries
            .store(cache.entries.len(), Ordering::Release);
        self.metrics
            .filter_cache_bytes
            .store(cache.bytes, Ordering::Release);
        Ok(Some(compiled))
    }

    async fn expire(&self, now: i64) {
        let expired_connections = self
            .sessions
            .iter()
            .filter(|entry| {
                entry.expires_at_ms <= now && !self.live_sessions.contains_key(&entry.connection_id)
            })
            .map(|entry| (entry.connection_id.clone(), entry.app_id.clone()))
            .collect::<HashMap<_, _>>();
        self.sessions.retain(|_, record| record.expires_at_ms > now);
        self.tokens.retain(|_, record| record.expires_ms > now);
        self.nonces.retain(|_, expires_ms| *expires_ms > now);

        let mut stale_channels = Vec::new();
        #[cfg(feature = "delta")]
        let delta_now = Instant::now();
        for entry in &self.channels {
            let state = lock_channel_state(entry.value());
            #[cfg(feature = "delta")]
            let mut state = state;
            #[cfg(feature = "delta")]
            for subscriber in state.subscribers.values_mut() {
                if let Some(delta) = subscriber.delta.as_mut() {
                    delta.expire(delta_now);
                }
            }
            if state.subscribers.is_empty()
                && state.attach_position.is_none()
                && now.saturating_sub(state.last_touched_ms)
                    > i64::try_from(DEFAULT_CONNECTION_STATE_TTL_MS).unwrap_or(i64::MAX)
            {
                stale_channels.push(entry.key().clone());
            }
        }
        for key in stale_channels {
            self.channels.remove(&key);
            self.metrics.expiry.fetch_add(1, Ordering::Relaxed);
        }
        for (connection_id, app_id) in expired_connections {
            let handler = self.handler.get().and_then(Weak::upgrade);
            let removals = if let Some(handler) = handler.as_ref() {
                match handler.app_manager().find_by_id(&app_id).await {
                    Ok(Some(app)) => PresenceService::new(Arc::clone(handler))
                        .unregister_connection(
                            &app,
                            &connection_id,
                            PresenceHistoryEventCause::Timeout,
                        )
                        .await
                        .map(|removals| {
                            let mut grouped = BTreeMap::<String, Vec<PresenceChange>>::new();
                            for removal in removals {
                                let wire_id = Some(removal.member.id.clone());
                                grouped
                                    .entry(removal.channel)
                                    .or_default()
                                    .push(PresenceChange {
                                        action: PresenceChangeAction::Leave,
                                        member: removal.member,
                                        wire_id,
                                    });
                            }
                            grouped
                        }),
                    Ok(None) => Ok(self.remove_connection_presence(&app_id, &connection_id)),
                    Err(error) => Err(error),
                }
            } else {
                Ok(self.remove_connection_presence(&app_id, &connection_id))
            };
            let removals = match removals {
                Ok(removals) => removals,
                Err(error) => {
                    warn!(
                        app_id = %app_id,
                        connection_id = %connection_id,
                        error = %error,
                        "failed to persist expired Ably presence leaves"
                    );
                    self.remove_connection_presence(&app_id, &connection_id)
                }
            };
            for (channel, changes) in removals {
                let replication = PresenceReplication {
                    changes,
                    unregister_connection: None,
                };
                if let Some(handler) = handler.as_ref() {
                    if let Err(error) = handler
                        .fanout_presence(&app_id, &channel, replication)
                        .await
                    {
                        warn!(
                            app_id = %app_id,
                            channel = %channel,
                            error = %error,
                            "failed to replicate expired presence leaves"
                        );
                    }
                } else if let Err(error) = self.deliver_presence(&app_id, &channel, &replication) {
                    warn!(
                        app_id = %app_id,
                        channel = %channel,
                        error = %error,
                        "failed to deliver expired presence leaves"
                    );
                }
            }
            if let Some(handler) = handler.as_ref()
                && let Err(error) = handler
                    .fanout_presence(
                        &app_id,
                        "",
                        PresenceReplication {
                            changes: Vec::new(),
                            unregister_connection: Some(connection_id.clone()),
                        },
                    )
                    .await
            {
                warn!(
                    app_id = %app_id,
                    connection_id = %connection_id,
                    error = %error,
                    "failed to replicate expired presence connection removal"
                );
            }
        }
    }

    fn bound_sessions(&self) {
        if self.sessions.len() < ABLY_COMPAT_MAX_SESSIONS {
            return;
        }
        if let Some(key) = self
            .sessions
            .iter()
            .min_by_key(|entry| entry.expires_at_ms)
            .map(|entry| entry.key().clone())
        {
            self.sessions.remove(&key);
        }
    }

    fn bound_tokens(&self) {
        if self.tokens.len() < ABLY_COMPAT_MAX_TOKENS {
            return;
        }
        if let Some(key) = self
            .tokens
            .iter()
            .min_by_key(|entry| entry.expires_ms)
            .map(|entry| entry.key().clone())
        {
            self.tokens.remove(&key);
        }
    }

    async fn claim_nonce(&self, key_name: &str, nonce: &str) -> Result<bool, AblyAuthError> {
        let cache_key = nonce_cache_key(key_name, nonce);
        if let Some(cache) = &self.cache {
            return cache
                .set_if_not_exists(&cache_key, "1", self.config.nonce_ttl_seconds.max(1))
                .await
                .map_err(|error| AblyAuthError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: 50000,
                    message: error.to_string(),
                });
        }

        if self.nonces.len() >= ABLY_COMPAT_MAX_TOKENS
            && let Some(oldest) = self
                .nonces
                .iter()
                .min_by_key(|entry| *entry.value())
                .map(|entry| entry.key().clone())
        {
            self.nonces.remove(&oldest);
        }
        let expires_ms = now_ms().saturating_add(
            i64::try_from(self.config.nonce_ttl_seconds.saturating_mul(1000)).unwrap_or(i64::MAX),
        );
        Ok(self.nonces.insert(cache_key, expires_ms).is_none())
    }

    async fn store_revocation(
        &self,
        app_id: &str,
        target_type: &str,
        target_value: &str,
        record: AblyRevocationRecord,
    ) -> Result<(), AblyAuthError> {
        let key = revocation_cache_key(app_id, target_type, target_value);
        if let Some(cache) = &self.cache {
            let encoded = serde_json::to_string(&record).map_err(|error| AblyAuthError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: 50000,
                message: error.to_string(),
            })?;
            cache
                .set(&key, &encoded, 24 * 60 * 60)
                .await
                .map_err(|error| AblyAuthError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: 50000,
                    message: error.to_string(),
                })?;
        }
        self.revocations.insert(key, record);
        Ok(())
    }

    async fn revocation(
        &self,
        app_id: &str,
        target_type: &str,
        target_value: &str,
    ) -> Option<AblyRevocationRecord> {
        let key = revocation_cache_key(app_id, target_type, target_value);
        if let Some(cache) = &self.cache {
            let encoded = cache.get(&key).await.ok()??;
            let record = serde_json::from_str::<AblyRevocationRecord>(&encoded).ok()?;
            self.revocations.insert(key, record.clone());
            return Some(record);
        }
        self.revocations.get(&key).map(|record| record.clone())
    }

    async fn authorization_is_revoked(
        &self,
        app_id: &str,
        authorization: &ConnectionAuthorization,
        attached_channels: &HashMap<String, AblyConnectionAttachment>,
    ) -> bool {
        if !authorization.revocable {
            return false;
        }
        let now = now_ms();
        let mut targets = Vec::with_capacity(attached_channels.len().saturating_add(2));
        if let Some(client_id) = authorization.client_id.as_deref() {
            targets.push(("clientId", client_id));
        }
        if let Some(revocation_key) = authorization.revocation_key.as_deref() {
            targets.push(("revocationKey", revocation_key));
        }
        for channel in attached_channels.keys() {
            targets.push(("channel", channel.as_str()));
        }
        for (target_type, target_value) in targets {
            if self
                .revocation(app_id, target_type, target_value)
                .await
                .is_some_and(|record| {
                    authorization.issued_ms < record.issued_before && now >= record.applies_at
                })
            {
                return true;
            }
        }
        let channel_records = if let Some(cache) = &self.cache {
            cache
                .scan_prefix(&format!("ably-compat:revocation:{app_id}:"), 1_000)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(_, encoded)| {
                    serde_json::from_str::<AblyRevocationRecord>(&encoded).ok()
                })
                .collect::<Vec<_>>()
        } else {
            self.revocations
                .iter()
                .map(|record| record.value().clone())
                .collect::<Vec<_>>()
        };
        for record in channel_records {
            if record.target_type == "channel"
                && authorization.issued_ms < record.issued_before
                && now >= record.applies_at
                && ensure_ably_capability(
                    authorization.capabilities.as_ref(),
                    &record.target_value,
                    AblyCapabilityCheck::AnyChannelAccess,
                )
                .is_ok()
            {
                return true;
            }
        }
        false
    }

    fn notify_revocation_change(&self, app_id: &str, reauth_hint: bool) {
        for session in &self.live_sessions {
            if session.app_id != app_id {
                continue;
            }
            let generation = session
                .authorization
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .generation;
            let command = if reauth_hint {
                AblySessionCommand::ReauthHint { generation }
            } else {
                AblySessionCommand::RevocationChanged { generation }
            };
            let _ = session.command_tx.try_send(command);
        }
    }

    fn has_subscribers(&self, app_id: &str, channel: &str) -> bool {
        let key = channel_key(app_id, channel);
        self.channels
            .get(&key)
            .is_some_and(|state| !lock_channel_state(state.value()).subscribers.is_empty())
    }

    fn channel_state(&self, app_id: &str, channel: &str) -> Arc<Mutex<AblyChannelState>> {
        self.channels
            .entry(channel_key(app_id, channel))
            .or_insert_with(|| Arc::new(Mutex::new(AblyChannelState::default())))
            .clone()
    }

    #[cfg(test)]
    fn presence_snapshot(&self, app_id: &str, channel: &str) -> Vec<AblyPresenceMessage> {
        self.presence_registry
            .snapshot(app_id, channel)
            .into_iter()
            .map(|member| ably_presence_from_record(member, 1))
            .collect()
    }

    fn remove_connection_presence(
        &self,
        app_id: &str,
        connection_id: &str,
    ) -> BTreeMap<String, Vec<PresenceChange>> {
        let mut leaves = BTreeMap::<String, Vec<PresenceChange>>::new();
        for removal in self
            .presence_registry
            .unregister_connection(app_id, connection_id)
        {
            let mut member = removal.member;
            member.timestamp_ms = now_ms();
            let wire_id = Some(member.id.clone());
            leaves
                .entry(removal.channel)
                .or_default()
                .push(PresenceChange {
                    action: PresenceChangeAction::Leave,
                    member,
                    wire_id,
                });
        }
        leaves
    }

    async fn begin_attach(
        &self,
        app_id: &str,
        channel: &AblyChannelName,
        attachment: &AblyAttachment<'_>,
    ) -> Result<(), StatsError> {
        let state = self.channel_state(app_id, channel.base());
        let activated = {
            let mut state = lock_channel_state(&state);
            let activated = state.subscribers.is_empty();
            state.subscribers.insert(
                subscriber_key(attachment.session_id, channel.requested()),
                AblySubscriber {
                    connection_id: Arc::from(attachment.connection_id),
                    sender: attachment.sender.clone(),
                    filter: attachment.filter.clone(),
                    mode_flags: attachment.mode_flags,
                    echo: attachment.echo,
                    #[cfg(feature = "delta")]
                    delta: ably_delta_state(&attachment.params),
                    attach_gate: Some(AblyAttachGate::default()),
                },
            );
            activated
        };
        if activated {
            let current = self.stats.channel_opened(app_id);
            let observation = StatsObservation::channel_opened(app_id, now_ms(), current)?;
            if let Err(error) = self.stats.record(observation).await {
                self.stats.channel_closed(app_id);
                let mut state = lock_channel_state(&state);
                state
                    .subscribers
                    .remove(&subscriber_key(attachment.session_id, channel.requested()));
                let empty = state.subscribers.is_empty();
                drop(state);
                if empty {
                    self.channels.remove(&channel_key(app_id, channel.base()));
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn finish_attach_gate(
        &self,
        app_id: &str,
        channel: &AblyChannelName,
        session_id: &str,
        high_water: Option<&AblyChannelPosition>,
    ) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
        let state = self.channel_state(app_id, channel.base());
        let mut state = lock_channel_state(&state);
        let key = subscriber_key(session_id, channel.requested());
        let gate = state
            .subscribers
            .get_mut(&key)
            .and_then(|subscriber| subscriber.attach_gate.take())
            .unwrap_or_default();
        if gate.overflowed {
            state.subscribers.remove(&key);
            return Err(AblyRecoveryFailure::channel(
                90003,
                format!(
                    "unable to attach channel '{}' because continuity buffering overflowed",
                    channel.requested()
                ),
            ));
        }

        let mut messages = Vec::with_capacity(gate.messages.len());
        for message in gate.messages {
            let Some(position) = message
                .channel_serial
                .as_deref()
                .and_then(|serial| parse_ably_channel_serial(serial).ok())
            else {
                messages.push(message);
                continue;
            };
            if let Some(high_water) = high_water {
                if position.stream_id != high_water.stream_id {
                    state.subscribers.remove(&key);
                    return Err(AblyRecoveryFailure::channel(
                        90005,
                        format!(
                            "unable to attach channel '{}' because the stream changed",
                            channel.requested()
                        ),
                    ));
                }
                if position.serial <= high_water.serial {
                    continue;
                }
            }
            messages.push(message);
        }
        Ok(messages)
    }

    #[cfg(feature = "delta")]
    fn set_subscriber_delta(
        &self,
        app_id: &str,
        channel: &AblyChannelName,
        session_id: &str,
        delta: AblyDeltaState,
    ) {
        let state = self.channel_state(app_id, channel.base());
        let mut state = lock_channel_state(&state);
        if let Some(subscriber) = state
            .subscribers
            .get_mut(&subscriber_key(session_id, channel.requested()))
        {
            subscriber.delta = Some(delta);
        }
    }

    fn attach_clean(
        &self,
        app_id: &str,
        channel: &AblyChannelName,
        attachment: AblyAttachment<'_>,
        channel_serial: Option<String>,
        mut replay: Vec<AblyProtocolMessage>,
    ) {
        let now = now_ms();
        let state = self.channel_state(app_id, channel.base());
        let mut state = lock_channel_state(&state);
        state
            .subscribers
            .entry(subscriber_key(attachment.session_id, channel.requested()))
            .or_insert_with(|| AblySubscriber {
                connection_id: Arc::from(attachment.connection_id),
                sender: attachment.sender.clone(),
                filter: attachment.filter.clone(),
                mode_flags: attachment.mode_flags,
                echo: attachment.echo,
                #[cfg(feature = "delta")]
                delta: ably_delta_state(&attachment.params),
                attach_gate: Some(AblyAttachGate::default()),
            });
        if let Some(position) = channel_serial
            .as_deref()
            .and_then(|serial| parse_ably_channel_serial(serial).ok())
        {
            state.current_stream_id = Some(position.stream_id.clone());
            state.attach_position = Some(position);
            state.last_touched_ms = now;
        }
        drop(state);
        let high_water = channel_serial
            .as_deref()
            .and_then(|serial| parse_ably_channel_serial(serial).ok());
        let buffered = match self.finish_attach_gate(
            app_id,
            channel,
            attachment.session_id,
            high_water.as_ref(),
        ) {
            Ok(buffered) => buffered,
            Err(failure) => {
                self.attach_failed(app_id, channel, attachment, channel_serial, failure);
                return;
            }
        };
        let has_backlog = !replay.is_empty();
        replay.extend(buffered);
        let flags = attachment.mode_flags | if has_backlog { FLAG_HAS_BACKLOG } else { 0 };
        let delta = send_ably_attached(
            &attachment.sender,
            channel.requested(),
            channel_serial,
            Some(flags),
            Some(attachment.params),
            attachment.presence,
            None,
            replay,
            attachment.filter.as_deref(),
        );
        #[cfg(feature = "delta")]
        if let Some(delta) = delta {
            self.set_subscriber_delta(app_id, channel, attachment.session_id, delta);
        }
        #[cfg(not(feature = "delta"))]
        let _ = delta;
    }

    async fn unsubscribe(
        &self,
        app_id: &str,
        channel: &AblyChannelName,
        session_id: &str,
    ) -> Result<(), StatsError> {
        let key = channel_key(app_id, channel.base());
        let Some(state) = self.channels.get(&key).map(|entry| entry.clone()) else {
            return Ok(());
        };
        let should_close = {
            let mut state = lock_channel_state(&state);
            let removed = state
                .subscribers
                .remove(&subscriber_key(session_id, channel.requested()))
                .is_some();
            let should_close = removed && state.subscribers.is_empty();
            if should_close {
                state.attach_position = None;
            }
            should_close
        };
        if should_close {
            self.channels.remove(&key);
            self.stats.channel_closed(app_id);
            let observation = StatsObservation::channel_closed(app_id, now_ms())?;
            self.stats.record(observation).await?;
        }
        Ok(())
    }

    fn until_attach_position(
        &self,
        app_id: &str,
        channel: &AblyChannelName,
    ) -> Option<AblyChannelPosition> {
        let state = self.channel_state(app_id, channel.base());
        lock_channel_state(&state).attach_position.clone()
    }

    fn broadcast(
        &self,
        app_id: &str,
        channel: &str,
        message: AblyProtocolMessage,
        publisher_connection_id: Option<&str>,
        echo_override: Option<bool>,
    ) {
        let now = now_ms();
        let mut delivered_messages = 0u64;
        let mut delivered_bytes = 0u64;
        let state = self.channel_state(app_id, channel);
        let subscribers = {
            let mut state = lock_channel_state(&state);
            if let Some(position) = message
                .channel_serial
                .as_deref()
                .and_then(|serial| parse_ably_channel_serial(serial).ok())
            {
                #[cfg(feature = "delta")]
                if state
                    .current_stream_id
                    .as_deref()
                    .is_some_and(|stream_id| stream_id != position.stream_id)
                {
                    for subscriber in state.subscribers.values_mut() {
                        if subscriber.delta.is_some() {
                            subscriber.delta = Some(AblyDeltaState::default());
                        }
                    }
                }
                state.current_stream_id = Some(position.stream_id);
                state.last_touched_ms = now;
            }

            let required_mode = match message.action {
                ACTION_MESSAGE => Some(ABLY_MODE_SUBSCRIBE),
                ACTION_PRESENCE => Some(ABLY_MODE_PRESENCE_SUBSCRIBE),
                ACTION_ANNOTATION => Some(ABLY_MODE_ANNOTATION_SUBSCRIBE),
                _ => None,
            };
            let message_bytes = state
                .subscribers
                .values()
                .any(|subscriber| subscriber.attach_gate.is_some())
                .then(|| {
                    sonic_rs::to_vec(&message)
                        .map(|bytes| bytes.len())
                        .unwrap_or(ABLY_ATTACH_GATE_MAX_BYTES.saturating_add(1))
                });
            let mut ready = Vec::new();
            for (key, subscriber) in state.subscribers.iter_mut() {
                if !should_deliver_to_subscriber(
                    publisher_connection_id,
                    subscriber.connection_id.as_ref(),
                    subscriber.echo,
                    echo_override,
                ) || required_mode.is_some_and(|mode| subscriber.mode_flags & mode == 0)
                {
                    continue;
                }
                if let Some(gate) = subscriber.attach_gate.as_mut() {
                    let message_bytes = message_bytes.unwrap_or_default();
                    if gate.overflowed
                        || gate.messages.len() >= ABLY_ATTACH_GATE_MAX_MESSAGES
                        || gate.bytes.saturating_add(message_bytes) > ABLY_ATTACH_GATE_MAX_BYTES
                    {
                        gate.overflowed = true;
                        gate.messages.clear();
                        gate.bytes = 0;
                    } else {
                        gate.bytes = gate.bytes.saturating_add(message_bytes);
                        gate.messages.push(message.clone());
                    }
                } else {
                    ready.push((key.clone(), subscriber.clone()));
                }
            }
            ready
        };
        #[cfg(feature = "delta")]
        let (delta_subscribers, subscribers): (Vec<_>, Vec<_>) =
            subscribers.into_iter().partition(|(_, subscriber)| {
                subscriber.delta.is_some() && message.action == ACTION_MESSAGE
            });

        #[cfg(feature = "delta")]
        let mut delta_updates = Vec::new();
        #[cfg(feature = "delta")]
        let mut stale = Vec::new();
        #[cfg(feature = "delta")]
        let mut delta_groups = HashMap::<
            (AblyFormat, Arc<str>, Option<Arc<str>>),
            Vec<(AblySubscriberKey, AblySubscriber)>,
        >::new();
        #[cfg(feature = "delta")]
        let delta_now = Instant::now();
        #[cfg(feature = "delta")]
        for (key, subscriber) in delta_subscribers {
            // A committed Ably message ID identifies immutable canonical data.
            // Format, requested-channel projection, and fresh base ID therefore
            // fully determine the encoded delta bytes shared by this group.
            let base_id = subscriber.delta.as_ref().and_then(|delta| {
                delta
                    .fresh_base(delta_now)
                    .map(|(message_id, _)| Arc::clone(message_id))
            });
            delta_groups
                .entry((
                    subscriber.sender.format(),
                    key.requested_channel.clone(),
                    base_id,
                ))
                .or_default()
                .push((key, subscriber));
        }
        #[cfg(feature = "delta")]
        for ((format, requested_channel, _), subscribers) in delta_groups {
            let mut projected = if requested_channel.as_ref() == channel
                && message.channel.as_deref() == Some(channel)
            {
                message.clone()
            } else {
                AblyProtocolMessage {
                    channel: Some(requested_channel.to_string()),
                    ..message.clone()
                }
            };
            let Some((_, first_subscriber)) = subscribers.first() else {
                continue;
            };
            if !ably_filter_protocol_message(&mut projected, first_subscriber.filter.as_deref()) {
                continue;
            }
            let (projected, next_state) = project_ably_delta_message(
                projected,
                first_subscriber
                    .delta
                    .as_ref()
                    .expect("partitioned delta subscriber"),
            );
            match encode_protocol_bytes(&projected, format) {
                Ok(bytes) => {
                    let encoded = Arc::new(bytes);
                    let byte_count = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
                    self.metrics.encoded.fetch_add(1, Ordering::Relaxed);
                    for (subscriber_key, subscriber) in subscribers {
                        if subscriber.sender.send_data(Arc::clone(&encoded)).is_ok() {
                            self.metrics.fanout.fetch_add(1, Ordering::Relaxed);
                            delivered_messages = delivered_messages.saturating_add(1);
                            delivered_bytes = delivered_bytes.saturating_add(byte_count);
                            delta_updates.push((subscriber_key, next_state.clone()));
                        } else {
                            stale.push(subscriber_key);
                        }
                    }
                }
                Err(error) => {
                    warn!(app_id = %app_id, channel = %channel, error = %error, "failed to encode Ably delta delivery");
                }
            }
        }

        let mut grouped =
            HashMap::<(AblyFormat, Arc<str>), Vec<(AblySubscriberKey, AblySubscriber)>>::new();
        for (key, subscriber) in subscribers {
            grouped
                .entry((subscriber.sender.format(), key.requested_channel.clone()))
                .or_default()
                .push((key, subscriber));
        }

        #[cfg(not(feature = "delta"))]
        let mut stale = Vec::new();
        for ((format, requested_channel), subscribers) in grouped {
            let mut projected = if requested_channel.as_ref() == channel
                && message.channel.as_deref() == Some(channel)
            {
                message.clone()
            } else {
                AblyProtocolMessage {
                    channel: Some(requested_channel.to_string()),
                    ..message.clone()
                }
            };
            let filter = subscribers
                .first()
                .and_then(|(_, subscriber)| subscriber.filter.as_deref());
            if !ably_filter_protocol_message(&mut projected, filter) {
                continue;
            }
            let encoded = match encode_protocol_bytes(&projected, format) {
                Ok(bytes) => Arc::new(bytes),
                Err(error) => {
                    warn!(app_id = %app_id, channel = %channel, error = %error, "failed to encode Ably compatibility delivery");
                    continue;
                }
            };
            self.metrics.encoded.fetch_add(1, Ordering::Relaxed);
            let encoded_bytes = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
            for (subscriber_key, subscriber) in subscribers {
                if subscriber.sender.send_data(Arc::clone(&encoded)).is_err() {
                    stale.push(subscriber_key);
                } else {
                    self.metrics.fanout.fetch_add(1, Ordering::Relaxed);
                    delivered_messages = delivered_messages.saturating_add(1);
                    delivered_bytes = delivered_bytes.saturating_add(encoded_bytes);
                }
            }
        }

        if message.action == ACTION_MESSAGE
            && delivered_messages > 0
            && let Ok(observation) = StatsObservation::messages(
                app_id,
                now,
                "outbound",
                "realtime",
                delivered_messages,
                delivered_bytes,
            )
            && let Err(error) = self.stats.try_record(observation)
        {
            debug!(app_id = %app_id, error = %error, "stats outbound observation was not queued");
        }

        #[cfg(feature = "delta")]
        if !delta_updates.is_empty() {
            let mut state = lock_channel_state(&state);
            for (key, next_state) in delta_updates {
                if let Some(subscriber) = state.subscribers.get_mut(&key) {
                    subscriber.delta = Some(next_state);
                }
            }
        }

        if !stale.is_empty() {
            let mut state = lock_channel_state(&state);
            for subscriber in stale {
                state.subscribers.remove(&subscriber);
            }
        }
    }

    fn attach_cold_recovery(
        &self,
        app_id: &str,
        channel: &AblyChannelName,
        attachment: AblyAttachment<'_>,
        position: &AblyChannelPosition,
        mut replay: Vec<AblyProtocolMessage>,
    ) {
        let now = now_ms();
        let state = self.channel_state(app_id, channel.base());
        let mut state = lock_channel_state(&state);
        state
            .subscribers
            .entry(subscriber_key(attachment.session_id, channel.requested()))
            .or_insert_with(|| AblySubscriber {
                connection_id: Arc::from(attachment.connection_id),
                sender: attachment.sender.clone(),
                filter: attachment.filter.clone(),
                mode_flags: attachment.mode_flags,
                echo: attachment.echo,
                #[cfg(feature = "delta")]
                delta: ably_delta_state(&attachment.params),
                attach_gate: Some(AblyAttachGate::default()),
            });
        if state
            .current_stream_id
            .as_deref()
            .is_some_and(|current| current != position.stream_id)
        {
            drop(state);
            self.attach_failed(
                app_id,
                channel,
                attachment,
                Some(encode_ably_channel_serial(
                    &position.stream_id,
                    position.serial,
                )),
                AblyRecoveryFailure::channel(
                    90005,
                    format!(
                        "unable to recover channel '{}' because the stream changed",
                        channel.requested()
                    ),
                ),
            );
            return;
        }

        state.current_stream_id = Some(position.stream_id.clone());
        state.last_touched_ms = now;
        drop(state);
        let high_water = replay
            .iter()
            .rev()
            .find_map(|message| {
                message
                    .channel_serial
                    .as_deref()
                    .and_then(|serial| parse_ably_channel_serial(serial).ok())
            })
            .unwrap_or_else(|| position.clone());
        let buffered = match self.finish_attach_gate(
            app_id,
            channel,
            attachment.session_id,
            Some(&high_water),
        ) {
            Ok(buffered) => buffered,
            Err(failure) => {
                self.attach_failed(
                    app_id,
                    channel,
                    attachment,
                    Some(encode_ably_channel_serial(
                        &position.stream_id,
                        position.serial,
                    )),
                    failure,
                );
                return;
            }
        };
        let has_backlog = !replay.is_empty();
        replay.extend(buffered);
        let flags = if !has_backlog {
            FLAG_RESUMED
        } else {
            FLAG_RESUMED | FLAG_HAS_BACKLOG
        };
        let delta = send_ably_attached(
            &attachment.sender,
            channel.requested(),
            Some(encode_ably_channel_serial(
                &position.stream_id,
                position.serial,
            )),
            Some(flags | attachment.mode_flags),
            Some(attachment.params),
            attachment.presence,
            None,
            replay,
            attachment.filter.as_deref(),
        );
        #[cfg(feature = "delta")]
        if let Some(delta) = delta {
            self.set_subscriber_delta(app_id, channel, attachment.session_id, delta);
        }
        #[cfg(not(feature = "delta"))]
        let _ = delta;
    }

    fn attach_failed(
        &self,
        app_id: &str,
        channel: &AblyChannelName,
        attachment: AblyAttachment<'_>,
        channel_serial: Option<String>,
        failure: AblyRecoveryFailure,
    ) {
        let state = self.channel_state(app_id, channel.base());
        let mut state = lock_channel_state(&state);
        state.subscribers.insert(
            subscriber_key(attachment.session_id, channel.requested()),
            AblySubscriber {
                connection_id: Arc::from(attachment.connection_id),
                sender: attachment.sender.clone(),
                filter: attachment.filter.clone(),
                mode_flags: attachment.mode_flags,
                echo: attachment.echo,
                #[cfg(feature = "delta")]
                delta: ably_delta_state(&attachment.params),
                attach_gate: None,
            },
        );
        let _ = send_ably_attached(
            &attachment.sender,
            channel.requested(),
            channel_serial,
            Some(attachment.mode_flags),
            Some(attachment.params),
            attachment.presence,
            Some(failure),
            Vec::new(),
            attachment.filter.as_deref(),
        );
    }

    async fn begin_connection(
        &self,
        app_id: &str,
        client_id: Option<&str>,
        resume: Option<&str>,
        recover: Option<&str>,
    ) -> AblyConnectionStart {
        let requested_key = resume.or(recover);
        let Some(requested_key) = requested_key else {
            return AblyConnectionStart::Fresh;
        };
        let now = now_ms();
        let expired_code = if recover.is_some() { 80018 } else { 80008 };
        let record = if let Some(record) = self.sessions.get(requested_key) {
            Some(record.clone())
        } else if let Some(cache) = &self.cache {
            match cache.get(&session_cache_key(requested_key)).await {
                Ok(Some(encoded)) => serde_json::from_str::<AblySessionRecord>(&encoded)
                    .ok()
                    .inspect(|record| {
                        self.sessions
                            .insert(requested_key.to_string(), record.clone());
                    }),
                Ok(None) => None,
                Err(error) => {
                    warn!(error = %error, "Ably connection state cache read failed");
                    None
                }
            }
        } else {
            None
        };
        let Some(record) = record else {
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    expired_code,
                    "unable to recover connection (connection expired)",
                ),
            };
        };
        if record.expires_at_ms <= now {
            self.sessions.remove(requested_key);
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    expired_code,
                    "unable to recover connection (connection expired)",
                ),
            };
        }
        if record.app_id != app_id || record.client_id.as_deref() != client_id {
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    80011,
                    "unable to recover connection (incompatible auth params)",
                ),
            };
        }
        AblyConnectionStart::Resumed {
            connection_id: record.connection_id,
        }
    }

    async fn remember_connection(
        &self,
        connection_key: String,
        app_id: &str,
        connection_id: &str,
        client_id: Option<String>,
    ) {
        self.bound_sessions();
        let record = AblySessionRecord {
            app_id: app_id.to_string(),
            connection_id: connection_id.to_string(),
            client_id,
            expires_at_ms: now_ms()
                .saturating_add(i64::try_from(DEFAULT_CONNECTION_STATE_TTL_MS).unwrap_or(0)),
        };
        self.sessions.insert(connection_key.clone(), record.clone());
        if let Some(cache) = &self.cache {
            match serde_json::to_string(&record) {
                Ok(encoded) => {
                    let ttl_seconds = DEFAULT_CONNECTION_STATE_TTL_MS.div_ceil(1000);
                    if let Err(error) = cache
                        .set(&session_cache_key(&connection_key), &encoded, ttl_seconds)
                        .await
                    {
                        warn!(error = %error, "Ably connection state cache write failed");
                    }
                }
                Err(error) => {
                    warn!(error = %error, "Ably connection state serialization failed");
                }
            }
        }
    }

    async fn forget_connection(&self, connection_key: &str) {
        self.sessions.remove(connection_key);
        if let Some(cache) = &self.cache
            && let Err(error) = cache.remove(&session_cache_key(connection_key)).await
        {
            warn!(error = %error, "Ably connection state cache removal failed");
        }
    }

    fn resolve_live_connection(
        &self,
        app_id: &str,
        connection_key: &str,
    ) -> Option<AblySessionRecord> {
        let record = self
            .sessions
            .get(connection_key)
            .filter(|record| record.app_id == app_id && record.expires_at_ms > now_ms())?
            .clone();
        self.live_sessions
            .get(&record.connection_id)
            .filter(|session| session.app_id == app_id)
            .map(|_| record)
    }

    fn register_live_session(
        &self,
        connection_id: String,
        session: AblyLiveSession,
    ) -> Option<tokio::sync::mpsc::Sender<AblySessionCommand>> {
        self.live_sessions
            .insert(connection_id, session)
            .map(|previous| previous.command_tx)
    }

    fn unregister_live_session(&self, connection_id: &str, session_id: &str) {
        self.live_sessions
            .remove_if(connection_id, |_, current| current.session_id == session_id);
    }

    async fn issue_token(
        &self,
        key: &ResolvedAblyKey,
        client_id: Option<String>,
        ttl_ms: i64,
        capability: String,
        capabilities: Option<ConnectionCapabilities>,
    ) -> Result<AblyTokenDetails, AblyAuthError> {
        let issued = now_ms();
        let expires = issued.saturating_add(ttl_ms);
        let token = format!("sockudo-ably-{}", Uuid::new_v4());
        let record = AblyTokenRecord {
            app_id: key.app.id.clone(),
            key_name: key.key_name.clone(),
            client_id: client_id.clone(),
            issued_ms: issued,
            expires_ms: expires,
            capabilities,
            revocable: key.revocable_tokens,
            rotation_id: key.rotation_id.clone(),
            revocation_key: None,
        };

        if let Some(cache) = &self.cache {
            let encoded = serde_json::to_string(&record).map_err(|error| AblyAuthError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: 50000,
                message: error.to_string(),
            })?;
            let ttl_seconds = u64::try_from(ttl_ms).unwrap_or(1).saturating_add(999) / 1000;
            cache
                .set(&token_cache_key(&token), &encoded, ttl_seconds.max(1))
                .await
                .map_err(|error| AblyAuthError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: 50000,
                    message: error.to_string(),
                })?;
        }
        self.bound_tokens();
        self.tokens.insert(token.clone(), record);
        Ok(AblyTokenDetails {
            token,
            key_name: key.key_name.clone(),
            issued,
            expires,
            client_id,
            capability,
        })
    }

    async fn resolve_token(&self, token: &str) -> Option<AblyTokenRecord> {
        let record = if let Some(record) = self.tokens.get(token).map(|entry| entry.clone()) {
            record
        } else {
            let encoded = self
                .cache
                .as_ref()?
                .get(&token_cache_key(token))
                .await
                .ok()??;
            let record = serde_json::from_str::<AblyTokenRecord>(&encoded).ok()?;
            self.bound_tokens();
            self.tokens.insert(token.to_string(), record.clone());
            record
        };
        Some(record)
    }
}

fn should_deliver_to_subscriber(
    publisher_connection_id: Option<&str>,
    subscriber_connection_id: &str,
    subscriber_echo: bool,
    echo_override: Option<bool>,
) -> bool {
    publisher_connection_id != Some(subscriber_connection_id)
        || echo_override.unwrap_or(subscriber_echo)
}

#[cfg(feature = "delta")]
fn project_ably_delta_message(
    mut protocol: AblyProtocolMessage,
    state: &AblyDeltaState,
) -> (AblyProtocolMessage, AblyDeltaState) {
    let Some(message) = protocol
        .messages
        .as_mut()
        .and_then(|messages| messages.first_mut())
    else {
        return (protocol, AblyDeltaState::default());
    };
    let Some(message_id) = message.id.as_deref().map(Arc::<str>::from) else {
        return (protocol, AblyDeltaState::default());
    };
    // Encoded JSON keeps its exact publish-time bytes. A message whose wire
    // encoding is still present (cipher, base64, or an unknown transform) is
    // not safe to reinterpret here, so it breaks the delta chain and remains
    // a canonical full delivery.
    let target = match (message.encoded_json.as_ref(), message.encoding.as_ref()) {
        (Some(encoded), _) => Arc::clone(encoded),
        (None, None) => match sonic_rs::to_vec(&message.data) {
            Ok(encoded) => Arc::<[u8]>::from(encoded),
            Err(_) => return (protocol, AblyDeltaState::default()),
        },
        (None, Some(_)) => return (protocol, AblyDeltaState::default()),
    };
    if target.len() > ABLY_DELTA_BASE_MAX_BYTES {
        return (protocol, AblyDeltaState::default());
    }

    let now = Instant::now();
    let next_state = AblyDeltaState::with_base(Arc::clone(&message_id), Arc::clone(&target), now);
    let full_payload = match std::str::from_utf8(&target) {
        Ok(payload) => payload,
        Err(_) => return (protocol, AblyDeltaState::default()),
    };

    if let Some((previous_id, previous_payload)) = state.fresh_base(now)
        && message.extras.as_ref().is_none_or(Value::is_object)
        && let Ok(delta) = sockudo_delta::compute_vcdiff(previous_payload, &target)
        && delta.len() < target.len()
    {
        let mut extras = message.extras.take().unwrap_or_else(|| json!({}));
        if let Some(object) = extras.as_object_mut() {
            object.insert(
                "delta",
                json!({ "from": previous_id.as_ref(), "format": "vcdiff" }),
            );
        }
        message.data = Some(json!(
            base64::engine::general_purpose::STANDARD.encode(delta)
        ));
        message.encoding = Some("json/vcdiff/base64".to_string());
        message.extras = Some(extras);
    } else {
        message.data = Some(json!(full_payload));
        message.encoding = Some("json".to_string());
    }
    (protocol, next_state)
}

pub async fn handle_ably_realtime_upgrade(
    Query(params): Query<AblyConnectQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    if !handler.is_accepting() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let format = match parse_ably_format(params.format.as_deref()) {
        Ok(format) => format,
        Err(message) => return ably_error_response(StatusCode::BAD_REQUEST, 40000, message),
    };

    let (resolved, credential_error) = match resolve_ably_auth_with_expiry(
        &runtime.hub,
        &handler,
        &headers,
        params.key.as_deref(),
        params.access_token.as_deref(),
        params.client_id.as_deref(),
        true,
    )
    .await
    {
        Ok(resolved) => (resolved, None),
        Err(error) if error.code == 40102 && params.access_token.is_some() => {
            match resolve_ably_auth_with_expiry(
                &runtime.hub,
                &handler,
                &headers,
                params.key.as_deref(),
                params.access_token.as_deref(),
                None,
                true,
            )
            .await
            {
                Ok(resolved) => (resolved, Some(error)),
                Err(_) => {
                    return ably_error_response_format(
                        error.status,
                        error.code,
                        error.message,
                        format,
                    );
                }
            }
        }
        Err(error) => {
            if error.status != StatusCode::UNAUTHORIZED {
                return ably_error_response_format(error.status, error.code, error.message, format);
            }
            let ws_cfg = handler.server_options().websocket.to_sockudo_ws_config(
                handler.server_options().websocket_max_payload_kb,
                handler.server_options().activity_timeout,
            );
            return ws
                .config(ws_cfg)
                .on_upgrade(move |socket| send_fatal_ably_socket_error(socket, format, error))
                .into_response();
        }
    };

    if let Some(allowed_origins) = resolved.app.allowed_origins_ref()
        && !allowed_origins.is_empty()
    {
        let origin = headers
            .get(header::ORIGIN)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !OriginValidator::validate_origin(origin, allowed_origins) {
            return ably_error_response(
                StatusCode::FORBIDDEN,
                40300,
                "Origin is not allowed for this app",
            );
        }
    }

    let ws_cfg = handler.server_options().websocket.to_sockudo_ws_config(
        handler.server_options().websocket_max_payload_kb,
        handler.server_options().activity_timeout,
    );
    if runtime.hub.config.realtime_admission == AblyRealtimeAdmission::PlacementConstraint {
        return ws
            .config(ws_cfg)
            .on_upgrade(move |socket| {
                send_ably_socket_failure(
                    socket,
                    format,
                    ACTION_DISCONNECTED,
                    error_info(
                        StatusCode::SERVICE_UNAVAILABLE,
                        50320,
                        "Active Traffic Management: traffic for this endpoint is temporarily redirected to a fallback host",
                    ),
                )
            })
            .into_response();
    }
    let hub = Arc::clone(&runtime.hub);
    let resume = params.resume.clone();
    let recover = params.recover.clone();
    let replace_presence_on_reenter = params.remain_present_for.is_some();
    let initial_error = credential_error.or_else(|| {
        resolved
            .expires_ms
            .filter(|expires_ms| *expires_ms <= now_ms())
            .map(|_| AblyAuthError::expired())
    });
    let stats_transport = if handler.server_options().ssl.enabled
        || headers
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("https"))
    {
        "tls"
    } else {
        "plain"
    };

    ws.config(ws_cfg)
        .on_upgrade(move |socket| async move {
            if let Err(error) = run_ably_realtime_socket(
                socket,
                AblyRealtimeSocketContext {
                    handler,
                    hub,
                    resolved,
                    initial_error,
                    resume,
                    recover,
                    format,
                    echo: params.echo,
                    replace_presence_on_reenter,
                    stats_transport,
                },
            )
            .await
            {
                warn!(error = %error, "Ably compatibility socket closed with error");
            }
        })
        .into_response()
}

async fn send_fatal_ably_socket_error(
    socket: sockudo_ws::axum_integration::WebSocket,
    format: AblyFormat,
    error: AblyAuthError,
) {
    send_ably_socket_failure(
        socket,
        format,
        ACTION_ERROR,
        error_info(error.status, error.code, error.message),
    )
    .await;
}

async fn send_ably_socket_failure(
    socket: sockudo_ws::axum_integration::WebSocket,
    format: AblyFormat,
    action: u8,
    error: AblyErrorInfo,
) {
    let (_, mut writer) = socket.split();
    let message = AblyProtocolMessage {
        action,
        error: Some(error),
        ..empty_protocol_message(action)
    };
    let Ok(bytes) = encode_protocol_bytes(&message, format) else {
        return;
    };
    let frame = match format {
        AblyFormat::Json => Message::Text(bytes),
        AblyFormat::MsgPack => Message::Binary(bytes),
    };
    let _ = writer.send(frame).await;
}

struct AblyRealtimeSocketContext {
    handler: Arc<ConnectionHandler>,
    hub: Arc<AblyCompatHub>,
    resolved: ResolvedAblyAuth,
    initial_error: Option<AblyAuthError>,
    resume: Option<String>,
    recover: Option<String>,
    format: AblyFormat,
    echo: bool,
    replace_presence_on_reenter: bool,
    stats_transport: &'static str,
}

async fn run_ably_realtime_socket(
    socket: sockudo_ws::axum_integration::WebSocket,
    context: AblyRealtimeSocketContext,
) -> SockudoResult<()> {
    let AblyRealtimeSocketContext {
        handler,
        hub,
        resolved,
        initial_error,
        resume,
        recover,
        format,
        echo,
        replace_presence_on_reenter,
        stats_transport,
    } = context;
    let app = resolved.app.clone();
    let mut authorization = ConnectionAuthorization::from_resolved(&resolved);

    let connection_start = hub
        .begin_connection(
            &app.id,
            authorization.client_id.as_deref(),
            resume.as_deref(),
            recover.as_deref(),
        )
        .await;
    let connection_id = match &connection_start {
        AblyConnectionStart::Resumed { connection_id } => connection_id.clone(),
        AblyConnectionStart::Fresh | AblyConnectionStart::Failed { .. } => {
            format!("sockudo-ably-{}", Uuid::new_v4().simple())
        }
    };
    let connection_key = format!("{}:{}", app.id, Uuid::new_v4().simple());
    let connection_error = match &connection_start {
        AblyConnectionStart::Failed { error } => Some(error.clone()),
        AblyConnectionStart::Fresh | AblyConnectionStart::Resumed { .. } => None,
    };
    hub.remember_connection(
        connection_key.clone(),
        &app.id,
        &connection_id,
        authorization.client_id.clone(),
    )
    .await;
    let active_connection_key = Arc::new(RwLock::new(connection_key.clone()));
    // A recovered transport keeps the stable Ably connection ID, but it must
    // not share subscriber ownership with the socket it supersedes. Otherwise
    // cleanup from the old socket can remove the recovered socket's channels.
    let session_id = format!("{}:{}", connection_id, Uuid::new_v4().simple());
    let (mut reader, mut writer) = socket.split();
    let (sender, mut outbound) =
        AblyOutbound::channel(format, OutboundLimits::default(), Arc::clone(&hub.metrics));
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = outbound.recv().await {
            let frame = match format {
                AblyFormat::Json => Message::Text((*frame.bytes).clone()),
                AblyFormat::MsgPack => Message::Binary((*frame.bytes).clone()),
            };
            if let Err(error) = writer.send(frame).await {
                debug!(error = %error, "Ably compatibility socket writer closed");
                break;
            }
        }
    });
    let heartbeat_sender = sender.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_millis(DEFAULT_MAX_IDLE_INTERVAL_MS / 2));
        loop {
            interval.tick().await;
            if heartbeat_sender
                .send_protocol(
                    &AblyProtocolMessage {
                        action: ACTION_HEARTBEAT,
                        ..empty_protocol_message(ACTION_HEARTBEAT)
                    },
                    OutboundPriority::Control,
                )
                .is_err()
            {
                break;
            }
        }
    });

    if let Some(error) = initial_error {
        send_protocol_disconnected(&sender, error.code, error.message);
        hub.forget_connection(&connection_key).await;
        drop(sender);
        heartbeat_task.abort();
        let _ = heartbeat_task.await;
        let _ = writer_task.await;
        return Ok(());
    }

    let current_connections = hub.stats.connection_opened(&app.id);
    let opened = StatsObservation::connection_opened(
        &app.id,
        now_ms(),
        stats_transport,
        current_connections,
    )
    .map_err(stats_sockudo_error)?;
    if let Err(error) = hub.stats.record(opened).await {
        hub.stats.connection_closed(&app.id);
        drop(sender);
        heartbeat_task.abort();
        let _ = heartbeat_task.await;
        let _ = writer_task.await;
        return Err(stats_sockudo_error(error));
    }

    let presence_service = PresenceService::new(Arc::clone(&handler));
    presence_service.register_connection(&app.id, &connection_id);

    let lease_hub = Arc::clone(&hub);
    let lease_key = Arc::clone(&active_connection_key);
    let lease_app_id = app.id.clone();
    let lease_connection_id = connection_id.clone();
    let lease_client_id = authorization.client_id.clone();
    let connection_lease_task = tokio::spawn(async move {
        let refresh_interval = Duration::from_millis(DEFAULT_CONNECTION_STATE_TTL_MS / 2);
        loop {
            tokio::time::sleep(refresh_interval).await;
            let connection_key = lease_key
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            lease_hub
                .remember_connection(
                    connection_key,
                    &lease_app_id,
                    &lease_connection_id,
                    lease_client_id.clone(),
                )
                .await;
        }
    });

    send_protocol(
        &sender,
        connected_message(
            &connection_id,
            &connection_key,
            authorization.connection_client_id.clone(),
            connection_error,
        ),
    );

    let mut attached_channels = HashMap::new();
    let shared_authorization = Arc::new(RwLock::new(authorization.clone()));
    let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(8);
    let previous_session = hub.register_live_session(
        connection_id.clone(),
        AblyLiveSession {
            session_id: session_id.clone(),
            app_id: app.id.clone(),
            authorization: Arc::clone(&shared_authorization),
            command_tx,
        },
    );
    if let Some(previous_session) = previous_session {
        let _ = previous_session.send(AblySessionCommand::Superseded).await;
    }
    hub.session_echo.insert(session_id.clone(), echo);
    let mut revocation_poll = tokio::time::interval(Duration::from_millis(500));
    revocation_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut renewal_hint_sent = false;
    let mut graceful_close = false;
    loop {
        let next_auth_deadline = authorization_deadline(&authorization, renewal_hint_sent);
        let auth_sleep = tokio::time::sleep(next_auth_deadline);
        tokio::pin!(auth_sleep);
        let frame = tokio::select! {
            frame = reader.next() => frame,
            command = command_rx.recv() => {
                match command {
                    Some(AblySessionCommand::ReauthHint { generation })
                        if generation == authorization.generation => {
                            send_protocol(&sender, empty_protocol_message(ACTION_AUTH));
                            renewal_hint_sent = true;
                            continue;
                        }
                    Some(AblySessionCommand::RevocationChanged { generation })
                        if generation == authorization.generation => {
                            if hub.authorization_is_revoked(&app.id, &authorization, &attached_channels).await {
                                send_protocol_disconnected(&sender, 40141, "Token revoked");
                                break;
                            }
                            continue;
                        }
                    Some(AblySessionCommand::Superseded) => break,
                    Some(_) => continue,
                    None => break,
                }
            }
            _ = revocation_poll.tick(), if authorization.revocable => {
                if hub.authorization_is_revoked(&app.id, &authorization, &attached_channels).await {
                    send_protocol_disconnected(&sender, 40141, "Token revoked");
                    break;
                }
                continue;
            }
            _ = &mut auth_sleep, if authorization.expires_ms.is_some() => {
                if should_send_renewal_hint(&authorization, renewal_hint_sent) {
                    send_protocol(&sender, empty_protocol_message(ACTION_AUTH));
                    renewal_hint_sent = true;
                    continue;
                }
                send_protocol_disconnected(&sender, 40142, "Token expired");
                break;
            }
        };
        let Some(frame) = frame else { break };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                debug!(error = %error, "Ably compatibility socket reader closed");
                break;
            }
        };
        let bytes = match frame {
            Message::Text(bytes) | Message::Binary(bytes) => bytes,
            Message::Ping(payload) => {
                send_protocol(
                    &sender,
                    AblyProtocolMessage {
                        action: ACTION_HEARTBEAT,
                        ..empty_protocol_message(ACTION_HEARTBEAT)
                    },
                );
                let _ = payload;
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => break,
        };
        let inbound = match decode_ably_protocol_message(bytes.as_ref(), format) {
            Ok(inbound) => inbound,
            Err(error) => {
                // A malformed frame is scoped to this connection. Reporting it
                // as a protocol ERROR keeps existing channel subscriptions
                // intact and lets the client decide whether to reconnect.
                send_protocol_error(
                    &sender,
                    40000,
                    format!("Malformed Ably ProtocolMessage: {error}"),
                );
                continue;
            }
        };
        if inbound.action == ACTION_AUTH {
            match handle_ably_auth_update(
                &hub,
                &handler,
                &app,
                &connection_id,
                &session_id,
                &sender,
                &mut attached_channels,
                &mut authorization,
                inbound,
            )
            .await
            {
                Ok(connection_key) => {
                    renewal_hint_sent = false;
                    *shared_authorization
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = authorization.clone();
                    *active_connection_key
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = connection_key;
                }
                Err(error) => {
                    send_protocol_disconnected(&sender, error.code, error.message);
                    break;
                }
            }
            continue;
        }
        match handle_ably_protocol_message(
            &handler,
            &hub,
            &app,
            &connection_id,
            &authorization,
            &session_id,
            &sender,
            &active_connection_key,
            &mut attached_channels,
            replace_presence_on_reenter,
            inbound,
        )
        .await
        {
            Ok(AblyProtocolControl::Continue) => {}
            Ok(AblyProtocolControl::Close) => {
                graceful_close = true;
                break;
            }
            Err(error) => {
                debug!(error = %error, "Ably compatibility protocol handler stopped");
                break;
            }
        }
    }

    if graceful_close {
        let removals = presence_service
            .unregister_connection(&app, &connection_id, PresenceHistoryEventCause::Disconnect)
            .await?;
        let mut leaves = BTreeMap::<String, Vec<PresenceChange>>::new();
        for removal in removals {
            leaves
                .entry(removal.channel)
                .or_default()
                .push(PresenceChange {
                    action: PresenceChangeAction::Leave,
                    wire_id: Some(removal.member.id.clone()),
                    member: removal.member,
                });
        }
        for (channel, changes) in leaves {
            if let Err(error) = handler
                .fanout_presence(
                    &app.id,
                    &channel,
                    PresenceReplication {
                        changes,
                        unregister_connection: None,
                    },
                )
                .await
            {
                warn!(
                    app_id = %app.id,
                    channel = %channel,
                    error = %error,
                    "failed to replicate graceful presence leaves"
                );
            }
        }
        if let Err(error) = handler
            .fanout_presence(
                &app.id,
                "",
                PresenceReplication {
                    changes: Vec::new(),
                    unregister_connection: Some(connection_id.clone()),
                },
            )
            .await
        {
            warn!(
                app_id = %app.id,
                connection_id = %connection_id,
                error = %error,
                "failed to replicate graceful presence connection removal"
            );
        }
    }
    for (requested, _) in attached_channels {
        if let Ok(channel) = AblyChannelName::parse(requested)
            && let Err(error) = hub.unsubscribe(&app.id, &channel, &session_id).await
        {
            warn!(app_id = %app.id, error = %error, "failed to persist channel close stats");
        }
    }
    hub.unregister_live_session(&connection_id, &session_id);
    hub.session_echo.remove(&session_id);
    connection_lease_task.abort();
    let _ = connection_lease_task.await;
    let final_connection_key = active_connection_key
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if graceful_close {
        hub.forget_connection(&final_connection_key).await;
    } else {
        hub.remember_connection(
            final_connection_key,
            &app.id,
            &connection_id,
            authorization.client_id.clone(),
        )
        .await;
    }
    heartbeat_task.abort();
    let _ = heartbeat_task.await;
    drop(sender);
    let _ = writer_task.await;
    hub.stats.connection_closed(&app.id);
    if let Ok(closed) = StatsObservation::connection_closed(&app.id, now_ms(), stats_transport)
        && let Err(error) = hub.stats.record(closed).await
    {
        warn!(app_id = %app.id, error = %error, "failed to persist connection close stats");
    }
    Ok(())
}

fn authorization_deadline(
    authorization: &ConnectionAuthorization,
    renewal_hint_sent: bool,
) -> Duration {
    let now = now_ms();
    let Some(expires_ms) = authorization.expires_ms else {
        return Duration::from_secs(24 * 60 * 60);
    };
    let deadline =
        if !renewal_hint_sent && expires_ms.saturating_sub(authorization.issued_ms) > 30_000 {
            expires_ms.saturating_sub(30_000)
        } else {
            expires_ms
        };
    Duration::from_millis(u64::try_from(deadline.saturating_sub(now)).unwrap_or(0))
}

fn should_send_renewal_hint(
    authorization: &ConnectionAuthorization,
    renewal_hint_sent: bool,
) -> bool {
    if renewal_hint_sent {
        return false;
    }
    let now = now_ms();
    authorization.expires_ms.is_some_and(|expires_ms| {
        expires_ms > now
            && expires_ms.saturating_sub(authorization.issued_ms) > 30_000
            && now >= expires_ms.saturating_sub(30_000)
    })
}

#[allow(clippy::too_many_arguments)]
async fn handle_ably_auth_update(
    hub: &Arc<AblyCompatHub>,
    handler: &Arc<ConnectionHandler>,
    app: &App,
    connection_id: &str,
    session_id: &str,
    sender: &AblySender,
    attached_channels: &mut HashMap<String, AblyConnectionAttachment>,
    authorization: &mut ConnectionAuthorization,
    inbound: AblyProtocolMessage,
) -> Result<String, AblyAuthError> {
    let access_token = inbound
        .auth
        .as_ref()
        .and_then(|auth| auth.get("accessToken"))
        .and_then(Value::as_str)
        .ok_or_else(|| AblyAuthError::unauthorized("AUTH requires auth.accessToken"))?;
    let resolved = resolve_ably_auth(
        hub,
        handler,
        &HeaderMap::new(),
        None,
        Some(access_token),
        authorization.client_id.as_deref(),
    )
    .await?;
    if resolved.app.id != app.id {
        return Err(AblyAuthError::unauthorized(
            "AUTH token belongs to a different app",
        ));
    }
    let mut next = authorization.clone();
    next.replace_from(&resolved);
    if next.revocable
        && hub
            .authorization_is_revoked(&app.id, &next, attached_channels)
            .await
    {
        return Err(AblyAuthError {
            status: StatusCode::UNAUTHORIZED,
            code: 40141,
            message: "Token revoked".to_string(),
        });
    }

    let downgraded = attached_channels
        .iter()
        .filter(|(requested, attachment)| {
            ensure_ably_channel_capability_parts(
                next.capabilities.as_ref(),
                requested,
                attachment.channel.base(),
                AblyCapabilityCheck::Subscribe,
            )
            .is_err()
        })
        .map(|(requested, _)| requested.clone())
        .collect::<Vec<_>>();
    for channel in downgraded {
        if let Ok(parsed) = AblyChannelName::parse(channel.clone())
            && let Err(error) = hub.unsubscribe(&app.id, &parsed, session_id).await
        {
            warn!(app_id = %app.id, error = %error, "failed to persist channel close stats during AUTH update");
        }
        attached_channels.remove(&channel);
        send_channel_error(
            sender,
            &channel,
            StatusCode::UNAUTHORIZED,
            40160,
            "Channel capability revoked by AUTH",
        );
    }

    *authorization = next;
    let connection_key = format!("{}:{}", app.id, Uuid::new_v4().simple());
    hub.remember_connection(
        connection_key.clone(),
        &app.id,
        connection_id,
        authorization.client_id.clone(),
    )
    .await;
    send_protocol(
        sender,
        connected_message(
            connection_id,
            &connection_key,
            authorization.connection_client_id.clone(),
            None,
        ),
    );
    Ok(connection_key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AblyProtocolControl {
    Continue,
    Close,
}

fn ably_protocol_control(action: u8) -> AblyProtocolControl {
    if matches!(action, ACTION_DISCONNECT | ACTION_CLOSE) {
        AblyProtocolControl::Close
    } else {
        AblyProtocolControl::Continue
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_ably_protocol_message(
    handler: &Arc<ConnectionHandler>,
    hub: &Arc<AblyCompatHub>,
    app: &App,
    connection_id: &str,
    authorization: &ConnectionAuthorization,
    session_id: &str,
    sender: &AblySender,
    active_connection_key: &RwLock<String>,
    attached_channels: &mut HashMap<String, AblyConnectionAttachment>,
    replace_presence_on_reenter: bool,
    mut inbound: AblyProtocolMessage,
) -> SockudoResult<AblyProtocolControl> {
    let control = ably_protocol_control(inbound.action);
    let client_id = authorization.client_id.as_deref();
    let connection_client_id = authorization.connection_client_id.as_deref();
    let capabilities = authorization.capabilities.as_ref();
    match inbound.action {
        ACTION_HEARTBEAT => {
            tokio::time::sleep(Duration::from_millis(1)).await;
            send_protocol(sender, heartbeat_response(inbound));
        }
        ACTION_CONNECT => {
            let connection_key = format!("{}:{}", app.id, Uuid::new_v4().simple());
            hub.remember_connection(
                connection_key.clone(),
                &app.id,
                connection_id,
                client_id.map(str::to_string),
            )
            .await;
            *active_connection_key
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = connection_key.clone();
            send_protocol(
                sender,
                connected_message(
                    connection_id,
                    &connection_key,
                    connection_client_id.map(str::to_string),
                    None,
                ),
            );
        }
        ACTION_ATTACH => {
            let Some(raw_channel) = inbound.channel else {
                send_protocol_error(sender, 40000, "ATTACH requires channel");
                return Ok(AblyProtocolControl::Continue);
            };
            let channel = match AblyChannelName::parse(raw_channel) {
                Ok(channel) => channel,
                Err(error) => {
                    send_channel_error(
                        sender,
                        error.requested(),
                        StatusCode::BAD_REQUEST,
                        40010,
                        error.to_string(),
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            let previous = attached_channels.get(channel.requested());
            if inbound.params.is_none() {
                inbound.params = previous
                    .filter(|attachment| !attachment.params.is_empty())
                    .map(|attachment| attachment.params.clone());
            }
            if inbound.flags.is_none() {
                inbound.flags = previous
                    .filter(|attachment| attachment.explicit_modes)
                    .map(|attachment| attachment.mode_flags);
            }
            if inbound.channel_serial.is_none() {
                inbound.channel_serial =
                    previous.and_then(|attachment| attachment.attach_position.clone());
            }
            if let Err(error) = ensure_ably_channel_capability(
                capabilities,
                &channel,
                AblyCapabilityCheck::Subscribe,
            ) {
                send_channel_error(
                    sender,
                    channel.requested(),
                    error.status,
                    error.code,
                    error.message,
                );
                return Ok(AblyProtocolControl::Continue);
            }
            let filter = match previous
                .filter(|attachment| attachment.channel == channel)
                .and_then(|attachment| attachment.filter.clone())
                .map_or_else(|| hub.message_filter(&channel), |filter| Ok(Some(filter)))
            {
                Ok(filter) => filter,
                Err(message) => {
                    send_channel_error(
                        sender,
                        channel.requested(),
                        StatusCode::BAD_REQUEST,
                        40010,
                        message,
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            let attach_options =
                AblyAttachOptions::from_wire(inbound.flags, inbound.params.clone());
            for (mode, check) in [
                (ABLY_MODE_PUBLISH, AblyCapabilityCheck::Publish),
                (ABLY_MODE_PRESENCE, AblyCapabilityCheck::Presence),
                (
                    ABLY_MODE_ANNOTATION_PUBLISH,
                    AblyCapabilityCheck::AnnotationMutate,
                ),
                (
                    ABLY_MODE_ANNOTATION_SUBSCRIBE,
                    AblyCapabilityCheck::AnnotationSubscribe,
                ),
            ] {
                if attach_options.explicit_modes
                    && attach_options.mode_flags & mode != 0
                    && let Err(error) =
                        ensure_ably_channel_capability(capabilities, &channel, check)
                {
                    send_channel_error(
                        sender,
                        channel.requested(),
                        error.status,
                        error.code,
                        error.message,
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            }
            let has_presence = previous.is_some_and(|attachment| attachment.has_presence)
                || !PresenceService::new(Arc::clone(handler))
                    .snapshot(&app.id, channel.base())
                    .await?
                    .is_empty();
            let attachment_state = AblyConnectionAttachment {
                channel: channel.clone(),
                params: attach_options.params.clone(),
                mode_flags: attach_options.mode_flags,
                explicit_modes: attach_options.explicit_modes,
                filter: filter.clone(),
                attach_position: inbound.channel_serial.clone(),
                has_presence,
            };
            if authorization.revocable {
                let target_channel =
                    HashMap::from([(channel.requested().to_string(), attachment_state.clone())]);
                if hub
                    .authorization_is_revoked(&app.id, authorization, &target_channel)
                    .await
                {
                    send_protocol_disconnected(sender, 40141, "Token revoked");
                    return Err(sockudo_core::error::Error::Auth(
                        "Ably token revoked".to_string(),
                    ));
                }
            }
            attached_channels.insert(channel.requested().to_string(), attachment_state);
            let attach = handle_ably_attach(
                handler,
                hub,
                app,
                connection_id,
                session_id,
                sender,
                &channel,
                filter,
                inbound.channel_serial,
                inbound.flags,
                inbound.params,
            );
            if tokio::time::timeout(Duration::from_millis(hub.config.attach_timeout_ms), attach)
                .await
                .is_err()
            {
                if let Err(error) = hub.unsubscribe(&app.id, &channel, session_id).await {
                    warn!(app_id = %app.id, channel = %channel.requested(), error = %error, "failed to clean up timed-out Ably attach");
                }
                attached_channels.remove(channel.requested());
                send_protocol(
                    sender,
                    AblyProtocolMessage {
                        action: ACTION_DETACHED,
                        channel: Some(channel.requested().to_string()),
                        error: Some(error_info(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            50003,
                            "channel attach timed out",
                        )),
                        ..empty_protocol_message(ACTION_DETACHED)
                    },
                );
            }
        }
        ACTION_DETACH => {
            let Some(raw_channel) = inbound.channel else {
                send_protocol_error(sender, 40000, "DETACH requires channel");
                return Ok(AblyProtocolControl::Continue);
            };
            let channel = match AblyChannelName::parse(raw_channel) {
                Ok(channel) => channel,
                Err(error) => {
                    send_channel_error(
                        sender,
                        error.requested(),
                        StatusCode::BAD_REQUEST,
                        40010,
                        error.to_string(),
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            let removals = PresenceService::new(Arc::clone(handler))
                .detach_connection(app, channel.base(), connection_id)
                .await?;
            if !removals.is_empty() {
                handler
                    .fanout_presence(
                        &app.id,
                        channel.base(),
                        PresenceReplication {
                            changes: removals
                                .into_iter()
                                .map(|removal| PresenceChange {
                                    action: PresenceChangeAction::Leave,
                                    wire_id: Some(removal.member.id.clone()),
                                    member: removal.member,
                                })
                                .collect(),
                            unregister_connection: None,
                        },
                    )
                    .await?;
            }
            hub.unsubscribe(&app.id, &channel, session_id)
                .await
                .map_err(stats_sockudo_error)?;
            attached_channels.remove(channel.requested());
            send_protocol(
                sender,
                AblyProtocolMessage {
                    action: ACTION_DETACHED,
                    channel: Some(channel.requested().to_string()),
                    ..empty_protocol_message(ACTION_DETACHED)
                },
            );
        }
        ACTION_MESSAGE => {
            if attached_channel_mode_denies(
                attached_channels,
                inbound.channel.as_deref(),
                ABLY_MODE_PUBLISH,
            ) {
                send_publish_nack(
                    sender,
                    &inbound,
                    40160,
                    "Channel mode does not permit publish",
                );
                return Ok(AblyProtocolControl::Continue);
            }
            handle_ably_publish(
                AblyPublishContext {
                    handler,
                    hub,
                    app,
                    connection_id,
                    client_id,
                    capabilities,
                    privileged_server: authorization.credential_id.starts_with("key:"),
                    sender,
                },
                inbound,
            )
            .await?;
        }
        ACTION_PRESENCE => {
            if attached_channel_mode_denies(
                attached_channels,
                inbound.channel.as_deref(),
                ABLY_MODE_PRESENCE,
            ) {
                send_publish_nack(
                    sender,
                    &inbound,
                    40160,
                    "Channel mode does not permit presence",
                );
                return Ok(AblyProtocolControl::Continue);
            }
            handle_ably_presence(
                handler,
                hub,
                app,
                connection_id,
                client_id,
                capabilities,
                sender,
                replace_presence_on_reenter,
                inbound,
            )
            .await;
        }
        ACTION_ANNOTATION => {
            if attached_channel_mode_denies(
                attached_channels,
                inbound.channel.as_deref(),
                ABLY_MODE_ANNOTATION_PUBLISH,
            ) {
                send_publish_nack(
                    sender,
                    &inbound,
                    40160,
                    "Channel mode does not permit annotation publish",
                );
                return Ok(AblyProtocolControl::Continue);
            }
            handle_ably_annotation(handler, app, client_id, capabilities, sender, inbound).await;
        }
        ACTION_SYNC => {
            let Some(raw_channel) = inbound.channel else {
                send_protocol_error(sender, 40000, "SYNC requires channel");
                return Ok(AblyProtocolControl::Continue);
            };
            let channel = match AblyChannelName::parse(raw_channel) {
                Ok(channel) => channel,
                Err(error) => {
                    send_channel_error(
                        sender,
                        error.requested(),
                        StatusCode::BAD_REQUEST,
                        40010,
                        error.to_string(),
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            send_presence_sync(
                sender,
                channel.requested(),
                PresenceService::new(Arc::clone(handler))
                    .snapshot(&app.id, channel.base())
                    .await?
                    .into_iter()
                    .map(|record| ably_presence_from_record(record, 1))
                    .collect(),
            );
        }
        ACTION_DISCONNECT | ACTION_CLOSE => {
            send_protocol(
                sender,
                AblyProtocolMessage {
                    action: ACTION_CLOSED,
                    ..empty_protocol_message(ACTION_CLOSED)
                },
            );
        }
        _ => {
            send_protocol_error(
                sender,
                40000,
                format!("Unsupported Ably ProtocolMessage action {}", inbound.action),
            );
        }
    }
    Ok(control)
}

fn ably_annotation_from_native_event(event: AnnotationEventData) -> AblyAnnotation {
    AblyAnnotation {
        action: Some(match event.action {
            AnnotationEventAction::Create => 0,
            AnnotationEventAction::Delete => 1,
        }),
        id: event.id,
        serial: Some(event.serial),
        message_serial: Some(event.message_serial),
        annotation_type: Some(event.annotation_type),
        name: event.name,
        client_id: event.client_id,
        count: event.count,
        data: event.data,
        encoding: event.encoding,
        timestamp: Some(event.timestamp),
    }
}

fn ably_summary_annotations(value: &Value) -> Option<Value> {
    let mut annotations = value.get("annotations")?.clone();
    let summary = annotations
        .as_object_mut()?
        .get_mut(&"summary")?
        .as_object_mut()?;
    for (annotation_type, names) in summary.iter_mut() {
        let is_multiple = AnnotationType::new(annotation_type.to_string())
            .and_then(|annotation_type| annotation_type.summarizer())
            .is_ok_and(|summarizer| {
                summarizer == sockudo_core::annotations::AnnotationSummarizer::Multiple
            });
        if !is_multiple {
            continue;
        }
        let Some(names) = names.as_object_mut() else {
            continue;
        };
        for (_, bucket) in names.iter_mut() {
            let Some(bucket) = bucket.as_object_mut() else {
                continue;
            };
            if let Some(client_counts) = bucket.remove(&"clientCounts") {
                bucket.insert("clientIds", client_counts);
            }
        }
    }
    Some(annotations)
}

enum AblyAnnotationCommand {
    Create {
        message_serial: MessageSerial,
        annotation_type: AnnotationType,
        id: Option<AnnotationId>,
        name: Option<String>,
        client_id: Option<String>,
        count: Option<u64>,
        data: Option<Value>,
        encoding: Option<String>,
    },
    Delete(AblyAnnotationDeleteSelector),
}

struct AblyAnnotationDeleteSelector {
    message_serial: MessageSerial,
    annotation_type: AnnotationType,
    id: Option<AnnotationId>,
    target_serial: Option<AnnotationSerial>,
    name: Option<String>,
    client_id: Option<String>,
}

fn parse_ably_annotation_command(
    annotation: AblyAnnotation,
    path_message_serial: Option<&MessageSerial>,
    authenticated_client_id: Option<&str>,
) -> Result<AblyAnnotationCommand, AppError> {
    let message_serial = match (annotation.message_serial, path_message_serial) {
        (Some(raw), Some(path)) => {
            let body = MessageSerial::new(raw)?;
            if &body != path {
                return Err(AppError::InvalidInput(
                    "annotation.messageSerial must match the request path".to_string(),
                ));
            }
            body
        }
        (Some(raw), None) => MessageSerial::new(raw)?,
        (None, Some(path)) => path.clone(),
        (None, None) => {
            return Err(AppError::InvalidInput(
                "annotation.messageSerial is required".to_string(),
            ));
        }
    };
    let annotation_type = AnnotationType::new(
        annotation
            .annotation_type
            .ok_or_else(|| AppError::InvalidInput("annotation.type is required".to_string()))?,
    )?;
    let id = annotation.id.map(AnnotationId::new).transpose()?;

    match annotation.action.unwrap_or(0) {
        0 => {
            let client_id = match (authenticated_client_id, annotation.client_id) {
                (Some(authenticated), Some(requested)) if authenticated != requested => {
                    return Err(AppError::Forbidden(
                        "annotation.clientId must match authenticated clientId".to_string(),
                    ));
                }
                (Some(authenticated), _) => Some(authenticated.to_string()),
                (None, requested) => requested,
            };
            Ok(AblyAnnotationCommand::Create {
                message_serial,
                annotation_type,
                id,
                name: annotation.name,
                client_id,
                count: annotation.count,
                data: annotation.data,
                encoding: annotation.encoding,
            })
        }
        1 => Ok(AblyAnnotationCommand::Delete(
            AblyAnnotationDeleteSelector {
                message_serial,
                annotation_type,
                id,
                target_serial: annotation.serial.map(AnnotationSerial::new).transpose()?,
                name: annotation.name,
                client_id: annotation
                    .client_id
                    .or_else(|| authenticated_client_id.map(str::to_string)),
            },
        )),
        action => Err(AppError::InvalidInput(format!(
            "unsupported annotation action {action}"
        ))),
    }
}

fn require_ably_annotations_enabled(
    handler: &ConnectionHandler,
    app: &App,
    channel: &str,
) -> Result<(), AppError> {
    if !handler.server_options().annotations.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Annotations are disabled globally for channel '{channel}'"
        )));
    }
    if !app.annotations_enabled_for_channel(channel) {
        return Err(AppError::Forbidden(format!(
            "Annotations are disabled by channel policy for channel '{channel}'"
        )));
    }
    Ok(())
}

fn authorize_ably_annotation_delete(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &AblyChannelName,
    actor_client_id: Option<&str>,
    target_client_id: Option<&str>,
) -> Result<(), AppError> {
    let Some(capabilities) = capabilities else {
        return Ok(());
    };
    let delete_any = ensure_ably_channel_capability(
        Some(capabilities),
        channel,
        AblyCapabilityCheck::AnnotationDeleteAny,
    )
    .is_ok();
    let delete_own = ensure_ably_channel_capability(
        Some(capabilities),
        channel,
        AblyCapabilityCheck::AnnotationDeleteOwn,
    )
    .is_ok()
        && actor_client_id.is_some()
        && actor_client_id == target_client_id;

    if delete_any && actor_client_id.is_none() {
        return Err(AppError::Forbidden(
            "annotation-delete-any requires an identified client".to_string(),
        ));
    }
    if delete_any || delete_own {
        Ok(())
    } else {
        Err(AppError::Forbidden(format!(
            "annotation-delete-own or annotation-delete-any capability is required for channel '{}'",
            channel.requested()
        )))
    }
}

async fn find_ably_annotation_delete_target(
    handler: &ConnectionHandler,
    app_id: &str,
    channel: &str,
    selector: &AblyAnnotationDeleteSelector,
) -> Result<StoredAnnotationEvent, AppError> {
    if let Some(target_serial) = selector.target_serial.as_ref() {
        let target = handler
            .annotation_store()
            .get_event_by_serial(AnnotationEventLookupRequest {
                app_id: app_id.to_string(),
                channel_id: channel.to_string(),
                annotation_serial: target_serial.clone(),
            })
            .await?
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "Annotation '{}' was not found in channel '{channel}'",
                    target_serial.as_str()
                ))
            })?;
        if target.message_serial() != &selector.message_serial
            || target.annotation_type() != &selector.annotation_type
            || target.annotation.action != AnnotationAction::Create
            || selector
                .id
                .as_ref()
                .is_some_and(|id| id != &target.annotation.id)
            || selector
                .name
                .as_deref()
                .is_some_and(|name| target.annotation.name.as_deref() != Some(name))
            || selector
                .client_id
                .as_deref()
                .is_some_and(|client_id| target.annotation.client_id.as_deref() != Some(client_id))
        {
            return Err(AppError::NotFound(format!(
                "Annotation '{}' does not match the requested message and type",
                target_serial.as_str()
            )));
        }
        return Ok(target);
    }

    let events = handler
        .annotation_store()
        .get_events(AnnotationEventsRequest {
            app_id: app_id.to_string(),
            channel_id: channel.to_string(),
            message_serial: selector.message_serial.clone(),
            annotation_type: selector.annotation_type.clone(),
        })
        .await?;
    let deleted = events
        .iter()
        .filter(|event| event.annotation.action == AnnotationAction::Delete)
        .map(|event| event.annotation.id.clone())
        .collect::<BTreeSet<_>>();
    events
        .into_iter()
        .rev()
        .find(|event| {
            event.annotation.action == AnnotationAction::Create
                && !deleted.contains(&event.annotation.id)
                && selector
                    .id
                    .as_ref()
                    .is_none_or(|id| id == &event.annotation.id)
                && selector
                    .name
                    .as_deref()
                    .is_none_or(|name| event.annotation.name.as_deref() == Some(name))
                && selector.client_id.as_deref().is_none_or(|client_id| {
                    event.annotation.client_id.as_deref() == Some(client_id)
                })
        })
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "No active annotation matched message '{}' and type '{}'",
                selector.message_serial.as_str(),
                selector.annotation_type.as_str()
            ))
        })
}

async fn apply_ably_annotation_command(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &AblyChannelName,
    capabilities: Option<&ConnectionCapabilities>,
    actor_client_id: Option<&str>,
    command: AblyAnnotationCommand,
) -> Result<(), AppError> {
    require_ably_annotations_enabled(handler, app, channel.base())?;
    match command {
        AblyAnnotationCommand::Create {
            message_serial,
            annotation_type,
            id,
            name,
            client_id,
            count,
            data,
            encoding,
        } => {
            ensure_ably_channel_capability_app_error(
                capabilities,
                channel,
                AblyCapabilityCheck::AnnotationPublish,
            )?;
            handler
                .publish_annotation_runtime(PublishAnnotationRuntimeRequest {
                    app: app.clone(),
                    channel: channel.base().to_string(),
                    message_serial,
                    annotation_type,
                    id,
                    name,
                    client_id,
                    count,
                    data,
                    encoding,
                })
                .await?;
        }
        AblyAnnotationCommand::Delete(selector) => {
            let target =
                find_ably_annotation_delete_target(handler, &app.id, channel.base(), &selector)
                    .await?;
            authorize_ably_annotation_delete(
                capabilities,
                channel,
                actor_client_id,
                target.annotation.client_id.as_deref(),
            )?;
            handler
                .delete_annotation_runtime(DeleteAnnotationRuntimeRequest {
                    app: app.clone(),
                    channel: channel.base().to_string(),
                    message_serial: selector.message_serial,
                    target_serial: target.annotation.serial,
                })
                .await?;
        }
    }
    Ok(())
}

async fn handle_ably_annotation(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    client_id: Option<&str>,
    capabilities: Option<&ConnectionCapabilities>,
    sender: &AblySender,
    inbound: AblyProtocolMessage,
) {
    let Some(raw_channel) = inbound.channel.clone() else {
        send_publish_nack(sender, &inbound, 40000, "ANNOTATION requires channel");
        return;
    };
    let channel = match AblyChannelName::parse(raw_channel) {
        Ok(channel) => channel,
        Err(error) => {
            send_publish_nack(sender, &inbound, 40010, error.to_string());
            return;
        }
    };
    let annotations = inbound.annotations.clone().unwrap_or_default();
    if annotations.is_empty() {
        send_publish_nack(sender, &inbound, 40000, "ANNOTATION requires annotations");
        return;
    }
    for annotation in annotations {
        let command = match parse_ably_annotation_command(annotation, None, client_id) {
            Ok(command) => command,
            Err(error) => {
                let error = ably_error_info_from_app_error(error);
                send_publish_nack(sender, &inbound, error.code, error.message);
                return;
            }
        };
        if let Err(error) =
            apply_ably_annotation_command(handler, app, &channel, capabilities, client_id, command)
                .await
        {
            let error = ably_error_info_from_app_error(error);
            send_publish_nack(sender, &inbound, error.code, error.message);
            return;
        }
    }
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ACK,
            msg_serial: inbound.msg_serial,
            count: Some(publish_ack_count(&inbound)),
            ..empty_protocol_message(ACTION_ACK)
        },
    );
}

fn heartbeat_response(inbound: AblyProtocolMessage) -> AblyProtocolMessage {
    AblyProtocolMessage {
        action: ACTION_HEARTBEAT,
        id: inbound.id,
        ..empty_protocol_message(ACTION_HEARTBEAT)
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_ably_attach(
    handler: &Arc<ConnectionHandler>,
    hub: &Arc<AblyCompatHub>,
    app: &App,
    connection_id: &str,
    session_id: &str,
    sender: &AblySender,
    channel: &AblyChannelName,
    filter: Option<Arc<AblyMessageFilter>>,
    channel_serial: Option<String>,
    flags: Option<u64>,
    params: Option<HashMap<String, String>>,
) {
    let attach_resume = flags.is_some_and(|flags| flags & FLAG_ATTACH_RESUME != 0);
    let options = AblyAttachOptions::from_wire(flags, params);
    let rewind_requested =
        options.params.contains_key("rewind") || options.params.contains_key("rewindCount");
    let rewind = resolve_ably_rewind(&options.params, attach_resume);
    if rewind_requested && !attach_resume && rewind.is_none() {
        send_channel_error(
            sender,
            channel.requested(),
            StatusCode::BAD_REQUEST,
            40000,
            "invalid rewind parameter",
        );
        return;
    }
    let echo = hub
        .session_echo
        .get(session_id)
        .is_none_or(|connection_echo| *connection_echo)
        && options
            .params
            .get("echo")
            .is_none_or(|value| !value.eq_ignore_ascii_case("false"));
    let presence = match PresenceService::new(Arc::clone(handler))
        .snapshot(&app.id, channel.base())
        .await
    {
        Ok(presence) => presence
            .into_iter()
            .map(|record| ably_presence_from_record(record, 1))
            .collect::<Vec<_>>(),
        Err(error) => {
            send_channel_error(
                sender,
                channel.requested(),
                StatusCode::SERVICE_UNAVAILABLE,
                50003,
                format!("presence state is unavailable: {error}"),
            );
            return;
        }
    };
    let attachment = || AblyAttachment {
        connection_id,
        session_id,
        sender: sender.clone(),
        filter: filter.clone(),
        params: options.params.clone(),
        mode_flags: options.mode_flags,
        echo,
        presence: presence.clone(),
    };
    if let Err(error) = hub.begin_attach(&app.id, channel, &attachment()).await {
        send_channel_error(
            sender,
            channel.requested(),
            StatusCode::SERVICE_UNAVAILABLE,
            50000,
            error.to_string(),
        );
        return;
    }
    let Some(channel_serial) = channel_serial else {
        let attach_serial = current_ably_channel_serial(handler, app, channel.base()).await;
        let replay = if let Some(rewind) = rewind.as_ref() {
            let position = attach_serial
                .as_deref()
                .and_then(|serial| parse_ably_channel_serial(serial).ok());
            match collect_ably_rewind(handler, app, channel.base(), rewind, position.as_ref()).await
            {
                Ok(replay) => replay,
                Err(failure) => {
                    hub.attach_failed(&app.id, channel, attachment(), attach_serial, failure);
                    return;
                }
            }
        } else {
            Vec::new()
        };
        hub.attach_clean(&app.id, channel, attachment(), attach_serial, replay);
        return;
    };

    let position = match parse_ably_channel_serial(&channel_serial) {
        Ok(position) => position,
        Err(failure) => {
            hub.attach_failed(
                &app.id,
                channel,
                attachment(),
                Some(channel_serial),
                failure,
            );
            return;
        }
    };

    hub.metrics.replay_source.fetch_add(1, Ordering::Relaxed);
    match collect_ably_cold_recovery(handler, app, channel.base(), &position).await {
        Ok(mut replay) => {
            if let Some(rewind) = rewind.as_ref() {
                match collect_ably_rewind(handler, app, channel.base(), rewind, Some(&position))
                    .await
                {
                    Ok(backlog) => replay.extend(backlog),
                    Err(failure) => {
                        hub.attach_failed(
                            &app.id,
                            channel,
                            attachment(),
                            Some(channel_serial),
                            failure,
                        );
                        return;
                    }
                }
            }
            hub.attach_cold_recovery(&app.id, channel, attachment(), &position, replay);
        }
        Err(failure) => hub.attach_failed(
            &app.id,
            channel,
            attachment(),
            Some(channel_serial),
            failure,
        ),
    }
}

async fn current_ably_channel_serial(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
) -> Option<String> {
    if handler.server_options().versioned_messages.enabled
        && let Ok(state) = handler.version_store().stream_state(&app.id, channel).await
        && let (Some(stream_id), Some(serial)) =
            (state.stream_id, state.newest_available_delivery_serial)
    {
        return Some(encode_ably_channel_serial(&stream_id, serial));
    }
    if app
        .resolved_history(channel, &handler.server_options().history)
        .enabled
        && let Ok(inspection) = handler
            .history_store()
            .stream_inspection(&app.id, channel)
            .await
        && let (Some(stream_id), Some(next_serial)) = (inspection.stream_id, inspection.next_serial)
    {
        return Some(encode_ably_channel_serial(
            &stream_id,
            next_serial.saturating_sub(1),
        ));
    }
    #[cfg(feature = "recovery")]
    {
        handler.replay_buffer().map(|replay_buffer| {
            let position = replay_buffer.current_position(&app.id, channel);
            encode_ably_channel_serial(&position.stream_id, position.serial)
        })
    }
    #[cfg(not(feature = "recovery"))]
    {
        let _ = (handler, app, channel);
        None
    }
}

async fn collect_ably_cold_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    if handler.server_options().versioned_messages.enabled {
        let state = handler
            .version_store()
            .stream_state(&app.id, channel)
            .await
            .map_err(|error| {
                AblyRecoveryFailure::channel(
                    90000,
                    format!("unable to recover channel '{channel}': {error}"),
                )
            })?;
        if state.stream_id.is_some() {
            return collect_ably_version_recovery(handler, app, channel, position).await;
        }
        #[cfg(feature = "recovery")]
        if let Some(replay_buffer) = handler.replay_buffer() {
            let current = replay_buffer.current_position(&app.id, channel);
            if current.stream_id == position.stream_id && current.serial == position.serial {
                return Ok(Vec::new());
            }
        }
    }

    collect_ably_history_recovery(handler, app, channel, position).await
}

async fn collect_ably_rewind(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    rewind: &SubscriptionRewind,
    high_water: Option<&AblyChannelPosition>,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    let policy = app.resolved_history(channel, &handler.server_options().history);
    if !policy.rewind_allowed() {
        return Err(AblyRecoveryFailure::channel(
            40000,
            format!("channel rewind is disabled by policy for channel '{channel}'"),
        ));
    }
    let limit = rewind.limit().min(policy.max_page_size).max(1);

    if handler.server_options().versioned_messages.enabled {
        let state = handler
            .version_store()
            .stream_state(&app.id, channel)
            .await
            .map_err(|error| {
                AblyRecoveryFailure::channel(
                    90000,
                    format!("unable to rewind channel '{channel}': {error}"),
                )
            })?;
        if let (Some(stream_id), Some(newest)) =
            (state.stream_id, state.newest_available_delivery_serial)
        {
            if high_water.is_some_and(|position| position.stream_id != stream_id) {
                return Err(AblyRecoveryFailure::channel(
                    90005,
                    format!("unable to rewind channel '{channel}' because the stream changed"),
                ));
            }
            let newest = high_water.map_or(newest, |position| position.serial.min(newest));
            let requested = u64::try_from(limit).unwrap_or(u64::MAX);
            let mut records = handler
                .version_store()
                .replay_after(sockudo_core::version_store::VersionReplayRequest {
                    app_id: app.id.clone(),
                    channel: channel.to_string(),
                    after_delivery_serial: newest.saturating_sub(requested),
                    limit,
                })
                .await
                .map_err(|error| {
                    AblyRecoveryFailure::channel(
                        90000,
                        format!("unable to rewind channel '{channel}': {error}"),
                    )
                })?;
            records.sort_by_key(|record| record.delivery_serial());
            let cutoff_ms = match rewind {
                SubscriptionRewind::Count(_) => None,
                SubscriptionRewind::Seconds(seconds) => {
                    let window_ms = i64::try_from(*seconds)
                        .unwrap_or(i64::MAX)
                        .saturating_mul(1000);
                    Some(now_ms().saturating_sub(window_ms))
                }
            };
            let mut replay = Vec::with_capacity(records.len());
            for record in records.into_iter().filter(|record| {
                record.delivery_serial() <= newest
                    && cutoff_ms.is_none_or(|cutoff| record.message.version.timestamp_ms >= cutoff)
            }) {
                let serial = record.delivery_serial();
                let runtime =
                    handler.build_runtime_message_from_record(&record, Some(stream_id.clone()));
                let channel_serial = Some(encode_ably_channel_serial(&stream_id, serial));
                replay.push(match record.envelope.as_ref() {
                    Some(envelope) => ably_protocol_message_from_envelope(
                        channel,
                        &runtime,
                        envelope,
                        AblyMessageProjection::Aggregate,
                        channel_serial,
                    )?,
                    None => ably_protocol_message_from_pusher(
                        channel,
                        &runtime,
                        AblyMessageProjection::Aggregate,
                        channel_serial,
                    )?,
                });
            }
            return Ok(replay);
        }
    }

    let mut page = handler
        .history_store()
        .read_page(build_ably_rewind_history_request(
            &app.id,
            channel,
            rewind,
            policy.max_page_size,
            high_water,
            now_ms(),
        ))
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to rewind channel '{channel}': {error}"),
            )
        })?;
    if let Some(high_water) = high_water
        && page.retained.stream_id.is_some()
        && page.retained.stream_id.as_deref() != Some(high_water.stream_id.as_str())
    {
        return Err(AblyRecoveryFailure::channel(
            90005,
            format!("unable to rewind channel '{channel}' because the stream changed"),
        ));
    }
    page.items.sort_by_key(|item| item.serial);
    let mut replay = Vec::with_capacity(page.items.len());
    for item in page.items {
        let stored =
            decode_stored_message_payload(item.payload_bytes.as_ref()).map_err(|error| {
                AblyRecoveryFailure::channel(
                    90000,
                    format!("unable to rewind channel '{channel}': {error}"),
                )
            })?;
        let channel_serial = Some(encode_ably_channel_serial(&item.stream_id, item.serial));
        replay.push(match stored.envelope.as_ref() {
            Some(envelope) => ably_protocol_message_from_envelope(
                channel,
                &stored.message,
                envelope,
                AblyMessageProjection::Aggregate,
                channel_serial,
            )?,
            None => ably_protocol_message_from_pusher(
                channel,
                &stored.message,
                AblyMessageProjection::Aggregate,
                channel_serial,
            )?,
        });
    }
    Ok(replay)
}

async fn collect_ably_version_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    let stream_state = handler
        .version_store()
        .stream_state(&app.id, channel)
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;

    if stream_state.stream_id.as_deref() != Some(position.stream_id.as_str()) {
        return Err(AblyRecoveryFailure::channel(
            90005,
            format!("unable to recover channel '{channel}' because the stream changed"),
        ));
    }

    if let Some(oldest) = stream_state.oldest_available_delivery_serial
        && position.serial.saturating_add(1) < oldest
    {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because messages expired"),
        ));
    }

    let newest = stream_state
        .newest_available_delivery_serial
        .unwrap_or(position.serial);
    if newest <= position.serial {
        return Ok(Vec::new());
    }
    let missing = newest.saturating_sub(position.serial);
    let limit = usize::try_from(missing).map_err(|_| {
        AblyRecoveryFailure::channel(
            90004,
            format!("unable to recover channel '{channel}' because the message limit was exceeded"),
        )
    })?;
    if limit > ABLY_COMPAT_MAX_REPLAY_MESSAGES {
        return Err(AblyRecoveryFailure::channel(
            90004,
            format!("unable to recover channel '{channel}' because the message limit was exceeded"),
        ));
    }

    let records = handler
        .version_store()
        .replay_after(sockudo_core::version_store::VersionReplayRequest {
            app_id: app.id.clone(),
            channel: channel.to_string(),
            after_delivery_serial: position.serial,
            limit,
        })
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90003,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;

    if records.len() != limit {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because continuity is unprovable"),
        ));
    }

    let mut replay = Vec::with_capacity(records.len());
    for record in records {
        let delivery_serial = record.delivery_serial();
        let runtime =
            handler.build_runtime_message_from_record(&record, Some(position.stream_id.clone()));
        let channel_serial = Some(encode_ably_channel_serial(
            &position.stream_id,
            delivery_serial,
        ));
        replay.push(match record.envelope.as_ref() {
            Some(envelope) => ably_protocol_message_from_envelope(
                channel,
                &runtime,
                envelope,
                AblyMessageProjection::Mutation,
                channel_serial,
            )?,
            None => ably_protocol_message_from_pusher(
                channel,
                &runtime,
                AblyMessageProjection::Mutation,
                channel_serial,
            )?,
        });
    }
    Ok(replay)
}

async fn collect_ably_history_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    let history_policy = app.resolved_history(channel, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because messages expired"),
        ));
    }

    let stream_state = handler
        .history_store()
        .stream_runtime_state(&app.id, channel)
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;
    if !stream_state.recovery_allowed {
        return Err(AblyRecoveryFailure::channel(
            if stream_state.reset_required {
                90005
            } else {
                90000
            },
            stream_state.reason.unwrap_or_else(|| {
                format!("unable to recover channel '{channel}' because continuity is unprovable")
            }),
        ));
    }
    if stream_state.stream_id.as_deref() != Some(position.stream_id.as_str()) {
        return Err(AblyRecoveryFailure::channel(
            90005,
            format!("unable to recover channel '{channel}' because the stream changed"),
        ));
    }

    let mut replay = Vec::new();
    let mut cursor = None;
    let bounds = HistoryQueryBounds {
        start_serial: Some(position.serial.saturating_add(1)),
        end_serial: None,
        start_time_ms: None,
        end_time_ms: None,
    };
    loop {
        if replay.len() >= ABLY_COMPAT_MAX_REPLAY_MESSAGES {
            return Err(AblyRecoveryFailure::channel(
                90004,
                format!(
                    "unable to recover channel '{channel}' because the message limit was exceeded"
                ),
            ));
        }
        let page = handler
            .history_store()
            .read_page(HistoryReadRequest {
                app_id: app.id.clone(),
                channel: channel.to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: history_policy
                    .max_page_size
                    .min(ABLY_COMPAT_MAX_REPLAY_MESSAGES.saturating_sub(replay.len()))
                    .max(1),
                cursor: cursor.clone(),
                bounds: bounds.clone(),
            })
            .await
            .map_err(|error| {
                AblyRecoveryFailure::channel(
                    90003,
                    format!("unable to recover channel '{channel}': {error}"),
                )
            })?;

        if page.retained.stream_id.as_deref() != Some(position.stream_id.as_str()) {
            return Err(AblyRecoveryFailure::channel(
                90005,
                format!("unable to recover channel '{channel}' because the stream changed"),
            ));
        }
        if page.truncated_by_retention {
            return Err(AblyRecoveryFailure::channel(
                90003,
                format!("unable to recover channel '{channel}' because messages expired"),
            ));
        }

        for item in page.items {
            let stored =
                decode_stored_message_payload(item.payload_bytes.as_ref()).map_err(|error| {
                    AblyRecoveryFailure::channel(
                        90000,
                        format!("unable to recover channel '{channel}': {error}"),
                    )
                })?;
            let raw_message = stored.message;
            replay.push(match stored.envelope.as_ref() {
                Some(envelope) => ably_protocol_message_from_envelope(
                    channel,
                    &raw_message,
                    envelope,
                    AblyMessageProjection::Mutation,
                    Some(encode_ably_channel_serial(&item.stream_id, item.serial)),
                )?,
                None => ably_protocol_message_from_pusher(
                    channel,
                    &raw_message,
                    AblyMessageProjection::Mutation,
                    Some(encode_ably_channel_serial(&item.stream_id, item.serial)),
                )?,
            });
        }

        if !page.has_more {
            break;
        }
        cursor = page.next_cursor;
    }

    Ok(replay)
}

struct AblyPublishContext<'a> {
    handler: &'a Arc<ConnectionHandler>,
    hub: &'a Arc<AblyCompatHub>,
    app: &'a App,
    connection_id: &'a str,
    client_id: Option<&'a str>,
    capabilities: Option<&'a ConnectionCapabilities>,
    privileged_server: bool,
    sender: &'a AblySender,
}

async fn handle_ably_publish(
    context: AblyPublishContext<'_>,
    inbound: AblyProtocolMessage,
) -> SockudoResult<()> {
    let AblyPublishContext {
        handler,
        hub,
        app,
        connection_id,
        client_id,
        capabilities,
        privileged_server,
        sender,
    } = context;
    let raw_channel = match inbound.channel.clone() {
        Some(channel) => channel,
        None => {
            send_publish_nack(sender, &inbound, 40000, "MESSAGE requires channel");
            return Ok(());
        }
    };
    let channel = match AblyChannelName::parse(raw_channel) {
        Ok(channel) => channel,
        Err(error) => {
            send_publish_nack(sender, &inbound, 40010, error.to_string());
            return Ok(());
        }
    };
    if let Err(error) =
        ensure_ably_channel_capability(capabilities, &channel, AblyCapabilityCheck::Publish)
    {
        send_publish_nack(sender, &inbound, error.code, error.message);
        return Ok(());
    }

    let messages = inbound.messages.clone().unwrap_or_default();
    let message_count = u64::try_from(messages.len()).unwrap_or(u64::MAX);
    let message_bytes = sonic_rs::to_vec(&messages)
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or_default();
    for message in &messages {
        if let Err(error) = validate_ably_publish_message(message, false)
            .and_then(|()| effective_ably_client_id(client_id, message).map(|_| ()))
        {
            send_publish_nack(sender, &inbound, 40000, error.to_string());
            return Ok(());
        }
    }
    let mut serials = Vec::with_capacity(messages.len());
    for (index, mut message) in messages.into_iter().enumerate() {
        if let Some(msg_serial) = inbound.msg_serial {
            message
                .id
                .get_or_insert_with(|| ably_realtime_message_id(connection_id, msg_serial, index));
        }
        let result = publish_ably_message(
            AblyMessagePublishContext {
                handler,
                hub,
                app,
                channel: channel.base(),
                connection_id: Some(connection_id),
                client_id,
                capabilities,
                privileged_server,
            },
            message,
        )
        .await;

        match result {
            Ok(serial) => serials.push(serial),
            Err(error) => {
                send_publish_nack(
                    sender,
                    &inbound,
                    40000,
                    format!("Failed to publish message {index}: {error}"),
                );
                return Ok(());
            }
        }
    }

    if message_count > 0 {
        hub.stats
            .record(
                StatsObservation::messages(
                    &app.id,
                    now_ms(),
                    "inbound",
                    "realtime",
                    message_count,
                    message_bytes,
                )
                .map_err(stats_sockudo_error)?,
            )
            .await
            .map_err(stats_sockudo_error)?;
    }

    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ACK,
            msg_serial: inbound.msg_serial,
            count: Some(publish_ack_count(&inbound)),
            res: Some(json!([{ "serials": serials }])),
            ..empty_protocol_message(ACTION_ACK)
        },
    );
    Ok(())
}

fn stats_sockudo_error(error: StatsError) -> sockudo_core::error::Error {
    sockudo_core::error::Error::Internal(error.to_string())
}

fn publish_ack_count(inbound: &AblyProtocolMessage) -> u64 {
    inbound.count.unwrap_or(1)
}

fn ably_realtime_message_id(connection_id: &str, msg_serial: u64, index: usize) -> String {
    format!("{connection_id}:{msg_serial}:{index}")
}

struct AblyMessagePublishContext<'a> {
    handler: &'a Arc<ConnectionHandler>,
    hub: &'a AblyCompatHub,
    app: &'a App,
    channel: &'a str,
    connection_id: Option<&'a str>,
    client_id: Option<&'a str>,
    capabilities: Option<&'a ConnectionCapabilities>,
    privileged_server: bool,
}

async fn publish_ably_message(
    context: AblyMessagePublishContext<'_>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let AblyMessagePublishContext {
        handler,
        hub,
        app,
        channel,
        connection_id,
        client_id,
        capabilities,
        privileged_server,
    } = context;
    let action = message.action.unwrap_or(MESSAGE_CREATE);
    match action {
        MESSAGE_CREATE => {
            let effective_client_id = effective_ably_client_id(client_id, &message)?;
            publish_ably_create(
                handler,
                hub,
                app,
                channel,
                connection_id,
                effective_client_id.as_deref(),
                message,
            )
            .await
        }
        MESSAGE_APPEND => {
            publish_ably_append(
                handler,
                app,
                channel,
                client_id,
                MutableMessageActor {
                    client_id: client_id.map(str::to_string),
                    capabilities: capabilities.cloned(),
                    privileged_server,
                },
                message,
            )
            .await
        }
        MESSAGE_UPDATE => {
            publish_ably_update(
                handler,
                app,
                channel,
                client_id,
                MutableMessageActor {
                    client_id: client_id.map(str::to_string),
                    capabilities: capabilities.cloned(),
                    privileged_server,
                },
                message,
            )
            .await
        }
        MESSAGE_DELETE => {
            publish_ably_delete(
                handler,
                app,
                channel,
                client_id,
                MutableMessageActor {
                    client_id: client_id.map(str::to_string),
                    capabilities: capabilities.cloned(),
                    privileged_server,
                },
                message,
            )
            .await
        }
        MESSAGE_SUMMARY => Err(AppError::InvalidInput(format!(
            "Ably message action {action} is not implemented by this compatibility layer"
        ))),
        other => Err(AppError::InvalidInput(format!(
            "Unsupported Ably message action {other}"
        ))),
    }
}

async fn publish_ably_create(
    handler: &Arc<ConnectionHandler>,
    hub: &AblyCompatHub,
    app: &App,
    channel: &str,
    connection_id: Option<&str>,
    client_id: Option<&str>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let effective_client_id = effective_ably_client_id(client_id, &message)?;
    let client_id = effective_client_id.as_deref();
    let event_name = message.name.clone();
    #[cfg(feature = "push")]
    let push_payload = message
        .extras
        .as_ref()
        .and_then(|extras| extras.get("push"))
        .map(ably_push_payload_value)
        .transpose()?;
    let encoding = message.encoding.clone();
    let raw_data = message.data.clone();
    let data = message
        .data
        .map(|value| ably_message_data_to_message_data(value, encoding.as_deref()));
    let mut extras = ably_extras_to_message_extras(message.extras)?;
    if let Some(client_id) = client_id
        && event_name.as_deref().is_some_and(is_ai_event)
    {
        stamp_ai_identity(
            &mut extras,
            event_name.as_deref().unwrap_or_default(),
            client_id,
        )?;
    }
    if let Some(extras) = extras.as_ref() {
        extras
            .validate_ai_headers()
            .map_err(|error| AppError::InvalidInput(error.message))?;
    }
    let pusher_message = PusherMessage {
        event: event_name.clone(),
        channel: Some(channel.to_string()),
        data,
        name: None,
        user_id: client_id.map(str::to_string),
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: message.id.clone(),
        stream_id: None,
        serial: None,
        idempotency_key: message.id.clone(),
        extras,
        delta_sequence: None,
        delta_conflation_key: None,
    };
    let mut envelope = MessageEnvelope::from_message(
        &pusher_message,
        None,
        connection_id.map(str::to_string),
        now_ms(),
    )
    .map_err(AppError::InvalidInput)?;
    envelope.name = message.name;
    envelope.encoding = encoding.map(sockudo_core::message_envelope::EncodingChain);
    envelope.message_id = pusher_message.message_id.clone();
    if let Some(raw_data) = raw_data
        && envelope.encoding.is_some()
        && let Some(raw) = raw_data.as_str()
    {
        envelope.data = Some(MessageContent::Text(raw.to_string()));
    }
    let publish_result = MessageService::new(Arc::clone(handler))
        .publish_message(
            app,
            channel,
            pusher_message,
            PublishContext {
                actor_client_id: client_id.map(str::to_string),
                publisher_connection_id: connection_id.map(str::to_string),
                envelope: Some(envelope),
                ..Default::default()
            },
        )
        .await?;
    #[cfg(feature = "push")]
    if !publish_result.duplicate
        && let (Some(store), Some(queue), Some(push_payload)) = (
            hub.push_store.as_ref(),
            hub.push_queue.as_ref(),
            push_payload,
        )
    {
        let summary = summarize_ably_push_channel_recipients(store, &app.id, channel).await?;
        accept_ably_push_intent(
            store.clone(),
            queue.clone(),
            hub.push_admission.as_deref(),
            AblyPushIntentRequest {
                app_id: app.id.clone(),
                publish_id: format!("message-push:{}", publish_result.receipt.acknowledgement_id),
                targets: vec![PublishTarget::Channel {
                    channel: channel.to_string(),
                }],
                required_providers: summary.providers,
                payload: push_payload,
                expected_recipients: summary.count,
            },
        )
        .await?;
    }
    #[cfg(not(feature = "push"))]
    let _ = hub;
    Ok(publish_result.receipt.acknowledgement_id)
}

async fn publish_ably_append(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    actor: MutableMessageActor,
    message: AblyMessage,
) -> Result<String, AppError> {
    let version_serial = message
        .version
        .as_ref()
        .filter(|version| {
            !version.serial.is_empty() && message.serial.as_deref() != Some(version.serial.as_str())
        })
        .map(|version| VersionSerial::new(version.serial.clone()))
        .transpose()?;
    let version_timestamp_ms = message
        .version
        .as_ref()
        .and_then(|version| version.timestamp);
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("appendMessage requires message.serial".to_string())
    })?;
    let data = match message.data.as_ref() {
        Some(value) => value.as_str().ok_or_else(|| {
            AppError::InvalidInput("appendMessage data must be a string".to_string())
        })?,
        None => "",
    }
    .to_string();
    let request = AppendMessageRequest {
        data,
        extras: ably_extras_to_message_extras(message.extras)?,
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_append_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        actor,
        request,
        version_serial,
        version_timestamp_ms,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

async fn publish_ably_update(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    actor: MutableMessageActor,
    message: AblyMessage,
) -> Result<String, AppError> {
    let version_serial = message
        .version
        .as_ref()
        .filter(|version| {
            !version.serial.is_empty() && message.serial.as_deref() != Some(version.serial.as_str())
        })
        .map(|version| VersionSerial::new(version.serial.clone()))
        .transpose()?;
    let version_timestamp_ms = message
        .version
        .as_ref()
        .and_then(|version| version.timestamp);
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("updateMessage requires message.serial".to_string())
    })?;
    let request = UpdateMessageRequest {
        name: message.name,
        data: message
            .data
            .map(|value| ably_message_data_to_message_data(value, message.encoding.as_deref())),
        extras: ably_extras_to_message_extras(message.extras)?,
        clear_fields: Vec::new(),
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_update_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        actor,
        request,
        version_serial,
        version_timestamp_ms,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

async fn publish_ably_delete(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    actor: MutableMessageActor,
    message: AblyMessage,
) -> Result<String, AppError> {
    let version_serial = message
        .version
        .as_ref()
        .filter(|version| {
            !version.serial.is_empty() && message.serial.as_deref() != Some(version.serial.as_str())
        })
        .map(|version| VersionSerial::new(version.serial.clone()))
        .transpose()?;
    let version_timestamp_ms = message
        .version
        .as_ref()
        .and_then(|version| version.timestamp);
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("deleteMessage requires message.serial".to_string())
    })?;
    let request = sockudo_protocol::versioned_messages::DeleteMessageRequest {
        data: message
            .data
            .map(|value| ably_message_data_to_message_data(value, message.encoding.as_deref())),
        extras: ably_extras_to_message_extras(message.extras)?,
        clear_fields: Vec::new(),
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_delete_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        actor,
        request,
        version_serial,
        version_timestamp_ms,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

#[allow(clippy::too_many_arguments)]
async fn handle_ably_presence(
    handler: &Arc<ConnectionHandler>,
    hub: &Arc<AblyCompatHub>,
    app: &App,
    connection_id: &str,
    client_id: Option<&str>,
    capabilities: Option<&ConnectionCapabilities>,
    sender: &AblySender,
    replace_presence_on_reenter: bool,
    mut inbound: AblyProtocolMessage,
) {
    let Some(raw_channel) = inbound.channel.clone() else {
        send_publish_nack(sender, &inbound, 40000, "PRESENCE requires channel");
        return;
    };
    let channel = match AblyChannelName::parse(raw_channel) {
        Ok(channel) => channel,
        Err(error) => {
            send_publish_nack(sender, &inbound, 40010, error.to_string());
            return;
        }
    };
    if let Err(error) =
        ensure_ably_channel_capability(capabilities, &channel, AblyCapabilityCheck::Presence)
    {
        send_publish_nack(sender, &inbound, error.code, error.message);
        return;
    }
    let mut presence = inbound.presence.take().unwrap_or_default();
    let msg_serial = inbound.msg_serial.unwrap_or(0);
    for (index, member) in presence.iter_mut().enumerate() {
        member.connection_id = Some(connection_id.to_string());
        if let Some(authenticated_client_id) = client_id {
            if member
                .client_id
                .as_deref()
                .is_some_and(|claimed| claimed != authenticated_client_id)
            {
                send_publish_nack(
                    sender,
                    &inbound,
                    40012,
                    "presence clientId must match authenticated clientId",
                );
                return;
            }
            member.client_id = Some(authenticated_client_id.to_string());
        }
        member.timestamp.get_or_insert_with(now_ms);
        member.id = Some(format!(
            "{connection_id}:{msg_serial}:{}",
            u64::try_from(index).unwrap_or(u64::MAX)
        ));
    }
    let presence_count = u64::try_from(presence.len()).unwrap_or(u64::MAX);
    let presence_bytes = sonic_rs::to_vec(&presence)
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or_default();
    let service = PresenceService::new(Arc::clone(handler));
    let mut changes = Vec::with_capacity(presence.len());
    for member in presence {
        let action = match member.action.unwrap_or(1) {
            0 | 3 => PresenceOperation::Leave,
            1 | 2 => PresenceOperation::Enter,
            4 => PresenceOperation::Update,
            _ => {
                send_publish_nack(sender, &inbound, 40000, "unsupported presence action");
                return;
            }
        };
        let record = match presence_record_from_ably(&member) {
            Ok(record) => record,
            Err(message) => {
                send_publish_nack(sender, &inbound, 40000, message);
                return;
            }
        };
        if replace_presence_on_reenter && action == PresenceOperation::Enter {
            match service
                .remove_superseded_client(
                    app,
                    channel.base(),
                    &record.client_id,
                    &record.connection_id,
                )
                .await
            {
                Ok(removals) => {
                    changes.extend(removals.into_iter().map(|removal| PresenceChange {
                        action: PresenceChangeAction::Leave,
                        member: removal.member,
                        wire_id: None,
                    }))
                }
                Err(error) => {
                    send_publish_nack(sender, &inbound, 50000, error.to_string());
                    return;
                }
            }
        }
        let cause = if action == PresenceOperation::Leave {
            PresenceHistoryEventCause::Disconnect
        } else {
            PresenceHistoryEventCause::Join
        };
        match service
            .apply(app, channel.base(), action, cause, record.clone())
            .await
        {
            Ok(Some(_)) => changes.push(PresenceChange {
                action: match action {
                    PresenceOperation::Enter => PresenceChangeAction::Enter,
                    PresenceOperation::Update => PresenceChangeAction::Update,
                    PresenceOperation::Leave => PresenceChangeAction::Leave,
                },
                member: record,
                wire_id: member.id,
            }),
            Ok(None) => {}
            Err(error) => {
                send_publish_nack(sender, &inbound, 50000, error.to_string());
                return;
            }
        }
    }
    if !changes.is_empty()
        && let Err(error) = handler
            .fanout_presence(
                &app.id,
                channel.base(),
                PresenceReplication {
                    changes,
                    unregister_connection: None,
                },
            )
            .await
    {
        send_publish_nack(sender, &inbound, 50000, error.to_string());
        return;
    }
    if presence_count > 0 {
        let observation =
            match StatsObservation::presence(&app.id, now_ms(), presence_count, presence_bytes) {
                Ok(observation) => observation,
                Err(error) => {
                    send_publish_nack(sender, &inbound, 50000, error.to_string());
                    return;
                }
            };
        if let Err(error) = hub.stats.record(observation).await {
            send_publish_nack(sender, &inbound, 50000, error.to_string());
            return;
        }
    }
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ACK,
            msg_serial: inbound.msg_serial,
            count: inbound.count.or(Some(1)),
            ..empty_protocol_message(ACTION_ACK)
        },
    );
}

pub async fn ably_time(Query(query): Query<AblyRestQuery>, headers: HeaderMap) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    encode_ably_rest_response(StatusCode::OK, format, &vec![now_ms()])
        .unwrap_or_else(ably_app_error_response)
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct AblyPushQuery {
    key: Option<String>,
    #[serde(rename = "access_token", alias = "accessToken")]
    access_token: Option<String>,
    client_id: Option<String>,
    channel: Option<String>,
    device_id: Option<String>,
    limit: Option<usize>,
    #[serde(rename = "cursor")]
    _cursor: Option<String>,
    format: Option<String>,
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AblyPushDeviceRequest {
    id: Option<String>,
    client_id: Option<String>,
    device_secret: Option<String>,
    platform: String,
    form_factor: String,
    metadata: Option<WireValue>,
    push: AblyPushDeviceDetailsRequest,
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize)]
struct AblyPushDeviceDetailsRequest {
    recipient: WireValue,
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct AblyPushSubscription {
    channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
}

#[cfg(feature = "push")]
#[derive(Debug, Deserialize)]
struct AblyPushPublishRequest {
    recipient: WireValue,
    #[serde(default)]
    notification: Option<WireValue>,
    #[serde(default)]
    data: Option<WireValue>,
}

#[cfg(feature = "push")]
fn ably_push_store(hub: &AblyCompatHub) -> Result<DynPushStore, AppError> {
    hub.push_store.clone().ok_or_else(|| {
        AppError::InternalError("Ably push requires the native Sockudo push store".to_string())
    })
}

#[cfg(feature = "push")]
fn ably_push_queue(hub: &AblyCompatHub) -> Result<DynPushQueue, AppError> {
    hub.push_queue.clone().ok_or_else(|| {
        AppError::InternalError("Ably push requires the native Sockudo push queue".to_string())
    })
}

#[cfg(feature = "push")]
fn ably_push_payload(
    notification: Option<Value>,
    data: Option<Value>,
) -> Result<PushPayload, AppError> {
    let string_field = |name: &'static str| -> Result<Option<String>, AppError> {
        notification
            .as_ref()
            .and_then(|value| value.get(name))
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    AppError::InvalidInput(format!("push notification {name} must be a string"))
                })
            })
            .transpose()
    };
    let payload = PushPayload {
        template_id: None,
        template_data: data.unwrap_or_else(Value::new_object),
        title: string_field("title")?,
        body: string_field("body")?,
        icon: string_field("icon")?,
        sound: string_field("sound")?,
        collapse_key: notification
            .as_ref()
            .and_then(|value| value.get("collapseKey"))
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    AppError::InvalidInput(
                        "push notification collapseKey must be a string".to_string(),
                    )
                })
            })
            .transpose()?,
    };
    payload
        .validate()
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(payload)
}

#[cfg(feature = "push")]
fn ably_push_payload_value(value: &Value) -> Result<PushPayload, AppError> {
    if !value.is_object() {
        return Err(AppError::InvalidInput(
            "message extras.push must be an object".to_string(),
        ));
    }
    ably_push_payload(
        value.get("notification").cloned(),
        value.get("data").cloned(),
    )
}

#[cfg(feature = "push")]
fn ably_push_pipeline_error(error: PushPipelineError) -> AppError {
    match error {
        PushPipelineError::Domain(error) => AppError::InvalidInput(error.to_string()),
        PushPipelineError::InvalidPayload(message) => AppError::InvalidInput(message),
        PushPipelineError::Backpressure(message) => AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message,
        },
        PushPipelineError::Storage(error) => ably_push_error(error),
        PushPipelineError::Queue(error) => {
            AppError::InternalError(format!("native push queue failed: {error}"))
        }
    }
}

#[cfg(feature = "push")]
struct AblyPushIntentRequest {
    app_id: String,
    publish_id: String,
    targets: Vec<PublishTarget>,
    required_providers: BTreeSet<PushProviderKind>,
    payload: PushPayload,
    expected_recipients: u64,
}

#[cfg(feature = "push")]
async fn accept_ably_push_intent(
    store: DynPushStore,
    queue: DynPushQueue,
    admission: Option<&dyn AblyPushAdmissionGuard>,
    request: AblyPushIntentRequest,
) -> Result<PushAcceptOutcome, AppError> {
    if let Some(message) = admission.and_then(|guard| {
        guard.rejection_for(
            &request.targets,
            &request.required_providers,
            request.expected_recipients,
        )
    }) {
        return Err(AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message,
        });
    }
    PushPipeline::new(store, queue, FanoutConfig::default())
        .accept_publish(
            PushAcceptRequest {
                intent: PublishIntent {
                    app_id: request.app_id,
                    publish_id: request.publish_id,
                    targets: request.targets,
                    payload: request.payload,
                    provider_overrides: Vec::new(),
                    not_before_ms: None,
                    expires_at_ms: None,
                },
                expected_recipients: request.expected_recipients,
            },
            u64::try_from(now_ms()).unwrap_or_default(),
        )
        .await
        .map_err(ably_push_pipeline_error)
}

#[cfg(feature = "push")]
#[derive(Clone)]
struct AblyRealtimePushDispatcher {
    handler: Weak<ConnectionHandler>,
}

#[cfg(feature = "push")]
impl AblyRealtimePushDispatcher {
    fn new(handler: Weak<ConnectionHandler>) -> Self {
        Self { handler }
    }

    fn is_bound(&self) -> bool {
        self.handler.strong_count() > 0
    }

    fn result(
        job: sockudo_push::DeliveryJob,
        outcome: DeliveryOutcome,
        provider_message_id: Option<String>,
        error: Option<ProviderError>,
    ) -> DeliveryResult {
        DeliveryResult {
            app_id: job.app_id,
            publish_id: job.publish_id,
            provider: job.provider,
            batch_id: job.batch_id,
            device_id: job.device_id,
            outcome,
            provider_message_id,
            error,
            attempt: job.attempt,
        }
    }

    fn unavailable(job: sockudo_push::DeliveryJob, reason: &'static str) -> DeliveryResult {
        Self::result(
            job,
            DeliveryOutcome::Retryable,
            None,
            Some(ProviderError {
                class: "unavailable".to_string(),
                failure_class: ProviderFailureClass::ProviderTransient,
                reason: Some(reason.to_string()),
                retry_after_ms: None,
            }),
        )
    }
}

#[cfg(feature = "push")]
fn ably_realtime_delivery_id(job: &DeliveryJob) -> String {
    stable_hash(
        format!(
            "{}:{}:{}",
            job.app_id,
            job.publish_id,
            job.recipient.token_hash()
        )
        .as_bytes(),
    )
}

#[cfg(feature = "push")]
#[async_trait::async_trait]
impl PushDispatcher for AblyRealtimePushDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Realtime
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let Some(handler) = self.handler.upgrade() else {
            return batch
                .jobs
                .into_iter()
                .map(|job| Self::unavailable(job, "realtime service is shutting down"))
                .collect();
        };
        let app = match handler.app_manager().find_by_id(&batch.app_id).await {
            Ok(Some(app)) => app,
            Ok(None) => {
                return batch
                    .jobs
                    .into_iter()
                    .map(|job| {
                        Self::result(
                            job,
                            DeliveryOutcome::Rejected,
                            None,
                            Some(ProviderError {
                                class: "invalid_target".to_string(),
                                failure_class: ProviderFailureClass::CallerPayload,
                                reason: Some("application is unavailable".to_string()),
                                retry_after_ms: None,
                            }),
                        )
                    })
                    .collect();
            }
            Err(_) => {
                return batch
                    .jobs
                    .into_iter()
                    .map(|job| Self::unavailable(job, "application lookup is unavailable"))
                    .collect();
            }
        };

        let mut results = Vec::with_capacity(batch.jobs.len());
        for job in batch.jobs {
            let PushRecipient::Realtime { channel } = &job.recipient else {
                results.push(Self::result(
                    job,
                    DeliveryOutcome::Rejected,
                    None,
                    Some(ProviderError {
                        class: "invalid_target".to_string(),
                        failure_class: ProviderFailureClass::CallerPayload,
                        reason: Some(
                            "realtime worker received another provider target".to_string(),
                        ),
                        retry_after_ms: None,
                    }),
                ));
                continue;
            };
            let Some(rendered) = job.rendered_payload.as_ref() else {
                results.push(Self::result(
                    job,
                    DeliveryOutcome::Rejected,
                    None,
                    Some(ProviderError {
                        class: "invalid_payload".to_string(),
                        failure_class: ProviderFailureClass::CallerPayload,
                        reason: Some("rendered realtime payload is missing".to_string()),
                        retry_after_ms: None,
                    }),
                ));
                continue;
            };
            let delivery_id = ably_realtime_delivery_id(&job);
            match publish_ably_push_event(&handler, &app, channel, &rendered.payload, &delivery_id)
                .await
            {
                Ok(provider_message_id) => results.push(Self::result(
                    job,
                    DeliveryOutcome::Accepted,
                    Some(provider_message_id),
                    None,
                )),
                Err(_) => results.push(Self::unavailable(job, "realtime fanout failed")),
            }
        }
        results
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Realtime,
            healthy: self.is_bound(),
            details: if self.is_bound() {
                "native realtime fanout is available".to_string()
            } else {
                "native realtime fanout is unavailable".to_string()
            },
        }
    }
}

#[cfg(feature = "push")]
async fn resolve_ably_push_auth(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    headers: &HeaderMap,
    query: &AblyPushQuery,
) -> Result<ResolvedAblyAuth, AblyAuthError> {
    let ordinary = resolve_ably_auth(
        hub,
        handler,
        headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await;
    if ordinary.is_ok() {
        return ordinary;
    }

    let token = bearer_token(headers).or_else(|| query.access_token.clone());
    let Some(token) = token else {
        return ordinary;
    };
    let Some((app_id, device_id)) = parse_ably_device_identity_token(&token) else {
        return ordinary;
    };
    let store = hub
        .push_store
        .as_ref()
        .ok_or_else(|| AblyAuthError::forbidden("Native push identity storage is unavailable"))?;
    let Some(device) = store
        .get_device(&app_id, &device_id)
        .await
        .map_err(|_| AblyAuthError::forbidden("Device identity could not be verified"))?
    else {
        return Err(AblyAuthError::invalid_credentials());
    };
    if !verify_device_identity_token(&token, &device.device_secret) {
        return Err(AblyAuthError::invalid_credentials());
    }
    let app = handler
        .app_manager()
        .find_by_id(&app_id)
        .await
        .map_err(|_| AblyAuthError::forbidden("Device application could not be loaded"))?
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let mut capabilities = restricted_ably_capabilities();
    capabilities.push_subscribe = Some(vec!["*".to_string()]);
    Ok(ResolvedAblyAuth {
        app,
        client_id: device.client_id.clone(),
        connection_client_id: device.client_id,
        capabilities: Some(capabilities),
        issued_ms: now_ms(),
        expires_ms: None,
        credential_id: format!("push-device:{}", stable_hash(token.as_bytes())),
        revocable: false,
        revocation_key: None,
        #[cfg(feature = "push")]
        push_device_id: Some(device_id),
    })
}

#[cfg(feature = "push")]
fn generate_ably_device_identity_token(app_id: &str, device_id: &str) -> SecretString {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let app_id = URL_SAFE_NO_PAD.encode(app_id.as_bytes());
    let device_id = URL_SAFE_NO_PAD.encode(device_id.as_bytes());
    let random = generate_device_identity_token();
    SecretString::new(format!(
        "v1.{app_id}.{device_id}.{}",
        random.expose_secret()
    ))
    .expect("generated device identity components are non-empty")
}

#[cfg(feature = "push")]
fn parse_ably_device_identity_token(token: &str) -> Option<(String, String)> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    let mut parts = token.split('.');
    if parts.next()? != "v1" {
        return None;
    }
    let app_id = parts.next()?;
    let device_id = parts.next()?;
    let random = parts.next()?;
    if random.is_empty() || parts.next().is_some() {
        return None;
    }
    let app_id = String::from_utf8(URL_SAFE_NO_PAD.decode(app_id).ok()?).ok()?;
    let device_id = String::from_utf8(URL_SAFE_NO_PAD.decode(device_id).ok()?).ok()?;
    Some((app_id, device_id))
}

#[cfg(feature = "push")]
fn ably_push_admin_allowed(
    capabilities: Option<&ConnectionCapabilities>,
    resource: Option<&str>,
) -> bool {
    let Some(capabilities) = capabilities else {
        return true;
    };
    resource.map_or_else(
        || {
            capabilities
                .push_admin
                .as_deref()
                .is_some_and(|patterns| !patterns.is_empty())
        },
        |resource| capabilities.allows_push_admin(resource),
    )
}

#[cfg(feature = "push")]
fn ensure_ably_push_admin(
    resolved: &ResolvedAblyAuth,
    resource: Option<&str>,
) -> Result<(), AppError> {
    if ably_push_admin_allowed(resolved.capabilities.as_ref(), resource) {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "push-admin capability is required".to_string(),
        ))
    }
}

#[cfg(feature = "push")]
fn ensure_ably_push_subscribe(
    resolved: &ResolvedAblyAuth,
    channel: Option<&str>,
) -> Result<(), AppError> {
    let Some(capabilities) = resolved.capabilities.as_ref() else {
        return Ok(());
    };
    let allowed = channel.map_or_else(
        || {
            capabilities
                .push_subscribe
                .as_deref()
                .is_some_and(|patterns| !patterns.is_empty())
        },
        |channel| {
            capabilities.allows_push_subscribe(channel) || capabilities.allows_push_admin(channel)
        },
    );
    if allowed {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "push-subscribe capability is required".to_string(),
        ))
    }
}

#[cfg(feature = "push")]
fn ably_device_identity_header(headers: &HeaderMap) -> Result<Option<&str>, AppError> {
    headers
        .get("x-ably-devicetoken")
        .map(|value| {
            value
                .to_str()
                .map_err(|_| AppError::Forbidden("Invalid device identity token".to_string()))
        })
        .transpose()
}

#[cfg(feature = "push")]
fn ensure_ably_device_owner(
    resolved: &ResolvedAblyAuth,
    headers: &HeaderMap,
    device: &DeviceDetails,
) -> Result<(), AppError> {
    if resolved.push_device_id.as_deref() == Some(device.id.as_str()) {
        return Ok(());
    }
    let Some(token) = ably_device_identity_header(headers)? else {
        return Err(AppError::Forbidden(
            "A matching device identity token is required".to_string(),
        ));
    };
    if verify_device_identity_token(token, &device.device_secret) {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "Invalid device identity token".to_string(),
        ))
    }
}

#[cfg(feature = "push")]
fn ably_push_error(error: impl std::fmt::Display) -> AppError {
    AppError::InternalError(format!("native push store failed: {error}"))
}

#[cfg(feature = "push")]
fn ably_push_wire_value(value: WireValue) -> Result<Value, AppError> {
    sonic_rs::to_value(&value)
        .map_err(|error| AppError::InvalidInput(format!("Invalid push payload value: {error}")))
}

#[cfg(feature = "push")]
fn ably_push_platform(value: &str) -> Result<Platform, AppError> {
    match value.to_ascii_lowercase().as_str() {
        "android" => Ok(Platform::Android),
        "ios" => Ok(Platform::Ios),
        "browser" => Ok(Platform::Browser),
        "windows" => Ok(Platform::Windows),
        "macos" => Ok(Platform::Macos),
        "watchos" => Ok(Platform::Watchos),
        "tvos" => Ok(Platform::Tvos),
        "other" => Ok(Platform::Other),
        _ => Err(AppError::InvalidInput(
            "Invalid push device platform".to_string(),
        )),
    }
}

#[cfg(feature = "push")]
fn ably_push_form_factor(value: &str) -> Result<FormFactor, AppError> {
    match value.to_ascii_lowercase().as_str() {
        "phone" => Ok(FormFactor::Phone),
        "tablet" => Ok(FormFactor::Tablet),
        "desktop" => Ok(FormFactor::Desktop),
        "tv" => Ok(FormFactor::Tv),
        "car" => Ok(FormFactor::Car),
        "watch" => Ok(FormFactor::Watch),
        "embedded" => Ok(FormFactor::Embedded),
        "other" => Ok(FormFactor::Other),
        _ => Err(AppError::InvalidInput(
            "Invalid push device formFactor".to_string(),
        )),
    }
}

#[cfg(feature = "push")]
fn ably_push_recipient(value: &Value) -> Result<PushRecipient, AppError> {
    let transport = value
        .get("transportType")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::InvalidInput("push recipient requires transportType".to_string())
        })?;
    let required_secret = |name: &'static str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::InvalidInput(format!("push recipient requires {name}")))
            .and_then(|value| {
                SecretString::new(value).map_err(|error| AppError::InvalidInput(error.to_string()))
            })
    };
    match transport {
        "ablyChannel" => Ok(PushRecipient::Realtime {
            channel: value
                .get("channel")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AppError::InvalidInput("ablyChannel recipient requires channel".to_string())
                })?
                .to_string(),
        }),
        "gcm" => Ok(PushRecipient::Fcm {
            registration_token: required_secret("registrationToken")?,
        }),
        "apns" => Ok(PushRecipient::Apns {
            device_token: required_secret("deviceToken")?,
        }),
        "hms" => Ok(PushRecipient::Hms {
            registration_token: required_secret("registrationToken")?,
        }),
        "wns" => Ok(PushRecipient::Wns {
            channel_uri: required_secret("channelUri")?,
        }),
        "web" => Ok(PushRecipient::Web {
            endpoint: required_secret("endpoint")?,
            p256dh: required_secret("p256dh")?,
            auth: required_secret("auth")?,
        }),
        _ => Err(AppError::InvalidInput(format!(
            "Unsupported push transportType '{transport}'"
        ))),
    }
}

#[cfg(feature = "push")]
fn ably_push_recipient_value(recipient: &PushRecipient) -> Value {
    match recipient {
        PushRecipient::Fcm { registration_token } => json!({
            "transportType": "gcm",
            "registrationToken": registration_token.expose_secret()
        }),
        PushRecipient::Apns { device_token } => json!({
            "transportType": "apns",
            "deviceToken": device_token.expose_secret()
        }),
        PushRecipient::Web {
            endpoint,
            p256dh,
            auth,
        } => json!({
            "transportType": "web",
            "endpoint": endpoint.expose_secret(),
            "p256dh": p256dh.expose_secret(),
            "auth": auth.expose_secret()
        }),
        PushRecipient::Hms { registration_token } => json!({
            "transportType": "hms",
            "registrationToken": registration_token.expose_secret()
        }),
        PushRecipient::Wns { channel_uri } => json!({
            "transportType": "wns",
            "channelUri": channel_uri.expose_secret()
        }),
        PushRecipient::Realtime { channel } => json!({
            "transportType": "ablyChannel",
            "channel": channel
        }),
    }
}

#[cfg(feature = "push")]
fn ably_push_device_value(device: &DeviceDetails, issued_token: Option<&str>) -> Value {
    const DEVICE_SECRET_KEY: &str = "sockudo_ably_device_secret";
    let mut value = Value::new_object();
    let object = value.as_object_mut().expect("fresh object");
    object.insert("id", Value::from(device.id.as_str()));
    if let Some(client_id) = device.client_id.as_ref() {
        object.insert("clientId", Value::from(client_id.as_str()));
    }
    object.insert(
        "platform",
        Value::from(ably_push_platform_label(&device.platform)),
    );
    object.insert(
        "formFactor",
        Value::from(ably_push_form_factor_label(&device.form_factor)),
    );
    let mut metadata = device.metadata.clone();
    let device_secret = metadata
        .as_object()
        .and_then(|object| object.get(&DEVICE_SECRET_KEY))
        .and_then(Value::as_str)
        .map(str::to_string);
    if let Some(object) = metadata.as_object_mut() {
        object.remove(&DEVICE_SECRET_KEY);
    }
    object.insert("metadata", metadata);
    if let Some(device_secret) = device_secret.as_deref() {
        object.insert("deviceSecret", Value::from(device_secret));
    }
    let mut push = Value::new_object();
    let push_object = push.as_object_mut().expect("fresh object");
    push_object.insert(
        "recipient",
        ably_push_recipient_value(&device.push.recipient),
    );
    push_object.insert("state", Value::from("ACTIVE"));
    object.insert("push", push);
    if let Some(token) = issued_token
        && let Some(object) = value.as_object_mut()
    {
        let issued = now_ms();
        let mut details = Value::new_object();
        let details_object = details.as_object_mut().expect("fresh object");
        details_object.insert("token", Value::from(token));
        details_object.insert("issued", Value::from(issued));
        details_object.insert(
            "expires",
            Value::from(issued.saturating_add(365 * 24 * 60 * 60 * 1000)),
        );
        details_object.insert("capability", Value::from("{}"));
        if let Some(client_id) = device.client_id.as_ref() {
            details_object.insert("clientId", Value::from(client_id.as_str()));
        }
        object.insert("deviceIdentityToken", details);
    }
    value
}

#[cfg(feature = "push")]
fn ably_push_platform_label(platform: &Platform) -> &'static str {
    match platform {
        Platform::Android => "android",
        Platform::Ios => "ios",
        Platform::Browser => "browser",
        Platform::Windows => "windows",
        Platform::Macos => "macos",
        Platform::Watchos => "watchos",
        Platform::Tvos => "tvos",
        Platform::Other => "other",
    }
}

#[cfg(feature = "push")]
fn ably_push_form_factor_label(form_factor: &FormFactor) -> &'static str {
    match form_factor {
        FormFactor::Phone => "phone",
        FormFactor::Tablet => "tablet",
        FormFactor::Desktop => "desktop",
        FormFactor::Tv => "tv",
        FormFactor::Car => "car",
        FormFactor::Watch => "watch",
        FormFactor::Embedded => "embedded",
        FormFactor::Other => "other",
    }
}

#[cfg(feature = "push")]
async fn save_ably_push_device(
    hub: &AblyCompatHub,
    app_id: &str,
    path_id: Option<String>,
    request: AblyPushDeviceRequest,
) -> Result<Value, AppError> {
    let store = ably_push_store(hub)?;
    if let (Some(path_id), Some(body_id)) = (path_id.as_deref(), request.id.as_deref())
        && path_id != body_id
    {
        return Err(AppError::InvalidInput(
            "push device path id does not match body id".to_string(),
        ));
    }
    let id = path_id.or(request.id).ok_or_else(|| {
        AppError::InvalidInput("push device registration requires id".to_string())
    })?;
    const DEVICE_SECRET_KEY: &str = "sockudo_ably_device_secret";
    let existing = store
        .get_device(app_id, &id)
        .await
        .map_err(ably_push_error)?;
    let issued_identity = existing
        .is_none()
        .then(|| generate_ably_device_identity_token(app_id, &id));
    let recipient = ably_push_wire_value(request.push.recipient)?;
    let mut metadata = request
        .metadata
        .map(ably_push_wire_value)
        .transpose()?
        .or_else(|| existing.as_ref().map(|device| device.metadata.clone()))
        .unwrap_or_else(|| json!({}));
    if !metadata.is_object() {
        metadata = json!({ "value": metadata });
    }
    if let Some(device_secret) = request.device_secret.as_deref()
        && let Some(object) = metadata.as_object_mut()
    {
        object.insert(DEVICE_SECRET_KEY, Value::from(device_secret));
    }
    let identity_hash = existing
        .as_ref()
        .map(|device| device.device_secret.clone())
        .unwrap_or_else(|| {
            hash_device_identity_token(
                issued_identity
                    .as_ref()
                    .expect("new registrations issue an identity token"),
            )
        });
    let device = DeviceDetails {
        app_id: app_id.to_string(),
        id,
        client_id: request.client_id,
        form_factor: ably_push_form_factor(&request.form_factor)?,
        platform: ably_push_platform(&request.platform)?,
        metadata,
        device_secret: identity_hash,
        timezone: "UTC".to_string(),
        locale: "en".to_string(),
        last_active_at_ms: u64::try_from(now_ms()).unwrap_or_default(),
        push: DevicePushDetails {
            recipient: ably_push_recipient(&recipient)?,
            state: DevicePushState::Active,
            failure_count: 0,
            error_reason: None,
        },
        push_rate_policy: None,
    };
    store
        .upsert_device(device.clone())
        .await
        .map_err(ably_push_error)?;
    Ok(ably_push_device_value(
        &device,
        issued_identity.as_ref().map(SecretString::expose_secret),
    ))
}

#[cfg(feature = "push")]
async fn ably_push_put_device(
    Path(device_id): Path<String>,
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    ably_push_save_device_response(Some(device_id), query, headers, runtime, handler, body).await
}

#[cfg(feature = "push")]
async fn ably_push_save_device(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    ably_push_save_device_response(None, query, headers, runtime, handler, body).await
}

#[cfg(feature = "push")]
async fn ably_push_save_device_response(
    path_id: Option<String>,
    query: AblyPushQuery,
    headers: HeaderMap,
    runtime: Arc<AblyCompatRuntime>,
    handler: Arc<ConnectionHandler>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let format = ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let request = match decode_value::<AblyPushDeviceRequest>(&body, request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(StatusCode::BAD_REQUEST, 40000, error, format);
        }
    };
    let requested_id = path_id.as_deref().or(request.id.as_deref());
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    if ensure_ably_push_admin(&resolved, None).is_err() {
        if let Err(error) = ensure_ably_push_subscribe(&resolved, None) {
            return ably_app_error_response_format(error, format);
        }
        let Some(device_id) = requested_id else {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                "push device registration requires id",
                format,
            );
        };
        let store = match ably_push_store(&runtime.hub) {
            Ok(store) => store,
            Err(error) => return ably_app_error_response_format(error, format),
        };
        let device = match store.get_device(&resolved.app.id, device_id).await {
            Ok(Some(device)) => device,
            Ok(None) => {
                return ably_app_error_response_format(
                    AppError::Forbidden("push-admin capability is required".to_string()),
                    format,
                );
            }
            Err(error) => return ably_app_error_response_format(ably_push_error(error), format),
        };
        if let Err(error) = ensure_ably_device_owner(&resolved, &headers, &device) {
            return ably_app_error_response_format(error, format);
        }
    }
    match save_ably_push_device(&runtime.hub, &resolved.app.id, path_id, request).await {
        Ok(device) => encode_ably_rest_response(StatusCode::CREATED, format, &device)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(error, format),
    }
}

#[cfg(feature = "push")]
async fn ably_push_get_device(
    Path(device_id): Path<String>,
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    match store.get_device(&resolved.app.id, &device_id).await {
        Ok(Some(device)) => {
            if ensure_ably_push_admin(&resolved, None).is_err()
                && let Err(error) = ensure_ably_push_subscribe(&resolved, None)
                    .and_then(|()| ensure_ably_device_owner(&resolved, &headers, &device))
            {
                return ably_app_error_response_format(error, format);
            }
            encode_ably_rest_response(
                StatusCode::OK,
                format,
                &ably_push_device_value(&device, None),
            )
            .unwrap_or_else(ably_app_error_response)
        }
        Ok(None) => {
            ably_error_response_format(StatusCode::NOT_FOUND, 40400, "Device not found", format)
        }
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
async fn ably_push_list_devices(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    if let Err(error) = ensure_ably_push_admin(&resolved, None) {
        return ably_app_error_response_format(error, format);
    }
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let limit = query.limit.unwrap_or(1_000).clamp(1, 1_000);
    match store.list_devices(&resolved.app.id, limit, None).await {
        Ok(page) => {
            let devices = page
                .items
                .into_iter()
                .filter(|device| {
                    query
                        .client_id
                        .as_ref()
                        .is_none_or(|client_id| device.client_id.as_ref() == Some(client_id))
                })
                .map(|device| ably_push_device_value(&device, None))
                .collect::<Vec<_>>();
            encode_ably_rest_response(StatusCode::OK, format, &devices)
                .unwrap_or_else(ably_app_error_response)
        }
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
async fn ably_push_delete_device(
    Path(device_id): Path<String>,
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    ably_push_delete_devices_inner(Some(device_id), query, headers, runtime, handler).await
}

#[cfg(feature = "push")]
async fn ably_push_delete_devices(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    ably_push_delete_devices_inner(None, query, headers, runtime, handler).await
}

#[cfg(feature = "push")]
async fn ably_push_delete_devices_inner(
    path_id: Option<String>,
    query: AblyPushQuery,
    headers: HeaderMap,
    runtime: Arc<AblyCompatRuntime>,
    handler: Arc<ConnectionHandler>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let result = if let Some(device_id) = path_id.or(query.device_id) {
        if ensure_ably_push_admin(&resolved, None).is_err() {
            if let Err(error) = ensure_ably_push_subscribe(&resolved, None) {
                return ably_app_error_response_format(error, format);
            }
            let device = match store.get_device(&resolved.app.id, &device_id).await {
                Ok(Some(device)) => device,
                Ok(None) => {
                    return ably_error_response_format(
                        StatusCode::NOT_FOUND,
                        40400,
                        "Device not found",
                        format,
                    );
                }
                Err(error) => {
                    return ably_app_error_response_format(ably_push_error(error), format);
                }
            };
            if let Err(error) = ensure_ably_device_owner(&resolved, &headers, &device) {
                return ably_app_error_response_format(error, format);
            }
        }
        store
            .delete_device(&resolved.app.id, &device_id)
            .await
            .map(|_| ())
    } else if let Some(client_id) = query.client_id {
        if let Err(error) = ensure_ably_push_admin(&resolved, None) {
            return ably_app_error_response_format(error, format);
        }
        store
            .delete_devices_by_client(&resolved.app.id, &client_id)
            .await
            .map(|_| ())
    } else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "Device delete requires deviceId or clientId",
            format,
        );
    };
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
fn ably_push_subscription_value(subscription: &ChannelSubscription) -> AblyPushSubscription {
    let logical_client = subscription.scoped_client_id().is_some();
    AblyPushSubscription {
        channel: subscription.channel.clone(),
        device_id: (!logical_client).then(|| subscription.device_id.clone()),
        client_id: subscription.client_id.clone(),
    }
}

#[cfg(feature = "push")]
async fn ably_push_save_subscription(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let format = ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let request = match decode_value::<AblyPushSubscription>(&body, request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(StatusCode::BAD_REQUEST, 40000, error, format);
        }
    };
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let subscription = if let Some(device_id) = request.device_id.as_deref() {
        match store.get_device(&resolved.app.id, device_id).await {
            Ok(Some(device)) => {
                if ensure_ably_push_admin(&resolved, Some(&request.channel)).is_err()
                    && let Err(error) =
                        ensure_ably_push_subscribe(&resolved, Some(&request.channel))
                            .and_then(|()| ensure_ably_device_owner(&resolved, &headers, &device))
                {
                    return ably_app_error_response_format(error, format);
                }
                ChannelSubscription::from_device(request.channel.clone(), &device)
            }
            Ok(None) => {
                return ably_error_response_format(
                    StatusCode::NOT_FOUND,
                    40400,
                    "Device not found",
                    format,
                );
            }
            Err(error) => return ably_app_error_response_format(ably_push_error(error), format),
        }
    } else if let Some(client_id) = request.client_id.as_deref() {
        if ensure_ably_push_admin(&resolved, Some(&request.channel)).is_err() {
            if let Err(error) = ensure_ably_push_subscribe(&resolved, Some(&request.channel)) {
                return ably_app_error_response_format(error, format);
            }
            if resolved.client_id.as_deref() != Some(client_id) {
                return ably_app_error_response_format(
                    AppError::Forbidden(
                        "push-subscribe clientId must match authenticated clientId".to_string(),
                    ),
                    format,
                );
            }
        }
        ChannelSubscription::from_client(
            resolved.app.id.clone(),
            request.channel.clone(),
            client_id,
        )
    } else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "Subscription requires deviceId or clientId",
            format,
        );
    };
    match store.upsert_subscription(subscription).await {
        Ok(()) => encode_ably_rest_response(StatusCode::CREATED, format, &request)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
async fn all_ably_push_subscriptions(
    store: &DynPushStore,
    app_id: &str,
) -> Result<Vec<ChannelSubscription>, AppError> {
    store
        .list_subscriptions(app_id, 1_000, None)
        .await
        .map(|page| page.items)
        .map_err(ably_push_error)
}

#[cfg(feature = "push")]
async fn ably_push_list_subscriptions(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let is_admin = ensure_ably_push_admin(&resolved, None).is_ok();
    if !is_admin {
        let Some(channel) = query.channel.as_deref() else {
            return ably_app_error_response_format(
                AppError::Forbidden(
                    "push-admin is required to list subscriptions across channels".to_string(),
                ),
                format,
            );
        };
        if let Err(error) = ensure_ably_push_subscribe(&resolved, Some(channel)) {
            return ably_app_error_response_format(error, format);
        }
        if query.device_id.is_none()
            && query.client_id.is_none()
            && resolved.push_device_id.is_none()
            && resolved.client_id.is_none()
        {
            return ably_app_error_response_format(
                AppError::Forbidden("push-subscribe can only list owned subscriptions".to_string()),
                format,
            );
        }
    }
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    match all_ably_push_subscriptions(&store, &resolved.app.id).await {
        Ok(subscriptions) => {
            let subscriptions = subscriptions
                .into_iter()
                .filter(|subscription| {
                    query
                        .channel
                        .as_ref()
                        .is_none_or(|channel| &subscription.channel == channel)
                })
                .filter(|subscription| {
                    query
                        .device_id
                        .as_ref()
                        .is_none_or(|device_id| &subscription.device_id == device_id)
                })
                .filter(|subscription| {
                    query
                        .client_id
                        .as_ref()
                        .is_none_or(|client_id| subscription.client_id.as_ref() == Some(client_id))
                })
                .filter(|subscription| {
                    is_admin
                        || resolved
                            .push_device_id
                            .as_ref()
                            .is_some_and(|device_id| &subscription.device_id == device_id)
                        || resolved.client_id.as_ref().is_some_and(|client_id| {
                            subscription.client_id.as_ref() == Some(client_id)
                        })
                })
                .map(|subscription| ably_push_subscription_value(&subscription))
                .collect::<Vec<_>>();
            encode_ably_rest_response(StatusCode::OK, format, &subscriptions)
                .unwrap_or_else(ably_app_error_response)
        }
        Err(error) => ably_app_error_response_format(error, format),
    }
}

#[cfg(feature = "push")]
async fn ably_push_delete_subscriptions(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    if ensure_ably_push_admin(&resolved, query.channel.as_deref()).is_err() {
        let Some(channel) = query.channel.as_deref() else {
            return ably_app_error_response_format(
                AppError::Forbidden(
                    "push-subscribe can only remove subscriptions from one channel".to_string(),
                ),
                format,
            );
        };
        if let Err(error) = ensure_ably_push_subscribe(&resolved, Some(channel)) {
            return ably_app_error_response_format(error, format);
        }
        if let Some(device_id) = query.device_id.as_deref() {
            let device = match store.get_device(&resolved.app.id, device_id).await {
                Ok(Some(device)) => device,
                Ok(None) => {
                    return ably_error_response_format(
                        StatusCode::NOT_FOUND,
                        40400,
                        "Device not found",
                        format,
                    );
                }
                Err(error) => {
                    return ably_app_error_response_format(ably_push_error(error), format);
                }
            };
            if let Err(error) = ensure_ably_device_owner(&resolved, &headers, &device) {
                return ably_app_error_response_format(error, format);
            }
        } else if let Some(client_id) = query.client_id.as_deref() {
            if resolved.client_id.as_deref() != Some(client_id) {
                return ably_app_error_response_format(
                    AppError::Forbidden(
                        "push-subscribe clientId must match authenticated clientId".to_string(),
                    ),
                    format,
                );
            }
        } else {
            return ably_app_error_response_format(
                AppError::Forbidden(
                    "push-subscribe can only remove owned subscriptions".to_string(),
                ),
                format,
            );
        }
    }
    let mut cursor = None;
    loop {
        let page = match store
            .list_subscriptions(&resolved.app.id, 1_000, cursor)
            .await
        {
            Ok(page) => page,
            Err(error) => return ably_app_error_response_format(ably_push_error(error), format),
        };
        let next_cursor = page.next_cursor;
        for subscription in page.items {
            if query
                .channel
                .as_ref()
                .is_none_or(|channel| &subscription.channel == channel)
                && query
                    .device_id
                    .as_ref()
                    .is_none_or(|device_id| &subscription.device_id == device_id)
                && query
                    .client_id
                    .as_ref()
                    .is_none_or(|client_id| subscription.client_id.as_ref() == Some(client_id))
                && let Err(error) = store
                    .delete_subscription(
                        &resolved.app.id,
                        &subscription.channel,
                        &subscription.device_id,
                    )
                    .await
            {
                return ably_app_error_response_format(ably_push_error(error), format);
            }
        }
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(feature = "push")]
async fn ably_push_list_channels(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    if let Err(error) = ensure_ably_push_admin(&resolved, None) {
        return ably_app_error_response_format(error, format);
    }
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    match store
        .list_subscription_channels(
            &resolved.app.id,
            query.limit.unwrap_or(1_000).clamp(1, 1_000),
            None,
        )
        .await
    {
        Ok(page) => encode_ably_rest_response(StatusCode::OK, format, &page.items)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(ably_push_error(error), format),
    }
}

#[cfg(feature = "push")]
async fn publish_ably_push_event(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    payload: &Value,
    delivery_id: &str,
) -> Result<String, AppError> {
    let data = sonic_rs::to_string(payload).map_err(|error| {
        AppError::InternalError(format!("push payload serialization failed: {error}"))
    })?;
    let result = MessageService::new(Arc::clone(handler))
        .publish_message(
            app,
            channel,
            PusherMessage {
                event: Some("__ably_push__".to_string()),
                channel: Some(channel.to_string()),
                data: Some(MessageData::String(data)),
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: Some(delivery_id.to_string()),
                stream_id: None,
                serial: None,
                idempotency_key: Some(delivery_id.to_string()),
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
            PublishContext::default(),
        )
        .await?;
    Ok(result.receipt.acknowledgement_id)
}

#[cfg(feature = "push")]
#[derive(Default)]
struct AblyPushRecipientSummary {
    count: u64,
    providers: BTreeSet<PushProviderKind>,
}

#[cfg(feature = "push")]
async fn summarize_ably_push_devices_for_client(
    store: &DynPushStore,
    app_id: &str,
    client_id: &str,
) -> Result<AblyPushRecipientSummary, AppError> {
    let mut cursor = None;
    let mut summary = AblyPushRecipientSummary::default();
    loop {
        let page = store
            .list_devices(app_id, 1_000, cursor)
            .await
            .map_err(ably_push_error)?;
        for device in page
            .items
            .iter()
            .filter(|device| device.client_id.as_deref() == Some(client_id))
        {
            summary.count = summary.count.saturating_add(1);
            summary.providers.insert(device.push.recipient.provider());
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(summary);
        }
    }
}

#[cfg(feature = "push")]
async fn summarize_ably_push_channel_recipients(
    store: &DynPushStore,
    app_id: &str,
    channel: &str,
) -> Result<AblyPushRecipientSummary, AppError> {
    let mut cursor = None;
    let mut summary = AblyPushRecipientSummary::default();
    loop {
        let page = store
            .list_channel_subscribers(app_id, channel, 1_000, cursor)
            .await
            .map_err(ably_push_error)?;
        for subscription in page.items {
            if let Some(client_id) = subscription.scoped_client_id() {
                let client =
                    summarize_ably_push_devices_for_client(store, app_id, client_id).await?;
                summary.count = summary.count.saturating_add(client.count);
                summary.providers.extend(client.providers);
            } else if let Some(device) = store
                .get_device(app_id, &subscription.device_id)
                .await
                .map_err(ably_push_error)?
            {
                summary.count = summary.count.saturating_add(1);
                summary.providers.insert(device.push.recipient.provider());
            }
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(summary);
        }
    }
}

#[cfg(feature = "push")]
fn ably_push_publish_id(headers: &HeaderMap) -> String {
    headers
        .get("idempotency-key")
        .or_else(|| headers.get("x-idempotency-key"))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

#[cfg(feature = "push")]
async fn ably_push_publish(
    Query(query): Query<AblyPushQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
    let request_format = ably_rest_request_format(&headers);
    let format = ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let resolved = match resolve_ably_push_auth(&runtime.hub, &handler, &headers, &query).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let request = match decode_value::<AblyPushPublishRequest>(&body, request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(StatusCode::BAD_REQUEST, 40000, error, format);
        }
    };
    let store = match ably_push_store(&runtime.hub) {
        Ok(store) => store,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let recipient = match ably_push_wire_value(request.recipient) {
        Ok(recipient) => recipient,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let admin_resource = recipient
        .get("transportType")
        .and_then(Value::as_str)
        .filter(|transport| *transport == "ablyChannel")
        .and_then(|_| recipient.get("channel"))
        .and_then(Value::as_str);
    if let Err(error) = ensure_ably_push_admin(&resolved, admin_resource) {
        return ably_app_error_response_format(error, format);
    }
    let notification = match request.notification.map(ably_push_wire_value).transpose() {
        Ok(notification) => notification,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let data = match request.data.map(ably_push_wire_value).transpose() {
        Ok(data) => data,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let payload = match ably_push_payload(notification, data) {
        Ok(payload) => payload,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let (target, summary) = if recipient.get("transportType").and_then(Value::as_str)
        == Some("ablyChannel")
    {
        match ably_push_recipient(&recipient) {
            Ok(recipient) => {
                let provider = recipient.provider();
                (
                    PublishTarget::Recipient { recipient },
                    AblyPushRecipientSummary {
                        count: 1,
                        providers: BTreeSet::from([provider]),
                    },
                )
            }
            Err(error) => return ably_app_error_response_format(error, format),
        }
    } else if let Some(device_id) = recipient.get("deviceId").and_then(Value::as_str) {
        match store.get_device(&resolved.app.id, device_id).await {
            Ok(Some(device)) => (
                PublishTarget::Device {
                    device_id: device_id.to_string(),
                },
                AblyPushRecipientSummary {
                    count: 1,
                    providers: BTreeSet::from([device.push.recipient.provider()]),
                },
            ),
            Ok(None) => {
                return ably_error_response_format(
                    StatusCode::NOT_FOUND,
                    40400,
                    "Device not found",
                    format,
                );
            }
            Err(error) => return ably_app_error_response_format(ably_push_error(error), format),
        }
    } else if let Some(client_id) = recipient.get("clientId").and_then(Value::as_str) {
        match summarize_ably_push_devices_for_client(&store, &resolved.app.id, client_id).await {
            Ok(summary) => (
                PublishTarget::Client {
                    client_id: client_id.to_string(),
                },
                summary,
            ),
            Err(error) => return ably_app_error_response_format(error, format),
        }
    } else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "Unsupported push recipient",
            format,
        );
    };
    let queue = match ably_push_queue(&runtime.hub) {
        Ok(queue) => queue,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    if let Err(error) = accept_ably_push_intent(
        store,
        queue,
        runtime.hub.push_admission.as_deref(),
        AblyPushIntentRequest {
            app_id: resolved.app.id.clone(),
            publish_id: ably_push_publish_id(&headers),
            targets: vec![target],
            required_providers: summary.providers,
            payload,
            expected_recipients: summary.count,
        },
    )
    .await
    {
        return ably_app_error_response_format(error, format);
    }
    if let Ok(observation) = StatsObservation::push(
        &resolved.app.id,
        now_ms(),
        true,
        summary.count,
        request_bytes,
    ) && let Err(error) = runtime.hub.stats.record(observation).await
    {
        return ably_app_error_response_format(stats_app_error(error), format);
    }
    encode_ably_rest_response(StatusCode::CREATED, format, &json!({}))
        .unwrap_or_else(ably_app_error_response)
}

const ABLY_MAX_STATS_FIXTURE_INTERVALS: usize = 10_000;

pub async fn ably_ingest_stats(
    Query(query): Query<AblyStatsQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    if !runtime.hub.config.stats_fixture_ingest_enabled {
        return ably_error_response_format(
            StatusCode::NOT_FOUND,
            40400,
            "Stats fixture ingestion is disabled",
            format,
        );
    }
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let fixtures = match serde_json::from_slice::<Vec<serde_json::Value>>(&body) {
        Ok(fixtures) => fixtures,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                format!("Invalid stats interval payload: {error}"),
                format,
            );
        }
    };
    if fixtures.len() > ABLY_MAX_STATS_FIXTURE_INTERVALS {
        return ably_error_response_format(
            StatusCode::PAYLOAD_TOO_LARGE,
            40009,
            format!("Stats fixture exceeds the {ABLY_MAX_STATS_FIXTURE_INTERVALS}-interval limit"),
            format,
        );
    }
    if let Err(error) = runtime
        .hub
        .stats
        .ingest_fixtures(&resolved.app.id, fixtures)
        .await
    {
        return ably_app_error_response_format(stats_app_error(error), format);
    }
    encode_ably_rest_response(StatusCode::CREATED, format, &json!({}))
        .unwrap_or_else(ably_app_error_response)
}

/// Return typed interval statistics from the canonical stats store.
pub async fn ably_stats(
    Query(query): Query<AblyStatsQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let stats_query = match StatsQuery::parse(
        query
            .by
            .as_deref()
            .or(query.unit.as_deref())
            .unwrap_or("minute"),
        query.direction.as_deref(),
        query.start.as_deref(),
        query.end.as_deref(),
        query.limit,
        query.cursor.clone(),
    ) {
        Ok(query) => query,
        Err(error) => return ably_app_error_response_format(stats_app_error(error), format),
    };
    match runtime
        .hub
        .stats
        .query(&resolved.app.id, &stats_query, now_ms())
        .await
    {
        Ok(page) => {
            let mut response = match encode_ably_rest_response(StatusCode::OK, format, &page.items)
            {
                Ok(response) => response,
                Err(error) => return ably_app_error_response(error),
            };
            match stats_link_header(&query, &stats_query, page.next_cursor.as_deref()).parse() {
                Ok(value) => {
                    response.headers_mut().insert(header::LINK, value);
                    response
                }
                Err(error) => ably_app_error_response(AppError::InternalError(format!(
                    "invalid stats Link header: {error}"
                ))),
            }
        }
        Err(error) => ably_app_error_response_format(stats_app_error(error), format),
    }
}

fn stats_app_error(error: StatsError) -> AppError {
    match error {
        StatsError::InvalidQuery(message) | StatsError::InvalidFixture(message) => {
            AppError::InvalidInput(message)
        }
        StatsError::Store(message) => AppError::InternalError(message),
        StatsError::Closed => AppError::InternalError("stats aggregator is closed".to_string()),
        StatsError::QueueFull => AppError::InternalError("stats queue is full".to_string()),
    }
}

fn stats_link_header(raw_query: &AblyStatsQuery, query: &StatsQuery, next: Option<&str>) -> String {
    let params = |cursor: Option<&str>| {
        let mut params = vec![
            format!("limit={}", query.limit),
            format!("direction={}", query.direction.as_str()),
            format!("unit={}", query.unit.as_str()),
        ];
        if let Some(start) = query.start.as_deref() {
            params.push(format!("start={}", urlencoding::encode(start)));
        }
        if let Some(end) = query.end.as_deref() {
            params.push(format!("end={}", urlencoding::encode(end)));
        }
        if let Some(format) = raw_query.format.as_deref() {
            params.push(format!("format={}", urlencoding::encode(format)));
        }
        if let Some(cursor) = cursor {
            params.push(format!("cursor={}", urlencoding::encode(cursor)));
        }
        params.join("&")
    };
    let mut links = vec![format!("<./stats?{}>; rel=\"first\"", params(None))];
    if let Some(next) = next {
        links.push(format!("<./stats?{}>; rel=\"next\"", params(Some(next))));
    }
    links.join(", ")
}

pub async fn ably_channel_history(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyHistoryQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    match ably_channel_history_inner(&runtime.hub, channel_name, query, headers, handler).await {
        Ok(response) => response,
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub async fn ably_not_found(Query(query): Query<AblyRestQuery>, headers: HeaderMap) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    ably_error_response_format(StatusCode::NOT_FOUND, 40400, "Not found", format)
}

async fn ably_channel_presence(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyPresenceQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.auth_client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let channel = match parse_ably_channel_name(channel_name) {
        Ok(channel) => channel,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel,
        AblyCapabilityCheck::Presence,
    ) {
        return ably_error_response_format(error.status, error.code, error.message, format);
    }
    let limit = query.limit.unwrap_or(100).clamp(1, 1_000);
    let offset = match decode_presence_snapshot_cursor(query.cursor.as_deref()) {
        Ok(offset) => offset,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let snapshot = match PresenceService::new(Arc::clone(&handler))
        .snapshot(&resolved.app.id, channel.base())
        .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => return ably_app_error_response_format(AppError::from(error), format),
    };
    let matching = snapshot
        .into_iter()
        .map(|record| ably_presence_from_record(record, 1))
        .filter(|member| {
            query
                .client_id
                .as_deref()
                .is_none_or(|client_id| member.client_id.as_deref() == Some(client_id))
                && query.connection_id.as_deref().is_none_or(|connection_id| {
                    member.connection_id.as_deref() == Some(connection_id)
                })
        })
        .collect::<Vec<_>>();
    let members = matching
        .iter()
        .skip(offset)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    let next_offset = (offset + members.len() < matching.len()).then_some(offset + members.len());
    let links = ably_presence_link_header(
        &query,
        "presence",
        limit,
        next_offset.map(|next| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(u64::try_from(next).unwrap_or(u64::MAX).to_be_bytes())
        }),
    );
    let mut response = match encode_ably_rest_response(StatusCode::OK, format, &members) {
        Ok(response) => response,
        Err(error) => return ably_app_error_response(error),
    };
    match links
        .parse()
        .map_err(|error| AppError::InternalError(format!("invalid presence Link header: {error}")))
    {
        Ok(value) => {
            response.headers_mut().insert(header::LINK, value);
            response
        }
        Err(error) => ably_app_error_response_format(error, format),
    }
}

async fn ably_channel_presence_history(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyPresenceQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    match ably_channel_presence_history_inner(&runtime.hub, channel_name, query, headers, handler)
        .await
    {
        Ok(response) => response,
        Err(error) => ably_app_error_response_format(error, format),
    }
}

async fn ably_channel_presence_history_inner(
    hub: &AblyCompatHub,
    channel_name: String,
    query: AblyPresenceQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
) -> Result<Response, AppError> {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    let resolved = resolve_ably_auth(
        hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.auth_client_id.as_deref(),
    )
    .await
    .map_err(ably_auth_app_error)?;
    let channel = parse_ably_channel_name(channel_name)?;
    ensure_ably_channel_capability_app_error(
        resolved.capabilities.as_ref(),
        &channel,
        AblyCapabilityCheck::History,
    )?;
    let policy = resolved
        .app
        .resolved_presence_history(channel.base(), &handler.server_options().presence_history);
    let direction = match query.direction.as_deref().unwrap_or("backwards") {
        "forwards" => PresenceHistoryDirection::OldestFirst,
        "backwards" => PresenceHistoryDirection::NewestFirst,
        other => {
            return Err(AppError::InvalidInput(format!(
                "Invalid Ably presence history direction '{other}'"
            )));
        }
    };
    let stream_state = handler
        .presence_history_store()
        .stream_runtime_state(&resolved.app.id, channel.base())
        .await?;
    if !stream_state.continuity_proven {
        return Err(AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message: format!(
                "presence history continuity is unavailable: {}",
                stream_state
                    .reason
                    .as_deref()
                    .unwrap_or(stream_state.durable_state.as_str())
            ),
        });
    }
    let limit = query
        .limit
        .unwrap_or(policy.max_page_size)
        .clamp(1, policy.max_page_size);
    let cursor = query
        .cursor
        .as_deref()
        .map(PresenceHistoryCursor::decode)
        .transpose()?;
    let bounds = PresenceHistoryQueryBounds {
        start_serial: None,
        end_serial: None,
        start_time_ms: query.start,
        end_time_ms: query.end,
    };
    let page = handler
        .presence_history_store()
        .read_filtered_page(
            PresenceHistoryReadRequest {
                app_id: resolved.app.id.clone(),
                channel: channel.base().to_string(),
                direction,
                limit,
                cursor,
                bounds,
            },
            PresenceHistoryFilter {
                user_id: query.client_id.clone(),
                connection_id: query.connection_id.clone(),
            },
        )
        .await?;
    if page.degraded {
        return Err(AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message: "presence history store is degraded".to_string(),
        });
    }
    let next_cursor = page
        .next_cursor
        .as_ref()
        .map(PresenceHistoryCursor::encode)
        .transpose()?;
    let mut messages = Vec::with_capacity(page.items.len());
    for item in page.items {
        #[derive(Deserialize)]
        struct StoredPresenceData {
            user_info: Option<Value>,
        }
        let payload: StoredPresenceData = sonic_rs::from_slice(item.payload_bytes.as_ref())
            .map_err(|error| AppError::InternalError(error.to_string()))?;
        let stored = payload
            .user_info
            .as_ref()
            .and_then(decode_stored_presence_data);
        let native = payload
            .user_info
            .as_ref()
            .and_then(|value| sonic_rs::from_value::<PresenceRecord>(value).ok());
        let message = AblyPresenceMessage {
            id: native
                .as_ref()
                .map(|native| native.id.clone())
                .or_else(|| stored.as_ref().and_then(|stored| stored.id.clone()))
                .or_else(|| Some(format!("{}:{}:0", item.stream_id, item.serial))),
            action: Some(match item.event {
                PresenceHistoryEventKind::MemberAdded => 2,
                PresenceHistoryEventKind::MemberUpdated => 4,
                PresenceHistoryEventKind::MemberRemoved => 3,
            }),
            client_id: Some(item.user_id.clone()),
            connection_id: item.connection_id.clone(),
            data: if let Some(native) = native.as_ref() {
                native.data.clone()
            } else if let Some(stored) = stored.as_ref() {
                stored.data.clone()
            } else {
                payload.user_info.clone()
            },
            encoding: native
                .as_ref()
                .and_then(|native| native.encoding.clone())
                .or_else(|| stored.as_ref().and_then(|stored| stored.encoding.clone())),
            timestamp: Some(
                native
                    .as_ref()
                    .map_or(item.published_at_ms, |native| native.timestamp_ms),
            ),
            extras: native
                .as_ref()
                .and_then(|native| native.extras.clone())
                .or_else(|| stored.as_ref().and_then(|stored| stored.extras.clone())),
        };
        messages.push(message);
    }
    let links = ably_presence_link_header(&query, "presence/history", limit, next_cursor);
    let mut response = encode_ably_rest_response(StatusCode::OK, format, &messages)?;
    response.headers_mut().insert(
        header::LINK,
        links.parse().map_err(|error| {
            AppError::InternalError(format!("invalid presence history Link header: {error}"))
        })?,
    );
    Ok(response)
}

pub async fn ably_batch_publish(
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    match ably_batch_publish_inner(Arc::clone(&runtime.hub), query, headers, handler, body).await {
        Ok(response) => response,
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub async fn ably_batch_presence(
    Query(query): Query<AblyBatchPresenceQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let result = async {
        let resolved = resolve_ably_auth(
            &runtime.hub,
            &handler,
            &headers,
            query.key.as_deref(),
            query.access_token.as_deref(),
            query.client_id.as_deref(),
        )
        .await
        .map_err(ably_auth_app_error)?;
        let requested = query
            .channels
            .split(',')
            .map(str::to_string)
            .collect::<Vec<_>>();
        if requested.is_empty() || requested.len() > ABLY_MAX_BATCH_CHANNELS {
            return Err(AppError::InvalidInput(format!(
                "Ably batch presence requires 1..={ABLY_MAX_BATCH_CHANNELS} channels"
            )));
        }
        let mut work = Vec::with_capacity(requested.len());
        for requested_channel in requested {
            let channel = match parse_ably_channel_name(requested_channel.clone()) {
                Ok(channel) => channel,
                Err(error) => {
                    work.push(AblyBatchPresenceWork::Ready(
                        AblyBatchPresenceChannelResponse::Failure {
                            channel: requested_channel,
                            error: error_info(StatusCode::BAD_REQUEST, 40010, error.to_string()),
                        },
                    ));
                    continue;
                }
            };
            if let Err(error) = ensure_ably_channel_capability(
                resolved.capabilities.as_ref(),
                &channel,
                AblyCapabilityCheck::Presence,
            ) {
                work.push(AblyBatchPresenceWork::Ready(
                    AblyBatchPresenceChannelResponse::Failure {
                        channel: channel.requested().to_string(),
                        error: error_info(StatusCode::UNAUTHORIZED, error.code, error.message),
                    },
                ));
                continue;
            }
            work.push(AblyBatchPresenceWork::Snapshot(channel));
        }
        let app_id: Arc<str> = Arc::from(resolved.app.id.as_str());
        let service = PresenceService::new(Arc::clone(&handler));
        let responses = run_bounded_ordered(work, ABLY_BATCH_CONCURRENCY, move |work| {
            let app_id = Arc::clone(&app_id);
            let service = service.clone();
            async move {
                match work {
                    AblyBatchPresenceWork::Ready(response) => response,
                    AblyBatchPresenceWork::Snapshot(channel) => {
                        match service.snapshot(&app_id, channel.base()).await {
                            Ok(records) => AblyBatchPresenceChannelResponse::Success {
                                channel: channel.requested().to_string(),
                                presence: records
                                    .into_iter()
                                    .map(|record| ably_presence_from_record(record, 1))
                                    .collect(),
                            },
                            Err(error) => AblyBatchPresenceChannelResponse::Failure {
                                channel: channel.requested().to_string(),
                                error: ably_error_info_from_app_error(AppError::from(error)),
                            },
                        }
                    }
                }
            }
        })
        .await?;
        let success_count = responses
            .iter()
            .filter(|result| matches!(result, AblyBatchPresenceChannelResponse::Success { .. }))
            .count();
        encode_ably_rest_response(
            StatusCode::OK,
            response_format,
            &AblyBatchResult {
                success_count,
                failure_count: responses.len().saturating_sub(success_count),
                results: responses,
            },
        )
    }
    .await;
    result.unwrap_or_else(|error| ably_app_error_response_format(error, response_format))
}

fn validate_ably_batch_publish_requests(
    handler: &ConnectionHandler,
    requests: &[AblyBatchPublishRequest],
) -> Result<(), AppError> {
    if requests.is_empty() || requests.len() > ABLY_MAX_BATCH_SPECS {
        return Err(AppError::InvalidInput(format!(
            "Ably batch requires 1..={ABLY_MAX_BATCH_SPECS} specs"
        )));
    }

    let native_channel_limit =
        usize::try_from(handler.server_options().event_limits.max_channels_at_once)
            .unwrap_or(usize::MAX);
    let native_message_limit =
        usize::try_from(handler.server_options().event_limits.max_batch_size).unwrap_or(usize::MAX);
    let channel_limit = ABLY_MAX_BATCH_CHANNELS.min(native_channel_limit);
    let message_limit = ABLY_MAX_BATCH_MESSAGES.min(native_message_limit);
    let mut total_results = 0usize;
    let mut total_operations = 0usize;

    for request in requests {
        if request.channels.is_empty() || request.channels.len() > channel_limit {
            return Err(AppError::InvalidInput(format!(
                "Ably batch requires 1..={channel_limit} channels per spec"
            )));
        }
        let message_count = request.messages.len();
        if message_count == 0 || message_count > message_limit {
            return Err(AppError::InvalidInput(format!(
                "Ably batch requires 1..={message_limit} messages per spec"
            )));
        }
        total_results = total_results
            .checked_add(request.channels.len())
            .ok_or_else(|| {
                AppError::InvalidInput("Ably batch result count overflow".to_string())
            })?;
        let operations = request
            .channels
            .len()
            .checked_mul(message_count)
            .ok_or_else(|| {
                AppError::InvalidInput("Ably batch operation count overflow".to_string())
            })?;
        total_operations = total_operations.checked_add(operations).ok_or_else(|| {
            AppError::InvalidInput("Ably batch operation count overflow".to_string())
        })?;
    }

    if total_results > ABLY_MAX_BATCH_RESULTS {
        return Err(AppError::InvalidInput(format!(
            "Ably batch exceeds the {ABLY_MAX_BATCH_RESULTS}-result limit"
        )));
    }
    if total_operations > ABLY_MAX_BATCH_OPERATIONS {
        return Err(AppError::InvalidInput(format!(
            "Ably batch exceeds the {ABLY_MAX_BATCH_OPERATIONS}-publish-operation limit"
        )));
    }
    Ok(())
}

async fn publish_ably_batch_channel(
    handler: Arc<ConnectionHandler>,
    hub: Arc<AblyCompatHub>,
    resolved: Arc<ResolvedAblyAuth>,
    prepared_messages: Arc<[PreparedAblyBatchMessage]>,
    message_id: Arc<str>,
    requested_channel: String,
) -> AblyBatchPublishChannelResponse {
    let channel = match parse_ably_channel_name(requested_channel.clone()) {
        Ok(channel) => channel,
        Err(error) => {
            return AblyBatchPublishChannelResponse::Failure {
                channel: requested_channel,
                error: error_info(StatusCode::BAD_REQUEST, 40010, error.to_string()),
            };
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel,
        AblyCapabilityCheck::Publish,
    ) {
        return AblyBatchPublishChannelResponse::Failure {
            channel: channel.requested().to_string(),
            error: error_info(StatusCode::UNAUTHORIZED, error.code, error.message),
        };
    }

    let mut serials = Vec::with_capacity(prepared_messages.len());
    for prepared in prepared_messages.iter() {
        match publish_ably_message(
            AblyMessagePublishContext {
                handler: &handler,
                hub: &hub,
                app: &resolved.app,
                channel: channel.base(),
                connection_id: prepared.connection_id.as_deref(),
                client_id: prepared.client_id.as_deref(),
                capabilities: resolved.capabilities.as_ref(),
                privileged_server: resolved.capabilities.is_none(),
            },
            prepared.message.clone(),
        )
        .await
        {
            Ok(serial) => serials.push(serial),
            Err(error) => {
                return AblyBatchPublishChannelResponse::Failure {
                    channel: channel.requested().to_string(),
                    error: ably_error_info_from_app_error(error),
                };
            }
        }
    }

    let message_count = u64::try_from(prepared_messages.len()).unwrap_or(u64::MAX);
    let message_bytes = sonic_rs::to_vec(
        &prepared_messages
            .iter()
            .map(|item| &item.message)
            .collect::<Vec<_>>(),
    )
    .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
    .unwrap_or_default();
    let stats_result = StatsObservation::messages(
        &resolved.app.id,
        now_ms(),
        "inbound",
        "rest",
        message_count,
        message_bytes,
    )
    .map_err(stats_app_error);
    if let Err(error) = match stats_result {
        Ok(observation) => hub.stats.record(observation).await.map_err(stats_app_error),
        Err(error) => Err(error),
    } {
        return AblyBatchPublishChannelResponse::Failure {
            channel: channel.requested().to_string(),
            error: ably_error_info_from_app_error(error),
        };
    }

    AblyBatchPublishChannelResponse::Success {
        channel: channel.requested().to_string(),
        message_id: message_id.to_string(),
        serials,
    }
}

async fn ably_batch_publish_inner(
    hub: Arc<AblyCompatHub>,
    query: AblyRestQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
    body: Bytes,
) -> Result<Response, AppError> {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    if body.len() > ABLY_MAX_BATCH_BYTES {
        return Err(AppError::Protocol {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: 40009,
            message: format!("Ably batch body exceeds the {ABLY_MAX_BATCH_BYTES}-byte limit"),
        });
    }
    let resolved = resolve_ably_auth(
        &hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    .map_err(ably_auth_app_error)?;
    let request_body: AblyBatchPublishBody = decode_value(body.as_ref(), request_format)
        .map_err(|error| AppError::InvalidInput(format!("Invalid Ably batch body: {error}")))?;
    let (requests, single_request) = match request_body {
        AblyBatchPublishBody::One(request) => (vec![request], true),
        AblyBatchPublishBody::Many(requests) => (requests, false),
    };
    validate_ably_batch_publish_requests(&handler, &requests)?;
    let resolved = Arc::new(resolved);
    let mut batch_responses = Vec::with_capacity(requests.len());
    for request in requests {
        let messages = request.messages.into_messages();
        let prepared_messages: Arc<[PreparedAblyBatchMessage]> = messages
            .into_iter()
            .map(|message| {
                validate_ably_publish_message(&message, true)?;
                let (connection_id, effective_client_id) = rest_publish_identity(
                    &hub,
                    &resolved.app.id,
                    resolved.client_id.as_deref(),
                    &message,
                )?;
                Ok(PreparedAblyBatchMessage {
                    message,
                    connection_id,
                    client_id: effective_client_id,
                })
            })
            .collect::<Result<Vec<_>, AppError>>()?
            .into();
        let message_id: Arc<str> = Arc::from(Uuid::new_v4().to_string());
        let responses = run_bounded_ordered(request.channels, ABLY_BATCH_CONCURRENCY, {
            let handler = Arc::clone(&handler);
            let hub = Arc::clone(&hub);
            let message_id = Arc::clone(&message_id);
            let prepared_messages = Arc::clone(&prepared_messages);
            let resolved = Arc::clone(&resolved);
            move |requested_channel| {
                publish_ably_batch_channel(
                    Arc::clone(&handler),
                    Arc::clone(&hub),
                    Arc::clone(&resolved),
                    Arc::clone(&prepared_messages),
                    Arc::clone(&message_id),
                    requested_channel,
                )
            }
        })
        .await?;
        let success_count = responses
            .iter()
            .filter(|result| matches!(result, AblyBatchPublishChannelResponse::Success { .. }))
            .count();
        batch_responses.push(AblyBatchResult {
            success_count,
            failure_count: responses.len().saturating_sub(success_count),
            results: responses,
        });
    }

    if single_request {
        let response = batch_responses
            .pop()
            .ok_or_else(|| AppError::InternalError("missing Ably batch response".to_string()))?;
        encode_ably_legacy_batch_publish_response(response_format, response.results)
    } else {
        encode_ably_rest_response(StatusCode::OK, response_format, &batch_responses)
    }
}

pub async fn ably_channel_publish(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let response_format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    match ably_channel_publish_inner(&runtime.hub, channel_name, query, headers, handler, body)
        .await
    {
        Ok(response) => response,
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

async fn ably_channel_publish_inner(
    hub: &AblyCompatHub,
    channel_name: String,
    query: AblyRestQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
    body: Bytes,
) -> Result<Response, AppError> {
    let request_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let resolved = resolve_ably_auth(
        hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    .map_err(ably_auth_app_error)?;
    let channel_name = parse_ably_channel_name(channel_name)?;
    ensure_ably_channel_capability_app_error(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::Publish,
    )?;

    let messages = decode_ably_publish_payload(body.as_ref(), request_format)?;
    if messages.is_empty() {
        return Err(AppError::InvalidInput(
            "Ably REST publish requires at least one message".to_string(),
        ));
    }
    let message_count = u64::try_from(messages.len()).unwrap_or(u64::MAX);

    let prepared_messages = messages
        .into_iter()
        .map(|message| {
            validate_ably_publish_message(&message, true)?;
            let (connection_id, effective_client_id) = rest_publish_identity(
                hub,
                &resolved.app.id,
                resolved.client_id.as_deref(),
                &message,
            )?;
            Ok((message, connection_id, effective_client_id))
        })
        .collect::<Result<Vec<_>, AppError>>()?;

    let mut serials = Vec::with_capacity(prepared_messages.len());
    for (index, (message, connection_id, effective_client_id)) in
        prepared_messages.into_iter().enumerate()
    {
        let serial = publish_ably_message(
            AblyMessagePublishContext {
                handler: &handler,
                hub,
                app: &resolved.app,
                channel: channel_name.base(),
                connection_id: connection_id.as_deref(),
                client_id: effective_client_id.as_deref(),
                capabilities: resolved.capabilities.as_ref(),
                privileged_server: resolved.capabilities.is_none(),
            },
            message,
        )
        .await
        .map_err(|error| {
            AppError::InvalidInput(format!("Failed to publish message {index}: {error}"))
        })?;
        serials.push(Some(serial));
    }

    let observation = StatsObservation::messages(
        &resolved.app.id,
        now_ms(),
        "inbound",
        "rest",
        message_count,
        request_bytes,
    )
    .map_err(stats_app_error)?;
    hub.stats
        .record(observation)
        .await
        .map_err(stats_app_error)?;

    encode_ably_rest_response(
        StatusCode::CREATED,
        response_format,
        &AblyPublishResponse { serials },
    )
}

async fn ably_channel_history_inner(
    hub: &AblyCompatHub,
    channel_name: String,
    query: AblyHistoryQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
) -> Result<Response, AppError> {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = resolve_ably_auth(
        hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    .map_err(ably_auth_app_error)?;
    let channel_name = parse_ably_channel_name(channel_name)?;
    ensure_ably_channel_capability_app_error(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    )?;
    let history_policy = resolved
        .app
        .resolved_history(channel_name.base(), &handler.server_options().history);
    if !history_policy.enabled {
        return encode_ably_rest_response(
            StatusCode::OK,
            response_format,
            &Vec::<AblyMessage>::new(),
        );
    }
    let limit = query
        .limit
        .unwrap_or(history_policy.max_page_size)
        .min(history_policy.max_page_size)
        .max(1);
    let direction = parse_ably_history_direction(query.direction.as_deref())?;
    let cursor = match query.cursor.as_deref() {
        Some(cursor) => Some(HistoryCursor::decode(cursor)?),
        None => None,
    };
    let attach_position = query
        .from_serial
        .as_deref()
        .and_then(|serial| parse_ably_channel_serial(serial).ok())
        .or_else(|| {
            query
                .until_attach
                .unwrap_or(false)
                .then(|| hub.until_attach_position(&resolved.app.id, &channel_name))
                .flatten()
        });
    let bounds = HistoryQueryBounds {
        start_serial: None,
        end_serial: attach_position.as_ref().map(|position| position.serial),
        start_time_ms: query.start,
        end_time_ms: query.end,
    };
    let page = handler
        .history_store()
        .read_page(HistoryReadRequest {
            app_id: resolved.app.id.clone(),
            channel: channel_name.base().to_string(),
            direction,
            limit,
            cursor,
            bounds,
        })
        .await?;

    let link_header =
        ably_history_link_header(&query, direction, limit, page.next_cursor.as_ref())?;
    let versioned_messages_enabled = handler.server_options().versioned_messages.enabled;
    let decoded_items = page
        .items
        .into_iter()
        .map(|item| {
            let stored = decode_stored_message_payload(item.payload_bytes.as_ref())
                .map_err(AppError::InternalError)?;
            let message_serial = versioned_messages_enabled
                .then(|| extract_runtime_message_serial(&stored.message))
                .flatten()
                .map(parse_message_serial)
                .transpose()?;
            Ok((item, stored, message_serial))
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    let message_serials = decoded_items
        .iter()
        .filter_map(|(_, _, serial)| serial.clone())
        .collect::<Vec<_>>();
    let latest_by_serial = if versioned_messages_enabled {
        handler
            .version_store()
            .get_latest_batch(&resolved.app.id, channel_name.base(), &message_serials)
            .await?
    } else {
        BTreeMap::new()
    };

    let mut items = Vec::with_capacity(decoded_items.len());
    for (item, stored, message_serial) in decoded_items {
        let raw_message = stored.message;
        let mut envelope = stored.envelope;
        let message = if versioned_messages_enabled {
            if let Some(message_serial) = message_serial {
                match latest_by_serial.get(&message_serial) {
                    Some(latest) => {
                        envelope = latest.envelope.clone().or(envelope);
                        handler
                            .build_runtime_message_from_record(latest, Some(item.stream_id.clone()))
                    }
                    None => raw_message,
                }
            } else {
                raw_message
            }
        } else {
            raw_message
        };
        items.push(match envelope.as_ref() {
            Some(envelope) => {
                envelope_to_ably_message(envelope, &message, AblyMessageProjection::Aggregate)
                    .map_err(AppError::InvalidInput)?
            }
            None => pusher_to_ably_message(&message, AblyMessageProjection::Aggregate)
                .map_err(AppError::InvalidInput)?,
        });
    }
    let mut response = encode_ably_rest_response(StatusCode::OK, response_format, &items)?;
    response.headers_mut().insert(
        header::LINK,
        link_header.parse().map_err(|error| {
            AppError::InternalError(format!("invalid history Link header: {error}"))
        })?,
    );
    Ok(response)
}

pub async fn ably_channel_status(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    let channel_name = match AblyChannelName::parse(channel_name) {
        Ok(channel_name) => channel_name,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40010,
                error.to_string(),
                response_format,
            );
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::AnyChannelAccess,
    ) {
        return ably_error_response_format(
            error.status,
            error.code,
            error.message,
            response_format,
        );
    }
    let occupancy = handler
        .connection_manager()
        .get_channel_socket_count(&resolved.app.id, channel_name.base())
        .await;
    encode_ably_rest_response(
        StatusCode::OK,
        response_format,
        &json!({
            "channelId": channel_name.requested(),
            "status": {
                "isActive": occupancy > 0,
                "occupancy": {
                    "metrics": {
                        "connections": occupancy,
                        "publishers": 0,
                        "subscribers": occupancy,
                        "presenceConnections": 0,
                        "presenceMembers": 0,
                        "presenceSubscribers": 0,
                    }
                }
            }
        }),
    )
    .unwrap_or_else(ably_app_error_response)
}

pub async fn ably_channel_message(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    let channel_name = match AblyChannelName::parse(channel_name) {
        Ok(channel_name) => channel_name,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40010,
                error.to_string(),
                response_format,
            );
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    ) {
        return ably_error_response_format(
            error.status,
            error.code,
            error.message,
            response_format,
        );
    }
    match ably_channel_message_inner(handler, resolved.app, channel_name, message_serial).await {
        Ok(message) => encode_ably_rest_response(StatusCode::OK, response_format, &message)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

async fn ably_channel_message_inner(
    handler: Arc<ConnectionHandler>,
    app: App,
    channel_name: AblyChannelName,
    message_serial: String,
) -> Result<AblyMessage, AppError> {
    let message_serial_value = parse_message_serial(&message_serial)?;
    let item = MessageService::new(Arc::clone(&handler))
        .get_message(&app.id, channel_name.base(), &message_serial_value)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                message_serial,
                channel_name.requested()
            ))
        })?;
    let runtime_message = handler.build_runtime_message_from_record(&item, None);
    match item.envelope.as_ref() {
        Some(envelope) => {
            envelope_to_ably_message(envelope, &runtime_message, AblyMessageProjection::Aggregate)
                .map_err(AppError::InvalidInput)
        }
        None => pusher_to_ably_message(&runtime_message, AblyMessageProjection::Aggregate)
            .map_err(AppError::InvalidInput),
    }
}

pub async fn ably_channel_message_versions(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    let channel_name = match AblyChannelName::parse(channel_name) {
        Ok(channel_name) => channel_name,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40010,
                error.to_string(),
                response_format,
            );
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    ) {
        return ably_error_response_format(
            error.status,
            error.code,
            error.message,
            response_format,
        );
    }
    match ably_channel_message_versions_inner(handler, resolved.app, channel_name, message_serial)
        .await
    {
        Ok(messages) => encode_ably_rest_response(StatusCode::OK, response_format, &messages)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub async fn ably_publish_annotations(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let result = async {
        let resolved = resolve_ably_auth(
            &runtime.hub,
            &handler,
            &headers,
            query.key.as_deref(),
            query.access_token.as_deref(),
            query.client_id.as_deref(),
        )
        .await
        .map_err(ably_auth_app_error)?;
        let channel = parse_ably_channel_name(channel_name)?;
        let message_serial = MessageSerial::new(message_serial)?;
        let annotations: Vec<AblyAnnotation> = decode_value(body.as_ref(), request_format)
            .map_err(|error| AppError::InvalidInput(format!("Invalid annotation body: {error}")))?;
        if annotations.is_empty() {
            return Err(AppError::InvalidInput(
                "annotation request must contain at least one annotation".to_string(),
            ));
        }
        for annotation in annotations {
            let command = parse_ably_annotation_command(
                annotation,
                Some(&message_serial),
                resolved.client_id.as_deref(),
            )?;
            apply_ably_annotation_command(
                &handler,
                &resolved.app,
                &channel,
                resolved.capabilities.as_ref(),
                resolved.client_id.as_deref(),
                command,
            )
            .await?;
        }
        encode_ably_rest_response(StatusCode::CREATED, response_format, &json!({}))
    }
    .await;
    result.unwrap_or_else(|error| ably_app_error_response_format(error, response_format))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AblyAnnotationCursor {
    version: u8,
    app_id: String,
    channel: String,
    message_serial: String,
    annotation_serial: String,
}

fn encode_ably_annotation_cursor(
    app_id: &str,
    channel: &str,
    message_serial: &MessageSerial,
    annotation_serial: &AnnotationSerial,
) -> Result<String, AppError> {
    let bytes = sonic_rs::to_vec(&AblyAnnotationCursor {
        version: 1,
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        message_serial: message_serial.as_str().to_string(),
        annotation_serial: annotation_serial.as_str().to_string(),
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_ably_annotation_cursor(
    raw: &str,
    app_id: &str,
    channel: &str,
    message_serial: &MessageSerial,
) -> Result<AnnotationSerial, AppError> {
    let invalid = || AppError::InvalidInput("invalid annotation cursor".to_string());
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| invalid())?;
    let cursor: AblyAnnotationCursor = sonic_rs::from_slice(&bytes).map_err(|_| invalid())?;
    if cursor.version != 1
        || cursor.app_id != app_id
        || cursor.channel != channel
        || cursor.message_serial != message_serial.as_str()
    {
        return Err(invalid());
    }
    AnnotationSerial::new(cursor.annotation_serial).map_err(|_| invalid())
}

pub async fn ably_channel_annotations(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let result = async {
        let resolved = resolve_ably_auth(
            &runtime.hub,
            &handler,
            &headers,
            query.key.as_deref(),
            query.access_token.as_deref(),
            query.client_id.as_deref(),
        )
        .await
        .map_err(ably_auth_app_error)?;
        let channel = parse_ably_channel_name(channel_name)?;
        ensure_ably_channel_capability_app_error(
            resolved.capabilities.as_ref(),
            &channel,
            AblyCapabilityCheck::AnnotationSubscribe,
        )?;
        let message_serial = MessageSerial::new(message_serial)?;
        require_ably_annotations_enabled(&handler, &resolved.app, channel.base())?;
        let limit = query.limit.unwrap_or(100).clamp(1, 1_000);
        let after = query
            .cursor
            .as_deref()
            .map(|cursor| {
                decode_ably_annotation_cursor(
                    cursor,
                    &resolved.app.id,
                    channel.base(),
                    &message_serial,
                )
            })
            .transpose()?;
        let raw = handler
            .annotation_store()
            .replay_raw(RawAnnotationReplayRequest {
                app_id: resolved.app.id.clone(),
                channel_id: channel.base().to_string(),
                message_serial: Some(message_serial.clone()),
                after_annotation_serial: after,
                limit: limit.saturating_add(1),
            })
            .await?;
        let mut matching = raw
            .into_iter()
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_next = matching.len() > limit;
        if has_next {
            matching.pop();
        }
        let next_cursor = has_next
            .then(|| matching.last())
            .flatten()
            .map(|event| {
                encode_ably_annotation_cursor(
                    &resolved.app.id,
                    channel.base(),
                    &message_serial,
                    &event.annotation.serial,
                )
            })
            .transpose()?;
        let items = matching
            .into_iter()
            .map(|event| AblyAnnotation {
                action: Some(match event.annotation.action {
                    sockudo_core::annotations::AnnotationAction::Create => 0,
                    sockudo_core::annotations::AnnotationAction::Delete => 1,
                }),
                id: Some(event.annotation.id.as_str().to_string()),
                serial: Some(event.annotation.serial.as_str().to_string()),
                message_serial: Some(event.annotation.message_serial.as_str().to_string()),
                annotation_type: Some(event.annotation.annotation_type.as_str().to_string()),
                name: event.annotation.name,
                client_id: event.annotation.client_id,
                count: event.annotation.count,
                data: event.annotation.data,
                encoding: event.annotation.encoding,
                timestamp: Some(event.annotation.timestamp),
            })
            .collect::<Vec<_>>();
        let mut response = encode_ably_rest_response(StatusCode::OK, response_format, &items)?;
        let format_query = query
            .format
            .as_deref()
            .map(|format| format!("&format={}", urlencoding::encode(format)))
            .unwrap_or_default();
        let mut links = vec![format!(
            "<./annotations?limit={limit}{format_query}>; rel=\"first\""
        )];
        if let Some(cursor) = next_cursor {
            links.push(format!(
                "<./annotations?limit={limit}&cursor={}{format_query}>; rel=\"next\"",
                urlencoding::encode(&cursor)
            ));
        }
        let link = HeaderValue::from_str(&links.join(", ")).map_err(|error| {
            AppError::InternalError(format!("invalid annotations Link header: {error}"))
        })?;
        response.headers_mut().insert(header::LINK, link);
        Ok(response)
    }
    .await;
    result.unwrap_or_else(|error| ably_app_error_response_format(error, response_format))
}

pub async fn ably_channel_message_mutation(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    let channel_name = match AblyChannelName::parse(channel_name) {
        Ok(channel_name) => channel_name,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40010,
                error.to_string(),
                response_format,
            );
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::Publish,
    ) {
        return ably_error_response_format(
            error.status,
            error.code,
            error.message,
            response_format,
        );
    }
    let mut messages = match decode_ably_publish_payload(body.as_ref(), request_format) {
        Ok(messages) => messages,
        Err(error) => return ably_app_error_response_format(error, response_format),
    };
    let Some(mut message) = messages.pop() else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "mutation message is required",
            response_format,
        );
    };
    if let Err(error) = validate_ably_publish_message(&message, true) {
        return ably_app_error_response_format(error, response_format);
    }
    if let Err(error) = reconcile_mutation_message_serial(&mut message, &message_serial) {
        return ably_app_error_response_format(error, response_format);
    }
    let (connection_id, effective_client_id) = match rest_publish_identity(
        &runtime.hub,
        &resolved.app.id,
        resolved.client_id.as_deref(),
        &message,
    ) {
        Ok(identity) => identity,
        Err(error) => return ably_app_error_response_format(error, response_format),
    };
    match publish_ably_message(
        AblyMessagePublishContext {
            handler: &handler,
            hub: &runtime.hub,
            app: &resolved.app,
            channel: channel_name.base(),
            connection_id: connection_id.as_deref(),
            client_id: effective_client_id.as_deref(),
            capabilities: resolved.capabilities.as_ref(),
            privileged_server: resolved.capabilities.is_none(),
        },
        message,
    )
    .await
    {
        Ok(version_serial) => encode_ably_rest_response(
            StatusCode::OK,
            response_format,
            &json!({"versionSerial": version_serial}),
        )
        .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

fn reconcile_mutation_message_serial(
    message: &mut AblyMessage,
    path_serial: &str,
) -> Result<(), AppError> {
    let path_serial = parse_message_serial(path_serial)?;
    match message.serial.as_deref() {
        Some(body_serial) if body_serial != path_serial.as_str() => Err(AppError::InvalidInput(
            "mutation body serial must match the request path".to_string(),
        )),
        Some(_) => Ok(()),
        None => {
            message.serial = Some(path_serial.as_str().to_string());
            Ok(())
        }
    }
}

async fn ably_channel_message_versions_inner(
    handler: Arc<ConnectionHandler>,
    app: App,
    channel_name: AblyChannelName,
    message_serial: String,
) -> Result<Vec<AblyMessage>, AppError> {
    let message_serial_value = parse_message_serial(&message_serial)?;
    let versions = MessageService::new(Arc::clone(&handler))
        .get_message_versions(sockudo_core::version_store::VersionStoreReadRequest {
            app_id: app.id.clone(),
            channel: channel_name.base().to_string(),
            message_serial: message_serial_value,
            direction: sockudo_core::version_store::VersionStoreDirection::NewestFirst,
            limit: handler
                .server_options()
                .versioned_messages
                .max_page_size
                .max(1),
            cursor: None,
        })
        .await?;
    versions
        .items
        .iter()
        .map(|record| {
            let runtime_message = handler.build_runtime_message_from_record(record, None);
            match record.envelope.as_ref() {
                Some(envelope) => envelope_to_ably_message(
                    envelope,
                    &runtime_message,
                    AblyMessageProjection::Mutation,
                )
                .map_err(AppError::InvalidInput),
                None => pusher_to_ably_message(&runtime_message, AblyMessageProjection::Mutation)
                    .map_err(AppError::InvalidInput),
            }
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AblyRevocationRequest {
    targets: Vec<String>,
    issued_before: Option<i64>,
    #[serde(default)]
    allow_reauth_margin: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AblyRevocationResult {
    success_count: usize,
    failure_count: usize,
    results: Vec<AblyRevocationTargetResult>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AblyRevocationTargetResult {
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    issued_before: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    applies_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<AblyErrorInfo>,
}

pub async fn ably_revoke_tokens(
    Path(key_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    if query.access_token.is_some() || bearer_token(&headers).is_some() {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40162,
            "Cannot revoke tokens when using token auth",
            response_format,
        );
    }
    let Some(credential) = query.key.clone().or_else(|| basic_credential(&headers)) else {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40101,
            "Invalid credentials",
            response_format,
        );
    };
    let (credential_key, credential_secret) = parse_ably_key(&credential);
    let key = match resolve_ably_key(&runtime.hub, &handler, credential_key).await {
        Ok(key)
            if credential_key == key_name
                && credential_secret.is_some_and(|secret| secure_compare(secret, &key.secret)) =>
        {
            key
        }
        _ => {
            return ably_error_response_format(
                StatusCode::UNAUTHORIZED,
                40101,
                "Invalid credentials",
                response_format,
            );
        }
    };
    if !key.revocable_tokens {
        return ably_error_response_format(
            StatusCode::FORBIDDEN,
            40160,
            "Key is not enabled for revocable tokens",
            response_format,
        );
    }
    if body.len() > ABLY_MAX_BATCH_BYTES {
        return ably_error_response_format(
            StatusCode::PAYLOAD_TOO_LARGE,
            40009,
            format!("Token revocation body exceeds the {ABLY_MAX_BATCH_BYTES}-byte limit"),
            response_format,
        );
    }
    let request: AblyRevocationRequest = match decode_value(body.as_ref(), request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                format!("Invalid token revocation request: {error}"),
                response_format,
            );
        }
    };
    if request.targets.is_empty() || request.targets.len() > 100 {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "Token revocation requires 1..=100 targets",
            response_format,
        );
    }
    let now = now_ms();
    let issued_before = request
        .issued_before
        .unwrap_or_else(|| now.saturating_add(1));
    if issued_before > now.saturating_add(1_000)
        || issued_before < now.saturating_sub(60 * 60 * 1000)
    {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "issuedBefore must be within the previous hour and not in the future",
            response_format,
        );
    }
    let applies_at = if request.allow_reauth_margin {
        now.saturating_add(30_001)
    } else {
        now
    };
    let app_id: Arc<str> = Arc::from(key.app.id.as_str());
    let results = match run_bounded_ordered(request.targets, ABLY_BATCH_CONCURRENCY, {
        let app_id = Arc::clone(&app_id);
        let hub = Arc::clone(&runtime.hub);
        move |target| {
            let app_id = Arc::clone(&app_id);
            let hub = Arc::clone(&hub);
            async move {
                let parsed = target.split_once(':').filter(|(target_type, value)| {
                    matches!(*target_type, "clientId" | "revocationKey" | "channel")
                        && !value.is_empty()
                        && value.len() <= 256
                });
                let Some((target_type, target_value)) = parsed else {
                    return AblyRevocationTargetResult {
                        target,
                        issued_before: None,
                        applies_at: None,
                        error: Some(error_info(
                            StatusCode::BAD_REQUEST,
                            40000,
                            "Invalid token revocation target",
                        )),
                    };
                };
                let record = AblyRevocationRecord {
                    target_type: target_type.to_string(),
                    target_value: target_value.to_string(),
                    issued_before,
                    applies_at,
                };
                let error = hub
                    .store_revocation(&app_id, target_type, target_value, record)
                    .await
                    .err()
                    .map(|error| error_info(error.status, error.code, error.message));
                AblyRevocationTargetResult {
                    target,
                    issued_before: error.is_none().then_some(issued_before),
                    applies_at: error.is_none().then_some(applies_at),
                    error,
                }
            }
        }
    })
    .await
    {
        Ok(results) => results,
        Err(error) => return ably_app_error_response_format(error, response_format),
    };
    let success_count = results
        .iter()
        .filter(|result| result.error.is_none())
        .count();
    if success_count > 0 {
        runtime
            .hub
            .notify_revocation_change(&key.app.id, request.allow_reauth_margin);
    }
    let result = AblyRevocationResult {
        success_count,
        failure_count: results.len().saturating_sub(success_count),
        results,
    };
    encode_ably_rest_response(StatusCode::OK, response_format, &result)
        .unwrap_or_else(ably_app_error_response)
}

pub async fn ably_request_token(
    Path(key_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let request: AblyTokenRequest = match decode_value(body.as_ref(), request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                format!("Invalid Ably token request: {error}"),
                response_format,
            );
        }
    };
    let body_key_name = request.key_name.as_deref().unwrap_or(&key_name);
    if body_key_name != key_name {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40101,
            "Invalid credentials",
            response_format,
        );
    }
    let resolved = match resolve_ably_key(&runtime.hub, &handler, body_key_name).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    if request.client_id.as_deref() == Some("") {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40012,
            "clientId can’t be an empty string",
            response_format,
        );
    }

    let ttl_ms = match parse_token_request_integer(request.ttl.as_ref(), "ttl") {
        Ok(Some(ttl)) if ttl > 0 && ttl <= runtime.hub.config.max_token_ttl_ms => ttl,
        Ok(None) => DEFAULT_TOKEN_TTL_MS,
        Ok(Some(_)) | Err(_) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                format!(
                    "TokenRequest ttl must be a positive integer no greater than {}",
                    runtime.hub.config.max_token_ttl_ms
                ),
                response_format,
            );
        }
    };
    let timestamp = match parse_token_request_integer(request.timestamp.as_ref(), "timestamp") {
        Ok(Some(timestamp)) => timestamp,
        _ => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                "TokenRequest timestamp must be an integer",
                response_format,
            );
        }
    };
    if now_ms().abs_diff(timestamp)
        > u64::try_from(runtime.hub.config.token_request_timestamp_skew_ms).unwrap_or(0)
    {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40104,
            "Timestamp not current",
            response_format,
        );
    }
    let Some(nonce) = request
        .nonce
        .as_deref()
        .filter(|nonce| !nonce.is_empty() && nonce.len() <= 256)
    else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "TokenRequest nonce is required and must not exceed 256 bytes",
            response_format,
        );
    };
    let Some(mac) = request
        .mac
        .as_deref()
        .filter(|mac| !mac.is_empty() && mac.len() <= 512)
    else {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40101,
            "Invalid credentials",
            response_format,
        );
    };
    let requested_capability = match request.capability.as_ref() {
        Some(serde_json::Value::String(capability)) => Some(capability.as_str()),
        Some(_) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                "TokenRequest capability must be a JSON object string",
                response_format,
            );
        }
        None => None,
    };
    let signing_input = token_request_signing_input(
        body_key_name,
        request.ttl.as_ref().and_then(serde_json::Value::as_i64),
        requested_capability,
        request.client_id.as_deref(),
        timestamp,
        nonce,
    );
    if !verify_token_request_mac(&resolved.secret, &signing_input, mac) {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40101,
            "Invalid credentials",
            response_format,
        );
    }

    let (capability, capabilities) =
        match intersect_ably_capability(&resolved.capability, requested_capability) {
            Ok(parsed) => parsed,
            Err(CapabilityIntersectionError::Invalid(message)) => {
                return ably_error_response_format(
                    StatusCode::BAD_REQUEST,
                    40000,
                    message,
                    response_format,
                );
            }
            Err(CapabilityIntersectionError::Empty) => {
                return ably_error_response_format(
                    StatusCode::UNAUTHORIZED,
                    40160,
                    "Requested capability is not permitted by this key",
                    response_format,
                );
            }
        };
    match runtime.hub.claim_nonce(body_key_name, nonce).await {
        Ok(true) => {}
        Ok(false) => {
            return ably_error_response_format(
                StatusCode::UNAUTHORIZED,
                40105,
                "Nonce value replayed",
                response_format,
            );
        }
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    }
    let token = match runtime
        .hub
        .issue_token(
            &resolved,
            request.client_id,
            ttl_ms,
            capability,
            capabilities,
        )
        .await
    {
        Ok(token) => token,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    encode_ably_rest_response(StatusCode::OK, response_format, &token)
        .unwrap_or_else(ably_app_error_response)
}

impl AblyRecoveryFailure {
    fn channel(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

fn lock_channel_state(
    state: &Arc<Mutex<AblyChannelState>>,
) -> std::sync::MutexGuard<'_, AblyChannelState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn encode_ably_channel_serial(stream_id: &str, serial: u64) -> String {
    format!("{stream_id}:{serial}")
}

fn parse_ably_channel_serial(raw: &str) -> Result<AblyChannelPosition, AblyRecoveryFailure> {
    let Some((stream_id, serial)) = raw.rsplit_once(':') else {
        return Err(AblyRecoveryFailure::channel(
            90005,
            "unable to recover channel (no matching epoch)",
        ));
    };
    if stream_id.is_empty() {
        return Err(AblyRecoveryFailure::channel(
            90005,
            "unable to recover channel (no matching epoch)",
        ));
    }
    let serial = serial.parse::<u64>().map_err(|_| {
        AblyRecoveryFailure::channel(90005, "unable to recover channel (invalid channel serial)")
    })?;
    Ok(AblyChannelPosition {
        stream_id: stream_id.to_string(),
        serial,
    })
}

#[allow(clippy::too_many_arguments)]
fn send_ably_attached(
    sender: &AblySender,
    channel: &str,
    channel_serial: Option<String>,
    flags: Option<u64>,
    params: Option<HashMap<String, String>>,
    presence: Vec<AblyPresenceMessage>,
    failure: Option<AblyRecoveryFailure>,
    replay: Vec<AblyProtocolMessage>,
    filter: Option<&AblyMessageFilter>,
) -> Option<AblyDeltaState> {
    #[cfg(feature = "delta")]
    let mut delta_state = params.as_ref().and_then(ably_delta_state);
    let flags = if presence.is_empty() {
        flags
    } else {
        Some(flags.unwrap_or_default() | FLAG_HAS_PRESENCE)
    };
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ATTACHED,
            channel: Some(channel.to_string()),
            flags,
            params,
            channel_serial,
            error: failure.map(|failure| error_info(failure.status, failure.code, failure.message)),
            ..empty_protocol_message(ACTION_ATTACHED)
        },
    );
    if !presence.is_empty() {
        send_presence_sync(sender, channel, presence);
    }
    for mut message in replay {
        if !ably_filter_protocol_message(&mut message, filter) {
            continue;
        }
        if message.channel.is_some() {
            message.channel = Some(channel.to_string());
        }
        #[cfg(feature = "delta")]
        if let Some(current) = delta_state.as_ref()
            && message.action == ACTION_MESSAGE
        {
            let (projected, next) = project_ably_delta_message(message.clone(), current);
            message = projected;
            delta_state = Some(next);
        }
        if let Err(error) = sender.send_protocol(&message, OutboundPriority::Data) {
            debug!(error = %error, "Ably compatibility replay queue is unavailable");
            break;
        }
    }
    #[cfg(feature = "delta")]
    return delta_state;
    #[cfg(not(feature = "delta"))]
    None
}

fn send_presence_sync(sender: &AblySender, channel: &str, presence: Vec<AblyPresenceMessage>) {
    let chunk_count = presence.len().div_ceil(ABLY_PRESENCE_SYNC_CHUNK_SIZE);
    let mut members = presence.into_iter();
    for index in 0..chunk_count {
        let channel_serial = (chunk_count > 1).then(|| {
            if index + 1 == chunk_count {
                "presence:".to_string()
            } else {
                format!("presence:{}", index + 1)
            }
        });
        send_protocol(
            sender,
            AblyProtocolMessage {
                action: ACTION_SYNC,
                channel: Some(channel.to_string()),
                channel_serial,
                presence: Some(
                    members
                        .by_ref()
                        .take(ABLY_PRESENCE_SYNC_CHUNK_SIZE)
                        .collect(),
                ),
                ..empty_protocol_message(ACTION_SYNC)
            },
        );
    }
}

fn ably_filter_protocol_message(
    message: &mut AblyProtocolMessage,
    filter: Option<&AblyMessageFilter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let Some(messages) = message.messages.as_mut() else {
        return true;
    };
    messages.retain(|message| match filter.matches(message) {
        Ok(matches) => matches,
        Err(error) => {
            warn!(error = %error, "failed to evaluate Ably derived-channel filter");
            false
        }
    });
    !messages.is_empty()
}

fn ably_protocol_message_from_pusher(
    channel: &str,
    message: &PusherMessage,
    projection: AblyMessageProjection,
    channel_serial: Option<String>,
) -> Result<AblyProtocolMessage, AblyRecoveryFailure> {
    let ably_message = pusher_to_ably_message(message, projection).map_err(|error| {
        AblyRecoveryFailure::channel(
            90000,
            format!("unable to recover channel '{channel}': {error}"),
        )
    })?;
    Ok(AblyProtocolMessage {
        action: ACTION_MESSAGE,
        timestamp: None,
        channel: Some(channel.to_string()),
        channel_serial,
        messages: Some(vec![ably_message]),
        ..empty_protocol_message(ACTION_MESSAGE)
    })
}

fn ably_protocol_message_from_envelope(
    channel: &str,
    message: &PusherMessage,
    envelope: &MessageEnvelope,
    projection: AblyMessageProjection,
    channel_serial: Option<String>,
) -> Result<AblyProtocolMessage, AblyRecoveryFailure> {
    if message.event.as_deref() == Some(ANNOTATION_EVENT_NAME) {
        let event: AnnotationEventData = decode_native_message_data(message.data.as_ref())
            .map_err(|error| {
                AblyRecoveryFailure::channel(
                    90000,
                    format!("unable to recover annotation on channel '{channel}': {error}"),
                )
            })?;
        return Ok(AblyProtocolMessage {
            action: ACTION_ANNOTATION,
            timestamp: Some(event.timestamp),
            channel: Some(channel.to_string()),
            channel_serial,
            annotations: Some(vec![ably_annotation_from_native_event(event)]),
            ..empty_protocol_message(ACTION_ANNOTATION)
        });
    }
    if message.event.as_deref() == Some(MESSAGE_SUMMARY_EVENT_NAME) {
        let value: Value = decode_native_message_data(message.data.as_ref()).map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover annotation summary on channel '{channel}': {error}"),
            )
        })?;
        let serial = value.get("serial").and_then(Value::as_str).ok_or_else(|| {
            AblyRecoveryFailure::channel(
                90000,
                format!(
                    "unable to recover annotation summary on channel '{channel}': missing serial"
                ),
            )
        })?;
        return Ok(AblyProtocolMessage {
            action: ACTION_MESSAGE,
            timestamp: envelope.published_at_ms,
            channel: Some(channel.to_string()),
            channel_serial,
            messages: Some(vec![AblyMessage {
                serial: Some(serial.to_string()),
                action: Some(MESSAGE_SUMMARY),
                annotations: ably_summary_annotations(&value),
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        });
    }
    let ably_message =
        envelope_to_ably_message(envelope, message, projection).map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;
    Ok(AblyProtocolMessage {
        action: ACTION_MESSAGE,
        timestamp: envelope.published_at_ms,
        channel: Some(channel.to_string()),
        channel_serial,
        messages: Some(vec![ably_message]),
        ..empty_protocol_message(ACTION_MESSAGE)
    })
}

fn decode_native_message_data<T>(data: Option<&MessageData>) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    match data {
        Some(MessageData::Json(value)) => sonic_rs::to_vec(value)
            .and_then(|bytes| sonic_rs::from_slice(&bytes))
            .map_err(|error| format!("invalid structured payload: {error}")),
        Some(MessageData::String(value)) => {
            sonic_rs::from_str(value).map_err(|error| format!("invalid JSON payload: {error}"))
        }
        Some(MessageData::Binary(value)) => sonic_rs::from_slice(value)
            .map_err(|error| format!("invalid binary JSON payload: {error}")),
        Some(data @ MessageData::Structured { .. }) => sonic_rs::to_value(data)
            .and_then(|value| sonic_rs::from_value(&value))
            .map_err(|error| format!("invalid structured payload: {error}")),
        None => Err("missing payload".to_string()),
    }
}

fn connected_message(
    connection_id: &str,
    connection_key: &str,
    client_id: Option<String>,
    error: Option<AblyErrorInfo>,
) -> AblyProtocolMessage {
    AblyProtocolMessage {
        action: ACTION_CONNECTED,
        timestamp: None,
        error,
        connection_id: Some(connection_id.to_string()),
        connection_details: Some(AblyConnectionDetails {
            client_id,
            connection_key: connection_key.to_string(),
            connection_state_ttl: DEFAULT_CONNECTION_STATE_TTL_MS,
            max_idle_interval: DEFAULT_MAX_IDLE_INTERVAL_MS,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_frame_size: DEFAULT_MAX_MESSAGE_SIZE,
        }),
        ..empty_protocol_message(ACTION_CONNECTED)
    }
}

#[derive(Debug, Clone, Copy)]
enum AblyCapabilityCheck {
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

fn ensure_ably_capability(
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

fn ensure_ably_channel_capability(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &AblyChannelName,
    check: AblyCapabilityCheck,
) -> Result<(), AblyAuthError> {
    ensure_ably_channel_capability_parts(capabilities, channel.requested(), channel.base(), check)
}

fn ensure_ably_channel_capability_parts(
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

fn ensure_ably_channel_capability_app_error(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &AblyChannelName,
    check: AblyCapabilityCheck,
) -> Result<(), AppError> {
    ensure_ably_channel_capability(capabilities, channel, check)
        .map_err(|error| AppError::Forbidden(error.message))
}

fn ably_auth_app_error(error: AblyAuthError) -> AppError {
    AppError::Protocol {
        status: error.status,
        code: error.code,
        message: error.message,
    }
}

#[derive(Debug)]
enum CapabilityIntersectionError {
    Invalid(String),
    Empty,
}

fn parse_token_request_integer(
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

fn token_request_signing_input(
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

fn verify_token_request_mac(secret: &str, input: &str, encoded_mac: &str) -> bool {
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

fn token_cache_key(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    format!(
        "ably-compat:token:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    )
}

fn session_cache_key(connection_key: &str) -> String {
    let digest = Sha256::digest(connection_key.as_bytes());
    format!(
        "ably-compat:session:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    )
}

fn nonce_cache_key(key_name: &str, nonce: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(key_name.as_bytes());
    digest.update([0]);
    digest.update(nonce.as_bytes());
    format!(
        "ably-compat:nonce:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
    )
}

fn revocation_cache_key(app_id: &str, target_type: &str, target_value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(target_type.as_bytes());
    digest.update([0]);
    digest.update(target_value.as_bytes());
    format!(
        "ably-compat:revocation:{app_id}:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
    )
}

fn intersect_ably_capability(
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

fn capability_maps_equivalent(
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

fn parse_ably_capability_object(
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

fn capability_operations(
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

fn capability_resource_matches(key_pattern: &str, requested_resource: &str) -> bool {
    ConnectionCapabilities::matches_any(&[key_pattern.to_string()], requested_resource)
        || key_pattern == requested_resource
}

fn all_ably_operations() -> &'static [&'static str] {
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

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

#[cfg(test)]
fn normalise_ably_token_capability(
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

fn ably_capability_value_to_sockudo(
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

fn restricted_ably_capabilities() -> ConnectionCapabilities {
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

fn add_ably_capability_operation(
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

fn add_all_supported_ably_capabilities(capabilities: &mut ConnectionCapabilities, resource: &str) {
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

fn add_capability_pattern(patterns: &mut Option<Vec<String>>, pattern: &str) {
    // Ably's all-qualifiers resource is the broad wildcard used by the pinned
    // fixture for both derived and ordinary channel instances.
    let pattern = if pattern == "[*]*" { "*" } else { pattern };
    patterns
        .get_or_insert_with(Vec::new)
        .push(pattern.to_string());
}

async fn resolve_ably_auth(
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
struct AblyJwtClaims {
    iat: i64,
    exp: i64,
    #[serde(default, rename = "x-ably-clientId", alias = "x-ably-client-id")]
    client_id: Option<String>,
    #[serde(default, rename = "x-ably-capability")]
    capability: Option<String>,
    #[serde(default, rename = "x-ably-revocation-key")]
    revocation_key: Option<String>,
}

#[allow(clippy::too_many_arguments)]
async fn resolve_ably_auth_with_expiry(
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

fn decode_ably_client_id_header(headers: &HeaderMap) -> Result<Option<String>, AblyAuthError> {
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

async fn resolve_ably_jwt(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    token: &str,
    requested_client_id: Option<&str>,
    allow_expired: bool,
) -> Result<ResolvedAblyAuth, AblyAuthError> {
    if token.len() > 8 * 1024 {
        return Err(AblyAuthError::unauthorized("JWT exceeds 8 KiB"));
    }
    let header = decode_header(token).map_err(|_| AblyAuthError::invalid_credentials())?;
    if header.alg != Algorithm::HS256 {
        return Err(AblyAuthError::unauthorized("JWT must use HS256"));
    }
    let key_name = header
        .kid
        .as_deref()
        .ok_or_else(AblyAuthError::invalid_credentials)?;
    let key = resolve_ably_key(hub, handler, key_name)
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
        })?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.required_spec_claims.clear();
    let decoded = decode::<AblyJwtClaims>(
        token,
        &DecodingKey::from_secret(key.secret.as_bytes()),
        &validation,
    )
    .map_err(|_| AblyAuthError::invalid_credentials())?;
    let claims = decoded.claims;
    let now_seconds = now_ms().div_euclid(1000);
    if claims.iat > now_seconds.saturating_add(30) || claims.exp <= claims.iat {
        return Err(AblyAuthError::unauthorized("JWT claims are invalid"));
    }
    if !allow_expired && claims.exp <= now_seconds {
        return Err(AblyAuthError::expired());
    }
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
        issued_ms: claims.iat.saturating_mul(1000),
        expires_ms: Some(claims.exp.saturating_mul(1000)),
        credential_id: credential_id(token),
        revocable: key.revocable_tokens,
        revocation_key: claims.revocation_key,
        #[cfg(feature = "push")]
        push_device_id: None,
    })
}

fn credential_id(token: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(token.as_bytes()))
}

async fn resolve_ably_key(
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
        let (_, capabilities) =
            intersect_ably_capability(&capability, None).map_err(|error| AblyAuthError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: 50000,
                message: format!("invalid configured Ably capability: {error:?}"),
            })?;
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

fn key_is_active(key: &AblyCompatKeyConfig, now: i64) -> bool {
    key.enabled
        && key.revoked_at_ms.is_none_or(|revoked_at| revoked_at > now)
        && key.created_at_ms.is_none_or(|created_at| created_at <= now)
        && key.expires_at_ms.is_none_or(|expires_at| expires_at > now)
}

fn token_key_is_current(hub: &AblyCompatHub, record: &AblyTokenRecord, now: i64) -> bool {
    hub.key_registry.get(&record.key_name).is_some_and(|key| {
        key_is_active(key, now)
            && key.app_id == record.app_id
            && key.rotation_id == record.rotation_id
    })
}

fn default_ably_capability() -> String {
    r#"{"*":["*"]}"#.to_string()
}

fn resolve_ably_token_client_id(
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

async fn find_enabled_app_by_key(
    handler: &Arc<ConnectionHandler>,
    app_key: &str,
) -> Result<App, AblyAuthError> {
    match handler.app_manager().find_by_key(app_key).await {
        Ok(Some(app)) if app.enabled => Ok(app),
        Ok(Some(_)) => Err(AblyAuthError::invalid_credentials()),
        Ok(None) => Err(AblyAuthError::invalid_credentials()),
        Err(error) => Err(AblyAuthError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: 50000,
            message: error.to_string(),
        }),
    }
}

async fn find_enabled_app_by_id(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
) -> Result<App, AblyAuthError> {
    match handler.app_manager().find_by_id(app_id).await {
        Ok(Some(app)) if app.enabled => Ok(app),
        Ok(Some(_)) => Err(AblyAuthError::invalid_credentials()),
        Ok(None) => Err(AblyAuthError::invalid_credentials()),
        Err(error) => Err(AblyAuthError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: 50000,
            message: error.to_string(),
        }),
    }
}

fn ably_rest_request_format(headers: &HeaderMap) -> AblyFormat {
    if header_contains(headers, header::CONTENT_TYPE, "msgpack") {
        AblyFormat::MsgPack
    } else {
        AblyFormat::Json
    }
}

fn ably_rest_response_format(
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

fn header_contains(headers: &HeaderMap, name: header::HeaderName, needle: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains(needle))
}

fn parse_ably_format(raw: Option<&str>) -> Result<AblyFormat, String> {
    match raw.unwrap_or("json").trim().to_ascii_lowercase().as_str() {
        "" | "json" => Ok(AblyFormat::Json),
        "msgpack" | "messagepack" => Ok(AblyFormat::MsgPack),
        other => Err(format!("Unsupported Ably protocol format '{other}'")),
    }
}

fn decode_ably_protocol_message(
    body: &[u8],
    format: AblyFormat,
) -> Result<AblyProtocolMessage, String> {
    decode_protocol_bytes(body, format)
}

fn decode_ably_publish_payload(
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

fn encode_ably_rest_response<T: Serialize>(
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

fn encode_ably_legacy_batch_publish_response(
    format: AblyFormat,
    responses: Vec<AblyBatchPublishChannelResponse>,
) -> Result<Response, AppError> {
    if responses
        .iter()
        .all(|response| matches!(response, AblyBatchPublishChannelResponse::Success { .. }))
    {
        return encode_ably_rest_response(StatusCode::CREATED, format, &responses);
    }

    let error = error_info(
        StatusCode::BAD_REQUEST,
        40020,
        "Batched response includes errors",
    );
    let mut response = encode_ably_rest_response(
        StatusCode::BAD_REQUEST,
        format,
        &AblyLegacyBatchResponse {
            error: error.clone(),
            batch_response: responses,
        },
    )?;
    insert_ably_error_headers(&mut response, &error);
    Ok(response)
}

fn effective_ably_client_id(
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

fn rest_publish_identity(
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

fn pusher_to_ably_message(
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
fn envelope_to_ably_message(
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

fn message_content_to_ably_value(content: &MessageContent) -> Result<Value, String> {
    match content {
        MessageContent::Text(value) => Ok(json!(value)),
        MessageContent::Structured(value) => Ok(value.clone()),
        MessageContent::Binary(value) => {
            sonic_rs::to_value(value).map_err(|error| error.to_string())
        }
    }
}

fn envelope_data_to_ably_value(
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

fn ably_projected_encoding(encoding: &str, projection: AblyMessageProjection) -> Option<String> {
    if projection == AblyMessageProjection::Aggregate {
        return Some(encoding.to_string());
    }
    match encoding {
        "json" | "utf-8/base64" => None,
        other => Some(other.to_string()),
    }
}

fn core_action_to_protocol(
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

fn protocol_action_to_ably(action: ProtocolMessageAction) -> u8 {
    match action {
        ProtocolMessageAction::Create => MESSAGE_CREATE,
        ProtocolMessageAction::Update => MESSAGE_UPDATE,
        ProtocolMessageAction::Delete => MESSAGE_DELETE,
        ProtocolMessageAction::Append => MESSAGE_APPEND,
        ProtocolMessageAction::Summary => MESSAGE_SUMMARY,
    }
}

fn message_version_from_runtime_headers(message: &PusherMessage) -> Option<AblyMessageVersion> {
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

fn ably_extras_from_message_extras(extras: &MessageExtras) -> Option<Value> {
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

fn ably_extras_to_message_extras(extras: Option<Value>) -> Result<Option<MessageExtras>, AppError> {
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

fn validate_ably_publish_message(
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

fn message_data_to_ably_value(data: &MessageData) -> Result<Value, String> {
    sonic_rs::to_value(data).map_err(|error| error.to_string())
}

fn ably_value_to_message_data(value: Value) -> MessageData {
    value
        .as_str()
        .map(|value| MessageData::String(value.to_string()))
        .unwrap_or(MessageData::Json(value))
}

fn ably_message_data_to_message_data(value: Value, encoding: Option<&str>) -> MessageData {
    if encoding.is_some_and(|encoding| encoding.eq_ignore_ascii_case("json"))
        && let Some(raw) = value.as_str()
        && let Ok(decoded) = sonic_rs::from_str::<Value>(raw)
    {
        return MessageData::Json(decoded);
    }
    ably_value_to_message_data(value)
}

fn stamp_ai_identity(
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

fn ai_client_id(message: &PusherMessage) -> Option<String> {
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

fn send_protocol_error(sender: &AblySender, code: u32, message: impl Into<String>) {
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ERROR,
            error: Some(error_info(StatusCode::BAD_REQUEST, code, message)),
            ..empty_protocol_message(ACTION_ERROR)
        },
    );
}

fn send_protocol_disconnected(sender: &AblySender, code: u32, message: impl Into<String>) {
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_DISCONNECTED,
            error: Some(error_info(StatusCode::UNAUTHORIZED, code, message)),
            ..empty_protocol_message(ACTION_DISCONNECTED)
        },
    );
}

fn send_channel_error(
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

fn send_publish_nack(
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

fn error_info(status: StatusCode, code: u32, message: impl Into<String>) -> AblyErrorInfo {
    AblyErrorInfo {
        message: message.into(),
        code,
        status_code: status.as_u16(),
    }
}

fn ably_error_info_from_app_error(error: AppError) -> AblyErrorInfo {
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

fn insert_ably_error_headers(response: &mut Response, error: &AblyErrorInfo) {
    if let Ok(value) = HeaderValue::from_str(&error.code.to_string()) {
        response.headers_mut().insert("x-ably-errorcode", value);
    }
    if let Ok(value) = HeaderValue::from_str(&error.message) {
        response.headers_mut().insert("x-ably-errormessage", value);
    }
}

fn ably_error_response(status: StatusCode, code: u32, message: impl Into<String>) -> Response {
    ably_error_response_format(status, code, message, AblyFormat::Json)
}

fn ably_error_response_format(
    status: StatusCode,
    code: u32,
    message: impl Into<String>,
    format: AblyFormat,
) -> Response {
    let error = error_info(status, code, message);
    let encoded = encode_ably_rest_response(
        status,
        format,
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

fn ably_app_error_response(error: AppError) -> Response {
    ably_app_error_response_format(error, AblyFormat::Json)
}

fn ably_app_error_response_format(error: AppError, format: AblyFormat) -> Response {
    let error = ably_error_info_from_app_error(error);
    ably_error_response_format(
        StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        error.code,
        error.message,
        format,
    )
}

fn parse_ably_history_direction(raw: Option<&str>) -> Result<HistoryDirection, AppError> {
    match raw.unwrap_or("backwards").to_ascii_lowercase().as_str() {
        "backwards" | "newest_first" | "reverse" => Ok(HistoryDirection::NewestFirst),
        "forwards" | "oldest_first" | "forward" => Ok(HistoryDirection::OldestFirst),
        other => Err(AppError::InvalidInput(format!(
            "Invalid Ably history direction '{other}'"
        ))),
    }
}

fn ably_history_next_link(
    query: &AblyHistoryQuery,
    direction: HistoryDirection,
    limit: usize,
    cursor: &HistoryCursor,
) -> Result<String, AppError> {
    let params = ably_history_link_params(query, direction, limit, Some(cursor))?;
    Ok(format!("<./messages?{}>; rel=\"next\"", params.join("&")))
}

fn ably_history_link_header(
    query: &AblyHistoryQuery,
    direction: HistoryDirection,
    limit: usize,
    next_cursor: Option<&HistoryCursor>,
) -> Result<String, AppError> {
    let first = ably_history_link_params(query, direction, limit, None)?;
    let mut links = vec![format!("<./messages?{}>; rel=\"first\"", first.join("&"))];
    if let Some(cursor) = next_cursor {
        links.push(ably_history_next_link(query, direction, limit, cursor)?);
    }
    Ok(links.join(", "))
}

fn ably_history_link_params(
    query: &AblyHistoryQuery,
    direction: HistoryDirection,
    limit: usize,
    cursor: Option<&HistoryCursor>,
) -> Result<Vec<String>, AppError> {
    let mut params = vec![
        format!("limit={limit}"),
        format!(
            "direction={}",
            match direction {
                HistoryDirection::NewestFirst => "backwards",
                HistoryDirection::OldestFirst => "forwards",
            }
        ),
    ];
    if let Some(cursor) = cursor {
        params.push(format!("cursor={}", urlencoding::encode(&cursor.encode()?)));
    }
    if let Some(start) = query.start {
        params.push(format!("start={start}"));
    }
    if let Some(end) = query.end {
        params.push(format!("end={end}"));
    }
    if let Some(until_attach) = query.until_attach {
        params.push(format!("untilAttach={until_attach}"));
    }
    if let Some(from_serial) = query.from_serial.as_deref() {
        params.push(format!("from_serial={}", urlencoding::encode(from_serial)));
    }
    if let Some(format) = query.format.as_deref() {
        params.push(format!("format={}", urlencoding::encode(format)));
    }
    Ok(params)
}

fn decode_presence_snapshot_cursor(cursor: Option<&str>) -> Result<usize, AppError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::InvalidInput("Invalid presence cursor".to_string()))?;
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| AppError::InvalidInput("Invalid presence cursor".to_string()))?;
    usize::try_from(u64::from_be_bytes(bytes))
        .map_err(|_| AppError::InvalidInput("Invalid presence cursor".to_string()))
}

fn ably_presence_link_header(
    query: &AblyPresenceQuery,
    path: &str,
    limit: usize,
    next_cursor: Option<String>,
) -> String {
    let params = |cursor: Option<&str>| {
        let mut params = vec![format!("limit={limit}")];
        if let Some(cursor) = cursor {
            params.push(format!("cursor={}", urlencoding::encode(cursor)));
        }
        if let Some(client_id) = query.client_id.as_deref() {
            params.push(format!("clientId={}", urlencoding::encode(client_id)));
        }
        if let Some(connection_id) = query.connection_id.as_deref() {
            params.push(format!(
                "connectionId={}",
                urlencoding::encode(connection_id)
            ));
        }
        if let Some(direction) = query.direction.as_deref() {
            params.push(format!("direction={}", urlencoding::encode(direction)));
        }
        if let Some(start) = query.start {
            params.push(format!("start={start}"));
        }
        if let Some(end) = query.end {
            params.push(format!("end={end}"));
        }
        if let Some(format) = query.format.as_deref() {
            params.push(format!("format={}", urlencoding::encode(format)));
        }
        params.join("&")
    };
    let mut links = vec![format!("<./{path}?{}>; rel=\"first\"", params(None))];
    if let Some(cursor) = next_cursor.as_deref() {
        links.push(format!("<./{path}?{}>; rel=\"next\"", params(Some(cursor))));
    }
    links.join(", ")
}

fn parse_ably_channel_name(raw: String) -> Result<AblyChannelName, AppError> {
    AblyChannelName::parse(raw).map_err(|error| AppError::Protocol {
        status: StatusCode::BAD_REQUEST,
        code: 40010,
        message: error.to_string(),
    })
}

fn channel_key(app_id: &str, channel: &str) -> AblyChannelKey {
    AblyChannelKey {
        app_id: app_id.to_string(),
        channel: channel.to_string(),
    }
}

fn subscriber_key(session_id: &str, requested_channel: &str) -> AblySubscriberKey {
    AblySubscriberKey {
        session_id: Arc::from(session_id),
        requested_channel: Arc::from(requested_channel),
    }
}

fn send_protocol(sender: &AblySender, message: AblyProtocolMessage) {
    if let Err(error) = sender.send_protocol(&message, OutboundPriority::Control) {
        debug!(error = %error, "Ably compatibility outbound queue is unavailable");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose;
    use sockudo_cache::MemoryCacheManager;
    use sockudo_core::{app::AppPolicy, options::MemoryCacheOptions};
    use sockudo_protocol::messages::{AiExtras, MessageExtras};
    use sockudo_protocol::versioned_messages::apply_runtime_metadata;

    #[test]
    fn publish_validation_accepts_nameless_and_dataless_messages() {
        validate_ably_publish_message(&AblyMessage::default(), false)
            .expect("Ably permits a message without name or data");
    }

    #[test]
    fn publish_validation_rejects_reserved_and_unknown_fields() {
        let reserved = AblyMessage {
            connection_id: Some("client-supplied".to_string()),
            ..AblyMessage::default()
        };
        assert!(validate_ably_publish_message(&reserved, true).is_err());

        let unknown = AblyMessage {
            extras: Some(json!({ "ephemeral": true })),
            ..AblyMessage::default()
        };
        assert!(validate_ably_publish_message(&unknown, false).is_err());
    }

    #[test]
    fn wire_connection_key_is_not_treated_as_delivered_connection_id() {
        let message: AblyMessage =
            sonic_rs::from_str(r#"{"connectionKey":"app.key.secret","clientId":"publisher"}"#)
                .expect("valid message");

        assert_eq!(message.connection_key.as_deref(), Some("app.key.secret"));
        assert!(message.connection_id.is_none());
    }

    #[tokio::test]
    async fn connection_key_resolves_only_a_live_same_app_identity() {
        let hub = AblyCompatHub::default();
        let connection_key = "app:key";
        hub.remember_connection(
            connection_key.to_string(),
            "app",
            "connection-a",
            Some("publisher".to_string()),
        )
        .await;
        let message = AblyMessage {
            client_id: Some("publisher".to_string()),
            connection_key: Some(connection_key.to_string()),
            ..AblyMessage::default()
        };

        assert!(rest_publish_identity(&hub, "app", Some("publisher"), &message).is_err());

        let (command_tx, _command_rx) = tokio::sync::mpsc::channel(1);
        hub.live_sessions.insert(
            "connection-a".to_string(),
            AblyLiveSession {
                session_id: "transport-a".to_string(),
                app_id: "app".to_string(),
                authorization: Arc::new(RwLock::new(ConnectionAuthorization {
                    generation: 1,
                    client_id: Some("publisher".to_string()),
                    connection_client_id: Some("publisher".to_string()),
                    capabilities: None,
                    issued_ms: now_ms(),
                    expires_ms: None,
                    credential_id: "test".to_string(),
                    revocable: false,
                    revocation_key: None,
                })),
                command_tx,
            },
        );

        assert_eq!(
            rest_publish_identity(&hub, "app", Some("publisher"), &message).unwrap(),
            (
                Some("connection-a".to_string()),
                Some("publisher".to_string())
            )
        );
        assert!(rest_publish_identity(&hub, "other-app", Some("publisher"), &message).is_err());
        assert!(rest_publish_identity(&hub, "app", Some("impostor"), &message).is_err());
    }

    #[tokio::test]
    async fn recovered_transport_supersedes_without_unregistering_replacement() {
        let hub = AblyCompatHub::default();
        let authorization = Arc::new(RwLock::new(ConnectionAuthorization {
            generation: 1,
            client_id: None,
            connection_client_id: None,
            capabilities: None,
            issued_ms: now_ms(),
            expires_ms: None,
            credential_id: "test".to_string(),
            revocable: false,
            revocation_key: None,
        }));
        let (old_tx, mut old_rx) = tokio::sync::mpsc::channel(1);
        assert!(
            hub.register_live_session(
                "connection-a".to_string(),
                AblyLiveSession {
                    session_id: "transport-old".to_string(),
                    app_id: "app".to_string(),
                    authorization: Arc::clone(&authorization),
                    command_tx: old_tx,
                },
            )
            .is_none()
        );

        let (new_tx, _new_rx) = tokio::sync::mpsc::channel(1);
        let previous = hub
            .register_live_session(
                "connection-a".to_string(),
                AblyLiveSession {
                    session_id: "transport-new".to_string(),
                    app_id: "app".to_string(),
                    authorization,
                    command_tx: new_tx,
                },
            )
            .expect("replacement returns the old transport command sender");
        previous
            .send(AblySessionCommand::Superseded)
            .await
            .expect("old transport still receives supersession");
        assert!(matches!(
            old_rx.recv().await,
            Some(AblySessionCommand::Superseded)
        ));

        hub.unregister_live_session("connection-a", "transport-old");
        assert_eq!(
            hub.live_sessions
                .get("connection-a")
                .map(|session| session.session_id.clone())
                .as_deref(),
            Some("transport-new")
        );
        hub.unregister_live_session("connection-a", "transport-new");
        assert!(!hub.live_sessions.contains_key("connection-a"));
    }

    #[test]
    fn client_id_header_requires_base64_utf8() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-ably-clientid",
            axum::http::HeaderValue::from_static("cHVibGlzaGVy"),
        );
        assert_eq!(
            decode_ably_client_id_header(&headers)
                .expect("valid header")
                .as_deref(),
            Some("publisher")
        );

        headers.insert(
            "x-ably-clientid",
            axum::http::HeaderValue::from_static("not-base64"),
        );
        assert!(decode_ably_client_id_header(&headers).is_err());
    }

    #[test]
    fn authenticated_identity_rejects_mutation_actor_spoofing() {
        let message = AblyMessage {
            action: Some(MESSAGE_UPDATE),
            serial: Some("message-serial".to_string()),
            version: Some(AblyMessageVersion {
                serial: "version-serial".to_string(),
                timestamp: None,
                client_id: Some("impostor".to_string()),
                description: None,
                metadata: None,
            }),
            ..AblyMessage::default()
        };

        assert!(effective_ably_client_id(Some("authenticated"), &message).is_err());
    }

    #[test]
    fn ack_count_covers_the_inbound_protocol_serial_range() {
        let inbound = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            count: Some(3),
            messages: Some(vec![AblyMessage::default(); 7]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };
        assert_eq!(publish_ack_count(&inbound), 3);
    }

    #[test]
    fn echo_filter_only_suppresses_the_originating_connection() {
        assert!(!should_deliver_to_subscriber(
            Some("connection-a"),
            "connection-a",
            false,
            None
        ));
        assert!(should_deliver_to_subscriber(
            Some("connection-a"),
            "connection-a",
            false,
            Some(true)
        ));
        assert!(should_deliver_to_subscriber(
            Some("connection-a"),
            "connection-b",
            false,
            Some(false)
        ));
    }

    #[test]
    fn history_next_link_preserves_page_shape_without_credentials() {
        let query = AblyHistoryQuery {
            key: Some("key:secret".to_string()),
            access_token: Some("token-secret".to_string()),
            limit: Some(2),
            direction: Some("forwards".to_string()),
            start: Some(10),
            end: Some(20),
            ..AblyHistoryQuery::default()
        };
        let cursor = HistoryCursor {
            version: 1,
            app_id: "app".to_string(),
            channel: "space channel".to_string(),
            stream_id: "stream".to_string(),
            serial: 11,
            direction: HistoryDirection::OldestFirst,
            bounds: HistoryQueryBounds {
                start_time_ms: Some(10),
                end_time_ms: Some(20),
                ..HistoryQueryBounds::default()
            },
        };

        let link = ably_history_next_link(&query, HistoryDirection::OldestFirst, 2, &cursor)
            .expect("valid Link header");
        assert!(link.starts_with("<./messages?limit=2&direction=forwards&cursor="));
        assert!(link.contains("&start=10&end=20"));
        assert!(link.ends_with(">; rel=\"next\""));
        assert!(!link.contains("secret"));
        assert!(!link.contains("access_token"));
        assert!(!link.contains("key="));
    }

    #[test]
    fn rewind_parser_accepts_count_and_seconds_minutes_hours() {
        assert_eq!(
            parse_ably_rewind_param("12"),
            Some(SubscriptionRewind::Count(12))
        );
        assert_eq!(
            parse_ably_rewind_param("15s"),
            Some(SubscriptionRewind::Seconds(15))
        );
        assert_eq!(
            parse_ably_rewind_param("2m"),
            Some(SubscriptionRewind::Seconds(120))
        );
        assert_eq!(
            parse_ably_rewind_param("3h"),
            Some(SubscriptionRewind::Seconds(10_800))
        );
    }

    #[test]
    fn rewind_parser_rejects_zero_invalid_and_overflowing_values() {
        assert_eq!(parse_ably_rewind_param("0"), None);
        assert_eq!(parse_ably_rewind_param("0s"), None);
        assert_eq!(parse_ably_rewind_param("forever"), None);
        assert_eq!(parse_ably_rewind_param("18446744073709551615h"), None);
    }

    #[test]
    fn attach_resume_suppresses_rewind_params() {
        let params = HashMap::from([("rewind".to_string(), "1m".to_string())]);

        assert_eq!(resolve_ably_rewind(&params, true), None);
        assert_eq!(
            resolve_ably_rewind(&params, false),
            Some(SubscriptionRewind::Seconds(60))
        );
    }

    #[test]
    fn rewind_history_request_is_bounded_and_ends_at_attach_high_water() {
        let position = AblyChannelPosition {
            stream_id: "stream".to_string(),
            serial: 41,
        };

        let count = build_ably_rewind_history_request(
            "app",
            "channel",
            &SubscriptionRewind::Count(500),
            100,
            Some(&position),
            1_000_000,
        );
        assert_eq!(count.limit, 100);
        assert_eq!(count.direction, HistoryDirection::NewestFirst);
        assert_eq!(count.bounds.end_serial, Some(41));

        let duration = build_ably_rewind_history_request(
            "app",
            "channel",
            &SubscriptionRewind::Seconds(60),
            100,
            Some(&position),
            1_000_000,
        );
        assert_eq!(duration.limit, 100);
        assert_eq!(duration.direction, HistoryDirection::OldestFirst);
        assert_eq!(duration.bounds.start_time_ms, Some(940_000));
        assert_eq!(duration.bounds.end_serial, Some(41));
    }

    #[test]
    fn history_link_header_includes_stable_first_and_next_relations() {
        let query = AblyHistoryQuery {
            key: Some("key:secret".to_string()),
            access_token: Some("token-secret".to_string()),
            limit: Some(2),
            direction: Some("forwards".to_string()),
            start: Some(10),
            end: Some(20),
            until_attach: Some(true),
            from_serial: Some("stream:7".to_string()),
            format: Some("msgpack".to_string()),
            ..AblyHistoryQuery::default()
        };
        let cursor = HistoryCursor {
            version: 1,
            app_id: "app".to_string(),
            channel: "space channel".to_string(),
            stream_id: "stream".to_string(),
            serial: 11,
            direction: HistoryDirection::OldestFirst,
            bounds: HistoryQueryBounds {
                start_time_ms: Some(10),
                end_time_ms: Some(20),
                ..HistoryQueryBounds::default()
            },
        };

        let header =
            ably_history_link_header(&query, HistoryDirection::OldestFirst, 2, Some(&cursor))
                .expect("valid Link header");

        assert!(header.contains("rel=\"first\""));
        assert!(header.contains("rel=\"next\""));
        assert!(header.contains("limit=2&direction=forwards"));
        assert!(header.contains("start=10&end=20&untilAttach=true"));
        assert!(header.contains("from_serial=stream%3A7&format=msgpack"));
        assert!(!header.contains("secret"));
        assert!(!header.contains("access_token"));
        assert!(!header.contains("key="));
    }

    #[test]
    fn annotation_delete_capabilities_are_independent_operations() {
        let capabilities = ably_capability_value_to_sockudo(&serde_json::json!({
            "mutable:*": ["annotation-delete-own"],
            "moderated:*": ["annotation-delete-any"]
        }))
        .expect("annotation delete capabilities should parse");

        assert!(capabilities.allows_annotation_delete_own("mutable:room"));
        assert!(!capabilities.allows_annotation_delete_any("mutable:room"));
        assert!(capabilities.allows_annotation_delete_any("moderated:room"));
        assert!(
            ensure_ably_channel_capability(
                Some(&capabilities),
                &AblyChannelName::parse("mutable:room".to_string()).unwrap(),
                AblyCapabilityCheck::AnnotationMutate,
            )
            .is_ok()
        );
        assert!(!capabilities.allows_message_mutation_any(
            sockudo_core::versioned_message_auth::MutationKind::Delete,
            "moderated:room"
        ));
    }

    #[test]
    fn annotation_cursor_is_opaque_and_scoped_to_app_channel_and_message() {
        let message_serial = MessageSerial::new("msg:1").unwrap();
        let annotation_serial = AnnotationSerial::new("ann:2").unwrap();
        let cursor = encode_ably_annotation_cursor(
            "app",
            "mutable:room",
            &message_serial,
            &annotation_serial,
        )
        .unwrap();

        assert!(!cursor.contains("ann:2"));
        assert_eq!(
            decode_ably_annotation_cursor(&cursor, "app", "mutable:room", &message_serial,)
                .unwrap(),
            annotation_serial
        );
        assert!(
            decode_ably_annotation_cursor(&cursor, "app", "mutable:other", &message_serial,)
                .is_err()
        );
        assert!(
            decode_ably_annotation_cursor(
                &cursor,
                "app",
                "mutable:room",
                &MessageSerial::new("msg:other").unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn multiple_summary_renames_native_client_counts_only_at_ably_edge() {
        let native = json!({
            "annotations": {
                "summary": {
                    "reaction:multiple.v1": {
                        "vote": {
                            "total": 3,
                            "clientCounts": { "client-1": 2 },
                            "totalUnidentified": 1,
                            "clipped": false,
                            "totalClientIds": 1
                        }
                    },
                    "reaction:distinct.v1": {
                        "like": { "total": 1, "clientIds": ["client-1"], "clipped": false }
                    }
                }
            }
        });

        let projected = ably_summary_annotations(&native).unwrap();
        let multiple = &projected["summary"]["reaction:multiple.v1"]["vote"];
        assert_eq!(multiple["clientIds"]["client-1"].as_u64(), Some(2));
        assert!(multiple.get("clientCounts").is_none());
        assert_eq!(
            projected["summary"]["reaction:distinct.v1"]["like"]["clientIds"][0].as_str(),
            Some("client-1")
        );
        assert!(native["summary"].is_null());
        assert_eq!(
            native["annotations"]["summary"]["reaction:multiple.v1"]["vote"]
                ["clientCounts"]["client-1"]
                .as_u64(),
            Some(2)
        );
    }

    #[test]
    fn delete_own_and_delete_any_authorize_independently_of_message_mutation() {
        let channel = AblyChannelName::parse("mutable:room".to_string()).unwrap();
        let own = ably_capability_value_to_sockudo(&serde_json::json!({
            "mutable:*": ["annotation-delete-own"]
        }))
        .unwrap();
        assert!(
            authorize_ably_annotation_delete(
                Some(&own),
                &channel,
                Some("client-1"),
                Some("client-1"),
            )
            .is_ok()
        );
        assert!(
            authorize_ably_annotation_delete(
                Some(&own),
                &channel,
                Some("client-2"),
                Some("client-1"),
            )
            .is_err()
        );

        let any = ably_capability_value_to_sockudo(&serde_json::json!({
            "mutable:*": ["annotation-delete-any"]
        }))
        .unwrap();
        assert!(
            authorize_ably_annotation_delete(
                Some(&any),
                &channel,
                Some("moderator"),
                Some("client-1"),
            )
            .is_ok()
        );
        assert!(
            authorize_ably_annotation_delete(Some(&any), &channel, None, Some("client-1")).is_err()
        );
    }

    #[test]
    fn delete_annotation_dto_keeps_native_target_selector_fields() {
        let command = parse_ably_annotation_command(
            AblyAnnotation {
                action: Some(1),
                id: Some("annotation-id".to_string()),
                serial: Some("ann:7".to_string()),
                message_serial: Some("msg:1".to_string()),
                annotation_type: Some("reaction:distinct.v1".to_string()),
                name: Some("like".to_string()),
                client_id: Some("client-1".to_string()),
                ..AblyAnnotation::default()
            },
            None,
            Some("moderator"),
        )
        .unwrap();

        let AblyAnnotationCommand::Delete(selector) = command else {
            panic!("expected delete command");
        };
        assert_eq!(selector.message_serial.as_str(), "msg:1");
        assert_eq!(selector.annotation_type.as_str(), "reaction:distinct.v1");
        assert_eq!(selector.id.unwrap().as_str(), "annotation-id");
        assert_eq!(selector.target_serial.unwrap().as_str(), "ann:7");
        assert_eq!(selector.name.as_deref(), Some("like"));
        assert_eq!(selector.client_id.as_deref(), Some("client-1"));
    }

    #[test]
    fn create_annotation_rejects_authenticated_client_id_spoofing() {
        let result = parse_ably_annotation_command(
            AblyAnnotation {
                action: Some(0),
                message_serial: Some("msg:1".to_string()),
                annotation_type: Some("reaction:flag.v1".to_string()),
                client_id: Some("other-client".to_string()),
                ..AblyAnnotation::default()
            },
            None,
            Some("authenticated-client"),
        );

        assert!(result.is_err());
    }

    #[test]
    fn recovery_projects_stored_annotation_as_action_21() {
        let event = AnnotationEventData {
            action: AnnotationEventAction::Create,
            id: Some("annotation-id".to_string()),
            serial: "ann:2".to_string(),
            message_serial: "msg:1".to_string(),
            annotation_type: "reaction:distinct.v1".to_string(),
            name: Some("like".to_string()),
            client_id: Some("client-1".to_string()),
            count: None,
            data: Some(json!({ "source": "recovery" })),
            encoding: Some("json".to_string()),
            timestamp: 123,
        };
        let message = PusherMessage {
            event: Some(ANNOTATION_EVENT_NAME.to_string()),
            channel: Some("mutable:room".to_string()),
            data: Some(MessageData::Json(sonic_rs::to_value(&event).unwrap())),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: Some("stream".to_string()),
            serial: Some(7),
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };

        let projected = ably_protocol_message_from_envelope(
            "mutable:room",
            &message,
            &MessageEnvelope::default(),
            AblyMessageProjection::Mutation,
            Some("stream:7".to_string()),
        )
        .expect("annotation recovery projection");

        assert_eq!(projected.action, ACTION_ANNOTATION);
        assert_eq!(projected.channel_serial.as_deref(), Some("stream:7"));
        let annotation = &projected.annotations.unwrap()[0];
        assert_eq!(annotation.serial.as_deref(), Some("ann:2"));
        assert_eq!(annotation.message_serial.as_deref(), Some("msg:1"));
        assert_eq!(annotation.data, Some(json!({ "source": "recovery" })));
        assert_eq!(annotation.encoding.as_deref(), Some("json"));
        assert_eq!(annotation.timestamp, Some(123));
    }

    #[test]
    fn recovery_projects_annotation_summary_with_original_message_serial() {
        let message = PusherMessage {
            event: Some(MESSAGE_SUMMARY_EVENT_NAME.to_string()),
            channel: Some("mutable:room".to_string()),
            data: Some(MessageData::Json(json!({
                "action": "message.summary",
                "serial": "msg:original",
                "annotations": {
                    "summary": {
                        "reaction:distinct.v1": {
                            "like": { "total": 2, "clientIds": ["a", "b"], "clipped": false }
                        }
                    }
                }
            }))),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: Some("stream".to_string()),
            serial: Some(8),
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };

        let projected = ably_protocol_message_from_envelope(
            "mutable:room",
            &message,
            &MessageEnvelope {
                published_at_ms: Some(456),
                ..MessageEnvelope::default()
            },
            AblyMessageProjection::Mutation,
            Some("stream:8".to_string()),
        )
        .expect("annotation summary recovery projection");

        assert_eq!(projected.action, ACTION_MESSAGE);
        assert_eq!(projected.channel_serial.as_deref(), Some("stream:8"));
        assert_eq!(projected.timestamp, Some(456));
        let summary = &projected.messages.unwrap()[0];
        assert_eq!(summary.action, Some(MESSAGE_SUMMARY));
        assert_eq!(summary.serial.as_deref(), Some("msg:original"));
        assert_eq!(
            summary.annotations.as_ref().unwrap()["summary"]["reaction:distinct.v1"]["like"]
                ["total"]
                .as_u64(),
            Some(2)
        );
    }

    #[test]
    fn attach_options_default_to_supported_channel_modes() {
        let options = AblyAttachOptions::from_wire(None, None);

        assert_eq!(options.mode_flags, ABLY_DEFAULT_MODE_FLAGS);
        assert!(options.params.is_empty());
    }

    #[test]
    fn attach_params_modes_override_protocol_mode_flags() {
        let options = AblyAttachOptions::from_wire(
            Some(ABLY_MODE_PUBLISH),
            Some(HashMap::from([
                ("modes".to_string(), "presence,subscribe".to_string()),
                ("delta".to_string(), "vcdiff".to_string()),
            ])),
        );

        assert_eq!(options.mode_flags, ABLY_MODE_PRESENCE | ABLY_MODE_SUBSCRIBE);
        assert_eq!(
            options.params.get("delta").map(String::as_str),
            Some("vcdiff")
        );
    }

    #[test]
    fn attach_options_drop_unknown_params_and_unknown_mode_bits() {
        let options = AblyAttachOptions::from_wire(
            Some(u64::MAX),
            Some(HashMap::from([(
                "nonexistent".to_string(),
                "value".to_string(),
            )])),
        );

        assert_eq!(options.mode_flags, ABLY_MODE_MASK);
        assert!(options.params.is_empty());
    }

    #[test]
    fn attach_options_accept_only_vcdiff_delta_mode() {
        let unsupported = AblyAttachOptions::from_wire(
            None,
            Some(HashMap::from([(
                "delta".to_string(),
                "unsupported".to_string(),
            )])),
        );
        let accepted = AblyAttachOptions::from_wire(
            None,
            Some(HashMap::from([("delta".to_string(), "VCDIFF".to_string())])),
        );

        assert!(!unsupported.params.contains_key("delta"));
        assert_eq!(
            accepted.params.get("delta").map(String::as_str),
            Some("vcdiff")
        );
    }

    #[test]
    fn attached_channel_modes_deny_only_explicitly_missing_operations() {
        let modes = HashMap::from([(
            "channel".to_string(),
            AblyConnectionAttachment {
                channel: AblyChannelName::parse("channel".to_string()).unwrap(),
                params: HashMap::new(),
                mode_flags: ABLY_MODE_SUBSCRIBE,
                explicit_modes: true,
                filter: None,
                attach_position: None,
                has_presence: false,
            },
        )]);

        assert!(attached_channel_mode_denies(
            &modes,
            Some("channel"),
            ABLY_MODE_PUBLISH
        ));
        assert!(!attached_channel_mode_denies(
            &modes,
            Some("channel"),
            ABLY_MODE_SUBSCRIBE
        ));
        assert!(!attached_channel_mode_denies(
            &modes,
            Some("unattached"),
            ABLY_MODE_PUBLISH
        ));
    }

    #[test]
    fn derived_filter_cache_reports_hits_and_misses() {
        let hub = AblyCompatHub::default();
        let source = "headers.kind == `\"accepted\"`";
        let encoded = base64::engine::general_purpose::STANDARD.encode(source);
        let channel = AblyChannelName::parse(format!("[filter={encoded}]cache-channel")).unwrap();

        assert!(hub.message_filter(&channel).unwrap().is_some());
        assert!(hub.message_filter(&channel).unwrap().is_some());

        let metrics = hub.metrics.snapshot();
        assert_eq!(metrics.filter_cache_misses, 1);
        assert_eq!(metrics.filter_cache_hits, 1);
        assert_eq!(metrics.filter_cache_entries, 1);
        assert!(metrics.filter_cache_bytes >= source.len());
    }

    #[test]
    fn derived_filter_cache_evicts_at_the_entry_bound() {
        let hub = AblyCompatHub::default();
        for index in 0..=ABLY_FILTER_CACHE_MAX_ENTRIES {
            let source = format!("headers.cacheKey == `{index}`");
            let encoded = base64::engine::general_purpose::STANDARD.encode(source);
            let channel =
                AblyChannelName::parse(format!("[filter={encoded}]cache-channel")).unwrap();
            assert!(hub.message_filter(&channel).unwrap().is_some());
        }

        let metrics = hub.metrics.snapshot();
        assert_eq!(metrics.filter_cache_entries, ABLY_FILTER_CACHE_MAX_ENTRIES);
        assert!(metrics.filter_cache_bytes <= ABLY_FILTER_CACHE_MAX_BYTES);
        assert_eq!(metrics.filter_cache_evictions, 1);
    }

    #[test]
    fn heartbeat_response_echoes_correlation_id() {
        let response = heartbeat_response(AblyProtocolMessage {
            action: ACTION_HEARTBEAT,
            id: Some("ping-1".to_string()),
            ..empty_protocol_message(ACTION_HEARTBEAT)
        });

        assert_eq!(response.action, ACTION_HEARTBEAT);
        assert_eq!(response.id.as_deref(), Some("ping-1"));
    }

    #[test]
    fn realtime_message_ids_are_stable_across_unacked_retries() {
        assert_eq!(
            ably_realtime_message_id("connection-a", 7, 2),
            "connection-a:7:2"
        );
    }

    #[test]
    fn close_and_disconnect_actions_are_terminal() {
        assert_eq!(
            ably_protocol_control(ACTION_CLOSE),
            AblyProtocolControl::Close
        );
        assert_eq!(
            ably_protocol_control(ACTION_DISCONNECT),
            AblyProtocolControl::Close
        );
        assert_eq!(
            ably_protocol_control(ACTION_HEARTBEAT),
            AblyProtocolControl::Continue
        );
    }

    #[tokio::test]
    async fn invalid_recover_and_resume_keys_use_distinct_error_codes() {
        let hub = AblyCompatHub::default();

        let AblyConnectionStart::Failed { error: recover } = hub
            .begin_connection("app", None, None, Some("missing"))
            .await
        else {
            panic!("missing recovery key must fail");
        };
        let AblyConnectionStart::Failed { error: resume } = hub
            .begin_connection("app", None, Some("missing"), None)
            .await
        else {
            panic!("missing resume key must fail");
        };

        assert_eq!(recover.code, 80018);
        assert_eq!(resume.code, 80008);
    }

    #[tokio::test]
    async fn recover_key_survives_handoff_to_another_runtime_node() {
        let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
            "ably-session-recovery".to_string(),
            MemoryCacheOptions::default(),
        ));
        let first_node = AblyCompatHub {
            cache: Some(Arc::clone(&cache)),
            ..AblyCompatHub::default()
        };
        let second_node = AblyCompatHub {
            cache: Some(cache),
            ..AblyCompatHub::default()
        };
        first_node
            .remember_connection(
                "recover-key".to_string(),
                "app",
                "connection-a",
                Some("client-a".to_string()),
            )
            .await;

        let recovered = second_node
            .begin_connection("app", Some("client-a"), None, Some("recover-key"))
            .await;
        assert!(matches!(
            recovered,
            AblyConnectionStart::Resumed { connection_id }
                if connection_id == "connection-a"
        ));
    }

    #[tokio::test]
    async fn active_connection_lease_refreshes_and_graceful_close_invalidates_it() {
        let hub = AblyCompatHub::default();
        hub.remember_connection(
            "connection-key".to_string(),
            "app",
            "connection-a",
            Some("client-a".to_string()),
        )
        .await;
        hub.sessions
            .get_mut("connection-key")
            .expect("stored lease")
            .expires_at_ms = 0;

        hub.remember_connection(
            "connection-key".to_string(),
            "app",
            "connection-a",
            Some("client-a".to_string()),
        )
        .await;
        assert!(matches!(
            hub.begin_connection("app", Some("client-a"), None, Some("connection-key"))
                .await,
            AblyConnectionStart::Resumed { .. }
        ));

        hub.forget_connection("connection-key").await;
        assert!(matches!(
            hub.begin_connection("app", Some("client-a"), None, Some("connection-key"))
                .await,
            AblyConnectionStart::Failed { .. }
        ));
    }

    #[test]
    fn batch_publish_body_decodes_single_message_without_expansion() {
        let request: AblyBatchPublishRequest = decode_value(
            br#"{"channels":["one","two"],"messages":{"name":"event","data":"value"}}"#,
            AblyFormat::Json,
        )
        .expect("valid batch body");

        assert_eq!(request.channels, ["one", "two"]);
        let messages = request.messages.into_messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].name.as_deref(), Some("event"));
    }

    #[test]
    fn batch_publish_accepts_single_and_many_request_shapes() {
        let single: AblyBatchPublishBody = decode_value(
            br#"{"channels":["one"],"messages":{"name":"event"}}"#,
            AblyFormat::Json,
        )
        .expect("single request object");
        assert!(matches!(single, AblyBatchPublishBody::One(_)));

        let many: AblyBatchPublishBody = decode_value(
            br#"[{"channels":["one"],"messages":{"name":"event"}}]"#,
            AblyFormat::Json,
        )
        .expect("request array");
        assert!(matches!(many, AblyBatchPublishBody::Many(requests) if requests.len() == 1));
    }

    #[tokio::test]
    async fn legacy_batch_publish_partial_failure_uses_40020_envelope() {
        let responses = vec![
            AblyBatchPublishChannelResponse::Success {
                channel: "valid".to_string(),
                message_id: "message-id".to_string(),
                serials: vec!["serial-1".to_string()],
            },
            AblyBatchPublishChannelResponse::Failure {
                channel: "[invalid".to_string(),
                error: error_info(StatusCode::BAD_REQUEST, 40010, "invalid channel"),
            },
        ];

        let response = encode_ably_legacy_batch_publish_response(AblyFormat::Json, responses)
            .expect("partial response encodes");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response
                .headers()
                .get("x-ably-errorcode")
                .and_then(|value| value.to_str().ok()),
            Some("40020")
        );
        let body = axum::body::to_bytes(response.into_body(), 4 * 1024)
            .await
            .expect("response body");
        let value: Value = decode_value(body.as_ref(), AblyFormat::Json).expect("JSON response");
        assert_eq!(value["error"]["code"].as_u64(), Some(40020));
        assert_eq!(value["error"]["statusCode"].as_u64(), Some(400));
        assert_eq!(
            value["batchResponse"].as_array().map(|items| items.len()),
            Some(2)
        );
        assert_eq!(value["batchResponse"][0]["channel"].as_str(), Some("valid"));
        assert_eq!(
            value["batchResponse"][1]["error"]["code"].as_u64(),
            Some(40010)
        );
    }

    #[tokio::test]
    async fn bounded_ordered_work_limits_inflight_and_preserves_input_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let results = run_bounded_ordered((0usize..12).collect(), 3, {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            move |item| {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(current, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    item
                }
            }
        })
        .await
        .expect("bounded work completes");

        assert_eq!(results, (0usize..12).collect::<Vec<_>>());
        assert!(peak.load(Ordering::SeqCst) <= 3);
    }

    #[test]
    fn presence_registry_projects_present_and_removes_leave_from_base_state() {
        let hub = AblyCompatHub::default();
        let member = AblyPresenceMessage {
            id: Some("connection:1:0".to_string()),
            action: Some(2),
            client_id: Some("client".to_string()),
            connection_id: Some("connection".to_string()),
            data: Some(json!("data")),
            encoding: None,
            timestamp: Some(1),
            extras: Some(json!({ "headers": { "key": "value" } })),
        };

        hub.presence_registry
            .register_connection("app", "connection");
        hub.presence_registry
            .enter(
                "app",
                "base",
                presence_record_from_ably(&member).expect("valid member"),
            )
            .expect("enter accepted");
        let snapshot = hub.presence_snapshot("app", "base");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].action, Some(1));
        assert_eq!(snapshot[0].extras, member.extras);

        hub.presence_registry
            .leave("app", "base", "connection", "client")
            .expect("leave accepted");
        assert!(hub.presence_snapshot("app", "base").is_empty());
    }

    #[tokio::test]
    async fn attach_gate_delivers_only_messages_after_captured_high_water() {
        let hub = AblyCompatHub::default();
        let (sender, mut receiver) = AblyOutbound::channel(
            AblyFormat::Json,
            OutboundLimits::default(),
            Arc::clone(&hub.metrics),
        );
        let channel = AblyChannelName::parse("attach-race".to_string()).unwrap();
        let attachment = AblyAttachment {
            connection_id: "connection",
            session_id: "session",
            sender: Arc::clone(&sender),
            filter: None,
            params: HashMap::new(),
            mode_flags: ABLY_DEFAULT_MODE_FLAGS,
            echo: true,
            presence: Vec::new(),
        };
        hub.begin_attach("app", &channel, &attachment)
            .await
            .unwrap();
        for serial in [1, 2] {
            hub.broadcast(
                "app",
                channel.base(),
                AblyProtocolMessage {
                    action: ACTION_MESSAGE,
                    channel: Some(channel.base().to_string()),
                    channel_serial: Some(encode_ably_channel_serial("stream-1", serial)),
                    messages: Some(vec![AblyMessage {
                        id: Some(format!("message-{serial}")),
                        ..AblyMessage::default()
                    }]),
                    ..empty_protocol_message(ACTION_MESSAGE)
                },
                None,
                None,
            );
        }
        hub.attach_clean(
            "app",
            &channel,
            attachment,
            Some(encode_ably_channel_serial("stream-1", 1)),
            Vec::new(),
        );

        let attached = receiver.recv().await.expect("ATTACHED frame");
        let attached = decode_protocol_bytes(attached.bytes.as_ref(), AblyFormat::Json).unwrap();
        assert_eq!(attached.action, ACTION_ATTACHED);
        let delivered = receiver.recv().await.expect("post-high-water message");
        let delivered = decode_protocol_bytes(delivered.bytes.as_ref(), AblyFormat::Json).unwrap();
        assert_eq!(delivered.channel_serial.as_deref(), Some("stream-1:2"));
        assert_eq!(
            delivered
                .messages
                .as_ref()
                .and_then(|messages| messages.first())
                .and_then(|message| message.id.as_deref()),
            Some("message-2")
        );
    }

    #[tokio::test]
    async fn presence_sync_chunks_110_members_in_stable_order_with_continuation() {
        let metrics = Arc::new(OutboundMetrics::default());
        let (sender, mut receiver) =
            AblyOutbound::channel(AblyFormat::Json, OutboundLimits::default(), metrics);
        let members = (0..110)
            .map(|index| AblyPresenceMessage {
                id: Some(format!("connection:{index}:0")),
                action: Some(1),
                client_id: Some(format!("client-{index:03}")),
                connection_id: Some("connection".to_string()),
                timestamp: Some(index),
                ..AblyPresenceMessage::default()
            })
            .collect::<Vec<_>>();

        send_presence_sync(&sender, "room", members);

        let first = receiver.recv().await.expect("first SYNC frame");
        let first = decode_protocol_bytes(first.bytes.as_ref(), AblyFormat::Json).unwrap();
        let second = receiver.recv().await.expect("final SYNC frame");
        let second = decode_protocol_bytes(second.bytes.as_ref(), AblyFormat::Json).unwrap();
        assert_eq!(first.action, ACTION_SYNC);
        assert_eq!(first.channel_serial.as_deref(), Some("presence:1"));
        assert_eq!(first.presence.as_ref().map(Vec::len), Some(100));
        assert_eq!(second.channel_serial.as_deref(), Some("presence:"));
        assert_eq!(second.presence.as_ref().map(Vec::len), Some(10));
        assert_eq!(
            second
                .presence
                .as_ref()
                .and_then(|presence| presence.first())
                .and_then(|member| member.client_id.as_deref()),
            Some("client-100")
        );
    }

    #[tokio::test]
    async fn attached_sets_has_presence_even_when_no_other_flags_are_present() {
        let metrics = Arc::new(OutboundMetrics::default());
        let (sender, mut receiver) =
            AblyOutbound::channel(AblyFormat::Json, OutboundLimits::default(), metrics);
        let member = AblyPresenceMessage {
            id: Some("connection:1:0".to_string()),
            action: Some(1),
            client_id: Some("client".to_string()),
            connection_id: Some("connection".to_string()),
            ..AblyPresenceMessage::default()
        };

        send_ably_attached(
            &sender,
            "room",
            None,
            None,
            None,
            vec![member],
            None,
            Vec::new(),
            None,
        );

        let attached = receiver.recv().await.expect("ATTACHED frame");
        let attached = decode_protocol_bytes(attached.bytes.as_ref(), AblyFormat::Json).unwrap();
        assert_eq!(attached.flags, Some(FLAG_HAS_PRESENCE));
        let sync = receiver.recv().await.expect("SYNC frame");
        let sync = decode_protocol_bytes(sync.bytes.as_ref(), AblyFormat::Json).unwrap();
        assert_eq!(sync.action, ACTION_SYNC);
    }

    #[test]
    fn presence_replication_converges_two_isolated_node_registries() {
        let node_a = AblyCompatHub::default();
        let node_b = AblyCompatHub::default();
        let record = |connection_id: &str| PresenceRecord {
            connection_id: connection_id.to_string(),
            client_id: "shared-client".to_string(),
            id: format!("{connection_id}:1:0"),
            data: Some(json!({ "node": connection_id })),
            encoding: Some("json".to_string()),
            extras: None,
            timestamp_ms: 1,
        };
        let enter = |connection_id: &str| PresenceChange {
            action: PresenceChangeAction::Enter,
            member: record(connection_id),
            wire_id: Some(format!("{connection_id}:1:0")),
        };
        let enters = PresenceReplication {
            changes: vec![enter("connection-a"), enter("connection-b")],
            unregister_connection: None,
        };

        node_a.replicate_presence("app", "room", &enters).unwrap();
        node_b.replicate_presence("app", "room", &enters).unwrap();
        assert_eq!(node_a.presence_registry.snapshot("app", "room").len(), 2);
        assert_eq!(
            node_a.presence_registry.snapshot("app", "room"),
            node_b.presence_registry.snapshot("app", "room")
        );

        let leaves = PresenceReplication {
            changes: ["connection-a", "connection-b"]
                .into_iter()
                .map(|connection_id| PresenceChange {
                    action: PresenceChangeAction::Leave,
                    member: record(connection_id),
                    wire_id: Some(format!("{connection_id}:2:0")),
                })
                .collect(),
            unregister_connection: None,
        };
        node_a.replicate_presence("app", "room", &leaves).unwrap();
        node_b.replicate_presence("app", "room", &leaves).unwrap();
        assert!(node_a.presence_registry.snapshot("app", "room").is_empty());
        assert!(node_b.presence_registry.snapshot("app", "room").is_empty());
    }

    #[tokio::test]
    async fn qualified_and_base_subscriptions_share_state_but_keep_wire_names() {
        let hub = AblyCompatHub::default();
        let metrics = Arc::clone(&hub.metrics);
        let (sender, mut receiver) =
            AblyOutbound::channel(AblyFormat::Json, OutboundLimits::default(), metrics);
        let base = AblyChannelName::parse("rooms:all".to_string()).expect("valid base channel");
        let qualified = AblyChannelName::parse("[filter=name == `message`]rooms:all".to_string())
            .expect("valid qualified channel");

        hub.attach_clean(
            "app",
            &base,
            AblyAttachment {
                connection_id: "connection",
                session_id: "session",
                sender: Arc::clone(&sender),
                filter: None,
                params: HashMap::new(),
                mode_flags: ABLY_DEFAULT_MODE_FLAGS,
                echo: true,
                presence: Vec::new(),
            },
            None,
            Vec::new(),
        );
        hub.attach_clean(
            "app",
            &qualified,
            AblyAttachment {
                connection_id: "connection",
                session_id: "session",
                sender: Arc::clone(&sender),
                filter: None,
                params: HashMap::new(),
                mode_flags: ABLY_DEFAULT_MODE_FLAGS,
                echo: true,
                presence: Vec::new(),
            },
            None,
            Vec::new(),
        );
        assert_eq!(
            hub.channels.len(),
            1,
            "qualifier must not create channel state"
        );
        assert_eq!(
            lock_channel_state(
                hub.channels
                    .get(&channel_key("app", "rooms:all"))
                    .expect("base state exists")
                    .value()
            )
            .subscribers
            .len(),
            2,
            "one session may attach both base and qualified identities"
        );

        for expected in [base.requested(), qualified.requested()] {
            let frame = receiver.recv().await.expect("ATTACHED frame");
            let decoded = decode_protocol_bytes(frame.bytes.as_ref(), AblyFormat::Json)
                .expect("valid JSON protocol frame");
            assert_eq!(decoded.channel.as_deref(), Some(expected));
        }

        hub.broadcast(
            "app",
            "rooms:all",
            AblyProtocolMessage {
                action: ACTION_MESSAGE,
                channel: Some("rooms:all".to_string()),
                messages: Some(Vec::new()),
                ..empty_protocol_message(ACTION_MESSAGE)
            },
            None,
            None,
        );
        let mut delivered_channels = Vec::new();
        for _ in 0..2 {
            let frame = receiver.recv().await.expect("MESSAGE frame");
            let decoded = decode_protocol_bytes(frame.bytes.as_ref(), AblyFormat::Json)
                .expect("valid JSON protocol frame");
            delivered_channels.push(decoded.channel.expect("delivery channel"));
        }
        delivered_channels.sort();
        let mut expected = vec![
            base.requested().to_string(),
            qualified.requested().to_string(),
        ];
        expected.sort();
        assert_eq!(delivered_channels, expected);
    }

    #[test]
    fn ably_key_parses_key_and_secret() {
        assert_eq!(
            parse_ably_key("app-key:secret"),
            ("app-key", Some("secret"))
        );
        assert_eq!(parse_ably_key("app-key"), ("app-key", None));
    }

    #[test]
    fn qualifier_wildcard_capability_matches_qualified_and_base_channels() {
        let capabilities = ably_capability_value_to_sockudo(&serde_json::json!({
            "[*]*": ["subscribe"]
        }))
        .expect("valid qualifier capability");
        let qualified = AblyChannelName::parse("[filter=name == `message`]chan".to_string())
            .expect("valid qualified channel");
        assert!(
            ensure_ably_channel_capability(
                Some(&capabilities),
                &qualified,
                AblyCapabilityCheck::Subscribe,
            )
            .is_ok()
        );
        let base = AblyChannelName::parse("chan".to_string()).expect("valid base channel");
        assert!(
            ensure_ably_channel_capability(
                Some(&capabilities),
                &base,
                AblyCapabilityCheck::Subscribe,
            )
            .is_ok(),
            "Ably's [*]* resource is the fixture's global channel wildcard"
        );
    }

    #[test]
    fn token_request_mac_uses_canonical_newline_terminated_input() {
        type HmacSha256 = Hmac<Sha256>;
        let input = token_request_signing_input(
            "app.key",
            Some(1234),
            Some(r#"{"chat":["subscribe"]}"#),
            Some("client"),
            1_700_000_000_000,
            "nonce",
        );
        assert_eq!(
            input,
            "app.key\n1234\n{\"chat\":[\"subscribe\"]}\nclient\n1700000000000\nnonce\n"
        );
        let mut signer = HmacSha256::new_from_slice(b"secret").unwrap();
        signer.update(input.as_bytes());
        let mac = general_purpose::STANDARD.encode(signer.finalize().into_bytes());
        assert!(verify_token_request_mac("secret", &input, &mac));
        assert!(!verify_token_request_mac("wrong", &input, &mac));
        assert!(!verify_token_request_mac("secret", "different", &mac));
    }

    #[test]
    fn rsa6_capability_intersection_preserves_equal_and_intersects_ops_and_paths() {
        let key = r#"{"channel0":["publish"],"channel2":["publish","subscribe"],"channel6":["*"]}"#;
        let (equal, _) = intersect_ably_capability(key, Some(key)).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&equal).unwrap(),
            serde_json::from_str::<serde_json::Value>(key).unwrap()
        );
        let ordered_key = r#"{"channel":["presence","publish"]}"#;
        let (reordered_equal, _) =
            intersect_ably_capability(ordered_key, Some(r#"{"channel":["publish","presence"]}"#))
                .unwrap();
        assert_eq!(reordered_equal, r#"{"channel":["publish","presence"]}"#);

        let (intersected, capabilities) = intersect_ably_capability(
            key,
            Some(r#"{"channel2":["presence","subscribe"],"missing":["publish"],"channel6":["publish","subscribe"]}"#),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&intersected).unwrap(),
            serde_json::json!({
                "channel2": ["subscribe"],
                "channel6": ["publish", "subscribe"]
            })
        );
        let capabilities = capabilities.unwrap();
        assert!(capabilities.allows_subscribe("channel2"));
        assert!(!capabilities.allows_publish("channel2"));
    }

    #[test]
    fn rsa6_rejects_empty_unknown_and_mixed_wildcard_operations() {
        let key = r#"{"*":["*"]}"#;
        for invalid in [
            r#"{"channel":[]}"#,
            r#"{"channel":["publish_"]}"#,
            r#"{"channel":["*","publish"]}"#,
        ] {
            assert!(matches!(
                intersect_ably_capability(key, Some(invalid)),
                Err(CapabilityIntersectionError::Invalid(_))
            ));
        }
    }

    #[test]
    fn key_rotation_metadata_controls_key_activity() {
        let mut key = AblyCompatKeyConfig {
            enabled: true,
            created_at_ms: Some(100),
            expires_at_ms: Some(300),
            ..Default::default()
        };
        assert!(!key_is_active(&key, 99));
        assert!(key_is_active(&key, 100));
        assert!(!key_is_active(&key, 300));
        key.revoked_at_ms = Some(200);
        assert!(!key_is_active(&key, 200));
        key.enabled = false;
        assert!(!key_is_active(&key, 150));
    }

    #[test]
    fn revocable_token_tracks_key_rotation_and_revocation() {
        let mut hub = AblyCompatHub::default();
        hub.key_registry.insert(
            "extra".to_string(),
            AblyCompatKeyConfig {
                app_id: "app".to_string(),
                key_name: "extra".to_string(),
                secret: "secret".to_string(),
                enabled: true,
                rotation_id: Some("v1".to_string()),
                ..Default::default()
            },
        );
        let record = AblyTokenRecord {
            app_id: "app".to_string(),
            key_name: "extra".to_string(),
            client_id: None,
            issued_ms: 0,
            expires_ms: 1_000,
            capabilities: None,
            revocable: true,
            rotation_id: Some("v1".to_string()),
            revocation_key: None,
        };
        assert!(token_key_is_current(&hub, &record, 100));
        hub.key_registry.get_mut("extra").unwrap().rotation_id = Some("v2".to_string());
        assert!(!token_key_is_current(&hub, &record, 100));
        hub.key_registry.get_mut("extra").unwrap().rotation_id = Some("v1".to_string());
        hub.key_registry.get_mut("extra").unwrap().revoked_at_ms = Some(100);
        assert!(!token_key_is_current(&hub, &record, 100));
    }

    #[tokio::test]
    async fn two_runtimes_share_nonce_replay_and_issued_tokens_through_cache() {
        let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
            "ably-two-node".to_string(),
            MemoryCacheOptions::default(),
        ));
        let dependencies = AblyCompatDependencies {
            cache: Some(cache),
            ..Default::default()
        };
        let first = AblyCompatRuntime::new(dependencies.clone());
        let second = AblyCompatRuntime::new(dependencies);

        assert!(first.hub.claim_nonce("key", "nonce").await.unwrap());
        assert!(!second.hub.claim_nonce("key", "nonce").await.unwrap());

        let key = ResolvedAblyKey {
            app: App::from_policy(
                "shared-app".to_string(),
                "primary".to_string(),
                "primary-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
            key_name: "extra-key".to_string(),
            secret: "secret".to_string(),
            capability: default_ably_capability(),
            capabilities: None,
            revocable_tokens: false,
            rotation_id: Some("v1".to_string()),
        };
        let details = first
            .hub
            .issue_token(
                &key,
                Some("client".to_string()),
                60_000,
                default_ably_capability(),
                None,
            )
            .await
            .unwrap();
        let record = second.hub.resolve_token(&details.token).await.unwrap();
        assert_eq!(record.app_id, "shared-app");
        assert_eq!(record.key_name, "extra-key");
        assert_eq!(record.client_id.as_deref(), Some("client"));
    }

    #[tokio::test]
    async fn ably_errors_have_the_same_shape_in_json_and_msgpack() {
        for format in [AblyFormat::Json, AblyFormat::MsgPack] {
            let response = ably_error_response_format(
                StatusCode::UNAUTHORIZED,
                40105,
                "Nonce value replayed",
                format,
            );
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let content_type = response
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            let bytes = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let body: AblyErrorBody = match format {
                AblyFormat::Json => serde_json::from_slice(&bytes).unwrap(),
                AblyFormat::MsgPack => rmp_serde::from_slice(&bytes).unwrap(),
            };
            assert_eq!(body.error.status_code, 401);
            assert_eq!(body.error.code, 40105);
            assert_eq!(body.error.message, "Nonce value replayed");
            assert_eq!(
                content_type,
                match format {
                    AblyFormat::Json => "application/json",
                    AblyFormat::MsgPack => "application/x-msgpack",
                }
            );
        }
    }

    #[test]
    fn bearer_token_accepts_raw_and_ably_base64_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer sockudo-ably-raw".parse().unwrap(),
        );
        assert_eq!(bearer_token(&headers).as_deref(), Some("sockudo-ably-raw"));

        let encoded = general_purpose::STANDARD.encode("sockudo-ably-encoded");
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {encoded}").parse().unwrap(),
        );
        assert_eq!(
            bearer_token(&headers).as_deref(),
            Some("sockudo-ably-encoded")
        );
    }

    #[test]
    fn pusher_to_ably_keeps_ai_extras_and_hides_runtime_headers() {
        let message = PusherMessage {
            event: Some("ai-input".to_string()),
            channel: Some("chat".to_string()),
            data: Some(MessageData::String("hello".to_string())),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: Some(42),
            idempotency_key: None,
            extras: Some(MessageExtras {
                ai: Some(AiExtras {
                    transport: Some(HashMap::from([(
                        "input-client-id".to_string(),
                        "client-1".to_string(),
                    )])),
                    codec: None,
                }),
                ..Default::default()
            }),
            delta_sequence: None,
            delta_conflation_key: None,
        };

        let converted = pusher_to_ably_message(&message, AblyMessageProjection::Mutation).unwrap();
        assert_eq!(converted.name.as_deref(), Some("ai-input"));
        assert_eq!(converted.serial.as_deref(), Some("42"));
        assert_eq!(converted.client_id.as_deref(), Some("client-1"));
        assert!(converted.extras.unwrap().get("ai").is_some());
    }

    #[test]
    fn stamp_ai_identity_rejects_client_id_spoofing() {
        let mut extras = Some(MessageExtras {
            ai: Some(AiExtras {
                transport: Some(HashMap::from([(
                    AI_HEADER_INPUT_CLIENT_ID.to_string(),
                    "other-client".to_string(),
                )])),
                codec: None,
            }),
            ..Default::default()
        });

        let error = stamp_ai_identity(&mut extras, AI_EVENT_INPUT, "client-1").unwrap_err();
        assert!(error.to_string().contains("authenticated clientId"));
    }

    #[test]
    fn rest_publish_payload_decodes_json_and_msgpack_arrays() {
        let messages = vec![AblyMessage {
            name: Some("chat".to_string()),
            data: Some(json!({ "ok": true })),
            encoding: Some("json".to_string()),
            client_id: Some("client-1".to_string()),
            ..Default::default()
        }];

        let json_body = sonic_rs::to_vec(&messages).unwrap();
        let decoded_json = decode_ably_publish_payload(&json_body, AblyFormat::Json).unwrap();
        assert_eq!(decoded_json[0].name.as_deref(), Some("chat"));
        assert_eq!(decoded_json[0].client_id.as_deref(), Some("client-1"));

        let msgpack_value = serde_json::json!([
            {
                "name": "chat",
                "data": { "ok": true },
                "encoding": "json",
                "clientId": "client-1"
            }
        ]);
        let msgpack_body = rmp_serde::to_vec(&msgpack_value).unwrap();
        let decoded_msgpack =
            decode_ably_publish_payload(&msgpack_body, AblyFormat::MsgPack).unwrap();
        assert_eq!(decoded_msgpack[0].name.as_deref(), Some("chat"));
        assert_eq!(decoded_msgpack[0].client_id.as_deref(), Some("client-1"));
    }

    #[test]
    fn rest_publish_payload_decodes_single_json_object() {
        let message = AblyMessage {
            name: Some("chat".to_string()),
            data: Some(json!("hello")),
            ..Default::default()
        };
        let body = sonic_rs::to_vec(&message).unwrap();
        let decoded = decode_ably_publish_payload(&body, AblyFormat::Json).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].name.as_deref(), Some("chat"));
    }

    #[cfg(feature = "push")]
    #[test]
    fn push_msgpack_payload_uses_binary_safe_wire_values() {
        let body = rmp_serde::to_vec_named(&serde_json::json!({
            "recipient": {
                "transportType": "ablyChannel",
                "channel": "device-inbox"
            },
            "notification": { "title": "hello" },
            "data": { "count": 1 }
        }))
        .unwrap();

        let request = decode_value::<AblyPushPublishRequest>(&body, AblyFormat::MsgPack).unwrap();
        let recipient = ably_push_wire_value(request.recipient).unwrap();
        assert_eq!(recipient["channel"], "device-inbox");
        assert!(matches!(
            ably_push_recipient(&recipient).unwrap(),
            PushRecipient::Realtime { channel } if channel == "device-inbox"
        ));
    }

    #[cfg(feature = "push")]
    #[tokio::test]
    async fn push_adapter_admits_to_native_pipeline_and_deduplicates() {
        use sockudo_push::{
            MemoryPushQueue, MemoryPushStore, PublishLifecycleState, PublishTarget, PushQueueStage,
        };

        let store: DynPushStore = Arc::new(MemoryPushStore::new());
        let queue: sockudo_push::DynPushQueue = Arc::new(MemoryPushQueue::new());
        let payload = ably_push_payload(
            Some(json!({ "title": "hello", "body": "world" })),
            Some(json!({ "message": "durable" })),
        )
        .unwrap();
        let target = PublishTarget::Recipient {
            recipient: PushRecipient::Realtime {
                channel: "device-inbox".to_string(),
            },
        };
        let providers = BTreeSet::from([PushProviderKind::Realtime]);

        let first = accept_ably_push_intent(
            store.clone(),
            queue.clone(),
            None,
            AblyPushIntentRequest {
                app_id: "app".to_string(),
                publish_id: "ably-publish-1".to_string(),
                targets: vec![target.clone()],
                required_providers: providers.clone(),
                payload: payload.clone(),
                expected_recipients: 1,
            },
        )
        .await
        .unwrap();
        let duplicate = accept_ably_push_intent(
            store.clone(),
            queue.clone(),
            None,
            AblyPushIntentRequest {
                app_id: "app".to_string(),
                publish_id: "ably-publish-1".to_string(),
                targets: vec![target],
                required_providers: providers,
                payload,
                expected_recipients: 1,
            },
        )
        .await
        .unwrap();

        assert_eq!(first.status.state, PublishLifecycleState::Queued);
        assert!(!first.duplicate);
        assert!(duplicate.duplicate);
        assert_eq!(first.publish_id, duplicate.publish_id);
        assert_eq!(
            queue
                .lag(PushQueueStage::PublishLog)
                .await
                .unwrap()
                .ready_depth,
            1
        );
        assert!(
            store
                .get_publish_status("app", &first.publish_id)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[cfg(feature = "push")]
    #[test]
    fn realtime_delivery_id_is_retry_stable_and_wire_bounded() {
        let job = DeliveryJob {
            app_id: "app".repeat(40),
            publish_id: "publish".repeat(40),
            provider: PushProviderKind::Realtime,
            batch_id: "batch-1".to_string(),
            device_id: Some("device-1".to_string()),
            recipient: PushRecipient::Realtime {
                channel: "recipient-channel".to_string(),
            },
            payload: Arc::new(PushPayload {
                template_id: None,
                template_data: json!({}),
                title: Some("title".to_string()),
                body: None,
                icon: None,
                sound: None,
                collapse_key: None,
            }),
            rendered_payload: None,
            attempt: 1,
            first_attempt_at_ms: None,
            not_before_ms: None,
            expires_at_ms: None,
        };
        let first = ably_realtime_delivery_id(&job);
        let mut retry = job;
        retry.batch_id = "batch-1-retry-2".to_string();
        retry.attempt = 2;

        assert_eq!(first, ably_realtime_delivery_id(&retry));
        assert!(first.len() <= 128);
    }

    #[cfg(feature = "push")]
    #[test]
    fn device_identity_token_is_scoped_and_secret_verified() {
        let token = generate_ably_device_identity_token("app/one", "device/one");
        assert_eq!(
            parse_ably_device_identity_token(token.expose_secret()),
            Some(("app/one".to_string(), "device/one".to_string()))
        );
        let hash = hash_device_identity_token(&token);
        assert!(verify_device_identity_token(token.expose_secret(), &hash));
        assert!(!verify_device_identity_token("wrong-token", &hash));
        assert!(!format!("{hash:?}").contains(hash.expose_secret()));
    }

    #[cfg(feature = "push")]
    #[tokio::test]
    async fn updating_device_preserves_identity_and_does_not_rotate_token() {
        use sockudo_push::MemoryPushStore;

        let store: DynPushStore = Arc::new(MemoryPushStore::new());
        let hub = AblyCompatHub {
            push_store: Some(store.clone()),
            ..Default::default()
        };
        let request = || {
            serde_json::from_value::<AblyPushDeviceRequest>(serde_json::json!({
                "id": "device-1",
                "clientId": "client-1",
                "deviceSecret": "ably-device-secret",
                "platform": "android",
                "formFactor": "phone",
                "push": {
                    "recipient": {
                        "transportType": "ablyChannel",
                        "channel": "device-inbox"
                    }
                }
            }))
            .unwrap()
        };

        let first = save_ably_push_device(&hub, "app-1", None, request())
            .await
            .unwrap();
        let first_hash = store
            .get_device("app-1", "device-1")
            .await
            .unwrap()
            .unwrap()
            .device_secret;
        let second = save_ably_push_device(&hub, "app-1", None, request())
            .await
            .unwrap();
        let second_hash = store
            .get_device("app-1", "device-1")
            .await
            .unwrap()
            .unwrap()
            .device_secret;

        assert!(first.get("deviceIdentityToken").is_some());
        assert!(second.get("deviceIdentityToken").is_none());
        assert_eq!(first_hash.expose_secret(), second_hash.expose_secret());
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_projection_preserves_exact_encoded_json_bytes() {
        let raw = r#"{"foo":"bar","count":2,"status":"active"}"#;
        let protocol = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            messages: Some(vec![AblyMessage {
                id: Some("message-1".to_string()),
                data: Some(json!({ "status": "active", "foo": "bar", "count": 2 })),
                encoded_json: Some(Arc::<[u8]>::from(raw.as_bytes())),
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };

        let (projected, next) = project_ably_delta_message(protocol, &AblyDeltaState::default());
        let projected = &projected.messages.unwrap()[0];
        assert_eq!(projected.data.as_ref(), Some(&json!(raw)));
        assert_eq!(projected.encoding.as_deref(), Some("json"));
        assert_eq!(next.previous_payload.as_deref(), Some(raw.as_bytes()));
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_projection_delivers_null_as_a_valid_baseline() {
        let protocol = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            messages: Some(vec![AblyMessage {
                id: Some("message-null".to_string()),
                data: None,
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };

        let (projected, next) = project_ably_delta_message(protocol, &AblyDeltaState::default());
        let projected = &projected.messages.unwrap()[0];
        assert_eq!(projected.data.as_ref(), Some(&json!("null")));
        assert_eq!(projected.encoding.as_deref(), Some("json"));
        assert_eq!(next.previous_payload.as_deref(), Some(b"null".as_slice()));
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_projection_does_not_expand_small_payloads() {
        let protocol = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            messages: Some(vec![AblyMessage {
                id: Some("message-2".to_string()),
                data: None,
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };
        let state = AblyDeltaState {
            previous_id: Some(Arc::from("message-1")),
            previous_payload: Some(Arc::<[u8]>::from(b"null".as_slice())),
            previous_at: Some(Instant::now()),
        };

        let (projected, _) = project_ably_delta_message(protocol, &state);
        let projected = &projected.messages.unwrap()[0];
        assert_eq!(projected.data.as_ref(), Some(&json!("null")));
        assert_eq!(projected.encoding.as_deref(), Some("json"));
        assert!(projected.extras.is_none());
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_projection_preserves_unsupported_encoding_and_resets_base() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"ciphertext");
        let protocol = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            messages: Some(vec![AblyMessage {
                id: Some("message-cipher".to_string()),
                data: Some(json!(encoded)),
                encoding: Some("cipher+aes-256-cbc/base64".to_string()),
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };
        let state = AblyDeltaState {
            previous_id: Some(Arc::from("message-1")),
            previous_payload: Some(Arc::<[u8]>::from(b"previous".as_slice())),
            previous_at: Some(Instant::now()),
        };

        let (projected, next) = project_ably_delta_message(protocol, &state);
        let projected = &projected.messages.unwrap()[0];
        assert_eq!(projected.data.as_ref(), Some(&json!(encoded)));
        assert_eq!(
            projected.encoding.as_deref(),
            Some("cipher+aes-256-cbc/base64")
        );
        assert!(projected.extras.is_none());
        assert!(next.previous_id.is_none());
        assert!(next.previous_payload.is_none());
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_projection_without_message_id_falls_back_to_full_delivery() {
        let protocol = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            messages: Some(vec![AblyMessage {
                data: Some(json!({ "safe": true })),
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };

        let (projected, next) = project_ably_delta_message(protocol, &AblyDeltaState::default());
        let projected = &projected.messages.unwrap()[0];
        assert_eq!(projected.data.as_ref(), Some(&json!({ "safe": true })));
        assert!(projected.encoding.is_none());
        assert!(next.previous_id.is_none());
        assert!(next.previous_payload.is_none());
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_projection_does_not_retain_oversized_base() {
        let raw = "x".repeat(64 * 1024 + 1);
        let protocol = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            messages: Some(vec![AblyMessage {
                id: Some("message-large".to_string()),
                data: Some(json!(raw)),
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };

        let (projected, next) = project_ably_delta_message(protocol, &AblyDeltaState::default());
        assert_eq!(projected.messages.unwrap()[0].data, Some(json!(raw)));
        assert!(next.previous_id.is_none());
        assert!(next.previous_payload.is_none());
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_projection_does_not_use_expired_base() {
        let raw = r#"{"foo":"bar","count":2,"status":"active"}"#;
        let protocol = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            messages: Some(vec![AblyMessage {
                id: Some("message-2".to_string()),
                data: Some(json!({ "foo": "bar", "count": 2, "status": "active" })),
                encoded_json: Some(Arc::<[u8]>::from(raw.as_bytes())),
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };
        let state = AblyDeltaState {
            previous_id: Some(Arc::from("message-1")),
            previous_payload: Some(Arc::<[u8]>::from(raw.as_bytes())),
            previous_at: Some(Instant::now() - ABLY_DELTA_BASE_MAX_AGE - Duration::from_secs(1)),
        };

        let (projected, next) = project_ably_delta_message(protocol, &state);
        let projected = &projected.messages.unwrap()[0];
        assert_eq!(projected.data.as_ref(), Some(&json!(raw)));
        assert_eq!(projected.encoding.as_deref(), Some("json"));
        assert!(projected.extras.is_none());
        assert_eq!(next.previous_id.as_deref(), Some("message-2"));
    }

    #[cfg(feature = "delta")]
    #[test]
    fn delta_projection_is_equivalent_on_json_and_msgpack_wire() {
        let body = "a".repeat(1024);
        let base = format!(r#"{{"body":"{body}","count":1}}"#);
        let target = format!(r#"{{"body":"{body}","count":2}}"#);
        let protocol = AblyProtocolMessage {
            action: ACTION_MESSAGE,
            channel: Some("room".to_string()),
            messages: Some(vec![AblyMessage {
                id: Some("message-2".to_string()),
                data: Some(json!({ "body": body, "count": 2 })),
                encoded_json: Some(Arc::<[u8]>::from(target.as_bytes())),
                ..AblyMessage::default()
            }]),
            ..empty_protocol_message(ACTION_MESSAGE)
        };
        let state = AblyDeltaState {
            previous_id: Some(Arc::from("message-1")),
            previous_payload: Some(Arc::<[u8]>::from(base.as_bytes())),
            previous_at: Some(Instant::now()),
        };

        let (projected, _) = project_ably_delta_message(protocol, &state);
        for format in [AblyFormat::Json, AblyFormat::MsgPack] {
            let encoded = encode_protocol_bytes(&projected, format).unwrap();
            let decoded = decode_protocol_bytes(&encoded, format).unwrap();
            let message = &decoded.messages.unwrap()[0];
            assert_eq!(message.encoding.as_deref(), Some("json/vcdiff/base64"));
            assert_eq!(
                message.extras.as_ref().unwrap()["delta"]["from"],
                "message-1"
            );
            assert_eq!(
                message.extras.as_ref().unwrap()["delta"]["format"],
                "vcdiff"
            );
            assert!(message.data.as_ref().is_some_and(Value::is_str));
        }
    }

    #[test]
    fn rest_msgpack_response_uses_named_message_fields() {
        let messages = vec![AblyMessage {
            name: Some("chat".to_string()),
            data: Some(json!("hello")),
            ..Default::default()
        }];
        let body = rmp_serde::to_vec_named(&messages).unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&body).unwrap();
        assert_eq!(decoded[0]["name"], "chat");
        assert_eq!(decoded[0]["data"], "hello");
    }

    #[test]
    fn realtime_msgpack_protocol_message_round_trips_named_fields() {
        let wire = serde_json::json!({
            "action": ACTION_MESSAGE,
            "channel": "chat",
            "msgSerial": 7,
            "messages": [
                {
                    "name": "chat-message",
                    "data": { "ok": true },
                    "encoding": "json"
                }
            ]
        });
        let body = rmp_serde::to_vec(&wire).unwrap();
        let decoded = decode_ably_protocol_message(&body, AblyFormat::MsgPack).unwrap();
        assert_eq!(decoded.action, ACTION_MESSAGE);
        assert_eq!(decoded.channel.as_deref(), Some("chat"));
        assert_eq!(decoded.msg_serial, Some(7));
        assert_eq!(
            decoded
                .messages
                .as_ref()
                .and_then(|messages| messages.first())
                .and_then(|message| message.name.as_deref()),
            Some("chat-message")
        );

        let encoded_body = encode_protocol_bytes(&decoded, AblyFormat::MsgPack).unwrap();
        let encoded_value: serde_json::Value =
            rmp_serde::from_slice(encoded_body.as_ref()).unwrap();
        assert_eq!(encoded_value["action"], ACTION_MESSAGE);
        assert_eq!(encoded_value["channel"], "chat");
        assert_eq!(encoded_value["msgSerial"], 7);
        assert_eq!(encoded_value["messages"][0]["name"], "chat-message");
    }

    #[test]
    fn ably_protocol_format_defaults_to_json_and_accepts_msgpack() {
        assert_eq!(parse_ably_format(None).unwrap(), AblyFormat::Json);
        assert_eq!(parse_ably_format(Some("json")).unwrap(), AblyFormat::Json);
        assert_eq!(
            parse_ably_format(Some("msgpack")).unwrap(),
            AblyFormat::MsgPack
        );
        assert!(parse_ably_format(Some("xml")).is_err());
    }

    #[test]
    fn rest_publish_rejects_message_client_id_spoofing() {
        let message = AblyMessage {
            client_id: Some("other-client".to_string()),
            ..Default::default()
        };
        let error = effective_ably_client_id(Some("client-1"), &message).unwrap_err();
        assert!(error.to_string().contains("authenticated clientId"));
    }

    #[test]
    fn ably_token_capability_maps_to_sockudo_capabilities() {
        let (_, capabilities) = normalise_ably_token_capability(Some(serde_json::json!({
            "chat:*": ["publish", "subscribe", "history"],
            "presence-chat:*": ["presence"],
            "mutable:*": ["message-update-any", "message-delete-own"],
            "object:*": ["object-subscribe"]
        })))
        .unwrap();
        let capabilities = capabilities.unwrap();

        assert!(capabilities.allows_publish("chat:one"));
        assert!(capabilities.allows_subscribe("chat:one"));
        assert!(capabilities.allows_history("chat:one"));
        assert!(!capabilities.allows_publish("other:one"));
        assert!(capabilities.presence.as_deref().is_some_and(|patterns| {
            ConnectionCapabilities::matches_any(patterns, "presence-chat:one")
        }));
        assert!(capabilities.allows_message_mutation_any(
            sockudo_core::versioned_message_auth::MutationKind::Update,
            "mutable:one"
        ));
        assert!(capabilities.allows_message_mutation_own(
            sockudo_core::versioned_message_auth::MutationKind::Delete,
            "mutable:one"
        ));
        assert!(
            ensure_ably_capability(
                Some(&capabilities),
                "other:one",
                AblyCapabilityCheck::Publish
            )
            .is_err()
        );
    }

    #[test]
    fn ably_token_capability_accepts_json_string_and_wildcard() {
        let (capability, capabilities) = normalise_ably_token_capability(Some(
            serde_json::Value::String(r#"{"*":["*"]}"#.to_string()),
        ))
        .unwrap();
        let capabilities = capabilities.unwrap();

        assert_eq!(capability.as_deref(), Some(r#"{"*":["*"]}"#));
        assert!(capabilities.allows_publish("any-channel"));
        assert!(capabilities.allows_subscribe("any-channel"));
        assert!(capabilities.allows_history("any-channel"));
        assert!(
            ensure_ably_capability(
                Some(&capabilities),
                "any-channel",
                AblyCapabilityCheck::AnyChannelAccess
            )
            .is_ok()
        );
    }

    #[test]
    fn ably_token_client_id_cannot_be_overridden() {
        assert_eq!(
            resolve_ably_token_client_id(Some("client-1".to_string()), Some("client-1")).unwrap(),
            Some("client-1".to_string())
        );
        assert!(
            resolve_ably_token_client_id(Some("client-1".to_string()), Some("other-client"))
                .is_err()
        );
        assert_eq!(
            resolve_ably_token_client_id(None, Some("client-1")).unwrap(),
            Some("client-1".to_string())
        );
        assert_eq!(
            resolve_ably_token_client_id(Some("*".to_string()), Some("client-2")).unwrap(),
            Some("client-2".to_string())
        );
        assert_eq!(
            resolve_ably_token_client_id(Some("*".to_string()), None).unwrap(),
            None
        );
    }

    #[test]
    fn connection_query_accepts_canonical_names_aliases_and_boolean_echo() {
        let canonical: AblyConnectQuery = serde_json::from_value(serde_json::json!({
            "access_token": "redacted",
            "client_id": "client",
            "echo": false
        }))
        .unwrap();
        assert_eq!(canonical.access_token.as_deref(), Some("redacted"));
        assert_eq!(canonical.client_id.as_deref(), Some("client"));
        assert!(!canonical.echo);

        let aliases: AblyConnectQuery = serde_json::from_value(serde_json::json!({
            "accessToken": "redacted",
            "clientId": "client",
            "echoMessages": true
        }))
        .unwrap();
        assert!(aliases.echo);
    }

    #[test]
    fn authorization_replacement_is_atomic_and_invalidates_stale_deadlines() {
        let app = App::from_policy(
            "app".to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            AppPolicy::default(),
        );
        let first = ResolvedAblyAuth {
            app: app.clone(),
            client_id: Some("client".to_string()),
            connection_client_id: Some("client".to_string()),
            capabilities: None,
            issued_ms: 100,
            expires_ms: Some(200),
            credential_id: "first".to_string(),
            revocable: true,
            revocation_key: None,
            #[cfg(feature = "push")]
            push_device_id: None,
        };
        let second = ResolvedAblyAuth {
            app,
            client_id: Some("client".to_string()),
            connection_client_id: Some("client".to_string()),
            capabilities: Some(restricted_ably_capabilities()),
            issued_ms: 150,
            expires_ms: Some(500),
            credential_id: "second".to_string(),
            revocable: true,
            revocation_key: Some("group".to_string()),
            #[cfg(feature = "push")]
            push_device_id: None,
        };
        let mut authorization = ConnectionAuthorization::from_resolved(&first);
        let stale_generation = authorization.generation;
        authorization.replace_from(&second);
        assert_ne!(authorization.generation, stale_generation);
        assert_eq!(authorization.credential_id, "second");
        assert_eq!(authorization.expires_ms, Some(500));
        assert_eq!(authorization.revocation_key.as_deref(), Some("group"));
    }

    #[tokio::test]
    async fn issued_before_revocation_does_not_disconnect_a_renewed_generation() {
        let hub = AblyCompatHub::default();
        let app_id = "app";
        hub.store_revocation(
            app_id,
            "clientId",
            "client",
            AblyRevocationRecord {
                target_type: "clientId".to_string(),
                target_value: "client".to_string(),
                issued_before: 200,
                applies_at: 0,
            },
        )
        .await
        .unwrap();
        let channels = HashMap::new();
        let old = ConnectionAuthorization {
            generation: 1,
            client_id: Some("client".to_string()),
            connection_client_id: Some("client".to_string()),
            capabilities: None,
            issued_ms: 100,
            expires_ms: Some(1_000),
            credential_id: "old".to_string(),
            revocable: true,
            revocation_key: None,
        };
        let mut renewed = old.clone();
        renewed.generation = 2;
        renewed.issued_ms = 201;
        renewed.credential_id = "new".to_string();
        assert!(hub.authorization_is_revoked(app_id, &old, &channels).await);
        assert!(
            !hub.authorization_is_revoked(app_id, &renewed, &channels)
                .await
        );
    }

    #[tokio::test]
    async fn revocation_is_observed_by_an_independent_runtime_through_shared_cache() {
        let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
            "ably-revocation-two-node".to_string(),
            MemoryCacheOptions::default(),
        ));
        let first = AblyCompatRuntime::new(AblyCompatDependencies {
            cache: Some(Arc::clone(&cache)),
            ..Default::default()
        });
        let second = AblyCompatRuntime::new(AblyCompatDependencies {
            cache: Some(cache),
            ..Default::default()
        });
        first
            .hub
            .store_revocation(
                "app",
                "clientId",
                "client",
                AblyRevocationRecord {
                    target_type: "clientId".to_string(),
                    target_value: "client".to_string(),
                    issued_before: 200,
                    applies_at: 0,
                },
            )
            .await
            .unwrap();
        let authorization = ConnectionAuthorization {
            generation: 1,
            client_id: Some("client".to_string()),
            connection_client_id: Some("client".to_string()),
            capabilities: None,
            issued_ms: 100,
            expires_ms: Some(1_000),
            credential_id: "credential".to_string(),
            revocable: true,
            revocation_key: None,
        };
        assert!(
            second
                .hub
                .authorization_is_revoked("app", &authorization, &HashMap::new())
                .await
        );
    }

    #[test]
    fn capability_downgrade_removes_subscribe_without_affecting_publish_upgrade() {
        let (_, subscribe_only) = normalise_ably_token_capability(Some(serde_json::json!({
            "channel": ["subscribe"]
        })))
        .unwrap();
        let subscribe_only = subscribe_only.unwrap();
        assert!(subscribe_only.allows_subscribe("channel"));
        assert!(!subscribe_only.allows_publish("channel"));

        let (_, other_channel) = normalise_ably_token_capability(Some(serde_json::json!({
            "other": ["subscribe", "publish"]
        })))
        .unwrap();
        let other_channel = other_channel.unwrap();
        assert!(!other_channel.allows_subscribe("channel"));
        assert!(other_channel.allows_publish("other"));
    }

    #[test]
    fn append_projection_uses_delta_for_mutations_and_aggregate_for_history() {
        let mut message = PusherMessage {
            event: Some("sockudo:message.append".to_string()),
            channel: Some("chat".to_string()),
            data: Some(MessageData::String("hello world".to_string())),
            name: Some("ai-output".to_string()),
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: Some("stream-1".to_string()),
            serial: Some(2),
            idempotency_key: None,
            extras: Some(MessageExtras {
                ai: Some(AiExtras {
                    transport: None,
                    codec: Some(HashMap::from([(
                        "status".to_string(),
                        "complete".to_string(),
                    )])),
                }),
                ..Default::default()
            }),
            delta_sequence: None,
            delta_conflation_key: None,
        };
        apply_runtime_metadata(
            &mut message,
            ProtocolMessageAction::Append,
            "msg:1",
            &sockudo_protocol::versioned_messages::MessageVersionMetadata {
                serial: "ver:2".to_string(),
                client_id: Some("client-1".to_string()),
                timestamp_ms: 2,
                description: None,
                metadata: None,
            },
            Some(10),
        );
        sockudo_protocol::versioned_messages::set_runtime_append_fragment(&mut message, " world");

        let mutation = pusher_to_ably_message(&message, AblyMessageProjection::Mutation).unwrap();
        assert_eq!(mutation.action, Some(MESSAGE_APPEND));
        assert_eq!(
            mutation.data.as_ref().and_then(Value::as_str),
            Some(" world")
        );
        assert_eq!(mutation.serial.as_deref(), Some("msg:1"));
        assert_eq!(
            mutation
                .version
                .as_ref()
                .map(|version| version.serial.as_str()),
            Some("ver:2")
        );

        let aggregate = pusher_to_ably_message(&message, AblyMessageProjection::Aggregate).unwrap();
        assert_eq!(aggregate.action, Some(MESSAGE_UPDATE));
        assert_eq!(
            aggregate.data.as_ref().and_then(Value::as_str),
            Some("hello world")
        );
        assert_eq!(aggregate.serial.as_deref(), Some("msg:1"));
    }

    #[test]
    fn ably_channel_serial_round_trips_stream_position() {
        let encoded = encode_ably_channel_serial("stream-1", 42);
        let parsed = parse_ably_channel_serial(&encoded).unwrap();
        assert_eq!(parsed.stream_id, "stream-1");
        assert_eq!(parsed.serial, 42);

        let failure = parse_ably_channel_serial("not-a-position").unwrap_err();
        assert_eq!(failure.code, 90005);
    }

    #[test]
    fn interest_check_does_not_create_compatibility_channel_state() {
        let hub = AblyCompatHub::default();
        assert!(!hub.has_subscribers("app", "chat"));
        assert!(hub.channels.is_empty());
    }

    #[test]
    fn constructed_runtimes_do_not_share_compatibility_state() {
        let first = Arc::new(AblyCompatRuntime::new(AblyCompatDependencies::default()));
        let second = Arc::new(AblyCompatRuntime::new(AblyCompatDependencies::default()));
        let first_state = first.hub.channel_state("app", "channel");
        let second_state = second.hub.channel_state("app", "channel");

        assert!(!Arc::ptr_eq(&first.hub, &second.hub));
        assert!(!Arc::ptr_eq(&first_state, &second_state));
    }

    #[tokio::test]
    async fn expiry_sweep_removes_expired_sessions_and_tokens() {
        let hub = AblyCompatHub::default();
        hub.sessions.insert(
            "expired-session".to_string(),
            AblySessionRecord {
                app_id: "app".to_string(),
                connection_id: "connection".to_string(),
                client_id: None,
                expires_at_ms: 10,
            },
        );
        hub.tokens.insert(
            "expired-token".to_string(),
            AblyTokenRecord {
                app_id: "app".to_string(),
                key_name: "key".to_string(),
                client_id: None,
                issued_ms: 0,
                expires_ms: 10,
                capabilities: None,
                revocable: false,
                rotation_id: None,
                revocation_key: None,
            },
        );

        hub.expire(11).await;

        assert!(hub.sessions.is_empty());
        assert!(hub.tokens.is_empty());
    }

    #[test]
    fn mutation_operation_without_serial_uses_message_serial() {
        let message: AblyMessage = sonic_rs::from_str(
            r#"{
            "serial": "stream:1",
            "action": "message.update",
            "version": {
                "clientId": "updater",
                "description": "changed",
                "metadata": {"reason": "test"}
            }
        }"#,
        )
        .unwrap();

        let version = message.version.unwrap();
        assert_eq!(version.serial, "stream:1");
        assert_eq!(version.client_id.as_deref(), Some("updater"));
        assert_eq!(version.description.as_deref(), Some("changed"));
    }

    #[test]
    fn mutation_path_is_authoritative_for_message_serial() {
        let mut absent = AblyMessage::default();
        reconcile_mutation_message_serial(&mut absent, "stream:1").unwrap();
        assert_eq!(absent.serial.as_deref(), Some("stream:1"));

        let mut matching = AblyMessage {
            serial: Some("stream:1".to_string()),
            ..AblyMessage::default()
        };
        reconcile_mutation_message_serial(&mut matching, "stream:1").unwrap();

        let mut mismatched = AblyMessage {
            serial: Some("stream:2".to_string()),
            ..AblyMessage::default()
        };
        assert!(reconcile_mutation_message_serial(&mut mismatched, "stream:1").is_err());
    }

    #[test]
    fn msgpack_binary_payload_is_projected_to_base64_without_rejection() {
        #[derive(Serialize)]
        struct BinaryMessage {
            name: &'static str,
            data: Vec<u8>,
        }
        #[derive(Serialize)]
        struct Protocol {
            action: u8,
            messages: Vec<BinaryMessage>,
        }
        let wire = Protocol {
            action: ACTION_MESSAGE,
            messages: vec![BinaryMessage {
                name: "binary",
                data: vec![0xde, 0xad, 0xbe, 0xef],
            }],
        };
        let mut body = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut body)
            .with_struct_map()
            .with_bytes(rmp_serde::config::BytesMode::ForceAll);
        wire.serialize(&mut serializer).unwrap();
        let decoded = decode_ably_protocol_message(&body, AblyFormat::MsgPack).unwrap();
        let message = decoded.messages.unwrap().pop().unwrap();
        assert_eq!(message.encoding.as_deref(), Some("base64"));
        assert_eq!(message.data.unwrap().as_str(), Some("3q2+7w=="));
    }
}
