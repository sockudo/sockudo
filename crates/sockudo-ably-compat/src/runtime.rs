//! Reduced Ably REST and Realtime compatibility for Ably AI Transport tests.
//!
//! This is an additive compatibility surface. Sockudo and Pusher clients still
//! use their native routes and protocol frames; Ably ProtocolMessages are
//! translated at the edge into Sockudo's existing publish, history, and version
//! stores.

mod auth_runtime;
mod publishing;
#[cfg(feature = "push")]
mod push;
mod realtime;
mod rest;
mod wire;

use auth_runtime::*;
use publishing::*;
#[cfg(feature = "push")]
use push::*;
use realtime::*;
use rest::*;
use wire::*;

pub use rest::ably_stats;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit as AeadKeyInit, Payload},
};
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
    error::{Error as SockudoError, Result as SockudoResult},
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
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
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

fn decode_presence_record(value: &Value) -> Option<PresenceRecord> {
    let encoded = sonic_rs::to_vec(value).ok()?;
    sonic_rs::from_slice(&encoded).ok()
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
const ABLY_DELIVERY_DEDUPE_MAX_ENTRIES: usize = ABLY_COMPAT_MAX_REPLAY_MESSAGES;
const ABLY_REVOCATION_MAX_ENTRIES: usize = ABLY_COMPAT_MAX_TOKENS;
const ABLY_REVOCATION_MAX_BYTES: usize = 64 * 1024 * 1024;
const ABLY_CONNECTION_KEY_MAX_BYTES: usize = 512;
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
    delivery_frontier: u64,
    pending_deliveries: HashSet<u64>,
    delivery_reset_required: bool,
    last_touched_ms: i64,
}

impl AblyChannelState {
    fn establish_delivery_frontier(&mut self, position: &AblyChannelPosition) {
        let stream_changed = self
            .current_stream_id
            .as_deref()
            .is_some_and(|current| current != position.stream_id);
        if stream_changed || self.delivery_reset_required {
            self.pending_deliveries.clear();
            self.delivery_frontier = position.serial;
        } else if position.serial > self.delivery_frontier {
            self.delivery_frontier = position.serial;
            let frontier = self.delivery_frontier;
            self.pending_deliveries.retain(|serial| *serial > frontier);
            while let Some(next) = self.delivery_frontier.checked_add(1) {
                if !self.pending_deliveries.remove(&next) {
                    break;
                }
                self.delivery_frontier = next;
            }
        }
        self.current_stream_id = Some(position.stream_id.clone());
        self.delivery_reset_required = false;
    }
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

struct StoredAblyRevocation {
    app_id: String,
    record: AblyRevocationRecord,
    expires_at_ms: i64,
    bytes: usize,
}

struct AblyRevocationStore {
    entries: HashMap<String, StoredAblyRevocation>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl AblyRevocationStore {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn prune_expired(&mut self, now_ms: i64) {
        let mut removed_bytes = 0usize;
        self.entries.retain(|_, stored| {
            let retain = stored.expires_at_ms > now_ms;
            if !retain {
                removed_bytes = removed_bytes.saturating_add(stored.bytes);
            }
            retain
        });
        self.bytes = self.bytes.saturating_sub(removed_bytes);
    }

    fn insert(
        &mut self,
        app_id: &str,
        key: String,
        record: AblyRevocationRecord,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<(), AblyAuthError> {
        self.prune_expired(now_ms);
        let bytes = std::mem::size_of::<StoredAblyRevocation>()
            .checked_add(app_id.len())
            .and_then(|bytes| bytes.checked_add(key.len()))
            .and_then(|bytes| bytes.checked_add(record.target_type.len()))
            .and_then(|bytes| bytes.checked_add(record.target_value.len()))
            .ok_or_else(AblyAuthError::internal)?;
        let replaced_bytes = self.entries.get(&key).map_or(0, |stored| stored.bytes);
        let next_bytes = self
            .bytes
            .checked_sub(replaced_bytes)
            .and_then(|current| current.checked_add(bytes))
            .ok_or_else(AblyAuthError::internal)?;
        let next_entries = self.entries.len() + usize::from(!self.entries.contains_key(&key));
        if next_entries > self.max_entries || next_bytes > self.max_bytes {
            return Err(AblyAuthError::revocation_capacity());
        }
        self.entries.insert(
            key,
            StoredAblyRevocation {
                app_id: app_id.to_string(),
                record,
                expires_at_ms,
                bytes,
            },
        );
        self.bytes = next_bytes;
        Ok(())
    }

    fn get(&mut self, key: &str, now_ms: i64) -> Option<AblyRevocationRecord> {
        self.prune_expired(now_ms);
        self.entries.get(key).map(|stored| stored.record.clone())
    }

    fn records(&mut self, app_id: &str, now_ms: i64) -> Vec<AblyRevocationRecord> {
        self.prune_expired(now_ms);
        self.entries
            .values()
            .filter(|stored| stored.app_id == app_id)
            .map(|stored| stored.record.clone())
            .collect()
    }
}

impl Default for AblyRevocationStore {
    fn default() -> Self {
        Self::new(ABLY_REVOCATION_MAX_ENTRIES, ABLY_REVOCATION_MAX_BYTES)
    }
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

    fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: 50000,
            message: "Internal server error".to_string(),
        }
    }

    fn backend_unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50000,
            message: "Authentication service temporarily unavailable".to_string(),
        }
    }

    fn revocation_capacity() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50000,
            message: "Token revocation capacity exceeded".to_string(),
        }
    }
}

fn auth_internal_failure(operation: &'static str) -> AblyAuthError {
    warn!(operation, "Ably authentication internal operation failed");
    AblyAuthError::internal()
}

fn auth_backend_failure(operation: &'static str) -> AblyAuthError {
    warn!(operation, "Ably authentication dependency is unavailable");
    AblyAuthError::backend_unavailable()
}

#[derive(Default)]
pub struct AblyCompatHub {
    channels: DashMap<AblyChannelKey, Arc<Mutex<AblyChannelState>>>,
    sessions: DashMap<String, AblySessionRecord>,
    tokens: DashMap<String, AblyTokenRecord>,
    revocations: Mutex<AblyRevocationStore>,
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

    #[cfg(feature = "bench")]
    pub(crate) fn register_benchmark_subscriber(
        &self,
        format: AblyFormat,
        session_id: &str,
        channel: &str,
    ) -> crate::outbound::AblyOutboundReceiver {
        let (sender, receiver) = AblyOutbound::channel(
            format,
            OutboundLimits {
                control_messages: 2,
                data_messages: 1,
                control_bytes: 16 * 1024,
                data_bytes: 128 * 1024,
            },
            Arc::clone(&self.hub.metrics),
        );
        let state = self.hub.channel_state("benchmark-app", channel);
        lock_channel_state(&state).subscribers.insert(
            subscriber_key(session_id, channel),
            AblySubscriber {
                connection_id: Arc::from(session_id),
                sender,
                filter: None,
                mode_flags: ABLY_DEFAULT_MODE_FLAGS,
                echo: true,
                #[cfg(feature = "delta")]
                delta: None,
                attach_gate: None,
            },
        );
        receiver
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
        if !self.accept_delivery_position(app_id, channel, envelope)? {
            debug!(
                app_id = %app_id,
                channel = %channel,
                stream_id = ?envelope.stream_id,
                delivery_serial = ?envelope.delivery_serial,
                "suppressed duplicate compatibility delivery"
            );
            return Ok(());
        }
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
    fn accept_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
        envelope: &MessageEnvelope,
    ) -> SockudoResult<bool> {
        let Some((stream_id, serial)) = envelope.stream_id.as_deref().zip(envelope.delivery_serial)
        else {
            return Ok(true);
        };
        let state = self.channel_state(app_id, channel);
        let mut state = lock_channel_state(&state);
        if state
            .current_stream_id
            .as_deref()
            .is_some_and(|current| current != stream_id)
        {
            state.current_stream_id = Some(stream_id.to_string());
            state.delivery_frontier = 0;
            state.pending_deliveries.clear();
            state.delivery_reset_required = false;
        } else if state.current_stream_id.is_none() {
            state.current_stream_id = Some(stream_id.to_string());
        }
        if state.delivery_reset_required {
            return Err(SockudoError::BufferFull(format!(
                "compatibility delivery continuity for {app_id}/{channel} requires reset"
            )));
        }
        if serial <= state.delivery_frontier || state.pending_deliveries.contains(&serial) {
            self.metrics
                .duplicate_suppression
                .fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }

        if serial == state.delivery_frontier.saturating_add(1) {
            state.delivery_frontier = serial;
            while let Some(next) = state.delivery_frontier.checked_add(1) {
                if !state.pending_deliveries.remove(&next) {
                    break;
                }
                state.delivery_frontier = next;
            }
            return Ok(true);
        }

        if state.pending_deliveries.len() >= ABLY_DELIVERY_DEDUPE_MAX_ENTRIES {
            state.delivery_reset_required = true;
            self.metrics.overflow.fetch_add(1, Ordering::Relaxed);
            self.metrics.continuity_lost.fetch_add(1, Ordering::Relaxed);
            self.metrics.reset_required.fetch_add(1, Ordering::Relaxed);
            return Err(SockudoError::BufferFull(format!(
                "compatibility delivery reorder window for {app_id}/{channel} exceeded {ABLY_DELIVERY_DEDUPE_MAX_ENTRIES} entries"
            )));
        }
        state.pending_deliveries.insert(serial);
        Ok(true)
    }

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
        lock_revocations(&self.revocations).prune_expired(now);

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
                .map_err(|_| auth_backend_failure("nonce cache claim"));
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
            let encoded = serde_json::to_string(&record)
                .map_err(|_| auth_internal_failure("revocation record serialization"))?;
            let ttl_seconds = u64::try_from(self.config.max_token_ttl_ms)
                .map_err(|_| auth_internal_failure("revocation retention conversion"))?
                .div_ceil(1_000)
                .max(1);
            cache
                .set(&key, &encoded, ttl_seconds)
                .await
                .map_err(|_| auth_backend_failure("revocation cache write"))?;
            return Ok(());
        }
        let now = now_ms();
        let expires_at_ms = now
            .max(record.issued_before)
            .checked_add(self.config.max_token_ttl_ms)
            .ok_or_else(|| auth_internal_failure("revocation retention calculation"))?;
        lock_revocations(&self.revocations).insert(app_id, key, record, expires_at_ms, now)
    }

    async fn revocation(
        &self,
        app_id: &str,
        target_type: &str,
        target_value: &str,
    ) -> Result<Option<AblyRevocationRecord>, AblyAuthError> {
        let key = revocation_cache_key(app_id, target_type, target_value);
        if let Some(cache) = &self.cache {
            let Some(encoded) = cache
                .get(&key)
                .await
                .map_err(|_| auth_backend_failure("revocation cache read"))?
            else {
                return Ok(None);
            };
            let record = serde_json::from_str::<AblyRevocationRecord>(&encoded)
                .map_err(|_| auth_backend_failure("revocation cache decode"))?;
            return Ok(Some(record));
        }
        Ok(lock_revocations(&self.revocations).get(&key, now_ms()))
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
            match self.revocation(app_id, target_type, target_value).await {
                Ok(Some(record))
                    if authorization.issued_ms < record.issued_before
                        && now >= record.applies_at =>
                {
                    return true;
                }
                Ok(_) => {}
                Err(error) => {
                    self.metrics.mark_backend_failure();
                    warn!(
                        app_id = %app_id,
                        error = %error.message,
                        "revocation backend read failed; authorization rejected"
                    );
                    return true;
                }
            }
        }
        if let Some(cache) = &self.cache {
            return match self
                .cached_channel_revocation_applies(cache.as_ref(), app_id, authorization, now)
                .await
            {
                Ok(revoked) => revoked,
                Err(error) => {
                    self.metrics.mark_backend_failure();
                    warn!(
                        app_id = %app_id,
                        error = %error.message,
                        "revocation backend scan failed; authorization rejected"
                    );
                    true
                }
            };
        }
        lock_revocations(&self.revocations)
            .records(app_id, now)
            .iter()
            .any(|record| channel_revocation_applies(record, authorization, now))
    }

    async fn cached_channel_revocation_applies(
        &self,
        cache: &dyn CacheManager,
        app_id: &str,
        authorization: &ConnectionAuthorization,
        now: i64,
    ) -> Result<bool, AblyAuthError> {
        const PAGE_SIZE: usize = 512;
        const MAX_PAGES: usize = ABLY_REVOCATION_MAX_ENTRIES.div_ceil(PAGE_SIZE) + 1;

        let prefix = format!("ably-compat:revocation:{app_id}:");
        let mut cursor = None;
        let mut entries_seen = 0usize;
        let mut bytes_seen = 0usize;
        for _ in 0..MAX_PAGES {
            let page = cache
                .scan_prefix_page(&prefix, cursor.clone(), PAGE_SIZE)
                .await
                .map_err(|_| auth_backend_failure("revocation cache scan"))?;
            if page.entries.len() > PAGE_SIZE {
                return Err(auth_backend_failure("revocation cache page bound"));
            }
            entries_seen = entries_seen
                .checked_add(page.entries.len())
                .filter(|count| *count <= ABLY_REVOCATION_MAX_ENTRIES)
                .ok_or_else(AblyAuthError::revocation_capacity)?;
            for (key, encoded) in page.entries {
                bytes_seen = bytes_seen
                    .checked_add(key.len())
                    .and_then(|bytes| bytes.checked_add(encoded.len()))
                    .filter(|bytes| *bytes <= ABLY_REVOCATION_MAX_BYTES)
                    .ok_or_else(AblyAuthError::revocation_capacity)?;
                let record = serde_json::from_str::<AblyRevocationRecord>(&encoded)
                    .map_err(|_| auth_backend_failure("revocation cache decode"))?;
                if channel_revocation_applies(&record, authorization, now) {
                    return Ok(true);
                }
            }
            match page.next_cursor {
                Some(next) if !next.is_empty() && cursor.as_deref() != Some(next.as_str()) => {
                    cursor = Some(next);
                }
                Some(_) => return Err(auth_backend_failure("revocation cache cursor")),
                None => return Ok(false),
            }
        }
        Err(AblyAuthError::revocation_capacity())
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
            state.establish_delivery_frontier(&position);
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
                    self.metrics.data_encoded.fetch_add(1, Ordering::Relaxed);
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
            self.metrics.data_encoded.fetch_add(1, Ordering::Relaxed);
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
        {
            let state = self.channel_state(app_id, channel.base());
            lock_channel_state(&state).establish_delivery_frontier(&high_water);
        }
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
        if !is_well_formed_connection_key(requested_key) {
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    80018,
                    "invalid connection id (invalid format)",
                ),
            };
        }
        let now = now_ms();
        let record = if let Some(cache) = &self.cache {
            match cache.get(&session_cache_key(requested_key)).await {
                Ok(Some(encoded)) => match serde_json::from_str::<AblySessionRecord>(&encoded) {
                    Ok(record) => {
                        self.sessions
                            .insert(requested_key.to_string(), record.clone());
                        Some(record)
                    }
                    Err(error) => {
                        self.sessions.remove(requested_key);
                        self.metrics.mark_backend_failure();
                        warn!(error = %error, "Ably connection state cache record was invalid");
                        None
                    }
                },
                Ok(None) => {
                    self.sessions.remove(requested_key);
                    None
                }
                Err(error) => {
                    self.sessions.remove(requested_key);
                    self.metrics.mark_backend_failure();
                    warn!(error = %error, "Ably connection state cache read failed");
                    None
                }
            }
        } else {
            self.sessions
                .get(requested_key)
                .map(|record| record.clone())
        };
        let Some(record) = record else {
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    80008,
                    "unable to recover connection (connection expired)",
                ),
            };
        };
        if record.expires_at_ms <= now {
            self.sessions.remove(requested_key);
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    80008,
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

    async fn replace_connection_key(
        &self,
        previous_connection_key: &str,
        connection_key: String,
        app_id: &str,
        connection_id: &str,
        client_id: Option<String>,
    ) {
        self.remember_connection(connection_key, app_id, connection_id, client_id)
            .await;
        self.forget_connection(previous_connection_key).await;
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

    async fn claim_session_owner(
        &self,
        app_id: &str,
        connection_id: &str,
        session_id: &str,
    ) -> Result<(), AblyAuthError> {
        let Some(cache) = &self.cache else {
            return Ok(());
        };
        cache
            .set(
                &session_owner_cache_key(app_id, connection_id),
                session_id,
                DEFAULT_CONNECTION_STATE_TTL_MS.div_ceil(1_000),
            )
            .await
            .map_err(|_| auth_backend_failure("session ownership cache claim"))
    }

    async fn refresh_session_owner(
        &self,
        app_id: &str,
        connection_id: &str,
        session_id: &str,
    ) -> bool {
        let Some(cache) = &self.cache else {
            return self
                .live_sessions
                .get(connection_id)
                .is_some_and(|session| session.session_id == session_id);
        };
        match cache
            .compare_and_swap(
                &session_owner_cache_key(app_id, connection_id),
                session_id,
                session_id,
                DEFAULT_CONNECTION_STATE_TTL_MS.div_ceil(1_000),
            )
            .await
        {
            Ok(current) => current,
            Err(error) => {
                self.metrics.mark_backend_failure();
                warn!(app_id = %app_id, error = %error, "session owner refresh failed");
                false
            }
        }
    }

    async fn session_is_current(
        &self,
        app_id: &str,
        connection_id: &str,
        session_id: &str,
    ) -> bool {
        let Some(cache) = &self.cache else {
            return self
                .live_sessions
                .get(connection_id)
                .is_some_and(|session| session.session_id == session_id);
        };
        match cache
            .get(&session_owner_cache_key(app_id, connection_id))
            .await
        {
            Ok(Some(owner)) => owner == session_id,
            Ok(None) => false,
            Err(error) => {
                self.metrics.mark_backend_failure();
                warn!(app_id = %app_id, error = %error, "session owner read failed");
                false
            }
        }
    }

    async fn release_session_owner(&self, app_id: &str, connection_id: &str, session_id: &str) {
        let Some(cache) = &self.cache else {
            return;
        };
        if let Err(error) = cache
            .compare_and_remove(&session_owner_cache_key(app_id, connection_id), session_id)
            .await
        {
            self.metrics.mark_backend_failure();
            warn!(app_id = %app_id, error = %error, "session owner release failed");
        }
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
        let expires = issued
            .checked_add(ttl_ms)
            .ok_or_else(|| auth_internal_failure("token expiry calculation"))?;
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
            let encoded = serde_json::to_string(&record)
                .map_err(|_| auth_internal_failure("token record serialization"))?;
            let ttl_seconds = u64::try_from(ttl_ms)
                .map_err(|_| auth_internal_failure("token retention conversion"))?
                .div_ceil(1_000);
            cache
                .set(&token_cache_key(&token), &encoded, ttl_seconds.max(1))
                .await
                .map_err(|_| auth_backend_failure("token cache write"))?;
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

pub async fn ably_time(Query(query): Query<AblyRestQuery>, headers: HeaderMap) -> Response {
    let format = ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    encode_ably_rest_response(StatusCode::OK, format, &vec![now_ms()])
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

fn lock_revocations(
    revocations: &Mutex<AblyRevocationStore>,
) -> std::sync::MutexGuard<'_, AblyRevocationStore> {
    revocations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn channel_revocation_applies(
    record: &AblyRevocationRecord,
    authorization: &ConnectionAuthorization,
    now: i64,
) -> bool {
    record.target_type == "channel"
        && authorization.issued_ms < record.issued_before
        && now >= record.applies_at
        && ensure_ably_capability(
            authorization.capabilities.as_ref(),
            &record.target_value,
            AblyCapabilityCheck::AnyChannelAccess,
        )
        .is_ok()
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

#[cfg(feature = "bench")]
pub(crate) fn benchmark_project_envelope(
    channel: &str,
    message: &PusherMessage,
    envelope: &MessageEnvelope,
    projection: AblyMessageProjection,
    channel_serial: Option<String>,
) -> Result<AblyProtocolMessage, String> {
    ably_protocol_message_from_envelope(channel, message, envelope, projection, channel_serial)
        .map_err(|failure| failure.message)
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

#[cfg(feature = "fuzzing")]
pub mod fuzzing {
    use super::*;

    const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

    pub fn protocol_and_rest(data: &[u8], msgpack: bool) {
        if data.len() > MAX_FUZZ_INPUT_BYTES {
            return;
        }
        let format = if msgpack {
            AblyFormat::MsgPack
        } else {
            AblyFormat::Json
        };
        if let Ok(message) = decode_ably_protocol_message(data, format)
            && let Ok(encoded) = encode_protocol_bytes(&message, format)
        {
            let decoded = decode_ably_protocol_message(&encoded, format)
                .expect("a compatibility frame must decode after encoding");
            let before = sonic_rs::to_vec(&message).expect("decoded frame must serialize");
            let after = sonic_rs::to_vec(&decoded).expect("round-tripped frame must serialize");
            assert_eq!(
                before, after,
                "compatibility wire round trip changed a frame"
            );
        }
        let _ = decode_ably_publish_payload(data, format);
        let _ = decode_value::<AblyBatchPublishBody>(data, format);
        let _ = decode_value::<Vec<AblyMessage>>(data, format);
    }

    pub fn channel_filter(data: &[u8]) {
        if data.len() > 16 * 1024 {
            return;
        }
        let Ok(raw) = std::str::from_utf8(data) else {
            return;
        };
        if let Ok(channel) = AblyChannelName::parse(raw.to_string())
            && let Ok(Some(source)) = AblyMessageFilter::source_from_channel(&channel)
        {
            let _ = AblyMessageFilter::compile(&source);
        }
    }

    pub fn auth(data: &[u8]) {
        if data.len() > 16 * 1024 {
            return;
        }
        let Ok(raw) = std::str::from_utf8(data) else {
            return;
        };
        let (key_capability, requested_capability) = raw
            .split_once('\0')
            .map_or((raw, None), |(key, requested)| (key, Some(requested)));
        let _ = intersect_ably_capability(key_capability, requested_capability);
        let _ = parse_ably_jose_header(raw);
        let _ = verify_ably_signed_jwt(raw, "fuzz.key", "fuzz-secret");
        let _ = decrypt_ably_compact_jwe(raw, "fuzz.key", "fuzz-secret");
        let _ = verify_token_request_mac("fuzz-secret", raw, raw);
        let value = serde_json::from_str::<serde_json::Value>(raw).ok();
        let _ = parse_token_request_integer(value.as_ref(), "fuzz");
    }

    pub fn continuity_and_state(data: &[u8]) {
        if data.len() > 16 * 1024 {
            return;
        }
        let Ok(raw) = std::str::from_utf8(data) else {
            return;
        };
        let _ = parse_ably_channel_serial(raw);
        let _ = decode_presence_snapshot_cursor(Some(raw));
        let _ = parse_message_serial(raw);
        if let Ok(message_serial) = MessageSerial::new("fuzz-message") {
            let _ = decode_ably_annotation_cursor(raw, "fuzz-app", "fuzz-channel", &message_serial);
        }
        let _ = serde_json::from_str::<AblyConnectQuery>(raw);
    }
}

fn send_protocol(sender: &AblySender, message: AblyProtocolMessage) {
    if let Err(error) = sender.send_protocol(&message, OutboundPriority::Control) {
        debug!(error = %error, "Ably compatibility outbound queue is unavailable");
    }
}

#[cfg(test)]
mod tests;
