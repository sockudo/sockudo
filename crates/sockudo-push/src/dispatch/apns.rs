use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::join_all;
use sonic_rs::prelude::*;
use sonic_rs::{Object, Value};

use super::auth::{CachedTokenProvider, auth_error};
use super::http::{
    ProviderEndpointConfig, ProviderHttpClient, ProviderHttpRequest, ProviderHttpResponse,
};
use super::outcome::{
    ProviderClassification, classify_http_result, json_field, json_request, recipient_token,
    rejected, render_payload_json, result_from_error, retryable,
};
use super::{HealthStatus, PushDispatcher};
use crate::domain::{
    ApnsChannelStoragePolicy, DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult,
    MAX_APNS_BROADCAST_PAYLOAD_BYTES, MAX_APNS_PAYLOAD_BYTES, ProviderError, ProviderFailureClass,
    PushProviderKind, PushRecipient, stable_hash,
};
use crate::pipeline::now_ms;
use crate::transform::apns_request_payload;
use crate::{PushMetrics, delivery_outcome_label};

const DEFAULT_APNS_BROADCAST_ENDPOINT: &str = "https://api-broadcast.push.apple.com";

/// ActivityKit delivery configuration shared by all APNs workers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApnsLiveActivityDispatchConfig {
    pub enabled: bool,
    pub broadcast_enabled: bool,
    pub topic: String,
    pub bundle_id: String,
    pub broadcast_base_url: String,
    pub default_expiration_secs: u64,
}

impl Default for ApnsLiveActivityDispatchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            broadcast_enabled: false,
            topic: String::new(),
            bundle_id: String::new(),
            broadcast_base_url: DEFAULT_APNS_BROADCAST_ENDPOINT.to_owned(),
            default_expiration_secs: 0,
        }
    }
}

#[derive(Clone)]
pub struct ApnsDispatcher {
    topic: String,
    endpoint: ProviderEndpointConfig,
    live_activity: ApnsLiveActivityDispatchConfig,
    token_provider: Option<CachedTokenProvider>,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
    metrics: PushMetrics,
}

impl ApnsDispatcher {
    pub fn new(
        topic: impl Into<String>,
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            topic: topic.into(),
            endpoint: ProviderEndpointConfig {
                base_url: "https://api.push.apple.com".to_owned(),
                credential_id: "apns".to_owned(),
            },
            live_activity: ApnsLiveActivityDispatchConfig::default(),
            token_provider: Some(token_provider),
            http,
            metrics: PushMetrics::default(),
        }
    }

    pub fn new_with_tls_identity(
        topic: impl Into<String>,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            topic: topic.into(),
            endpoint: ProviderEndpointConfig {
                base_url: "https://api.push.apple.com".to_owned(),
                credential_id: "apns".to_owned(),
            },
            live_activity: ApnsLiveActivityDispatchConfig::default(),
            token_provider: None,
            http,
            metrics: PushMetrics::default(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    pub fn with_live_activities(mut self, config: ApnsLiveActivityDispatchConfig) -> Self {
        self.live_activity = config;
        self
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let authorization = if let Some(token_provider) = &self.token_provider {
            Some(
                token_provider
                    .bearer_token(now_ms())
                    .await
                    .map_err(auth_error)?,
            )
        } else {
            None
        };
        let rendered = render_payload_json(PushProviderKind::Apns, job)?;
        let headers = rendered
            .get("headers")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();

        let (url, request_headers, max_payload_bytes) = match &job.recipient {
            PushRecipient::ApnsLiveActivity { activity_token } => {
                self.validate_live_activity_configuration(false)?;
                validate_apns_identifier("activity token", activity_token.expose_secret())?;
                validate_live_activity_payload(&rendered, false)?;
                let mut request_headers = self.live_activity_headers(&headers, job, false);
                request_headers.insert("apns-topic".to_owned(), self.live_activity.topic.clone());
                (
                    self.endpoint
                        .joined_url(&format!("/3/device/{}", activity_token.expose_secret())),
                    request_headers,
                    MAX_APNS_PAYLOAD_BYTES,
                )
            }
            PushRecipient::ApnsLiveActivityBroadcast {
                channel_id,
                storage_policy,
            } => {
                self.validate_live_activity_configuration(true)?;
                validate_apns_identifier("channel ID", channel_id.expose_secret())?;
                validate_live_activity_payload(&rendered, true)?;
                let mut request_headers = self.live_activity_headers(&headers, job, true);
                request_headers.insert(
                    "apns-channel-id".to_owned(),
                    channel_id.expose_secret().to_owned(),
                );
                apply_broadcast_expiration(
                    &mut request_headers,
                    *storage_policy,
                    job,
                    self.live_activity.default_expiration_secs,
                )?;
                (
                    format!(
                        "{}/4/broadcasts/apps/{}",
                        self.live_activity.broadcast_base_url.trim_end_matches('/'),
                        self.live_activity.bundle_id
                    ),
                    request_headers,
                    MAX_APNS_BROADCAST_PAYLOAD_BYTES,
                )
            }
            PushRecipient::Apns { .. } => {
                let device_token = recipient_token(&job.recipient).ok_or_else(|| {
                    provider_error(
                        "invalid_token",
                        ProviderFailureClass::DeviceTerminal,
                        "apns device token is missing",
                    )
                })?;
                validate_apns_identifier("device token", device_token)?;
                if header_string(&headers, "apns-push-type").as_deref() == Some("liveactivity") {
                    return Err(provider_error(
                        "invalid_payload",
                        ProviderFailureClass::CallerPayload,
                        "liveactivity payload requires an APNs Live Activity recipient",
                    ));
                }
                let mut request_headers = BTreeMap::new();
                request_headers.insert("apns-topic".to_owned(), self.topic.clone());
                request_headers.insert(
                    "apns-push-type".to_owned(),
                    header_string(&headers, "apns-push-type").unwrap_or_else(|| "alert".to_owned()),
                );
                let priority =
                    header_string(&headers, "apns-priority").unwrap_or_else(|| "10".to_owned());
                if !matches!(priority.as_str(), "5" | "10") {
                    return Err(provider_error(
                        "invalid_payload",
                        ProviderFailureClass::CallerPayload,
                        "APNs device priority must be 5 or 10",
                    ));
                }
                request_headers.insert("apns-priority".to_owned(), priority);
                copy_optional_header(&headers, &mut request_headers, "apns-collapse-id");
                copy_optional_header(&headers, &mut request_headers, "apns-expiration");
                (
                    self.endpoint
                        .joined_url(&format!("/3/device/{device_token}")),
                    request_headers,
                    MAX_APNS_PAYLOAD_BYTES,
                )
            }
            _ => {
                return Err(provider_error(
                    "invalid_token",
                    ProviderFailureClass::DeviceTerminal,
                    "APNs dispatcher received a non-APNs recipient",
                ));
            }
        };

        let request = json_request(
            url,
            request_headers,
            authorization,
            apns_request_payload(&rendered),
        )?;
        if request.body.len() > max_payload_bytes {
            return Err(provider_error(
                "invalid_payload",
                ProviderFailureClass::CallerPayload,
                if max_payload_bytes == MAX_APNS_BROADCAST_PAYLOAD_BYTES {
                    "APNs broadcast payload exceeds 5120 bytes"
                } else {
                    "APNs device payload exceeds 4096 bytes"
                },
            ));
        }
        Ok(request)
    }

    fn validate_live_activity_configuration(&self, broadcast: bool) -> Result<(), ProviderError> {
        if !self.live_activity.enabled {
            return Err(provider_error(
                "feature_disabled",
                ProviderFailureClass::CallerPayload,
                "APNs Live Activities are disabled",
            ));
        }
        if self.live_activity.topic.trim().is_empty() {
            return Err(provider_error(
                "invalid_configuration",
                ProviderFailureClass::CredentialAuth,
                "APNs Live Activity topic is not configured",
            ));
        }
        if broadcast
            && (!self.live_activity.broadcast_enabled
                || self.live_activity.bundle_id.trim().is_empty())
        {
            return Err(provider_error(
                "feature_disabled",
                ProviderFailureClass::CallerPayload,
                "APNs Live Activity broadcast is disabled or missing a bundle ID",
            ));
        }
        Ok(())
    }

    fn live_activity_headers(
        &self,
        rendered_headers: &Object,
        job: &DeliveryJob,
        broadcast: bool,
    ) -> BTreeMap<String, String> {
        let mut headers = BTreeMap::new();
        headers.insert("apns-push-type".to_owned(), "liveactivity".to_owned());
        headers.insert(
            "apns-priority".to_owned(),
            header_string(rendered_headers, "apns-priority").unwrap_or_else(|| "5".to_owned()),
        );
        headers.insert(
            if broadcast {
                "apns-request-id"
            } else {
                "apns-id"
            }
            .to_owned(),
            deterministic_apns_id(job),
        );
        headers
    }
}

#[async_trait]
impl PushDispatcher for ApnsDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Apns
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let activity_metric = live_activity_metric(&job);
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => {
                    let result = result_from_error(job, DeliveryOutcome::Rejected, error);
                    self.record_live_activity_metric(activity_metric, &result);
                    return result;
                }
            };
            let mut response = self.http.send(request).await;
            if self.token_provider.is_some()
                && response
                    .as_ref()
                    .is_ok_and(is_apns_expired_provider_token_response)
            {
                if let Some(token_provider) = &self.token_provider {
                    token_provider.invalidate().await;
                }
                response = match self.build_request(&job).await {
                    Ok(request) => self.http.send(request).await,
                    Err(error) => {
                        let result = result_from_error(job, DeliveryOutcome::Retryable, error);
                        self.record_live_activity_metric(activity_metric, &result);
                        return result;
                    }
                };
            }
            let result = classify_http_result(job, response, classify_apns_response);
            self.record_live_activity_metric(activity_metric, &result);
            result
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        let live_activity_ready = !self.live_activity.enabled
            || (!self.live_activity.topic.trim().is_empty()
                && (!self.live_activity.broadcast_enabled
                    || !self.live_activity.bundle_id.trim().is_empty()));
        HealthStatus {
            provider: PushProviderKind::Apns,
            healthy: !self.topic.trim().is_empty() && live_activity_ready,
            details: if self.live_activity.broadcast_enabled {
                "apns http/2 dispatcher configured with live activity broadcast".to_owned()
            } else if self.live_activity.enabled {
                "apns http/2 dispatcher configured with live activities".to_owned()
            } else {
                "apns http/2 dispatcher configured".to_owned()
            },
        }
    }
}

impl ApnsDispatcher {
    fn record_live_activity_metric(
        &self,
        activity_metric: Option<(&'static str, String)>,
        result: &DeliveryResult,
    ) {
        let Some((mode, event)) = activity_metric else {
            return;
        };
        self.metrics.counter(
            "sockudo_push_apns_live_activity_requests_total",
            &[
                ("mode", mode),
                ("event", event.as_str()),
                ("status", delivery_outcome_label(result.outcome)),
                ("app", result.app_id.as_str()),
            ],
            1,
        );
    }
}

fn live_activity_metric(job: &DeliveryJob) -> Option<(&'static str, String)> {
    let mode = match &job.recipient {
        PushRecipient::ApnsLiveActivity { .. } => "direct",
        PushRecipient::ApnsLiveActivityBroadcast { .. } => "broadcast",
        _ => return None,
    };
    let event = job
        .rendered_payload
        .as_deref()
        .and_then(|rendered| rendered.payload.get("aps"))
        .and_then(|aps| aps.get("event"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    Some((mode, event))
}

pub(super) fn classify_apns_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if (200..300).contains(&response.status) {
        return (
            DeliveryOutcome::Accepted,
            None,
            response
                .headers
                .get("apns-unique-id")
                .or_else(|| response.headers.get("apns-id"))
                .or_else(|| response.headers.get("apns-request-id"))
                .cloned(),
        );
    }
    let reason = apns_reason(response);
    match response.status {
        400 if apns_reason_matches(reason.as_deref(), &["BadDeviceToken"]) => rejected(
            "invalid_token",
            ProviderFailureClass::DeviceTerminal,
            response,
            reason.as_deref(),
        ),
        400 if apns_reason_matches(
            reason.as_deref(),
            &[
                "BadCertificateEnvironment",
                "BadTopic",
                "DeviceTokenNotForTopic",
                "TopicDisallowed",
                "FeatureNotEnabled",
            ],
        ) =>
        {
            rejected(
                "apns_topic_mismatch",
                ProviderFailureClass::CredentialAuth,
                response,
                reason.as_deref(),
            )
        }
        400 if apns_reason_matches(
            reason.as_deref(),
            &["BadChannelId", "ChannelNotRegistered", "MissingChannelId"],
        ) =>
        {
            rejected(
                "invalid_channel",
                ProviderFailureClass::CallerPayload,
                response,
                reason.as_deref(),
            )
        }
        400 => rejected(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            response,
            reason.as_deref(),
        ),
        403 if is_apns_expired_provider_token_response(response) => retryable(
            "auth_failure",
            ProviderFailureClass::CredentialAuth,
            response,
        ),
        403 => rejected(
            "auth_failure",
            ProviderFailureClass::CredentialAuth,
            response,
            reason.as_deref(),
        ),
        410 => rejected(
            "invalid_token",
            ProviderFailureClass::DeviceTerminal,
            response,
            Some("unregistered"),
        ),
        413 => rejected(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            response,
            reason.as_deref(),
        ),
        429 if apns_reason_matches(reason.as_deref(), &["TooManyProviderTokenUpdates"]) => {
            retryable(
                "auth_failure",
                ProviderFailureClass::CredentialAuth,
                response,
            )
        }
        429 => retryable("quota", ProviderFailureClass::ProviderQuota, response),
        500 | 503 => retryable_apns_unavailable(response, reason.as_deref()),
        _ => rejected(
            "provider_rejected",
            ProviderFailureClass::Unknown,
            response,
            reason.as_deref(),
        ),
    }
}

fn retryable_apns_unavailable(
    response: &ProviderHttpResponse,
    reason: Option<&str>,
) -> ProviderClassification {
    (
        DeliveryOutcome::Retryable,
        Some(ProviderError {
            class: "unavailable".to_owned(),
            failure_class: ProviderFailureClass::ProviderTransient,
            reason: reason
                .map(str::to_owned)
                .or_else(|| Some(format!("provider status {}", response.status))),
            // Apple directs providers to wait 15 minutes before retrying 5xx payloads.
            retry_after_ms: Some(now_ms().saturating_add(15 * 60 * 1_000)),
        }),
        None,
    )
}

fn validate_live_activity_payload(rendered: &Value, broadcast: bool) -> Result<(), ProviderError> {
    let headers = rendered.get("headers").and_then(Value::as_object);
    if headers
        .and_then(|headers| header_string(headers, "apns-push-type"))
        .as_deref()
        != Some("liveactivity")
    {
        return Err(provider_error(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            "APNs Live Activity payload must use apns-push-type liveactivity",
        ));
    }
    let priority = headers
        .and_then(|headers| header_string(headers, "apns-priority"))
        .unwrap_or_else(|| "5".to_owned());
    if !matches!(priority.as_str(), "5" | "10") && !(broadcast && priority == "1") {
        return Err(provider_error(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            if broadcast {
                "APNs broadcast Live Activity priority must be 1, 5, or 10"
            } else {
                "APNs direct Live Activity priority must be 5 or 10"
            },
        ));
    }
    let Some(aps) = rendered.get("aps").and_then(Value::as_object) else {
        return Err(provider_error(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            "APNs Live Activity payload requires an aps object",
        ));
    };
    if aps.get(&"timestamp").and_then(Value::as_u64).is_none()
        || !aps.get(&"content-state").is_some_and(Value::is_object)
    {
        return Err(provider_error(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            "APNs Live Activity payload requires timestamp and content-state",
        ));
    }
    let event = aps.get(&"event").and_then(Value::as_str);
    if !matches!(event, Some("start" | "update" | "end")) {
        return Err(provider_error(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            "APNs Live Activity event must be start, update, or end",
        ));
    }
    if broadcast && event == Some("start") {
        return Err(provider_error(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            "APNs broadcast notifications cannot start a Live Activity",
        ));
    }
    if event == Some("start")
        && (aps
            .get(&"attributes-type")
            .and_then(Value::as_str)
            .is_none()
            || !aps.get(&"attributes").is_some_and(Value::is_object)
            || !aps.get(&"alert").is_some_and(Value::is_object))
    {
        return Err(provider_error(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            "APNs Live Activity start requires attributes-type, attributes, and alert",
        ));
    }
    Ok(())
}

fn apply_broadcast_expiration(
    headers: &mut BTreeMap<String, String>,
    policy: ApnsChannelStoragePolicy,
    job: &DeliveryJob,
    default_expiration_secs: u64,
) -> Result<(), ProviderError> {
    let expiration = match policy {
        ApnsChannelStoragePolicy::NoStorage => 0,
        ApnsChannelStoragePolicy::MostRecent => job
            .expires_at_ms
            .map(|value| value / 1_000)
            .unwrap_or_else(|| {
                (now_ms() / 1_000).saturating_add(default_expiration_secs.min(8 * 60 * 60))
            }),
    };
    if policy == ApnsChannelStoragePolicy::MostRecent && expiration <= now_ms() / 1_000 {
        return Err(provider_error(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            "stored APNs broadcast requires a future expiration",
        ));
    }
    headers.insert("apns-expiration".to_owned(), expiration.to_string());
    Ok(())
}

fn deterministic_apns_id(job: &DeliveryJob) -> String {
    let digest = stable_hash(
        format!(
            "{}:{}:{}:{}",
            job.app_id,
            job.publish_id,
            job.batch_id,
            job.recipient.token_hash()
        )
        .as_bytes(),
    );
    format!(
        "{}-{}-{}-{}-{}",
        &digest[0..8],
        &digest[8..12],
        &digest[12..16],
        &digest[16..20],
        &digest[20..32]
    )
}

fn validate_apns_identifier(label: &str, value: &str) -> Result<(), ProviderError> {
    let valid = if label == "channel ID" {
        value.len() <= 4_096
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'+' | b'/' | b'=')
            })
    } else {
        value.len() <= 512
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    };
    if value.is_empty() || !valid {
        return Err(provider_error(
            "invalid_token",
            ProviderFailureClass::CallerPayload,
            if label == "channel ID" {
                "APNs channel ID has an invalid format"
            } else if label == "activity token" {
                "APNs Live Activity token has an invalid format"
            } else {
                "APNs device token has an invalid format"
            },
        ));
    }
    Ok(())
}

fn copy_optional_header(source: &Object, destination: &mut BTreeMap<String, String>, name: &str) {
    if let Some(value) = header_string(source, name) {
        destination.insert(name.to_owned(), value);
    }
}

fn provider_error(class: &str, failure_class: ProviderFailureClass, reason: &str) -> ProviderError {
    ProviderError {
        class: class.to_owned(),
        failure_class,
        reason: Some(reason.to_owned()),
        retry_after_ms: None,
    }
}

fn header_string(map: &Object, name: &str) -> Option<String> {
    map.get(&name).and_then(|value| {
        value
            .as_str()
            .map(str::to_owned)
            .or_else(|| value.is_number().then(|| value.to_string()))
    })
}

fn is_apns_expired_provider_token_response(response: &ProviderHttpResponse) -> bool {
    response.status == 403
        && String::from_utf8_lossy(&response.body)
            .to_ascii_lowercase()
            .contains("expiredprovidertoken")
}

fn apns_reason(response: &ProviderHttpResponse) -> Option<String> {
    json_field(&response.body, &["reason"]).or_else(|| json_field(&response.body, &["error"]))
}

fn apns_reason_matches(reason: Option<&str>, values: &[&str]) -> bool {
    let Some(reason) = reason else {
        return false;
    };
    values
        .iter()
        .any(|value| reason.eq_ignore_ascii_case(value))
}
