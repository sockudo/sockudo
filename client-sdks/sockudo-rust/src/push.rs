//! Push notification registration and publishing (feature `http`).
//!
//! [`PushRegistration`] talks to **your backend's** push endpoint, which
//! forwards to the Sockudo push API with server credentials. Device tokens
//! are credentials: they are never logged by this client.

use std::collections::BTreeMap;
use std::fmt;

use reqwest::Method;
use serde::Serialize;
use sonic_rs::Value;

use crate::auth::Endpoint;
use crate::error::{Error, Result};

/// Push provider family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PushProviderKind {
    /// Firebase Cloud Messaging.
    Fcm,
    /// Apple Push Notification service.
    Apns,
    /// Web Push.
    WebPush,
    /// Huawei Mobile Services.
    Hms,
    /// Windows Notification Service.
    Wns,
}

/// APNs channel storage policy for broadcast Live Activities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApnsChannelStoragePolicy {
    /// Do not store.
    NoStorage,
    /// Keep the most recent message.
    MostRecent,
}

/// Where a notification is delivered.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "transportType", rename_all = "camelCase")]
pub enum PushRecipient {
    /// FCM registration token.
    #[serde(rename = "gcm", rename_all = "camelCase")]
    Gcm {
        /// Registration token.
        registration_token: String,
    },
    /// APNs device token.
    #[serde(rename = "apns", rename_all = "camelCase")]
    Apns {
        /// Device token.
        device_token: String,
    },
    /// APNs Live Activity push token.
    #[serde(rename = "apnsLiveActivity", rename_all = "camelCase")]
    ApnsLiveActivity {
        /// Activity token.
        activity_token: String,
    },
    /// APNs broadcast channel for Live Activities.
    #[serde(rename = "apnsLiveActivityBroadcast", rename_all = "camelCase")]
    ApnsLiveActivityBroadcast {
        /// Channel id.
        channel_id: String,
        /// Storage policy.
        storage_policy: ApnsChannelStoragePolicy,
    },
    /// Web Push subscription.
    #[serde(rename = "web", rename_all = "camelCase")]
    Web {
        /// Push service endpoint.
        endpoint: String,
        /// Client public key.
        p256dh: String,
        /// Auth secret.
        auth: String,
    },
    /// HMS registration token.
    #[serde(rename = "hms", rename_all = "camelCase")]
    Hms {
        /// Registration token.
        registration_token: String,
    },
    /// WNS channel URI.
    #[serde(rename = "wns", rename_all = "camelCase")]
    Wns {
        /// Channel URI.
        channel_uri: String,
    },
}

impl fmt::Debug for PushRecipient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Gcm { .. } => "gcm",
            Self::Apns { .. } => "apns",
            Self::ApnsLiveActivity { .. } => "apnsLiveActivity",
            Self::ApnsLiveActivityBroadcast { .. } => "apnsLiveActivityBroadcast",
            Self::Web { .. } => "web",
            Self::Hms { .. } => "hms",
            Self::Wns { .. } => "wns",
        };
        write!(f, "PushRecipient({kind}, <redacted>)")
    }
}

/// Push state of a device registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct PushDevicePush {
    /// Recipient.
    pub recipient: PushRecipient,
    /// Registration state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// Consecutive failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_count: Option<u32>,
    /// Last error reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_reason: Option<String>,
}

/// A device registration.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct PushDeviceDetails {
    /// Device id.
    pub id: String,
    /// Client id the device belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// `phone`, `tablet`, `desktop`, …
    pub form_factor: String,
    /// `ios`, `android`, `browser`, …
    pub platform: String,
    /// Free-form metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// IANA timezone.
    pub timezone: String,
    /// BCP 47 locale.
    pub locale: String,
    /// Last activity (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_active_at_ms: Option<u64>,
    /// Push details.
    pub push: PushDevicePush,
}

impl PushDeviceDetails {
    /// A registration with the required fields.
    pub fn new(
        id: impl Into<String>,
        platform: impl Into<String>,
        form_factor: impl Into<String>,
        recipient: PushRecipient,
    ) -> Self {
        Self {
            id: id.into(),
            client_id: None,
            form_factor: form_factor.into(),
            platform: platform.into(),
            metadata: None,
            timezone: "UTC".into(),
            locale: "en-US".into(),
            last_active_at_ms: None,
            push: PushDevicePush {
                recipient,
                state: None,
                failure_count: None,
                error_reason: None,
            },
        }
    }

    /// Set the client id (builder style).
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Set timezone and locale (builder style).
    pub fn locale(mut self, timezone: impl Into<String>, locale: impl Into<String>) -> Self {
        self.timezone = timezone.into();
        self.locale = locale.into();
        self
    }

    /// Set metadata (builder style).
    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// A channel push subscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct PushChannelSubscription {
    /// Channel.
    pub channel: String,
    /// Device id.
    pub device_id: String,
    /// Client id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// Provider.
    pub provider: PushProviderKind,
    /// Hash of the device token.
    pub token_hash: String,
    /// Provider credential version.
    pub credential_version: u32,
}

impl PushChannelSubscription {
    /// Build a subscription.
    pub fn new(
        channel: impl Into<String>,
        device_id: impl Into<String>,
        provider: PushProviderKind,
        token_hash: impl Into<String>,
        credential_version: u32,
    ) -> Self {
        Self {
            channel: channel.into(),
            device_id: device_id.into(),
            client_id: None,
            provider,
            token_hash: token_hash.into(),
            credential_version,
        }
    }
}

/// Notification content.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct PushPayload {
    /// Template id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    /// Template data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_data: Option<Value>,
    /// Title.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Icon.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// Sound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sound: Option<String>,
    /// Collapse key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collapse_key: Option<String>,
}

impl PushPayload {
    /// A title/body notification.
    pub fn notification(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            title: Some(title.into()),
            body: Some(body.into()),
            ..Self::default()
        }
    }
}

/// Who a publish targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum PushPublishTarget {
    /// One device.
    #[serde(rename_all = "camelCase")]
    Device {
        /// Device id.
        device_id: String,
    },
    /// All devices of a client.
    #[serde(rename_all = "camelCase")]
    Client {
        /// Client id.
        client_id: String,
    },
    /// Channel push subscribers.
    Channel {
        /// Channel.
        channel: String,
    },
    /// A registered topic.
    RegisteredTopic {
        /// Topic.
        topic: String,
    },
    /// A user topic.
    UserTopic {
        /// Topic.
        topic: String,
    },
    /// A raw recipient.
    Recipient {
        /// Recipient.
        recipient: PushRecipient,
    },
}

/// Live Activity lifecycle event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApnsLiveActivityEvent {
    /// Start.
    Start,
    /// Update.
    Update,
    /// End.
    End,
}

/// Live Activity priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApnsLiveActivityPriority {
    /// Low power.
    LowPower,
    /// Conserve power.
    ConservePower,
    /// Immediate.
    Immediate,
}

/// Live Activity content.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ApnsLiveActivityPayload {
    /// Event.
    pub event: ApnsLiveActivityEvent,
    /// Timestamp (seconds).
    pub timestamp: u64,
    /// Content state.
    pub content_state: Value,
    /// Attributes type (start only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes_type: Option<String>,
    /// Attributes (start only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Value>,
    /// Alert.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alert: Option<Value>,
    /// Stale date (seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_date: Option<u64>,
    /// Dismissal date (seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dismissal_date: Option<u64>,
    /// Relevance score.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevance_score: Option<f64>,
    /// Request a push-to-start token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_push_token: Option<bool>,
    /// Broadcast channel to join.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_push_channel: Option<String>,
    /// Priority.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<ApnsLiveActivityPriority>,
}

impl ApnsLiveActivityPayload {
    /// Required fields.
    pub fn new(event: ApnsLiveActivityEvent, timestamp: u64, content_state: Value) -> Self {
        Self {
            event,
            timestamp,
            content_state,
            attributes_type: None,
            attributes: None,
            alert: None,
            stale_date: None,
            dismissal_date: None,
            relevance_score: None,
            input_push_token: None,
            input_push_channel: None,
            priority: None,
        }
    }
}

/// Provider-specific payload override.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct ProviderOverride {
    /// Provider.
    pub provider: PushProviderKind,
    /// Raw provider payload.
    pub payload: Value,
}

impl ProviderOverride {
    /// Build an override.
    pub fn new(provider: PushProviderKind, payload: Value) -> Self {
        Self { provider, payload }
    }
}

/// A publish request.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct PushPublishRequest {
    /// Idempotent publish id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_id: Option<String>,
    /// Targets.
    pub recipients: Vec<PushPublishTarget>,
    /// Content.
    pub payload: PushPayload,
    /// Provider overrides.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub provider_overrides: Vec<ProviderOverride>,
    /// Live Activity content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_activity: Option<ApnsLiveActivityPayload>,
    /// Do not deliver before (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_before_ms: Option<u64>,
    /// Expire at (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    /// Always `false` from clients: publishes are asynchronous.
    sync: bool,
}

impl PushPublishRequest {
    /// Publish `payload` to `recipients`.
    pub fn new(recipients: Vec<PushPublishTarget>, payload: PushPayload) -> Self {
        Self {
            publish_id: None,
            recipients,
            payload,
            provider_overrides: Vec::new(),
            live_activity: None,
            not_before_ms: None,
            expires_at_ms: None,
            sync: false,
        }
    }

    /// Set the publish id (builder style).
    pub fn publish_id(mut self, id: impl Into<String>) -> Self {
        self.publish_id = Some(id.into());
        self
    }

    /// Schedule (builder style).
    pub fn not_before_ms(mut self, at: u64) -> Self {
        self.not_before_ms = Some(at);
        self
    }

    /// Expire (builder style).
    pub fn expires_at_ms(mut self, at: u64) -> Self {
        self.expires_at_ms = Some(at);
        self
    }

    /// Attach Live Activity content (builder style).
    pub fn live_activity(mut self, payload: ApnsLiveActivityPayload) -> Self {
        self.live_activity = Some(payload);
        self
    }

    /// Add a provider override (builder style).
    pub fn provider_override(mut self, value: ProviderOverride) -> Self {
        self.provider_overrides.push(value);
        self
    }
}

/// Cursor parameters for list calls.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PushListParams {
    /// Page size.
    pub limit: Option<u32>,
    /// Cursor.
    pub cursor: Option<String>,
    /// Filter by channel (subscriptions).
    pub channel: Option<String>,
    /// Filter by device (subscriptions).
    pub device_id: Option<String>,
    /// Additional filters.
    pub extra: BTreeMap<String, String>,
}

impl PushListParams {
    /// Default parameters.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the page size (builder style).
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Set the cursor (builder style).
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }
    /// Filter by channel (builder style).
    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = Some(channel.into());
        self
    }
    /// Filter by device (builder style).
    pub fn device_id(mut self, device_id: impl Into<String>) -> Self {
        self.device_id = Some(device_id.into());
        self
    }

    fn pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        if let Some(limit) = self.limit {
            pairs.push(("limit".into(), limit.to_string()));
        }
        if let Some(cursor) = self.cursor.as_ref() {
            pairs.push(("cursor".into(), cursor.clone()));
        }
        if let Some(channel) = self.channel.as_ref() {
            pairs.push(("channel".into(), channel.clone()));
        }
        if let Some(device_id) = self.device_id.as_ref() {
            pairs.push(("deviceId".into(), device_id.clone()));
        }
        pairs.extend(self.extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        pairs
    }
}

/// Client for your backend's push endpoint.
#[derive(Clone)]
pub struct PushRegistration {
    endpoint: Endpoint,
    client: reqwest::Client,
}

impl fmt::Debug for PushRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushRegistration")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

fn encode_segment(value: &str) -> String {
    form_urlencoded::byte_serialize(value.as_bytes())
        .collect::<String>()
        .replace('+', "%20")
}

impl PushRegistration {
    /// A client for `endpoint` (trailing slashes are trimmed).
    pub fn new(endpoint: impl Into<Endpoint>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("sockudo-client-rust/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| Error::HttpTransport(error.to_string()))?;
        Ok(Self::with_client(endpoint, client))
    }

    /// Share an existing HTTP client.
    pub fn with_client(endpoint: impl Into<Endpoint>, client: reqwest::Client) -> Self {
        let mut endpoint = endpoint.into();
        while endpoint.url.ends_with('/') {
            endpoint.url.pop();
        }
        Self { endpoint, client }
    }

    async fn request<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&T>,
        query: &[(String, String)],
        extra_headers: &[(String, String)],
    ) -> Result<Option<Value>> {
        let url = format!("{}{path}", self.endpoint.url);
        let mut headers = self.endpoint.resolved_headers();
        headers.extend_from_slice(extra_headers);
        let body = body
            .map(|body| sonic_rs::to_vec(body).map_err(|error| Error::Encode(error.to_string())))
            .transpose()?;
        crate::http::json_request(&self.client, method, &url, &headers, query, body).await
    }

    /// Register a device.
    pub async fn activate_device(&self, device: &PushDeviceDetails) -> Result<Option<Value>> {
        self.request(Method::POST, "/deviceRegistrations", Some(device), &[], &[])
            .await
    }

    /// Update a registration, proving ownership with the device identity token.
    pub async fn update_device_registration(
        &self,
        device: &PushDeviceDetails,
        device_identity_token: &str,
    ) -> Result<Option<Value>> {
        let headers = [(
            "X-Sockudo-Device-Identity-Token".to_owned(),
            device_identity_token.to_owned(),
        )];
        self.request(
            Method::POST,
            "/deviceRegistrations",
            Some(device),
            &[],
            &headers,
        )
        .await
    }

    /// List registrations.
    pub async fn list_device_registrations(
        &self,
        params: &PushListParams,
    ) -> Result<Option<Value>> {
        self.request::<()>(
            Method::GET,
            "/deviceRegistrations",
            None,
            &params.pairs(),
            &[],
        )
        .await
    }

    /// Fetch one registration.
    pub async fn get_device_registration(&self, device_id: &str) -> Result<Option<Value>> {
        let path = format!("/deviceRegistrations/{}", encode_segment(device_id));
        self.request::<()>(Method::GET, &path, None, &[], &[]).await
    }

    /// Remove one registration.
    pub async fn delete_device_registration(&self, device_id: &str) -> Result<Option<Value>> {
        let path = format!("/deviceRegistrations/{}", encode_segment(device_id));
        self.request::<()>(Method::DELETE, &path, None, &[], &[])
            .await
    }

    /// Create or update a channel push subscription.
    pub async fn upsert_channel_subscription(
        &self,
        subscription: &PushChannelSubscription,
    ) -> Result<Option<Value>> {
        self.request(
            Method::POST,
            "/channelSubscriptions",
            Some(subscription),
            &[],
            &[],
        )
        .await
    }

    /// List channel push subscriptions.
    pub async fn list_channel_subscriptions(
        &self,
        params: &PushListParams,
    ) -> Result<Option<Value>> {
        self.request::<()>(
            Method::GET,
            "/channelSubscriptions",
            None,
            &params.pairs(),
            &[],
        )
        .await
    }

    /// Delete channel push subscriptions matching the filters.
    pub async fn delete_channel_subscriptions(
        &self,
        params: &PushListParams,
    ) -> Result<Option<Value>> {
        self.request::<()>(
            Method::DELETE,
            "/channelSubscriptions",
            None,
            &params.pairs(),
            &[],
        )
        .await
    }

    /// Publish (asynchronously processed).
    pub async fn publish(&self, request: &PushPublishRequest) -> Result<Option<Value>> {
        self.request(Method::POST, "/publish", Some(request), &[], &[])
            .await
    }

    /// Publish a Live Activity update.
    pub async fn publish_live_activity(
        &self,
        request: &PushPublishRequest,
    ) -> Result<Option<Value>> {
        if request.live_activity.is_none() {
            return Err(Error::invalid(
                "publish_live_activity requires live_activity",
            ));
        }
        self.publish(request).await
    }

    /// Publish several requests.
    pub async fn publish_batch(&self, requests: &[PushPublishRequest]) -> Result<Option<Value>> {
        self.request(Method::POST, "/batch/publish", Some(requests), &[], &[])
            .await
    }

    /// Publish at `not_before_ms`.
    pub async fn schedule_publish(&self, request: &PushPublishRequest) -> Result<Option<Value>> {
        if request.not_before_ms.is_none() {
            return Err(Error::invalid("schedule_publish requires not_before_ms"));
        }
        self.publish(request).await
    }

    /// Status of a publish.
    pub async fn get_publish_status(&self, publish_id: &str) -> Result<Option<Value>> {
        let path = format!("/publish/{}/status", encode_segment(publish_id));
        self.request::<()>(Method::GET, &path, None, &[], &[]).await
    }

    /// Cancel a scheduled publish.
    pub async fn cancel_scheduled_publish(&self, publish_id: &str) -> Result<Option<Value>> {
        let path = format!("/scheduled/{}", encode_segment(publish_id));
        self.request::<()>(Method::DELETE, &path, None, &[], &[])
            .await
    }

    /// Report a client-side delivery status event.
    pub async fn post_delivery_status(&self, event: &Value) -> Result<Option<Value>> {
        self.request(Method::POST, "/deliveryStatus", Some(event), &[], &[])
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_like_the_js_sdk() {
        let device = PushDeviceDetails::new(
            "d1",
            "ios",
            "phone",
            PushRecipient::Apns {
                device_token: "tok".into(),
            },
        )
        .client_id("u1");
        assert_eq!(
            sonic_rs::to_value(&device).unwrap(),
            sonic_rs::json!({
                "id": "d1", "clientId": "u1", "formFactor": "phone", "platform": "ios",
                "timezone": "UTC", "locale": "en-US",
                "push": {"recipient": {"transportType": "apns", "deviceToken": "tok"}}
            })
        );
        let request = PushPublishRequest::new(
            vec![
                PushPublishTarget::Device {
                    device_id: "d1".into(),
                },
                PushPublishTarget::RegisteredTopic {
                    topic: "news".into(),
                },
            ],
            PushPayload::notification("Hi", "There"),
        );
        assert_eq!(
            sonic_rs::to_value(&request).unwrap(),
            sonic_rs::json!({
                "recipients": [{"type": "device", "deviceId": "d1"}, {"type": "registeredTopic", "topic": "news"}],
                "payload": {"title": "Hi", "body": "There"},
                "sync": false
            })
        );
    }

    #[test]
    fn recipients_never_debug_print_tokens() {
        let recipient = PushRecipient::Gcm {
            registration_token: "secret".into(),
        };
        assert!(!format!("{recipient:?}").contains("secret"));
    }

    #[test]
    fn encodes_path_segments() {
        assert_eq!(encode_segment("a b/c"), "a%20b%2Fc");
    }
}
