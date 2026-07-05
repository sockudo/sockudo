mod apns;
mod auth;
mod fcm;
mod http;
mod outcome;
mod stubs;
mod webpush;
mod worker;

#[cfg(feature = "push-webpush")]
pub use self::webpush::NativeWebPushCrypto;
pub use self::webpush::{
    PassthroughWebPushCrypto, WebPushCrypto, WebPushDispatcher, WebPushPreparedRequest,
};

pub use self::apns::ApnsDispatcher;

pub use self::fcm::FcmDispatcher;
#[cfg(feature = "push-fcm")]
pub use self::fcm::FcmServiceAccountTokenSource;

pub use self::stubs::{AcceptAllDispatcher, RetryAfterDispatcher};

pub use self::worker::{
    AdaptiveRateLimiter, CircuitState, ProviderCircuitBreaker, ProviderDispatchWorker,
    WeightedFairScheduler,
};

pub use self::auth::{
    CachedTokenProvider, ProviderAccessToken, ProviderAuthError, ProviderTokenSource,
    StaticTokenSource,
};
#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
pub use self::http::ReqwestProviderHttpClient;
pub use self::http::{
    ProviderEndpointConfig, ProviderHttpClient, ProviderHttpMethod, ProviderHttpRequest,
    ProviderHttpResponse,
};

use self::auth::auth_error;
use self::http::validate_webpush_target;
use self::outcome::{
    ProviderClassification, classify_http_result, json_field, json_request,
    json_request_with_content_type, recipient_token, rejected, render_payload_json,
    result_from_error, retryable,
};

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};

use crate::domain::{
    DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError,
    ProviderFailureClass, PushProviderKind,
};
use crate::pipeline::now_ms;

#[async_trait]
pub trait PushDispatcher: Send + Sync {
    fn provider(&self) -> PushProviderKind;

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult>;

    async fn health_check(&self) -> HealthStatus;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthStatus {
    pub provider: PushProviderKind,
    pub healthy: bool,
    pub details: String,
}

#[derive(Clone)]
pub struct HmsDispatcher {
    app_id: String,
    endpoint: ProviderEndpointConfig,
    token_provider: CachedTokenProvider,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

impl HmsDispatcher {
    pub fn new(
        app_id: impl Into<String>,
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            app_id: app_id.into(),
            endpoint: ProviderEndpointConfig {
                base_url: "https://push-api.cloud.huawei.com".to_owned(),
                credential_id: "hms".to_owned(),
            },
            token_provider,
            http,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let authorization = self
            .token_provider
            .bearer_token(now_ms())
            .await
            .map_err(auth_error)?;
        let mut rendered = render_payload_json(PushProviderKind::Hms, job)?;
        if let Some(token) = recipient_token(&job.recipient) {
            rendered["message"]["token"] = json!([token]);
        }
        json_request(
            self.endpoint
                .joined_url(&format!("/v1/{}/messages:send", self.app_id)),
            BTreeMap::new(),
            Some(authorization),
            rendered,
        )
    }
}

#[async_trait]
impl PushDispatcher for HmsDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Hms
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            classify_http_result(job, self.http.send(request).await, classify_hms_response)
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Hms,
            healthy: !self.app_id.trim().is_empty(),
            details: "hms http dispatcher configured".to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct WnsDispatcher {
    token_provider: CachedTokenProvider,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

impl WnsDispatcher {
    pub fn new(
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            token_provider,
            http,
        }
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let channel_uri = recipient_token(&job.recipient).ok_or_else(|| ProviderError {
            class: "invalid_token".to_owned(),
            failure_class: ProviderFailureClass::DeviceTerminal,
            reason: Some("wns channel URI is missing".to_owned()),
            retry_after_ms: None,
        })?;
        validate_webpush_target(channel_uri)?;
        let authorization = self
            .token_provider
            .bearer_token(now_ms())
            .await
            .map_err(auth_error)?;
        let rendered = render_payload_json(PushProviderKind::Wns, job)?;
        validate_wns_payload(&rendered)?;
        let mut headers = BTreeMap::new();
        headers.insert("x-wns-type".to_owned(), wns_type(&rendered));
        let content_type = if wns_type(&rendered) == "wns/raw" {
            Some("application/octet-stream")
        } else {
            None
        };
        json_request_with_content_type(
            channel_uri.to_owned(),
            headers,
            Some(authorization),
            rendered,
            content_type,
        )
    }
}

#[async_trait]
impl PushDispatcher for WnsDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Wns
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            classify_http_result(job, self.http.send(request).await, classify_wns_response)
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Wns,
            healthy: true,
            details: "wns http dispatcher configured".to_owned(),
        }
    }
}

fn classify_hms_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if (200..300).contains(&response.status)
        && json_field(&response.body, &["code"]).is_none_or(|code| code == "80000000")
    {
        return (
            DeliveryOutcome::Accepted,
            None,
            json_field(&response.body, &["msg"])
                .or_else(|| json_field(&response.body, &["requestId"])),
        );
    }
    let body = String::from_utf8_lossy(&response.body).to_ascii_lowercase();
    if matches!(response.status, 404 | 410)
        || (body.contains("token") && (body.contains("invalid") || body.contains("not exist")))
    {
        rejected(
            "invalid_token",
            ProviderFailureClass::DeviceTerminal,
            response,
            None,
        )
    } else if response.status == 413 {
        rejected(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            response,
            Some("payload_too_large"),
        )
    } else if response.status == 429 || body.contains("quota") {
        retryable("quota", ProviderFailureClass::ProviderQuota, response)
    } else if matches!(response.status, 401 | 403) {
        rejected(
            "auth_failure",
            ProviderFailureClass::CredentialAuth,
            response,
            None,
        )
    } else if response.status >= 500 {
        retryable(
            "unavailable",
            ProviderFailureClass::ProviderTransient,
            response,
        )
    } else {
        rejected(
            "provider_rejected",
            ProviderFailureClass::Unknown,
            response,
            None,
        )
    }
}

fn classify_wns_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if matches!(response.status, 200..=202) {
        return (
            DeliveryOutcome::Accepted,
            None,
            response.headers.get("x-wns-msg-id").cloned(),
        );
    }
    match response.status {
        404 | 410 => rejected(
            "invalid_token",
            ProviderFailureClass::DeviceTerminal,
            response,
            None,
        ),
        401 | 403 => rejected(
            "auth_failure",
            ProviderFailureClass::CredentialAuth,
            response,
            None,
        ),
        413 => rejected(
            "invalid_payload",
            ProviderFailureClass::CallerPayload,
            response,
            Some("payload_too_large"),
        ),
        429 => retryable("quota", ProviderFailureClass::ProviderQuota, response),
        500..=599 => retryable(
            "unavailable",
            ProviderFailureClass::ProviderTransient,
            response,
        ),
        _ => rejected(
            "provider_rejected",
            ProviderFailureClass::Unknown,
            response,
            None,
        ),
    }
}

fn validate_wns_payload(payload: &Value) -> Result<(), ProviderError> {
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("toast");
    if !matches!(kind, "toast" | "tile" | "raw") {
        return Err(ProviderError {
            class: "invalid_payload".to_owned(),
            failure_class: ProviderFailureClass::CallerPayload,
            reason: Some("invalid WNS notification type".to_owned()),
            retry_after_ms: None,
        });
    }
    Ok(())
}

fn wns_type(payload: &Value) -> String {
    match payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("toast")
    {
        "tile" => "wns/tile",
        "raw" => "wns/raw",
        _ => "wns/toast",
    }
    .to_owned()
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
