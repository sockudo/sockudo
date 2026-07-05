use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::join_all;
use sonic_rs::prelude::*;
use sonic_rs::{Object, Value, json};

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
    DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError,
    ProviderFailureClass, PushProviderKind,
};
use crate::pipeline::now_ms;

#[derive(Clone)]
pub struct ApnsDispatcher {
    topic: String,
    endpoint: ProviderEndpointConfig,
    token_provider: Option<CachedTokenProvider>,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
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
            token_provider: Some(token_provider),
            http,
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
            token_provider: None,
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
        let device_token = recipient_token(&job.recipient).ok_or_else(|| ProviderError {
            class: "invalid_token".to_owned(),
            failure_class: ProviderFailureClass::DeviceTerminal,
            reason: Some("apns device token is missing".to_owned()),
            retry_after_ms: None,
        })?;
        let rendered = render_payload_json(PushProviderKind::Apns, job)?;
        let headers = rendered
            .get("headers")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut request_headers = BTreeMap::new();
        request_headers.insert("apns-topic".to_owned(), self.topic.clone());
        request_headers.insert(
            "apns-push-type".to_owned(),
            header_string(&headers, "apns-push-type").unwrap_or_else(|| "alert".to_owned()),
        );
        request_headers.insert(
            "apns-priority".to_owned(),
            header_string(&headers, "apns-priority").unwrap_or_else(|| "10".to_owned()),
        );
        if let Some(collapse_id) = header_string(&headers, "apns-collapse-id") {
            request_headers.insert("apns-collapse-id".to_owned(), collapse_id);
        }
        if let Some(expiration) = header_string(&headers, "apns-expiration") {
            request_headers.insert("apns-expiration".to_owned(), expiration);
        }

        json_request(
            self.endpoint.joined_url(&format!("/3/device/{device_token}")),
            request_headers,
            authorization,
            rendered
                .get("aps")
                .map(|aps| json!({ "aps": aps, "data": rendered.get("data").cloned().unwrap_or_else(Value::new_null) }))
                .unwrap_or(rendered),
        )
    }
}

#[async_trait]
impl PushDispatcher for ApnsDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Apns
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
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
                        return result_from_error(job, DeliveryOutcome::Retryable, error);
                    }
                };
            }
            classify_http_result(job, response, classify_apns_response)
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Apns,
            healthy: !self.topic.trim().is_empty(),
            details: "apns http/2 dispatcher configured".to_owned(),
        }
    }
}

pub(super) fn classify_apns_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if (200..300).contains(&response.status) {
        return (
            DeliveryOutcome::Accepted,
            None,
            response.headers.get("apns-id").cloned(),
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
        403 if apns_reason_matches(reason.as_deref(), &["TooManyProviderTokenUpdates"]) => {
            retryable(
                "auth_failure",
                ProviderFailureClass::CredentialAuth,
                response,
            )
        }
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
        429 => retryable("quota", ProviderFailureClass::ProviderQuota, response),
        500 | 503 => retryable(
            "unavailable",
            ProviderFailureClass::ProviderTransient,
            response,
        ),
        _ => rejected(
            "provider_rejected",
            ProviderFailureClass::Unknown,
            response,
            reason.as_deref(),
        ),
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
