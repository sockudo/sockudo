use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::join_all;
use sonic_rs::json;

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

pub(super) fn classify_hms_response(response: &ProviderHttpResponse) -> ProviderClassification {
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
