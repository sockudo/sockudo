use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::join_all;
use sonic_rs::Value;

use super::auth::{CachedTokenProvider, auth_error};
use super::http::{
    ProviderHttpClient, ProviderHttpRequest, ProviderHttpResponse, validate_webpush_target,
};
use super::outcome::{
    ProviderClassification, classify_http_result, json_request_with_content_type, recipient_token,
    rejected, render_payload_json, result_from_error, retryable,
};
use super::{HealthStatus, PushDispatcher};
use crate::domain::{
    DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError, PushProviderKind,
};
use crate::pipeline::now_ms;

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

pub(super) fn classify_wns_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if matches!(response.status, 200..=202) {
        return (
            DeliveryOutcome::Accepted,
            None,
            response.headers.get("x-wns-msg-id").cloned(),
        );
    }
    match response.status {
        404 | 410 => rejected("invalid_token", response, None),
        401 | 403 => rejected("auth_failure", response, None),
        413 => rejected("invalid_payload", response, Some("payload_too_large")),
        429 => retryable("quota", response),
        500..=599 => retryable("unavailable", response),
        _ => rejected("provider_rejected", response, None),
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
