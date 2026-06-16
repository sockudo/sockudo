use std::collections::BTreeMap;

use sonic_rs::Value;
use sonic_rs::prelude::*;

use super::http::{ProviderHttpMethod, ProviderHttpRequest, ProviderHttpResponse};
use crate::domain::{
    DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError, PushProviderKind, PushRecipient,
    SecretString,
};
use crate::pipeline::now_ms;
use crate::transform::render_provider_payload;

pub(super) type ProviderClassification = (DeliveryOutcome, Option<ProviderError>, Option<String>);
pub(super) type ProviderResponseClassifier = fn(&ProviderHttpResponse) -> ProviderClassification;

pub(super) fn render_payload_json(
    provider: PushProviderKind,
    job: &DeliveryJob,
) -> Result<Value, ProviderError> {
    render_provider_payload(provider, &job.payload, &[])
        .map(|rendered| rendered.payload)
        .map_err(|error| ProviderError {
            class: "invalid_payload".to_owned(),
            reason: Some(error.to_string()),
            retry_after_ms: None,
        })
}

pub(super) fn json_request(
    url: String,
    headers: BTreeMap<String, String>,
    authorization: Option<SecretString>,
    payload: Value,
) -> Result<ProviderHttpRequest, ProviderError> {
    json_request_with_content_type(url, headers, authorization, payload, None)
}

pub(super) fn json_request_with_content_type(
    url: String,
    mut headers: BTreeMap<String, String>,
    authorization: Option<SecretString>,
    payload: Value,
    content_type: Option<&'static str>,
) -> Result<ProviderHttpRequest, ProviderError> {
    headers
        .entry("content-type".to_owned())
        .or_insert_with(|| content_type.unwrap_or("application/json").to_owned());
    let body = sonic_rs::to_vec(&payload).map_err(|_| ProviderError {
        class: "invalid_payload".to_owned(),
        reason: Some("provider payload serialization failed".to_owned()),
        retry_after_ms: None,
    })?;
    Ok(ProviderHttpRequest {
        method: ProviderHttpMethod::Post,
        url,
        headers,
        authorization,
        body,
    })
}

pub(super) fn recipient_token(recipient: &PushRecipient) -> Option<&str> {
    match recipient {
        PushRecipient::Fcm { registration_token } | PushRecipient::Hms { registration_token } => {
            Some(registration_token.expose_secret())
        }
        PushRecipient::Apns { device_token } => Some(device_token.expose_secret()),
        PushRecipient::Web { endpoint, .. } => Some(endpoint.expose_secret()),
        PushRecipient::Wns { channel_uri } => Some(channel_uri.expose_secret()),
    }
}

pub(super) fn classify_http_result(
    job: DeliveryJob,
    response: Result<ProviderHttpResponse, String>,
    classifier: ProviderResponseClassifier,
) -> DeliveryResult {
    match response {
        Ok(response) => {
            let (outcome, error, provider_message_id) = classifier(&response);
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
        Err(error) => result_from_error(
            job,
            DeliveryOutcome::Retryable,
            ProviderError {
                class: "unavailable".to_owned(),
                reason: Some(error),
                retry_after_ms: None,
            },
        ),
    }
}

pub(super) fn rejected(
    class: &str,
    response: &ProviderHttpResponse,
    reason: Option<&str>,
) -> ProviderClassification {
    (
        DeliveryOutcome::Rejected,
        Some(provider_error(class, response, reason, None)),
        None,
    )
}

pub(super) fn retryable(class: &str, response: &ProviderHttpResponse) -> ProviderClassification {
    (
        DeliveryOutcome::Retryable,
        Some(provider_error(
            class,
            response,
            None,
            retry_after_ms(&response.headers),
        )),
        None,
    )
}

fn provider_error(
    class: &str,
    response: &ProviderHttpResponse,
    reason: Option<&str>,
    retry_after_ms: Option<u64>,
) -> ProviderError {
    ProviderError {
        class: class.to_owned(),
        reason: reason
            .map(str::to_owned)
            .or_else(|| json_field(&response.body, &["error", "status"]))
            .or_else(|| json_field(&response.body, &["reason"]))
            .or_else(|| Some(format!("provider status {}", response.status))),
        retry_after_ms,
    }
}

fn retry_after_ms(headers: &BTreeMap<String, String>) -> Option<u64> {
    headers
        .get("retry-after")
        .and_then(|raw| {
            raw.parse::<u64>()
                .ok()
                .map(|seconds| now_ms().saturating_add(seconds.saturating_mul(1000)))
                .or_else(|| {
                    httpdate::parse_http_date(raw).ok().and_then(|deadline| {
                        deadline
                            .duration_since(std::time::SystemTime::now())
                            .ok()
                            .map(|duration| {
                                now_ms().saturating_add(
                                    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                                )
                            })
                    })
                })
        })
        .map(apply_retry_jitter)
}

fn apply_retry_jitter(deadline_ms: u64) -> u64 {
    let now = now_ms();
    let delay = deadline_ms.saturating_sub(now);
    if delay < 1_000 {
        return deadline_ms;
    }
    let spread = (delay / 5).max(1);
    let offset = u64::from(rand::random::<u32>()) % (spread.saturating_mul(2).saturating_add(1));
    now.saturating_add(delay.saturating_sub(spread).saturating_add(offset))
}

pub(super) fn result_from_error(
    job: DeliveryJob,
    outcome: DeliveryOutcome,
    error: ProviderError,
) -> DeliveryResult {
    DeliveryResult {
        app_id: job.app_id,
        publish_id: job.publish_id,
        provider: job.provider,
        batch_id: job.batch_id,
        device_id: job.device_id,
        outcome,
        provider_message_id: None,
        error: Some(error),
        attempt: job.attempt,
    }
}

pub(super) fn json_field(body: &[u8], path: &[&str]) -> Option<String> {
    let value: Value = sonic_rs::from_slice(body).ok()?;
    let mut current = &value;
    for segment in path {
        current = current.get(segment)?;
    }
    current.as_str().map(str::to_owned)
}
