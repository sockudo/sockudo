//! Current internal queue payloads with legacy read compatibility.
use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    DeliveryBatch, DeliveryJob, PushPayload, PushProviderKind, PushQueueError, PushQueuePayload,
    PushRecipient, RenderedProviderPayload,
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchWire<'a> {
    kind: &'static str,
    format_version: u8,
    app_id: &'a str,
    publish_id: &'a str,
    provider: PushProviderKind,
    batch_id: &'a str,
    payloads: Vec<PayloadRef<'a>>,
    jobs: Vec<JobRef<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PayloadRef<'a> {
    payload: &'a PushPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    rendered_payload: Option<&'a RenderedProviderPayload>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobRef<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    app_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    publish_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<PushProviderKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<&'a str>,
    recipient: &'a PushRecipient,
    payload_group: usize,
    attempt: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_attempt_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    not_before_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchOwned {
    app_id: String,
    publish_id: String,
    provider: PushProviderKind,
    batch_id: String,
    payloads: Vec<PayloadOwned>,
    jobs: Vec<JobOwned>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PayloadOwned {
    payload: PushPayload,
    rendered_payload: Option<RenderedProviderPayload>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobOwned {
    app_id: Option<String>,
    publish_id: Option<String>,
    provider: Option<PushProviderKind>,
    batch_id: Option<String>,
    device_id: Option<String>,
    recipient: PushRecipient,
    payload_group: usize,
    attempt: u32,
    first_attempt_at_ms: Option<u64>,
    not_before_ms: Option<u64>,
    expires_at_ms: Option<u64>,
}

/// Encode a queue payload without changing the public push HTTP representation.
/// Shared payload groups apply only to delivery batches. All live writers use this format.
pub fn encode_queue_payload(payload: &PushQueuePayload) -> Result<String, PushQueueError> {
    let PushQueuePayload::DeliveryBatch(batch) = payload else {
        return sonic_rs::to_string(payload).map_err(wire_error);
    };
    let mut payloads = Vec::new();
    let mut groups = HashMap::new();
    let mut jobs = Vec::with_capacity(batch.jobs.len());
    let mut previous_group = None;
    for job in &batch.jobs {
        let identity = (
            Arc::as_ptr(&job.payload),
            job.rendered_payload.as_ref().map(Arc::as_ptr),
        );
        // Fanout and retry batches commonly contain adjacent recipients of one immutable
        // projection. Reuse that group without hashing the same pointer pair per job.
        let payload_group = if let Some((previous, group)) = previous_group
            && previous == identity
        {
            group
        } else {
            *groups.entry(identity).or_insert_with(|| {
                let index = payloads.len();
                payloads.push(PayloadRef {
                    payload: job.payload.as_ref(),
                    rendered_payload: job.rendered_payload.as_deref(),
                });
                index
            })
        };
        previous_group = Some((identity, payload_group));
        jobs.push(JobRef {
            app_id: (job.app_id != batch.app_id).then_some(job.app_id.as_str()),
            publish_id: (job.publish_id != batch.publish_id).then_some(job.publish_id.as_str()),
            provider: (job.provider != batch.provider).then_some(job.provider),
            batch_id: (job.batch_id != batch.batch_id).then_some(job.batch_id.as_str()),
            device_id: job.device_id.as_deref(),
            recipient: &job.recipient,
            payload_group,
            attempt: job.attempt,
            first_attempt_at_ms: job.first_attempt_at_ms,
            not_before_ms: job.not_before_ms,
            expires_at_ms: job.expires_at_ms,
        });
    }
    sonic_rs::to_string(&BatchWire {
        kind: "deliveryBatch",
        format_version: 2,
        app_id: &batch.app_id,
        publish_id: &batch.publish_id,
        provider: batch.provider,
        batch_id: &batch.batch_id,
        payloads,
        jobs,
    })
    .map_err(wire_error)
}

/// Read both legacy payloads and V2 batches, retaining shared immutable replay
/// context. Unknown versions and dangling group references fail closed.
pub fn decode_queue_payload(encoded: &str) -> Result<PushQueuePayload, PushQueueError> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Header {
        kind: String,
        format_version: Option<u8>,
    }
    let header: Header = sonic_rs::from_str(encoded).map_err(wire_error)?;
    match header.format_version {
        None | Some(1) => match header.kind.as_str() {
            "publishLog" => sonic_rs::from_str(encoded)
                .map(|value| PushQueuePayload::PublishLog(Box::new(value)))
                .map_err(wire_error),
            "shardJob" => sonic_rs::from_str(encoded)
                .map(|value| PushQueuePayload::ShardJob(Box::new(value)))
                .map_err(wire_error),
            "deliveryBatch" => sonic_rs::from_str(encoded)
                .map(|value| PushQueuePayload::DeliveryBatch(Box::new(value)))
                .map_err(wire_error),
            "deliveryResult" => sonic_rs::from_str(encoded)
                .map(|value| PushQueuePayload::DeliveryResult(Box::new(value)))
                .map_err(wire_error),
            "deliveryFeedback" => sonic_rs::from_str(encoded)
                .map(|value| PushQueuePayload::DeliveryFeedback(Box::new(value)))
                .map_err(wire_error),
            "deadLetter" => sonic_rs::from_str(encoded)
                .map(|value| PushQueuePayload::DeadLetter(Box::new(value)))
                .map_err(wire_error),
            "retrySchedule" => sonic_rs::from_str(encoded)
                .map(|value| PushQueuePayload::RetrySchedule(Box::new(value)))
                .map_err(wire_error),
            _ => Err(PushQueueError::Backend(
                "unsupported push queue payload kind".to_owned(),
            )),
        },
        Some(2) if header.kind == "deliveryBatch" => {
            let batch: BatchOwned = sonic_rs::from_str(encoded).map_err(wire_error)?;
            let groups = batch
                .payloads
                .into_iter()
                .map(|payload| {
                    (
                        Arc::new(payload.payload),
                        payload.rendered_payload.map(Arc::new),
                    )
                })
                .collect::<Vec<_>>();
            let mut jobs = Vec::with_capacity(batch.jobs.len());
            for job in batch.jobs {
                let (payload, rendered) = groups.get(job.payload_group).ok_or_else(|| {
                    PushQueueError::Backend("invalid push payload group reference".to_owned())
                })?;
                jobs.push(DeliveryJob {
                    app_id: job.app_id.unwrap_or_else(|| batch.app_id.clone()),
                    publish_id: job.publish_id.unwrap_or_else(|| batch.publish_id.clone()),
                    provider: job.provider.unwrap_or(batch.provider),
                    batch_id: job.batch_id.unwrap_or_else(|| batch.batch_id.clone()),
                    device_id: job.device_id,
                    recipient: job.recipient,
                    payload: Arc::clone(payload),
                    rendered_payload: rendered.clone(),
                    attempt: job.attempt,
                    first_attempt_at_ms: job.first_attempt_at_ms,
                    not_before_ms: job.not_before_ms,
                    expires_at_ms: job.expires_at_ms,
                });
            }
            Ok(PushQueuePayload::DeliveryBatch(Box::new(DeliveryBatch {
                app_id: batch.app_id,
                publish_id: batch.publish_id,
                provider: batch.provider,
                batch_id: batch.batch_id,
                jobs,
            })))
        }
        _ => Err(PushQueueError::Backend(
            "unsupported push queue payload version".to_owned(),
        )),
    }
}

fn wire_error(_: sonic_rs::Error) -> PushQueueError {
    // Parser diagnostics may contain credentials and payload fragments.
    PushQueueError::Backend("invalid push queue payload encoding".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeliveryFeedback, DeliveryOutcome, DeliveryResult, SecretString};
    use sonic_rs::json;

    fn batch() -> DeliveryBatch {
        let payload = Arc::new(PushPayload {
            template_id: None,
            template_data: json!({"frozen": "x".repeat(2048)}),
            title: Some("unchanged title".into()),
            body: None,
            icon: None,
            sound: None,
            collapse_key: None,
        });
        DeliveryBatch {
            app_id: "app".into(),
            publish_id: "publish".into(),
            provider: PushProviderKind::Fcm,
            batch_id: "batch".into(),
            jobs: (0..32)
                .map(|i| DeliveryJob {
                    app_id: "app".into(),
                    publish_id: "publish".into(),
                    provider: PushProviderKind::Fcm,
                    batch_id: "batch".into(),
                    device_id: Some(format!("device-{i}")),
                    recipient: PushRecipient::Fcm {
                        registration_token: SecretString::new(format!("token-{i}")).unwrap(),
                    },
                    payload: Arc::clone(&payload),
                    rendered_payload: None,
                    attempt: 2,
                    first_attempt_at_ms: Some(10),
                    not_before_ms: Some(20),
                    expires_at_ms: Some(30),
                })
                .collect(),
        }
    }

    #[test]
    fn legacy_and_compact_restart_roundtrips_preserve_context_and_share_payloads() {
        let original = PushQueuePayload::DeliveryBatch(Box::new(batch()));
        let v1 = sonic_rs::to_string(&original).unwrap();
        let v2 = encode_queue_payload(&original).unwrap();
        assert!(v2.len() < v1.len() / 3);
        assert_eq!(decode_queue_payload(&v1).unwrap(), original);
        let restored = decode_queue_payload(&v2).unwrap();
        assert_eq!(restored, original);
        let PushQueuePayload::DeliveryBatch(batch) = &restored else {
            panic!("batch expected")
        };
        assert!(Arc::ptr_eq(&batch.jobs[0].payload, &batch.jobs[31].payload));
        // The rollback drain converts queued V2 work to the exact legacy shape.
        assert_eq!(sonic_rs::to_string(&restored).unwrap(), v1);
    }

    #[test]
    fn mixed_payload_groups_and_retry_feedback_survive_restart() {
        let mut original = batch();
        original.jobs[1].payload = Arc::new(PushPayload {
            title: Some("recipient override".into()),
            ..original.jobs[1].payload.as_ref().clone()
        });
        original.jobs[1].app_id = "explicit-job-app".to_owned();
        original.jobs[1].publish_id = "explicit-job-publish".to_owned();
        original.jobs[1].batch_id = "explicit-job-batch".to_owned();
        let encoded =
            encode_queue_payload(&PushQueuePayload::DeliveryBatch(Box::new(original.clone())))
                .unwrap();
        let PushQueuePayload::DeliveryBatch(restored) = decode_queue_payload(&encoded).unwrap()
        else {
            panic!("batch expected")
        };
        assert_eq!(*restored, original);
        assert!(!Arc::ptr_eq(
            &restored.jobs[0].payload,
            &restored.jobs[1].payload
        ));
        let job = restored.jobs[1].clone();
        let result = DeliveryResult {
            app_id: job.app_id.clone(),
            publish_id: job.publish_id.clone(),
            provider: job.provider,
            batch_id: job.batch_id.clone(),
            device_id: job.device_id.clone(),
            outcome: DeliveryOutcome::Retryable,
            provider_message_id: None,
            error: None,
            attempt: job.attempt,
        };
        let mut feedback = DeliveryFeedback::from_result(result);
        feedback.retry_job = Some(Box::new(job));
        let feedback = PushQueuePayload::DeliveryFeedback(Box::new(feedback));
        let wire = encode_queue_payload(&feedback).unwrap();
        assert_eq!(decode_queue_payload(&wire).unwrap(), feedback);
    }

    #[test]
    fn unknown_versions_and_missing_groups_fail_without_payload_diagnostics() {
        for encoded in [
            r#"{"kind":"deliveryBatch","formatVersion":99,"secret":"do-not-return"}"#,
            "not-json",
        ] {
            let error = decode_queue_payload(encoded).unwrap_err().to_string();
            assert!(!error.contains("do-not-return"));
        }
        let encoded =
            encode_queue_payload(&PushQueuePayload::DeliveryBatch(Box::new(batch()))).unwrap();
        let corrupted = encoded.replace("\"payloadGroup\":0", "\"payloadGroup\":999");
        assert!(decode_queue_payload(&corrupted).is_err());
    }
}
