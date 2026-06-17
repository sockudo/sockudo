use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};

use crate::domain::{DeliveryOutcome, DevicePushState, PushProviderKind};
use crate::metrics::{delivery_outcome_label, device_state_label, provider_label};

pub const PUSH_META_LOG_TARGET: &str = "[meta]log:push";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PushMetaEvent {
    pub event: PushMetaEventKind,
    pub app_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<Cow<'static, str>>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub detail: Value,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PushMetaEventKind {
    PushAccepted,
    PublishCompleted,
    DeviceStateChanged,
    ProviderRejected,
    TokenInvalidated,
    QuotaEvent,
    SchedulerEvent,
    CircuitBreakerEvent,
    DeadLetter,
}

impl PushMetaEvent {
    pub fn accepted(app_id: &str, publish_id: &str, expected_recipients: u64) -> Self {
        Self {
            event: PushMetaEventKind::PushAccepted,
            app_id: app_id.to_owned(),
            publish_id: Some(publish_id.to_owned()),
            provider: None,
            detail: json!({ "expectedRecipients": expected_recipients }),
        }
    }

    pub fn completed(app_id: &str, publish_id: &str, status: &str) -> Self {
        Self {
            event: PushMetaEventKind::PublishCompleted,
            app_id: app_id.to_owned(),
            publish_id: Some(publish_id.to_owned()),
            provider: None,
            detail: json!({ "status": status }),
        }
    }

    pub fn device_state_changed(
        app_id: &str,
        publish_id: &str,
        from: DevicePushState,
        to: DevicePushState,
    ) -> Self {
        Self {
            event: PushMetaEventKind::DeviceStateChanged,
            app_id: app_id.to_owned(),
            publish_id: Some(publish_id.to_owned()),
            provider: None,
            detail: json!({
                "from": device_state_label(from),
                "to": device_state_label(to)
            }),
        }
    }

    pub fn provider_rejected(
        app_id: &str,
        publish_id: &str,
        provider: PushProviderKind,
        outcome: DeliveryOutcome,
        class: Option<&str>,
    ) -> Self {
        Self {
            event: PushMetaEventKind::ProviderRejected,
            app_id: app_id.to_owned(),
            publish_id: Some(publish_id.to_owned()),
            provider: Some(Cow::Borrowed(provider_label(provider))),
            detail: json!({
                "outcome": delivery_outcome_label(outcome),
                "class": class
            }),
        }
    }

    pub fn token_invalidated(app_id: &str, publish_id: &str, provider: PushProviderKind) -> Self {
        Self {
            event: PushMetaEventKind::TokenInvalidated,
            app_id: app_id.to_owned(),
            publish_id: Some(publish_id.to_owned()),
            provider: Some(Cow::Borrowed(provider_label(provider))),
            detail: Value::new_null(),
        }
    }

    pub fn quota_event(app_id: &str, publish_id: Option<&str>, reason: &str) -> Self {
        Self {
            event: PushMetaEventKind::QuotaEvent,
            app_id: app_id.to_owned(),
            publish_id: publish_id.map(str::to_owned),
            provider: None,
            detail: json!({ "reason": reason }),
        }
    }

    pub fn scheduler_event(app_id: &str, publish_id: &str, status: &str) -> Self {
        Self {
            event: PushMetaEventKind::SchedulerEvent,
            app_id: app_id.to_owned(),
            publish_id: Some(publish_id.to_owned()),
            provider: None,
            detail: json!({ "status": status }),
        }
    }

    pub fn circuit_breaker_event(
        app_id: &str,
        provider: PushProviderKind,
        action: &str,
        retry_at_ms: u64,
    ) -> Self {
        Self {
            event: PushMetaEventKind::CircuitBreakerEvent,
            app_id: app_id.to_owned(),
            publish_id: None,
            provider: Some(Cow::Borrowed(provider_label(provider))),
            detail: json!({ "action": action, "retryAtMs": retry_at_ms }),
        }
    }

    pub fn dead_letter(app_id: &str, publish_id: &str, stage: &str, reason: &str) -> Self {
        Self {
            event: PushMetaEventKind::DeadLetter,
            app_id: app_id.to_owned(),
            publish_id: Some(publish_id.to_owned()),
            provider: None,
            detail: json!({ "stage": stage, "reason": reason }),
        }
    }
}

pub fn emit_push_meta_event(event: PushMetaEvent) {
    tracing::info!(
        target: PUSH_META_LOG_TARGET,
        app_id = %event.app_id,
        publish_id = event.publish_id.as_deref(),
        provider = event.provider.as_deref(),
        event = ?event.event,
        detail = %event.detail,
        "push meta-channel event"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_event_serialization_is_redaction_safe() {
        let event = PushMetaEvent::provider_rejected(
            "app-1",
            "publish-1",
            PushProviderKind::Fcm,
            DeliveryOutcome::Rejected,
            Some("invalid_token"),
        );
        let serialized = sonic_rs::to_string(&event).unwrap();

        assert!(serialized.contains("provider-rejected"));
        assert!(serialized.contains("invalid_token"));
        assert!(!serialized.contains("registrationToken"));
        assert!(!serialized.contains("credential"));
    }
}
