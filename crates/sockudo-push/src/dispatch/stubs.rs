use async_trait::async_trait;

use super::{HealthStatus, PushDispatcher};
use crate::domain::{
    DeliveryBatch, DeliveryOutcome, DeliveryResult, ProviderError, PushProviderKind,
};

pub struct AcceptAllDispatcher {
    provider: PushProviderKind,
}

impl AcceptAllDispatcher {
    pub fn new(provider: PushProviderKind) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl PushDispatcher for AcceptAllDispatcher {
    fn provider(&self) -> PushProviderKind {
        self.provider
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        batch
            .jobs
            .into_iter()
            .map(|job| DeliveryResult {
                app_id: job.app_id,
                publish_id: job.publish_id,
                provider: job.provider,
                batch_id: job.batch_id,
                device_id: job.device_id,
                outcome: DeliveryOutcome::Accepted,
                provider_message_id: Some("memory-provider-accepted".to_owned()),
                error: None,
                attempt: job.attempt,
            })
            .collect()
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: self.provider,
            healthy: true,
            details: "accept-all test dispatcher".to_owned(),
        }
    }
}

pub struct RetryAfterDispatcher {
    provider: PushProviderKind,
    retry_after_ms: u64,
}

impl RetryAfterDispatcher {
    pub fn new(provider: PushProviderKind, retry_after_ms: u64) -> Self {
        Self {
            provider,
            retry_after_ms,
        }
    }
}

#[async_trait]
impl PushDispatcher for RetryAfterDispatcher {
    fn provider(&self) -> PushProviderKind {
        self.provider
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        batch
            .jobs
            .into_iter()
            .map(|job| DeliveryResult {
                app_id: job.app_id,
                publish_id: job.publish_id,
                provider: job.provider,
                batch_id: job.batch_id,
                device_id: job.device_id,
                outcome: DeliveryOutcome::Retryable,
                provider_message_id: None,
                error: Some(ProviderError {
                    class: "quota".to_owned(),
                    reason: Some("retry-after".to_owned()),
                    retry_after_ms: Some(self.retry_after_ms),
                }),
                attempt: job.attempt,
            })
            .collect()
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: self.provider,
            healthy: true,
            details: "retry-after test dispatcher".to_owned(),
        }
    }
}
