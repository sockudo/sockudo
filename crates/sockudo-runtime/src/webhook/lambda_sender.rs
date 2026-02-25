use crate::error::{Error, Result};
use sockudo_types::webhook::Webhook;
use sonic_rs::Value;

#[derive(Clone, Default)]
pub struct LambdaWebhookSender {
    inner: sockudo_webhook_lambda::LambdaWebhookSender,
}

impl LambdaWebhookSender {
    pub fn new() -> Self {
        Self {
            inner: sockudo_webhook_lambda::LambdaWebhookSender::new(),
        }
    }

    pub async fn invoke_lambda(
        &self,
        webhook: &Webhook,
        triggering_event_name: &str,
        app_id: &str,
        pusher_webhook_payload: Value,
    ) -> Result<()> {
        self.inner
            .invoke_lambda(
                webhook,
                triggering_event_name,
                app_id,
                pusher_webhook_payload,
            )
            .await
            .map_err(Error::Other)
    }

    #[allow(dead_code)]
    pub async fn invoke_lambda_sync(
        &self,
        webhook: &Webhook,
        triggering_event_name: &str,
        app_id: &str,
        pusher_webhook_payload: Value,
    ) -> Result<Option<Value>> {
        self.inner
            .invoke_lambda_sync(
                webhook,
                triggering_event_name,
                app_id,
                pusher_webhook_payload,
            )
            .await
            .map_err(Error::Other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lambda_webhook_sender_new() {
        let _sender = LambdaWebhookSender::new();
    }
}
