use crate::WebhookResult;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvocationType;
use dashmap::DashMap;
use sockudo_types::webhook::{LambdaConfig, Webhook};
use sonic_rs::{Value, json};
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct LambdaWebhookSender {
    clients: DashMap<String, LambdaClient>,
}

impl Default for LambdaWebhookSender {
    fn default() -> Self {
        Self::new()
    }
}

impl LambdaWebhookSender {
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
        }
    }

    async fn get_client(&self, region: &str) -> WebhookResult<LambdaClient> {
        if let Some(client_ref) = self.clients.get(region) {
            return Ok(client_ref.clone());
        }

        let region_provider = RegionProviderChain::first_try(Region::new(region.to_string()))
            .or_default_provider()
            .or_else(Region::new("us-east-1"));

        let shared_config = aws_config::from_env()
            .region(region_provider)
            .timeout_config(
                aws_sdk_lambda::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(10))
                    .build(),
            )
            .load()
            .await;

        let client = LambdaClient::new(&shared_config);
        self.clients.insert(region.to_string(), client.clone());
        Ok(client)
    }

    pub async fn invoke_lambda(
        &self,
        webhook: &Webhook,
        triggering_event_name: &str,
        app_id: &str,
        pusher_webhook_payload: Value,
    ) -> WebhookResult<()> {
        let temp_owned_config: LambdaConfig;

        let lambda_config_ref: &LambdaConfig = match &webhook.lambda {
            Some(config_struct) => config_struct,
            None => {
                if let Some(function_name_str) = &webhook.lambda_function {
                    warn!(
                        "Webhook for app {app_id} uses legacy 'lambda_function' field. Defaulting region to 'us-east-1'."
                    );
                    temp_owned_config = LambdaConfig {
                        function_name: function_name_str.clone(),
                        region: "us-east-1".to_string(),
                    };
                    &temp_owned_config
                } else {
                    error!("Missing Lambda configuration in webhook for app_id: {app_id}");
                    return Err("Missing Lambda configuration".to_string());
                }
            }
        };

        let client = self.get_client(&lambda_config_ref.region).await?;

        let payload_bytes = sonic_rs::to_vec(&pusher_webhook_payload)
            .map_err(|e| format!("Failed to serialize Pusher Webhook payload for Lambda: {e}"))?;

        info!(
            "Invoking Lambda function '{}' in region '{}' for app '{}', triggered by '{}'. Payload size: {} bytes.",
            lambda_config_ref.function_name,
            lambda_config_ref.region,
            app_id,
            triggering_event_name,
            payload_bytes.len()
        );

        client
            .invoke()
            .function_name(&lambda_config_ref.function_name)
            .payload(Blob::new(payload_bytes))
            .invocation_type(InvocationType::Event)
            .send()
            .await
            .map_err(|e| {
                error!(
                    "Failed to invoke Lambda function {}: {e}",
                    lambda_config_ref.function_name
                );
                format!("Failed to invoke Lambda function: {e}")
            })?;

        info!(
            "Successfully invoked Lambda function {} for app '{}', triggered by '{}'.",
            lambda_config_ref.function_name, app_id, triggering_event_name
        );

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn invoke_lambda_sync(
        &self,
        webhook: &Webhook,
        triggering_event_name: &str,
        app_id: &str,
        pusher_webhook_payload: Value,
    ) -> WebhookResult<Option<Value>> {
        let temp_owned_config: LambdaConfig;
        let lambda_config_ref: &LambdaConfig = match &webhook.lambda {
            Some(config_struct) => config_struct,
            None => {
                if let Some(function_name_str) = &webhook.lambda_function {
                    temp_owned_config = LambdaConfig {
                        function_name: function_name_str.clone(),
                        region: "us-east-1".to_string(),
                    };
                    &temp_owned_config
                } else {
                    return Err("Missing Lambda configuration".to_string());
                }
            }
        };

        let client = self.get_client(&lambda_config_ref.region).await?;

        let payload_bytes = sonic_rs::to_vec(&pusher_webhook_payload).map_err(|e| {
            format!("Failed to serialize Pusher Webhook payload for Lambda sync: {e}")
        })?;

        info!(
            "Invoking Lambda function {} synchronously for app '{}', triggered by '{}'.",
            lambda_config_ref.function_name, app_id, triggering_event_name
        );

        let output = client
            .invoke()
            .function_name(&lambda_config_ref.function_name)
            .payload(Blob::new(payload_bytes))
            .invocation_type(InvocationType::RequestResponse)
            .send()
            .await
            .map_err(|e| {
                format!(
                    "Failed to invoke Lambda function {} synchronously: {e}",
                    lambda_config_ref.function_name
                )
            })?;

        if let Some(response_payload_blob) = output.payload() {
            match sonic_rs::from_slice::<Value>(response_payload_blob.as_ref()) {
                Ok(json_response) => Ok(Some(json_response)),
                Err(e) => {
                    warn!("Failed to parse Lambda response as JSON: {e}. Returning raw response.");
                    let response_str = String::from_utf8_lossy(response_payload_blob.as_ref());
                    Ok(Some(json!({ "raw_response": response_str.to_string() })))
                }
            }
        } else {
            Ok(None)
        }
    }
}
