// src/webhook/lambda_sender.rs

use crate::error::{Error, Result};
use crate::log::Log;
use crate::webhook::types::{LambdaConfig, Webhook};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_lambda::config::{Credentials, Region};
use aws_sdk_lambda::error::SdkError;
use aws_sdk_lambda::operation::invoke::{InvokeError, InvokeOutput};
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvocationType;
use aws_sdk_lambda::Client as LambdaClient;
use serde_json::{json, Value};
use std::time::Duration;

/// Handles invoking AWS Lambda functions for webhooks
#[derive(Clone)]
pub struct LambdaWebhookSender {
    // Cache Lambda clients by region to avoid recreating them
    clients: dashmap::DashMap<String, LambdaClient>,
}

impl LambdaWebhookSender {
    /// Create a new Lambda webhook sender
    pub fn new() -> Self {
        Self {
            clients: dashmap::DashMap::new(),
        }
    }

    /// Get or create a Lambda client for a specific region
    async fn get_client(&self, region: &str) -> Result<LambdaClient> {
        // Check if client already exists in cache
        if let Some(client) = self.clients.get(region) {
            return Ok(client.clone());
        }

        // Create AWS config with the specified region
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

        // Create Lambda client
        let client = LambdaClient::new(&shared_config);

        // Store in cache and return
        self.clients.insert(region.to_string(), client.clone());
        Ok(client)
    }

    /// Invoke a Lambda function with the provided webhook and payload
    pub async fn invoke_lambda(
        &self,
        webhook: &Webhook,
        event: &str,
        app_id: &str,
        payload: Value,
    ) -> Result<()> {
        // Get Lambda configuration
        let lambda_config = match &webhook.lambda {
            Some(config) => config,
            None => {
                // If lambda field is not set, try to use the legacy lambda_function field
                if let Some(function_name) = &webhook.lambda_function {
                    // Create a temporary config with default region
                    &LambdaConfig {
                        function_name: function_name.clone(),
                        region: "us-east-1".to_string(), // Default region
                    }
                } else {
                    Log::error("Missing Lambda configuration in webhook");
                    return Err(Error::InternalError(
                        "Missing Lambda configuration".to_string(),
                    ));
                }
            }
        };

        // Get the Lambda client for this region
        let client = self.get_client(&lambda_config.region).await?;

        // Add metadata to the payload
        let lambda_payload = json!({
            "webhook_event": event,
            "app_id": app_id,
            "payload": payload,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        // Convert payload to bytes
        let payload_bytes = serde_json::to_vec(&lambda_payload)
            .map_err(|e| Error::Other(format!("Failed to serialize Lambda payload: {}", e)))?;

        // Invoke Lambda function
        Log::webhook_sender(format!(
            "Invoking Lambda function {} in region {} for event '{}'",
            lambda_config.function_name, lambda_config.region, event
        ));

        match client
            .invoke()
            .function_name(&lambda_config.function_name)
            .payload(Blob::new(payload_bytes))
            .invocation_type(InvocationType::Event) // Asynchronous invocation
            .send()
            .await
        {
            Ok(_) => {
                Log::webhook_sender(format!(
                    "Successfully invoked Lambda function {} for event '{}'",
                    lambda_config.function_name, event
                ));
                Ok(())
            }
            Err(e) => {
                Log::error(format!(
                    "Failed to invoke Lambda function {}: {}",
                    lambda_config.function_name, e
                ));
                Err(Error::Other(format!(
                    "Failed to invoke Lambda function: {}",
                    e
                )))
            }
        }
    }

    /// Invoke a Lambda function synchronously and parse the response
    #[allow(dead_code)] // May be useful for future features
    pub async fn invoke_lambda_sync(
        &self,
        webhook: &Webhook,
        event: &str,
        app_id: &str,
        payload: Value,
    ) -> Result<Option<Value>> {
        // Get Lambda configuration (same as above)
        let lambda_config = match &webhook.lambda {
            Some(config) => config,
            None => {
                if let Some(function_name) = &webhook.lambda_function {
                    &LambdaConfig {
                        function_name: function_name.clone(),
                        region: "us-east-1".to_string(),
                    }
                } else {
                    return Err(Error::InternalError(
                        "Missing Lambda configuration".to_string(),
                    ));
                }
            }
        };

        // Get the Lambda client for this region
        let client = self.get_client(&lambda_config.region).await?;

        // Prepare payload (same as above)
        let lambda_payload = json!({
            "webhook_event": event,
            "app_id": app_id,
            "payload": payload,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        let payload_bytes = serde_json::to_vec(&lambda_payload)
            .map_err(|e| Error::Other(format!("Failed to serialize Lambda payload: {}", e)))?;

        // Invoke Lambda function synchronously
        Log::webhook_sender(format!(
            "Invoking Lambda function {} synchronously for event '{}'",
            lambda_config.function_name, event
        ));

        let result: core::result::Result<InvokeOutput, SdkError<InvokeError>> = client
            .invoke()
            .function_name(&lambda_config.function_name)
            .payload(Blob::new(payload_bytes))
            .invocation_type(InvocationType::RequestResponse) // Synchronous invocation
            .send()
            .await;

        match result {
            Ok(output) => {
                // Parse the response payload if available
                if let Some(response_payload) = output.payload() {
                    match serde_json::from_slice::<Value>(response_payload.as_ref()) {
                        Ok(json_response) => {
                            Log::webhook_sender(format!(
                                "Received response from Lambda function {}: {:?}",
                                lambda_config.function_name, json_response
                            ));
                            Ok(Some(json_response))
                        }
                        Err(e) => {
                            Log::warning(format!("Failed to parse Lambda response as JSON: {}", e));
                            // Return the raw response as a string
                            let response_str = String::from_utf8_lossy(response_payload.as_ref());
                            Ok(Some(json!({"raw_response": response_str})))
                        }
                    }
                } else {
                    Log::webhook_sender(format!(
                        "Lambda function {} returned no payload",
                        lambda_config.function_name
                    ));
                    Ok(None)
                }
            }
            Err(e) => {
                Log::error(format!(
                    "Failed to invoke Lambda function {} synchronously: {}",
                    lambda_config.function_name, e
                ));
                Err(Error::Other(format!(
                    "Failed to invoke Lambda function: {}",
                    e
                )))
            }
        }
    }
}

impl Default for LambdaWebhookSender {
    fn default() -> Self {
        Self::new()
    }
}
