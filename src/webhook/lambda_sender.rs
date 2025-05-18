// src/webhook/lambda_sender.rs

use crate::error::{Error, Result};

use crate::webhook::types::{LambdaConfig, PusherWebhookPayload, Webhook}; // Added PusherWebhookPayload for clarity
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_lambda::config::Region; // Credentials not directly used here for client creation
use aws_sdk_lambda::error::SdkError; // Keep for sync if needed
use aws_sdk_lambda::operation::invoke::{InvokeError, InvokeOutput}; // Keep for sync if needed
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvocationType;
use aws_sdk_lambda::Client as LambdaClient;
use serde_json::{json, Value};
use std::time::Duration;
use tracing::{error, info, warn};

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

    /// Invoke a Lambda function with the provided webhook and payload.
    /// The `payload` argument is expected to be the complete PusherWebhookPayload.
    /// The `triggering_event_name` is for logging/context, could be "batch_events" or a specific event.
    pub async fn invoke_lambda(
        &self,
        webhook: &Webhook,
        triggering_event_name: &str,
        app_id: &str,
        pusher_webhook_payload: Value,
    ) -> Result<()> {
        // This `temp_owned_config` allows us to create an owned LambdaConfig if needed
        // for the legacy `lambda_function` case, and then take a reference to it.
        // It must be declared outside the match so its lifetime extends for `lambda_config_ref`.
        let temp_owned_config: LambdaConfig;

        let lambda_config_ref: &LambdaConfig = match &webhook.lambda {
            Some(config_struct) => {
                // If the structured 'lambda' field exists, use it directly.
                config_struct
            }
            None => {
                // If 'lambda' is None, try the legacy 'lambda_function' string.
                if let Some(function_name_str) = &webhook.lambda_function {
                    warn!("{}", format!(
                        "Webhook for app {} uses legacy 'lambda_function' field. Defaulting region to 'us-east-1'. Consider updating to structured 'lambda' config.",
                        app_id
                    ));
                    // Assign to the outer `temp_owned_config`
                    temp_owned_config = LambdaConfig {
                        function_name: function_name_str.clone(),
                        region: "us-east-1".to_string(),
                    };
                    // Now `lambda_config_ref` can borrow from `temp_owned_config`.
                    &temp_owned_config
                } else {
                    // If both 'lambda' and 'lambda_function' are None, it's an error.
                    error!(
                        "{}",
                        format!(
                            "Missing Lambda configuration in webhook for app_id: {}",
                            app_id
                        )
                    );
                    return Err(Error::InternalError(
                        "Missing Lambda configuration: Neither 'lambda' struct nor 'lambda_function' string provided.".to_string(),
                    ));
                }
            }
        };

        // Now `lambda_config_ref` is guaranteed to be a valid reference to a LambdaConfig.
        let client = self.get_client(&lambda_config_ref.region).await?;

        let payload_bytes = serde_json::to_vec(&pusher_webhook_payload).map_err(|e| {
            Error::Other(format!(
                "Failed to serialize Pusher Webhook payload for Lambda: {}",
                e
            ))
        })?;

        info!("{}", format!(
            "Invoking Lambda function '{}' in region '{}' for app '{}', triggered by '{}'. Payload size: {} bytes.",
            lambda_config_ref.function_name, lambda_config_ref.region, app_id, triggering_event_name, payload_bytes.len()
        ));

        match client
            .invoke()
            .function_name(&lambda_config_ref.function_name)
            .payload(Blob::new(payload_bytes))
            .invocation_type(InvocationType::Event)
            .send()
            .await
        {
            Ok(_) => {
                info!(
                    "{}",
                    format!(
                        "Successfully invoked Lambda function {} for app '{}', triggered by '{}'",
                        lambda_config_ref.function_name, app_id, triggering_event_name
                    )
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "{}",
                    format!(
                        "Failed to invoke Lambda function {}: {}",
                        lambda_config_ref.function_name, e
                    )
                );
                Err(Error::Other(format!(
                    "Failed to invoke Lambda function: {}",
                    e
                )))
            }
        }
    }

    /// Invoke a Lambda function synchronously and parse the response
    #[allow(dead_code)]
    pub async fn invoke_lambda_sync(
        &self,
        webhook: &Webhook,
        triggering_event_name: &str,
        app_id: &str,
        pusher_webhook_payload: Value,
    ) -> Result<Option<Value>> {
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
                    return Err(Error::InternalError(
                        "Missing Lambda configuration".to_string(),
                    ));
                }
            }
        };

        let client = self.get_client(&lambda_config_ref.region).await?;

        let payload_bytes = serde_json::to_vec(&pusher_webhook_payload).map_err(|e| {
            Error::Other(format!(
                "Failed to serialize Pusher Webhook payload for Lambda sync: {}",
                e
            ))
        })?;

        info!(
            "{}",
            format!(
                "Invoking Lambda function {} synchronously for app '{}', triggered by '{}'",
                lambda_config_ref.function_name, app_id, triggering_event_name
            )
        );

        let result: core::result::Result<InvokeOutput, SdkError<InvokeError>> = client
            .invoke()
            .function_name(&lambda_config_ref.function_name)
            .payload(Blob::new(payload_bytes))
            .invocation_type(InvocationType::RequestResponse)
            .send()
            .await;

        match result {
            Ok(output) => {
                if let Some(response_payload_blob) = output.payload() {
                    match serde_json::from_slice::<Value>(response_payload_blob.as_ref()) {
                        Ok(json_response) => {
                            info!(
                                "{}",
                                format!(
                                    "Received response from Lambda function {}: {:?}",
                                    lambda_config_ref.function_name, json_response
                                )
                            );
                            Ok(Some(json_response))
                        }
                        Err(e) => {
                            warn!(
                                "{}",
                                format!(
                                "Failed to parse Lambda response as JSON: {}. Raw response: {:?}",
                                e, response_payload_blob
                            )
                            );
                            let response_str =
                                String::from_utf8_lossy(response_payload_blob.as_ref());
                            Ok(Some(json!({"raw_response": response_str.to_string() })))
                        }
                    }
                } else {
                    info!(
                        "{}",
                        format!(
                            "Lambda function {} returned no payload",
                            lambda_config_ref.function_name
                        )
                    );
                    Ok(None)
                }
            }
            Err(e) => {
                error!(
                    "{}",
                    format!(
                        "Failed to invoke Lambda function {} synchronously: {}",
                        lambda_config_ref.function_name, e
                    )
                );
                Err(Error::Other(format!(
                    "Failed to invoke Lambda function synchronously: {}",
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
