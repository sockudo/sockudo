use aws_config::meta::region::RegionProviderChain;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::error::SdkError;
use aws_sdk_lambda::operation::invoke::{InvokeError, InvokeOutput};
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvocationType;
use sockudo_core::error::{Error, Result};
use sockudo_core::webhook_types::{LambdaConfig, Webhook};
use sonic_rs::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing::{debug, error, info, warn};

/// Handles invoking AWS Lambda functions for webhooks
#[derive(Clone)]
pub struct LambdaWebhookSender {
    clients: Arc<parking_lot::Mutex<ahash::AHashMap<String, Arc<OnceCell<LambdaClient>>>>>,
}

impl LambdaWebhookSender {
    /// Create a new Lambda webhook sender
    pub fn new() -> Self {
        Self {
            clients: Arc::new(parking_lot::Mutex::new(ahash::AHashMap::new())),
        }
    }

    /// Get or create a Lambda client for a specific region
    async fn get_client(&self, region: &str) -> Result<LambdaClient> {
        // Drop the map guard before async initialization; clones share one regional
        // cell, and cancellation lets another waiter finish initialization.
        let cell = {
            let mut clients = self.clients.lock();
            if let Some(cell) = clients.get(region) {
                Arc::clone(cell)
            } else {
                // Configured regions can churn as apps are reloaded. Eviction
                // releases only our reference; active invocations retain theirs.
                if clients.len() >= 64
                    && let Some(key) = clients.keys().next().cloned()
                {
                    clients.remove(&key);
                }
                let cell = Arc::new(OnceCell::new());
                clients.insert(region.to_owned(), Arc::clone(&cell));
                cell
            }
        };
        let client = cell
            .get_or_init(|| async {
                let region_provider =
                    RegionProviderChain::first_try(Region::new(region.to_string()))
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

                LambdaClient::new(&shared_config)
            })
            .await;
        Ok(client.clone())
    }

    /// Invoke a Lambda function with the provided webhook and payload.
    pub async fn invoke_lambda(
        &self,
        webhook: &Webhook,
        triggering_event_name: &str,
        app_id: &str,
        pusher_webhook_payload: Value,
    ) -> Result<()> {
        let temp_owned_config: LambdaConfig;

        let lambda_config_ref: &LambdaConfig = match &webhook.lambda {
            Some(config_struct) => config_struct,
            None => {
                if let Some(function_name_str) = &webhook.lambda_function {
                    warn!(
                        app_id = %app_id,
                        "legacy lambda_function field used, consider migrating to structured lambda config"
                    );
                    temp_owned_config = LambdaConfig {
                        function_name: function_name_str.clone(),
                        region: "us-east-1".to_string(),
                    };
                    &temp_owned_config
                } else {
                    error!(app_id = %app_id, "missing lambda configuration in webhook");
                    return Err(Error::Internal(
                        "Missing Lambda configuration: Neither 'lambda' struct nor 'lambda_function' string provided.".to_string(),
                    ));
                }
            }
        };

        let client = self.get_client(&lambda_config_ref.region).await?;

        let payload_bytes = sonic_rs::to_vec(&pusher_webhook_payload).map_err(|e| {
            Error::Other(format!(
                "Failed to serialize Pusher Webhook payload for Lambda: {e}"
            ))
        })?;

        info!(
            app_id = %app_id,
            function_name = %lambda_config_ref.function_name,
            event = %triggering_event_name,
            payload_bytes = payload_bytes.len(),
            "invoking lambda function"
        );

        match client
            .invoke()
            .function_name(&lambda_config_ref.function_name)
            .payload(Blob::new(payload_bytes))
            .invocation_type(InvocationType::Event)
            .send()
            .await
        {
            Ok(_) => {
                debug!(
                    app_id = %app_id,
                    function_name = %lambda_config_ref.function_name,
                    event = %triggering_event_name,
                    "lambda function invoked successfully"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    app_id = %app_id,
                    function_name = %lambda_config_ref.function_name,
                    error = %e,
                    "lambda function invocation failed"
                );
                Err(Error::Other(format!(
                    "Failed to invoke Lambda function: {e}"
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
                    return Err(Error::Internal("Missing Lambda configuration".to_string()));
                }
            }
        };

        let client = self.get_client(&lambda_config_ref.region).await?;

        let payload_bytes = sonic_rs::to_vec(&pusher_webhook_payload).map_err(|e| {
            Error::Other(format!(
                "Failed to serialize Pusher Webhook payload for Lambda sync: {e}"
            ))
        })?;

        info!(
            app_id = %app_id,
            function_name = %lambda_config_ref.function_name,
            event = %triggering_event_name,
            "invoking lambda function synchronously"
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
                    match sonic_rs::from_slice::<Value>(response_payload_blob.as_ref()) {
                        Ok(json_response) => {
                            debug!(
                                function_name = %lambda_config_ref.function_name,
                                "lambda function returned response"
                            );
                            Ok(Some(json_response))
                        }
                        Err(e) => {
                            warn!(
                                function_name = %lambda_config_ref.function_name,
                                error = %e,
                                "lambda function response is not valid json"
                            );
                            let response_str =
                                String::from_utf8_lossy(response_payload_blob.as_ref());
                            Ok(Some(json!({"raw_response": response_str.to_string() })))
                        }
                    }
                } else {
                    info!(
                        function_name = %lambda_config_ref.function_name,
                        "lambda function returned no payload"
                    );
                    Ok(None)
                }
            }
            Err(e) => {
                error!(
                    function_name = %lambda_config_ref.function_name,
                    error = %e,
                    "lambda function synchronous invocation failed"
                );
                Err(Error::Other(format!(
                    "Failed to invoke Lambda function synchronously: {e}"
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

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::webhook_types::Webhook;

    #[tokio::test]
    async fn cloned_senders_share_one_cancellation_safe_region_initialization() {
        let sender = LambdaWebhookSender::new();
        let clone = sender.clone();
        assert!(Arc::ptr_eq(&sender.clients, &clone.clients));
        let cell = Arc::new(OnceCell::new());
        sender
            .clients
            .lock()
            .insert("test-region".into(), cell.clone());
        let first = {
            let cell = cell.clone();
            tokio::spawn(async move {
                cell.get_or_init(|| async { std::future::pending::<LambdaClient>().await })
                    .await
                    .clone()
            })
        };
        tokio::task::yield_now().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let config = aws_sdk_lambda::config::Builder::new()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .build();
        let client = LambdaClient::from_conf(config);
        assert!(cell.set(client).is_ok());
        let from_clone = clone.clients.lock().get("test-region").unwrap().clone();
        assert!(Arc::ptr_eq(&cell, &from_clone));
        assert!(from_clone.get().is_some());
    }

    #[test]
    fn test_lambda_webhook_sender_new() {
        let sender = LambdaWebhookSender::new();
        assert!(sender.clients.lock().is_empty());
    }

    #[tokio::test]
    async fn test_lambda_webhook_sender_with_clients() {
        let sender = LambdaWebhookSender::new();
        let _client = sender.get_client("us-east-1").await.unwrap();
        assert!(!sender.clients.lock().is_empty());
    }

    #[tokio::test]
    async fn test_invoke_lambda_success() {
        let sender = LambdaWebhookSender::new();
        let webhook = Webhook {
            lambda: Some(LambdaConfig {
                function_name: "test-function".to_string(),
                region: "us-east-1".to_string(),
            }),
            ..Default::default()
        };
        let result = sender
            .invoke_lambda(&webhook, "test_event", "test_app", json!({}))
            .await;
        // Should fail without AWS credentials, which is expected in test environment
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invoke_lambda_error() {
        let sender = LambdaWebhookSender::new();
        let webhook = Webhook {
            lambda: Some(LambdaConfig {
                function_name: "test-function".to_string(),
                region: "invalid-region".to_string(),
            }),
            ..Default::default()
        };
        let result = sender
            .invoke_lambda(&webhook, "test_event", "test_app", json!({}))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invoke_lambda_sync() {
        let sender = LambdaWebhookSender::new();
        let webhook = Webhook {
            lambda: Some(LambdaConfig {
                function_name: "test-function".to_string(),
                region: "us-east-1".to_string(),
            }),
            ..Default::default()
        };
        let result = sender
            .invoke_lambda_sync(&webhook, "test_event", "test_app", json!({}))
            .await;
        // Should fail without AWS credentials, which is expected in test environment
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invoke_lambda_sync_error() {
        let sender = LambdaWebhookSender::new();
        let webhook = Webhook {
            lambda: Some(LambdaConfig {
                function_name: "test-function".to_string(),
                region: "invalid-region".to_string(),
            }),
            ..Default::default()
        };
        let result = sender
            .invoke_lambda_sync(&webhook, "test_event", "test_app", json!({}))
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_lambda_config_serialization() {
        let config = LambdaConfig {
            function_name: "test-function".to_string(),
            region: "us-east-1".to_string(),
        };
        let serialized = sonic_rs::to_string(&config).unwrap();
        let deserialized: LambdaConfig = sonic_rs::from_str(&serialized).unwrap();
        assert_eq!(config.function_name, deserialized.function_name);
        assert_eq!(config.region, deserialized.region);
    }
}
