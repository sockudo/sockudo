use crate::WebhookResult;
use ahash::AHashMap;
use reqwest::{Client, header};
use std::time::Duration;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct HttpWebhookSender {
    client: Client,
}

impl Default for HttpWebhookSender {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpWebhookSender {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self { client }
    }

    pub async fn send_pusher_webhook(
        &self,
        url: &str,
        app_key: &str,
        signature: &str,
        json_body: String,
        custom_headers: AHashMap<String, String>,
    ) -> WebhookResult<()> {
        debug!("Sending Pusher webhook to URL: {url}");

        let mut request_builder = self
            .client
            .post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .header("X-Pusher-Key", app_key)
            .header("X-Pusher-Signature", signature);

        for (key, value) in custom_headers {
            request_builder = request_builder.header(key, value);
        }

        match request_builder.body(json_body).send().await {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    info!("Successfully sent Pusher webhook to {url} (status: {status})");
                    Ok(())
                } else {
                    let error_text = response.text().await.unwrap_or_default();
                    error!("Pusher webhook to {url} failed with status {status}: {error_text}");
                    Err(format!("Webhook to {url} failed with status {status}"))
                }
            }
            Err(e) => {
                error!("Failed to send Pusher webhook to {url}: {e}");
                Err(format!("HTTP request failed for webhook to {url}: {e}"))
            }
        }
    }
}
