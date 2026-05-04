use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::json;
use sockudo_push::{
    CachedTokenProvider, DeliveryBatch, DeliveryJob, NativeWebPushCrypto, PushDispatcher,
    PushPayload, PushProviderKind, PushRecipient, ReqwestProviderHttpClient, SecretString,
    StaticTokenSource,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserPushSubscription {
    endpoint: String,
    keys: BrowserPushKeys,
}

#[derive(Debug, Deserialize)]
struct BrowserPushKeys {
    p256dh: String,
    auth: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscription_json = std::env::var("WEBPUSH_SUBSCRIPTION_JSON")
        .map_err(|_| "WEBPUSH_SUBSCRIPTION_JSON is required")?;
    let subscription: BrowserPushSubscription = serde_json::from_str(&subscription_json)?;
    let vapid_private_key =
        std::env::var("VAPID_PRIVATE_KEY").map_err(|_| "VAPID_PRIVATE_KEY is required")?;
    let vapid_contact = std::env::var("VAPID_CONTACT")
        .unwrap_or_else(|_| "mailto:sockudo-webpush-e2e@example.com".to_owned());
    let title =
        std::env::var("WEBPUSH_TITLE").unwrap_or_else(|_| "Sockudo Web Push E2E".to_owned());
    let body = std::env::var("WEBPUSH_BODY")
        .unwrap_or_else(|_| "This notification was sent by Sockudo.".to_owned());
    let now_ms = now_ms();
    let publish_id =
        std::env::var("WEBPUSH_PUBLISH_ID").unwrap_or_else(|_| format!("webpush-e2e-{now_ms}"));
    let collapse_key = std::env::var("WEBPUSH_COLLAPSE_KEY")
        .unwrap_or_else(|_| format!("sockudo-webpush-e2e-{now_ms}"));

    let dispatcher = sockudo_push::WebPushDispatcher::new(
        "browser-push",
        CachedTokenProvider::new(Arc::new(StaticTokenSource::new(
            SecretString::new("unused-for-vapid").map_err(|e| e.to_string())?,
            now_ms + 600_000,
        ))),
        Arc::new(NativeWebPushCrypto::new(vapid_private_key, vapid_contact)),
        Arc::new(ReqwestProviderHttpClient::new()?),
    );

    let batch = DeliveryBatch {
        app_id: "webpush-e2e".to_owned(),
        publish_id: publish_id.clone(),
        provider: PushProviderKind::WebPush,
        batch_id: "batch-1".to_owned(),
        jobs: vec![DeliveryJob {
            app_id: "webpush-e2e".to_owned(),
            publish_id,
            provider: PushProviderKind::WebPush,
            batch_id: "batch-1".to_owned(),
            device_id: Some("browser".to_owned()),
            recipient: PushRecipient::Web {
                endpoint: SecretString::new(subscription.endpoint).map_err(|e| e.to_string())?,
                p256dh: SecretString::new(subscription.keys.p256dh).map_err(|e| e.to_string())?,
                auth: SecretString::new(subscription.keys.auth).map_err(|e| e.to_string())?,
            },
            payload: PushPayload {
                template_id: None,
                template_data: json!({
                    "source": "sockudo-webpush-e2e",
                    "sentAtMs": now_ms
                }),
                title: Some(title),
                body: Some(body),
                icon: None,
                sound: None,
                collapse_key: Some(collapse_key),
            },
            attempt: 1,
            not_before_ms: None,
            expires_at_ms: None,
        }],
    };

    let results = dispatcher.dispatch(batch).await;
    println!("{}", serde_json::to_string_pretty(&results)?);
    if results
        .iter()
        .all(|result| matches!(result.outcome, sockudo_push::DeliveryOutcome::Accepted))
    {
        Ok(())
    } else {
        Err("web push dispatch was not accepted".into())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before unix epoch")
        .as_millis() as u64
}
