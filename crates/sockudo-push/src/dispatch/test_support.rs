use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use sonic_rs::{Value, json};
use tokio::sync::Mutex;

use super::{
    ProviderAccessToken, ProviderAuthError, ProviderHttpClient, ProviderHttpRequest,
    ProviderHttpResponse, ProviderTokenSource,
};
use crate::domain::{
    DeliveryBatch, DeliveryJob, PushPayload, PushProviderKind, PushRecipient, SecretString,
};

#[derive(Default)]
pub(crate) struct MockHttpClient {
    requests: Mutex<Vec<ProviderHttpRequest>>,
    responses: Mutex<VecDeque<ProviderHttpResponse>>,
}

impl MockHttpClient {
    pub(crate) fn with_responses(responses: Vec<ProviderHttpResponse>) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
        })
    }

    pub(crate) async fn requests(&self) -> Vec<ProviderHttpRequest> {
        self.requests.lock().await.clone()
    }
}

#[async_trait]
impl ProviderHttpClient for MockHttpClient {
    async fn send(&self, request: ProviderHttpRequest) -> Result<ProviderHttpResponse, String> {
        self.requests.lock().await.push(request);
        Ok(self
            .responses
            .lock()
            .await
            .pop_front()
            .unwrap_or_else(|| response(202, json!({}))))
    }
}

pub(crate) struct CountingTokenSource {
    pub(crate) count: AtomicUsize,
    pub(crate) first_expiry: u64,
}

#[async_trait]
impl ProviderTokenSource for CountingTokenSource {
    async fn fetch_token(&self, _now_ms: u64) -> Result<ProviderAccessToken, ProviderAuthError> {
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        Ok(ProviderAccessToken {
            token: SecretString::new(format!("token-{count}")).unwrap(),
            expires_at_ms: self.first_expiry,
        })
    }
}

pub(crate) fn batch(provider: PushProviderKind) -> DeliveryBatch {
    DeliveryBatch {
        app_id: "app-1".to_owned(),
        publish_id: "publish-1".to_owned(),
        provider,
        batch_id: "batch-1".to_owned(),
        jobs: vec![DeliveryJob {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            provider,
            batch_id: "batch-1".to_owned(),
            device_id: Some("device-1".to_owned()),
            recipient: recipient(provider),
            payload: Arc::new(PushPayload {
                template_id: None,
                template_data: json!({"k": "v"}),
                title: Some("Hello".to_owned()),
                body: Some("Body".to_owned()),
                icon: None,
                sound: None,
                collapse_key: Some("collapse".to_owned()),
            }),
            attempt: 1,
            first_attempt_at_ms: None,
            not_before_ms: None,
            expires_at_ms: None,
        }],
    }
}

fn recipient(provider: PushProviderKind) -> PushRecipient {
    match provider {
        PushProviderKind::Fcm => PushRecipient::Fcm {
            registration_token: SecretString::new("fcm-token").unwrap(),
        },
        PushProviderKind::Apns => PushRecipient::Apns {
            device_token: SecretString::new("apns-token").unwrap(),
        },
        PushProviderKind::WebPush => PushRecipient::Web {
            endpoint: SecretString::new("https://push.example/subscription").unwrap(),
            p256dh: SecretString::new("p256dh").unwrap(),
            auth: SecretString::new("auth").unwrap(),
        },
        PushProviderKind::Hms => PushRecipient::Hms {
            registration_token: SecretString::new("hms-token").unwrap(),
        },
        PushProviderKind::Wns => PushRecipient::Wns {
            channel_uri: SecretString::new("https://wns.example/channel").unwrap(),
        },
    }
}

pub(crate) fn response(status: u16, body: Value) -> ProviderHttpResponse {
    ProviderHttpResponse {
        status,
        headers: BTreeMap::new(),
        body: sonic_rs::to_vec(&body).unwrap(),
    }
}
