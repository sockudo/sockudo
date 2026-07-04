use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Mutex;

use crate::domain::{PushPayload, PushRecipient, SecretString};
use crate::pipeline::{PushQueuePayload, PushQueueStage, QueueMessage};

use super::apns::classify_apns_response;
use super::fcm::classify_fcm_response;
use super::webpush::classify_webpush_response;
use super::*;

#[derive(Default)]
struct MockHttpClient {
    requests: Mutex<Vec<ProviderHttpRequest>>,
    responses: Mutex<VecDeque<ProviderHttpResponse>>,
}

impl MockHttpClient {
    fn with_responses(responses: Vec<ProviderHttpResponse>) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
        })
    }

    async fn requests(&self) -> Vec<ProviderHttpRequest> {
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

struct CountingTokenSource {
    count: AtomicUsize,
    first_expiry: u64,
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

#[tokio::test]
async fn provider_dispatchers_build_expected_headers_and_payloads() {
    let http = MockHttpClient::with_responses(vec![
        response(200, json!({"name": "fcm-message"})),
        response(200, json!({})),
        response(201, json!({})),
        response(200, json!({"code": "80000000", "requestId": "hms-id"})),
        response(201, json!({})),
    ]);
    let token = cached_static_token("access-token", now_ms() + 600_000);

    let dispatchers: Vec<Box<dyn PushDispatcher + Send + Sync>> = vec![
        Box::new(
            FcmDispatcher::new("project-1", token.clone(), http.clone())
                .with_base_url("https://fcm.test"),
        ),
        Box::new(
            ApnsDispatcher::new("com.example.app", token.clone(), http.clone())
                .with_base_url("https://apns.test"),
        ),
        Box::new(WebPushDispatcher::new(
            "https://updates.push.services.mozilla.com",
            token.clone(),
            Arc::new(PassthroughWebPushCrypto),
            http.clone(),
        )),
        Box::new(
            HmsDispatcher::new("hms-app", token.clone(), http.clone())
                .with_base_url("https://hms.test"),
        ),
        Box::new(WnsDispatcher::new(token, http.clone())),
    ];
    let providers = [
        PushProviderKind::Fcm,
        PushProviderKind::Apns,
        PushProviderKind::WebPush,
        PushProviderKind::Hms,
        PushProviderKind::Wns,
    ];
    for (dispatcher, provider) in dispatchers.into_iter().zip(providers) {
        let results = dispatcher.dispatch(batch(provider)).await;
        assert_eq!(results[0].outcome, DeliveryOutcome::Accepted);
    }

    let requests = http.requests().await;
    assert_eq!(requests.len(), 5);
    assert!(
        requests[0]
            .url
            .contains("/v1/projects/project-1/messages:send")
    );
    assert!(String::from_utf8_lossy(&requests[0].body).contains("\"token\""));
    assert_eq!(requests[1].headers["apns-topic"], "com.example.app");
    assert_eq!(requests[2].headers["content-encoding"], "aes128gcm");
    assert!(requests[3].url.contains("/v1/hms-app/messages:send"));
    assert_eq!(requests[4].headers["x-wns-type"], "wns/toast");
    for request in requests {
        assert_eq!(
            request
                .authorization
                .as_ref()
                .map(SecretString::expose_secret),
            Some("Bearer access-token")
        );
    }
}

#[tokio::test]
async fn auth_cache_refreshes_only_inside_five_minute_window() {
    let source = Arc::new(CountingTokenSource {
        count: AtomicUsize::new(0),
        first_expiry: now_ms() + 600_000,
    });
    let provider = CachedTokenProvider::new(source.clone());

    let first = provider.access_token(now_ms()).await.unwrap();
    let second = provider.access_token(now_ms()).await.unwrap();

    assert_eq!(first.expose_secret(), "token-0");
    assert_eq!(second.expose_secret(), "token-0");
    assert_eq!(source.count.load(Ordering::SeqCst), 1);
}

#[test]
fn classifies_provider_error_classes_and_retry_after() {
    let retry_at = classify_fcm_response(&ProviderHttpResponse {
        status: 429,
        headers: BTreeMap::from([("retry-after".to_owned(), "7".to_owned())]),
        body: br#"{"error":{"status":"RESOURCE_EXHAUSTED"}}"#.to_vec(),
    })
    .1
    .unwrap()
    .retry_after_ms
    .unwrap();
    assert!(retry_at > now_ms());

    for (provider, status, class) in [
        (PushProviderKind::Fcm, 400, "invalid_payload"),
        (PushProviderKind::Apns, 410, "invalid_token"),
        (PushProviderKind::WebPush, 413, "invalid_payload"),
        (PushProviderKind::Hms, 401, "auth_failure"),
        (PushProviderKind::Wns, 410, "invalid_token"),
    ] {
        let error = match provider {
            PushProviderKind::Fcm => classify_fcm_response(&response(status, json!({}))).1,
            PushProviderKind::Apns => classify_apns_response(&response(status, json!({}))).1,
            PushProviderKind::WebPush => classify_webpush_response(&response(status, json!({}))).1,
            PushProviderKind::Hms => classify_hms_response(&response(status, json!({}))).1,
            PushProviderKind::Wns => classify_wns_response(&response(status, json!({}))).1,
        }
        .unwrap();
        assert_eq!(error.class, class);
    }
}

#[test]
fn provider_outage_responses_are_retryable() {
    let fcm = classify_fcm_response(&ProviderHttpResponse {
        status: 429,
        headers: BTreeMap::from([("retry-after".to_owned(), "3".to_owned())]),
        body: br#"{"error":{"status":"RESOURCE_EXHAUSTED"}}"#.to_vec(),
    });
    assert_eq!(fcm.0, DeliveryOutcome::Retryable);
    assert_eq!(fcm.1.as_ref().unwrap().class, "quota");
    assert!(fcm.1.as_ref().unwrap().retry_after_ms.is_some());

    for status in [429, 500] {
        let apns = classify_apns_response(&response(status, json!({"reason": "TooManyRequests"})));
        assert_eq!(apns.0, DeliveryOutcome::Retryable, "apns {status}");
    }
    for status in [429, 503] {
        let webpush = classify_webpush_response(&response(status, json!({})));
        assert_eq!(webpush.0, DeliveryOutcome::Retryable, "webpush {status}");
    }

    let mut batch = batch(PushProviderKind::Fcm);
    let job = batch.jobs.remove(0);
    let timeout = classify_http_result(
        job,
        Err("transport timeout".to_owned()),
        classify_fcm_response,
    );
    assert_eq!(timeout.outcome, DeliveryOutcome::Retryable);
    assert_eq!(timeout.error.unwrap().class, "unavailable");
}

#[test]
fn adaptive_rate_limiter_shrinks_and_grows_slowly() {
    let mut limiter = AdaptiveRateLimiter::default();
    assert_eq!(limiter.limit("app-1", PushProviderKind::Fcm), 100);
    limiter.record_throttle("app-1", PushProviderKind::Fcm, 1_000);
    assert_eq!(limiter.limit("app-1", PushProviderKind::Fcm), 50);
    limiter.record_success_window("app-1", PushProviderKind::Fcm, 30_000);
    assert_eq!(limiter.limit("app-1", PushProviderKind::Fcm), 50);
    limiter.record_success_window("app-1", PushProviderKind::Fcm, 62_000);
    assert_eq!(limiter.limit("app-1", PushProviderKind::Fcm), 51);
}

#[test]
fn circuit_breaker_opens_half_opens_and_closes() {
    let mut breaker = ProviderCircuitBreaker::default();
    for _ in 0..4 {
        assert!(!breaker.record_failure(1_000));
    }
    assert!(breaker.record_failure(1_000));
    assert!(breaker.is_open(2_000));
    assert!(!breaker.is_open(31_000));
    breaker.record_success();
    assert!(!breaker.is_open(31_001));
}

#[test]
fn weighted_scheduler_downgrades_over_quota_tenants_and_caps_each_lane() {
    let mut scheduler = WeightedFairScheduler::default()
        .with_over_quota_tenants(["noisy".to_owned()])
        .with_tenant_inflight_cap(3);
    for index in 0..12 {
        scheduler.push(queue_message("noisy", index));
    }
    for index in 0..4 {
        scheduler.push(queue_message("quiet-a", index));
        scheduler.push(queue_message("quiet-b", index));
    }

    let mut order = Vec::new();
    while let Some(message) = scheduler.pop_next() {
        if let PushQueuePayload::DeliveryBatch(batch) = message.payload {
            order.push(batch.app_id);
        }
    }

    assert_eq!(
        order
            .iter()
            .filter(|app_id| app_id.as_str() == "noisy")
            .count(),
        3
    );
    assert_eq!(
        order
            .iter()
            .filter(|app_id| app_id.as_str() == "quiet-a")
            .count(),
        3
    );
    assert_eq!(
        order
            .iter()
            .filter(|app_id| app_id.as_str() == "quiet-b")
            .count(),
        3
    );
    assert_ne!(order.first().map(String::as_str), Some("noisy"));
}

#[test]
fn provider_request_debug_redacts_credentials_and_tokens() {
    let request = ProviderHttpRequest {
        method: ProviderHttpMethod::Post,
        url: "https://push.example/send?token=secret-token".to_owned(),
        headers: BTreeMap::from([
            ("authorization".to_owned(), "Bearer secret".to_owned()),
            ("x-test".to_owned(), "visible".to_owned()),
        ]),
        authorization: SecretString::new("Bearer stored-secret").ok(),
        body: br#"{"token":"secret-token"}"#.to_vec(),
    };
    let debug = format!("{request:?}");
    assert!(!debug.contains("secret-token"));
    assert!(!debug.contains("Bearer secret"));
    assert!(!debug.contains("stored-secret"));
    assert!(debug.contains("[REDACTED]"));
}

#[cfg(feature = "push-webpush")]
#[tokio::test]
async fn native_web_push_crypto_encrypts_and_signs_request() {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use web_push_native::p256::{
        SecretKey,
        elliptic_curve::{rand_core::OsRng, sec1::ToEncodedPoint},
    };

    let ua_secret = SecretKey::random(&mut OsRng);
    let ua_public = ua_secret.public_key();
    let auth_bytes = [7u8; 16];
    let p256dh =
        SecretString::new(URL_SAFE_NO_PAD.encode(ua_public.to_encoded_point(false).as_bytes()))
            .unwrap();
    let auth = SecretString::new(URL_SAFE_NO_PAD.encode(auth_bytes)).unwrap();
    let vapid_private = SecretString::new(URL_SAFE_NO_PAD.encode([1u8; 32])).unwrap();
    let crypto = NativeWebPushCrypto::new(
        vapid_private.expose_secret(),
        "mailto:push-admin@example.com",
    );
    let payload = br#"{"title":"Hello","body":"Body"}"#;

    let prepared = crypto
        .prepare_request(
            "https://push.example/subscription",
            &p256dh,
            &auth,
            payload,
            SecretString::new("unused-bearer").unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(prepared.headers["content-encoding"], "aes128gcm");
    assert_eq!(prepared.headers["content-type"], "application/octet-stream");
    let authorization = prepared.authorization.as_ref().unwrap();
    assert!(authorization.expose_secret().starts_with("vapid t="));
    assert!(!String::from_utf8_lossy(&prepared.body).contains("Hello"));

    let auth = web_push_native::Auth::clone_from_slice(&auth_bytes);
    let decrypted = web_push_native::decrypt(prepared.body, &ua_secret, &auth).unwrap();
    assert_eq!(decrypted, payload);
}

#[test]
fn invalid_token_classes_are_cleanup_signals() {
    let (_, error, _) = classify_webpush_response(&response(410, json!({})));
    assert_eq!(error.unwrap().class, "invalid_token");
    let (_, error, _) = classify_apns_response(&response(410, json!({})));
    assert_eq!(error.unwrap().class, "invalid_token");
}

fn cached_static_token(raw: &str, expires_at_ms: u64) -> CachedTokenProvider {
    CachedTokenProvider::new(Arc::new(StaticTokenSource::new(
        SecretString::new(raw).unwrap(),
        expires_at_ms,
    )))
}

fn batch(provider: PushProviderKind) -> DeliveryBatch {
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

fn queue_message(app_id: &str, index: usize) -> QueueMessage {
    let mut batch = batch(PushProviderKind::Fcm);
    batch.app_id = app_id.to_owned();
    batch.batch_id = format!("batch-{index}");
    for job in &mut batch.jobs {
        job.app_id = app_id.to_owned();
        job.batch_id = batch.batch_id.clone();
    }
    QueueMessage {
        message_id: format!("{app_id}-{index}"),
        stage: PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
        key: batch.queue_key(),
        partition_key: app_id.to_owned(),
        partition: 0,
        payload: PushQueuePayload::DeliveryBatch(Box::new(batch)),
        attempt: 1,
        not_before_ms: None,
        lease_deadline_ms: 0,
        ack: crate::pipeline::QueueAckToken {
            stage: PushQueueStage::DeliveryJobs(PushProviderKind::Fcm),
            message_id: format!("{app_id}-{index}"),
        },
    }
}

fn response(status: u16, body: Value) -> ProviderHttpResponse {
    ProviderHttpResponse {
        status,
        headers: BTreeMap::new(),
        body: sonic_rs::to_vec(&body).unwrap(),
    }
}
