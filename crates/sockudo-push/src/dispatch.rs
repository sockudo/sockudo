use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
#[cfg(feature = "push-webpush")]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use crate::domain::{
    DeliveryBatch, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError, PushProviderKind,
    PushRecipient, SecretString,
};
use crate::meta::{PushMetaEvent, emit_push_meta_event};
use crate::metrics::PushMetrics;
use crate::pipeline::{PushPipelineResult, PushQueuePayload, PushQueueStage, QueueMessage, now_ms};
use crate::transform::render_provider_payload;

#[async_trait]
pub trait PushDispatcher: Send + Sync {
    fn provider(&self) -> PushProviderKind;

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult>;

    async fn health_check(&self) -> HealthStatus;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthStatus {
    pub provider: PushProviderKind,
    pub healthy: bool,
    pub details: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAccessToken {
    pub token: SecretString,
    pub expires_at_ms: u64,
}

#[async_trait]
pub trait ProviderTokenSource: Send + Sync {
    async fn fetch_token(&self, now_ms: u64) -> Result<ProviderAccessToken, ProviderAuthError>;
}

#[derive(Clone)]
pub struct StaticTokenSource {
    token: SecretString,
    expires_at_ms: u64,
}

impl StaticTokenSource {
    pub fn new(token: SecretString, expires_at_ms: u64) -> Self {
        Self {
            token,
            expires_at_ms,
        }
    }
}

#[async_trait]
impl ProviderTokenSource for StaticTokenSource {
    async fn fetch_token(&self, _now_ms: u64) -> Result<ProviderAccessToken, ProviderAuthError> {
        Ok(ProviderAccessToken {
            token: self.token.clone(),
            expires_at_ms: self.expires_at_ms,
        })
    }
}

#[derive(Clone)]
pub struct CachedTokenProvider {
    source: Arc<dyn ProviderTokenSource + Send + Sync>,
    cache: Arc<tokio::sync::RwLock<Option<ProviderAccessToken>>>,
    refresh_skew_ms: u64,
}

impl CachedTokenProvider {
    pub fn new(source: Arc<dyn ProviderTokenSource + Send + Sync>) -> Self {
        Self {
            source,
            cache: Arc::new(tokio::sync::RwLock::new(None)),
            refresh_skew_ms: 5 * 60 * 1000,
        }
    }

    pub async fn access_token(&self, now_ms: u64) -> Result<SecretString, ProviderAuthError> {
        if let Some(cached) = self.cache.read().await.as_ref()
            && cached.expires_at_ms > now_ms.saturating_add(self.refresh_skew_ms)
        {
            return Ok(cached.token.clone());
        }

        let refreshed = self.source.fetch_token(now_ms).await?;
        let token = refreshed.token.clone();
        *self.cache.write().await = Some(refreshed);
        Ok(token)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAuthError {
    pub class: &'static str,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderHttpMethod {
    Get,
    Post,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProviderHttpRequest {
    pub method: ProviderHttpMethod,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl fmt::Debug for ProviderHttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderHttpRequest")
            .field("method", &self.method)
            .field("url", &redact_url(&self.url))
            .field("headers", &redacted_headers(&self.headers))
            .field("body", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderHttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

type ProviderClassification = (DeliveryOutcome, Option<ProviderError>, Option<String>);
type ProviderResponseClassifier = fn(&ProviderHttpResponse) -> ProviderClassification;

#[async_trait]
pub trait ProviderHttpClient: Send + Sync {
    async fn send(&self, request: ProviderHttpRequest) -> Result<ProviderHttpResponse, String>;
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
#[derive(Clone)]
pub struct ReqwestProviderHttpClient {
    client: reqwest::Client,
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
impl ReqwestProviderHttpClient {
    pub fn new() -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self { client })
    }
}

#[cfg(any(
    feature = "push-fcm",
    feature = "push-apns",
    feature = "push-webpush",
    feature = "push-hms",
    feature = "push-wns"
))]
#[async_trait]
impl ProviderHttpClient for ReqwestProviderHttpClient {
    async fn send(&self, request: ProviderHttpRequest) -> Result<ProviderHttpResponse, String> {
        let method = match request.method {
            ProviderHttpMethod::Get => reqwest::Method::GET,
            ProviderHttpMethod::Post => reqwest::Method::POST,
        };
        let mut builder = self.client.request(method, &request.url);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .body(request.body)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_ascii_lowercase(), value.to_owned()))
            })
            .collect();
        let body = response.bytes().await.map_err(|error| error.to_string())?;
        Ok(ProviderHttpResponse {
            status,
            headers,
            body: body.to_vec(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderEndpointConfig {
    pub base_url: String,
    pub credential_id: String,
}

impl ProviderEndpointConfig {
    fn joined_url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

#[derive(Clone)]
pub struct ProviderDispatchWorker {
    provider: PushProviderKind,
    queue: crate::pipeline::DynPushQueue,
    dispatcher: Arc<dyn PushDispatcher + Send + Sync>,
    circuit_breaker: ProviderCircuitBreaker,
    rate_limiter: AdaptiveRateLimiter,
    metrics: PushMetrics,
    max_batches_per_tick: usize,
    over_quota_tenants: BTreeSet<String>,
    tenant_inflight_cap: usize,
}

impl ProviderDispatchWorker {
    pub fn new(
        provider: PushProviderKind,
        queue: crate::pipeline::DynPushQueue,
        dispatcher: Arc<dyn PushDispatcher + Send + Sync>,
    ) -> Self {
        Self {
            provider,
            queue,
            dispatcher,
            circuit_breaker: ProviderCircuitBreaker::default(),
            rate_limiter: AdaptiveRateLimiter::default(),
            metrics: PushMetrics::default(),
            max_batches_per_tick: 32,
            over_quota_tenants: BTreeSet::new(),
            tenant_inflight_cap: 8,
        }
    }

    pub fn with_circuit_breaker(mut self, circuit_breaker: ProviderCircuitBreaker) -> Self {
        self.circuit_breaker = circuit_breaker;
        self
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_rate_limiter(mut self, rate_limiter: AdaptiveRateLimiter) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    pub fn with_over_quota_tenants(mut self, tenants: impl IntoIterator<Item = String>) -> Self {
        self.over_quota_tenants = tenants.into_iter().collect();
        self
    }

    pub fn with_tenant_inflight_cap(mut self, cap: usize) -> Self {
        self.tenant_inflight_cap = cap.max(1);
        self
    }

    pub async fn run_once(&mut self, consumer_group: &str) -> PushPipelineResult<usize> {
        let messages = self
            .queue
            .consume(
                PushQueueStage::DeliveryJobs(self.provider),
                consumer_group,
                self.max_batches_per_tick,
                30_000,
            )
            .await?;
        let mut scheduler = WeightedFairScheduler::default()
            .with_over_quota_tenants(self.over_quota_tenants.clone())
            .with_tenant_inflight_cap(self.tenant_inflight_cap);
        for message in messages {
            scheduler.push(message);
        }

        let mut processed = 0;
        while let Some(message) = scheduler.pop_next() {
            if let PushQueuePayload::DeliveryBatch(batch) = &message.payload {
                self.metrics.wfq_dispatched(self.provider, &batch.app_id);
            }
            self.handle_message(message).await?;
            processed += 1;
        }
        for message in scheduler.drain_remaining() {
            self.queue
                .nack(message.ack, Some(now_ms().saturating_add(1_000)))
                .await?;
        }
        Ok(processed)
    }

    async fn handle_message(&mut self, message: QueueMessage) -> PushPipelineResult<()> {
        let PushQueuePayload::DeliveryBatch(batch) = message.payload.clone() else {
            self.queue
                .dead_letter(
                    message.ack,
                    "unexpected payload for provider worker".to_owned(),
                )
                .await?;
            return Ok(());
        };
        let app_id = batch.app_id.clone();
        self.metrics
            .worker_pool(self.provider, self.max_batches_per_tick, 1);

        if self.circuit_breaker.is_open(now_ms()) {
            self.metrics
                .increment(format!("circuit.{:?}.deferred", self.provider), 1);
            self.metrics
                .circuit_breaker_state(self.provider, &app_id, true);
            self.queue
                .nack(
                    message.ack,
                    Some(self.circuit_breaker.retry_after_ms(now_ms())),
                )
                .await?;
            return Ok(());
        }

        if !self.rate_limiter.acquire(&app_id, self.provider) {
            self.metrics.rate_limiter_throttled(self.provider, &app_id);
            self.queue
                .nack(message.ack, Some(now_ms().saturating_add(1_000)))
                .await?;
            return Ok(());
        }

        let started = Instant::now();
        self.metrics.dispatch_started(self.provider, &app_id);
        let results = self.dispatcher.dispatch(batch).await;
        let mut saw_retry_after = None;
        let mut failures = 0_u64;
        for result in results {
            if !matches!(result.outcome, DeliveryOutcome::Accepted) {
                failures += 1;
                tracing::warn!(
                    app_id = %result.app_id,
                    publish_id = %result.publish_id,
                    provider = ?result.provider,
                    batch_id = %result.batch_id,
                    outcome = ?result.outcome,
                    error_class = result.error.as_ref().map(|error| error.class.as_str()),
                    "push dispatch failure"
                );
            } else {
                tracing::info!(
                    app_id = %result.app_id,
                    publish_id = %result.publish_id,
                    provider = ?result.provider,
                    batch_id = %result.batch_id,
                    "push dispatch success"
                );
            }
            self.metrics.dispatch_finished(
                result.provider,
                &result.app_id,
                result.outcome,
                started.elapsed(),
            );
            if let Some(retry_after_ms) =
                result.error.as_ref().and_then(|error| error.retry_after_ms)
            {
                saw_retry_after = Some(retry_after_ms);
            }
            self.queue
                .produce(
                    PushQueueStage::DeliveryResults,
                    result_key(&result),
                    PushQueuePayload::DeliveryResult(result),
                )
                .await?;
        }

        if let Some(retry_after_ms) = saw_retry_after {
            self.rate_limiter
                .record_throttle(&app_id, self.provider, now_ms());
            self.circuit_breaker.defer_until(retry_after_ms);
            self.metrics
                .circuit_breaker_state(self.provider, &app_id, true);
            self.emit_circuit_event("open_retry_after", retry_after_ms);
        } else if failures == 0 {
            self.rate_limiter
                .record_success_window(&app_id, self.provider, now_ms());
            self.circuit_breaker.record_success();
            self.metrics
                .circuit_breaker_state(self.provider, &app_id, false);
        } else {
            if self.circuit_breaker.record_failure(now_ms()) {
                self.metrics
                    .circuit_breaker_state(self.provider, &app_id, true);
                self.emit_circuit_event("open_failure_rate", self.circuit_breaker.open_until_ms);
            }
        }

        self.rate_limiter.release(&app_id, self.provider);
        self.metrics
            .worker_pool(self.provider, self.max_batches_per_tick, 0);
        self.queue.ack(message.ack).await?;
        Ok(())
    }

    fn emit_circuit_event(&self, action: &'static str, retry_at_ms: u64) {
        self.metrics
            .increment(format!("circuit.{:?}.{action}", self.provider), 1);
        emit_push_meta_event(PushMetaEvent::circuit_breaker_event(
            "unknown",
            self.provider,
            action,
            retry_at_ms,
        ));
    }
}

#[derive(Clone, Debug)]
pub struct ProviderCircuitBreaker {
    state: CircuitState,
    failure_count: u32,
    open_until_ms: u64,
    failure_threshold: u32,
    cool_down_ms: u64,
}

impl Default for ProviderCircuitBreaker {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            open_until_ms: 0,
            failure_threshold: 5,
            cool_down_ms: 30_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl ProviderCircuitBreaker {
    pub fn is_open(&mut self, now_ms: u64) -> bool {
        match self.state {
            CircuitState::Open if now_ms >= self.open_until_ms => {
                self.state = CircuitState::HalfOpen;
                false
            }
            CircuitState::Open => true,
            CircuitState::Closed | CircuitState::HalfOpen => false,
        }
    }

    pub fn retry_after_ms(&self, now_ms: u64) -> u64 {
        self.open_until_ms.max(now_ms.saturating_add(1_000))
    }

    pub fn defer_until(&mut self, retry_after_ms: u64) {
        self.state = CircuitState::Open;
        self.open_until_ms = retry_after_ms;
    }

    pub fn record_success(&mut self) {
        self.state = CircuitState::Closed;
        self.failure_count = 0;
        self.open_until_ms = 0;
    }

    pub fn record_failure(&mut self, now_ms: u64) -> bool {
        self.failure_count = self.failure_count.saturating_add(1);
        if self.failure_count >= self.failure_threshold {
            self.state = CircuitState::Open;
            self.open_until_ms = now_ms.saturating_add(self.cool_down_ms);
            return true;
        }
        false
    }
}

#[derive(Clone, Debug)]
pub struct AdaptiveRateLimiter {
    lanes: BTreeMap<(String, PushProviderKind), AdaptiveLane>,
    default_limit: u32,
    min_limit: u32,
    max_limit: u32,
    grow_after_ms: u64,
}

#[derive(Clone, Debug)]
struct AdaptiveLane {
    limit: u32,
    inflight: u32,
    last_throttle_ms: u64,
    last_growth_ms: u64,
}

impl Default for AdaptiveRateLimiter {
    fn default() -> Self {
        Self {
            lanes: BTreeMap::new(),
            default_limit: 100,
            min_limit: 1,
            max_limit: 10_000,
            grow_after_ms: 60_000,
        }
    }
}

impl AdaptiveRateLimiter {
    pub fn acquire(&mut self, app_id: &str, provider: PushProviderKind) -> bool {
        let lane = self.lane(app_id, provider);
        if lane.inflight >= lane.limit {
            return false;
        }
        lane.inflight += 1;
        true
    }

    pub fn release(&mut self, app_id: &str, provider: PushProviderKind) {
        let lane = self.lane(app_id, provider);
        lane.inflight = lane.inflight.saturating_sub(1);
    }

    pub fn record_throttle(&mut self, app_id: &str, provider: PushProviderKind, now_ms: u64) {
        let min_limit = self.min_limit;
        let lane = self.lane(app_id, provider);
        lane.limit = (lane.limit / 2).max(min_limit);
        lane.last_throttle_ms = now_ms;
        lane.last_growth_ms = now_ms;
    }

    pub fn record_success_window(&mut self, app_id: &str, provider: PushProviderKind, now_ms: u64) {
        let grow_after_ms = self.grow_after_ms;
        let max_limit = self.max_limit;
        let lane = self.lane(app_id, provider);
        if now_ms.saturating_sub(lane.last_throttle_ms) >= grow_after_ms
            && now_ms.saturating_sub(lane.last_growth_ms) >= grow_after_ms
        {
            lane.limit = lane.limit.saturating_add(1).min(max_limit);
            lane.last_growth_ms = now_ms;
        }
    }

    pub fn limit(&mut self, app_id: &str, provider: PushProviderKind) -> u32 {
        self.lane(app_id, provider).limit
    }

    fn lane(&mut self, app_id: &str, provider: PushProviderKind) -> &mut AdaptiveLane {
        self.lanes
            .entry((app_id.to_owned(), provider))
            .or_insert_with(|| AdaptiveLane {
                limit: self.default_limit,
                inflight: 0,
                last_throttle_ms: 0,
                last_growth_ms: 0,
            })
    }
}

pub struct WeightedFairScheduler {
    lanes: BTreeMap<String, TenantLane>,
    order: VecDeque<String>,
    over_quota_tenants: BTreeSet<String>,
    tenant_inflight_cap: usize,
}

impl Default for WeightedFairScheduler {
    fn default() -> Self {
        Self {
            lanes: BTreeMap::new(),
            order: VecDeque::new(),
            over_quota_tenants: BTreeSet::new(),
            tenant_inflight_cap: 8,
        }
    }
}

struct TenantLane {
    messages: VecDeque<QueueMessage>,
    deficit: u32,
    weight_units: u32,
    dispatched: usize,
}

impl WeightedFairScheduler {
    const DEFAULT_WEIGHT_UNITS: u32 = 10;
    const OVER_QUOTA_WEIGHT_UNITS: u32 = 1;
    const MESSAGE_COST_UNITS: u32 = 10;

    pub fn with_over_quota_tenants(mut self, tenants: impl IntoIterator<Item = String>) -> Self {
        self.over_quota_tenants = tenants.into_iter().collect();
        self
    }

    pub fn with_tenant_inflight_cap(mut self, cap: usize) -> Self {
        self.tenant_inflight_cap = cap.max(1);
        self
    }

    pub fn push(&mut self, message: QueueMessage) {
        let app_id = match &message.payload {
            PushQueuePayload::DeliveryBatch(batch) => batch.app_id.clone(),
            _ => "[unknown]".to_owned(),
        };
        if !self.lanes.contains_key(&app_id) {
            self.order.push_back(app_id.clone());
        }
        let weight_units = if self.over_quota_tenants.contains(&app_id) {
            Self::OVER_QUOTA_WEIGHT_UNITS
        } else {
            Self::DEFAULT_WEIGHT_UNITS
        };
        self.lanes
            .entry(app_id)
            .or_insert_with(|| TenantLane {
                messages: VecDeque::new(),
                deficit: 0,
                weight_units,
                dispatched: 0,
            })
            .messages
            .push_back(message);
    }

    pub fn pop_next(&mut self) -> Option<QueueMessage> {
        let mut scanned = 0_usize;
        let scan_limit = self.order.len().saturating_mul(12).max(1);
        while let Some(app_id) = self.order.pop_front() {
            scanned += 1;
            let Some(lane) = self.lanes.get_mut(&app_id) else {
                continue;
            };
            if lane.dispatched >= self.tenant_inflight_cap {
                self.order.push_back(app_id);
                if scanned >= scan_limit {
                    return None;
                }
                continue;
            }
            lane.deficit = lane.deficit.saturating_add(lane.weight_units);
            if lane.deficit < Self::MESSAGE_COST_UNITS {
                self.order.push_back(app_id);
                if scanned >= scan_limit {
                    return None;
                }
                continue;
            }
            if let Some(message) = lane.messages.pop_front() {
                lane.deficit = lane.deficit.saturating_sub(Self::MESSAGE_COST_UNITS);
                lane.dispatched = lane.dispatched.saturating_add(1);
                if lane.messages.is_empty() {
                    self.lanes.remove(&app_id);
                } else {
                    self.order.push_back(app_id);
                }
                return Some(message);
            }
            self.lanes.remove(&app_id);
            if scanned >= scan_limit {
                return None;
            }
        }
        None
    }

    pub fn drain_remaining(self) -> Vec<QueueMessage> {
        self.lanes
            .into_values()
            .flat_map(|lane| lane.messages)
            .collect()
    }
}

#[derive(Clone)]
pub struct FcmDispatcher {
    project_id: String,
    endpoint: ProviderEndpointConfig,
    token_provider: CachedTokenProvider,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

impl FcmDispatcher {
    pub fn new(
        project_id: impl Into<String>,
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            endpoint: ProviderEndpointConfig {
                base_url: "https://fcm.googleapis.com".to_owned(),
                credential_id: "fcm".to_owned(),
            },
            token_provider,
            http,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let token = self
            .token_provider
            .access_token(now_ms())
            .await
            .map_err(auth_error)?;
        let mut payload = render_payload_json(PushProviderKind::Fcm, job)?;
        if let Some(token) = recipient_token(&job.recipient) {
            payload["message"]["token"] = Value::String(token);
        }
        Ok(json_request(
            self.endpoint
                .joined_url(&format!("/v1/projects/{}/messages:send", self.project_id)),
            bearer_headers(token),
            payload,
        ))
    }
}

#[async_trait]
impl PushDispatcher for FcmDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Fcm
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            let response = self.http.send(request).await;
            classify_http_result(job, response, classify_fcm_response)
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Fcm,
            healthy: !self.project_id.trim().is_empty(),
            details: "fcm http-v1 dispatcher configured".to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct ApnsDispatcher {
    topic: String,
    endpoint: ProviderEndpointConfig,
    token_provider: CachedTokenProvider,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

impl ApnsDispatcher {
    pub fn new(
        topic: impl Into<String>,
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            topic: topic.into(),
            endpoint: ProviderEndpointConfig {
                base_url: "https://api.push.apple.com".to_owned(),
                credential_id: "apns".to_owned(),
            },
            token_provider,
            http,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let token = self
            .token_provider
            .access_token(now_ms())
            .await
            .map_err(auth_error)?;
        let device_token = recipient_token(&job.recipient).ok_or_else(|| ProviderError {
            class: "invalid_token".to_owned(),
            reason: Some("apns device token is missing".to_owned()),
            retry_after_ms: None,
        })?;
        let rendered = render_payload_json(PushProviderKind::Apns, job)?;
        let headers = rendered
            .get("headers")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut request_headers = bearer_headers(token);
        request_headers.insert("apns-topic".to_owned(), self.topic.clone());
        request_headers.insert(
            "apns-push-type".to_owned(),
            header_string(&headers, "apns-push-type").unwrap_or_else(|| "alert".to_owned()),
        );
        request_headers.insert(
            "apns-priority".to_owned(),
            header_string(&headers, "apns-priority").unwrap_or_else(|| "10".to_owned()),
        );
        if let Some(collapse_id) = header_string(&headers, "apns-collapse-id") {
            request_headers.insert("apns-collapse-id".to_owned(), collapse_id);
        }
        if let Some(expiration) = header_string(&headers, "apns-expiration") {
            request_headers.insert("apns-expiration".to_owned(), expiration);
        }

        Ok(json_request(
            self.endpoint.joined_url(&format!("/3/device/{device_token}")),
            request_headers,
            rendered
                .get("aps")
                .map(|aps| json!({ "aps": aps, "data": rendered.get("data").cloned().unwrap_or(Value::Null) }))
                .unwrap_or(rendered),
        ))
    }
}

#[async_trait]
impl PushDispatcher for ApnsDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Apns
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            classify_http_result(job, self.http.send(request).await, classify_apns_response)
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Apns,
            healthy: !self.topic.trim().is_empty(),
            details: "apns http/2 dispatcher configured".to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct WebPushDispatcher {
    token_provider: CachedTokenProvider,
    crypto: Arc<dyn WebPushCrypto + Send + Sync>,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

pub struct WebPushPreparedRequest {
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait WebPushCrypto: Send + Sync {
    async fn prepare_request(
        &self,
        endpoint: &str,
        p256dh: &SecretString,
        auth: &SecretString,
        payload: &[u8],
        fallback_bearer: SecretString,
    ) -> Result<WebPushPreparedRequest, ProviderError>;
}

#[derive(Clone)]
pub struct PassthroughWebPushCrypto;

#[async_trait]
impl WebPushCrypto for PassthroughWebPushCrypto {
    async fn prepare_request(
        &self,
        _endpoint: &str,
        _p256dh: &SecretString,
        _auth: &SecretString,
        payload: &[u8],
        fallback_bearer: SecretString,
    ) -> Result<WebPushPreparedRequest, ProviderError> {
        let mut headers = bearer_headers(fallback_bearer);
        headers.insert("content-encoding".to_owned(), "aes128gcm".to_owned());
        headers.insert("ttl".to_owned(), "2419200".to_owned());
        headers.insert("urgency".to_owned(), "normal".to_owned());
        Ok(WebPushPreparedRequest {
            headers,
            body: payload.to_vec(),
        })
    }
}

#[cfg(feature = "push-webpush")]
#[derive(Clone)]
pub struct NativeWebPushCrypto {
    vapid_private_key: String,
    contact: String,
    valid_for: std::time::Duration,
}

#[cfg(feature = "push-webpush")]
impl NativeWebPushCrypto {
    pub fn new(vapid_private_key: impl Into<String>, contact: impl Into<String>) -> Self {
        Self {
            vapid_private_key: vapid_private_key.into(),
            contact: contact.into(),
            valid_for: std::time::Duration::from_secs(12 * 60 * 60),
        }
    }

    pub fn with_valid_for(mut self, valid_for: std::time::Duration) -> Self {
        self.valid_for = valid_for;
        self
    }
}

#[cfg(feature = "push-webpush")]
#[async_trait]
impl WebPushCrypto for NativeWebPushCrypto {
    async fn prepare_request(
        &self,
        endpoint: &str,
        p256dh: &SecretString,
        auth: &SecretString,
        payload: &[u8],
        _fallback_bearer: SecretString,
    ) -> Result<WebPushPreparedRequest, ProviderError> {
        use web_push_native::{
            Auth, WebPushBuilder, jwt_simple::algorithms::ES256KeyPair, p256::PublicKey,
        };

        let vapid_key_bytes = URL_SAFE_NO_PAD
            .decode(self.vapid_private_key.as_bytes())
            .map_err(|_| ProviderError {
                class: "auth_failure".to_owned(),
                reason: Some("invalid VAPID private key encoding".to_owned()),
                retry_after_ms: None,
            })?;
        let vapid_key = ES256KeyPair::from_bytes(&vapid_key_bytes).map_err(|_| ProviderError {
            class: "auth_failure".to_owned(),
            reason: Some("invalid VAPID private key".to_owned()),
            retry_after_ms: None,
        })?;
        let p256dh_bytes = URL_SAFE_NO_PAD
            .decode(p256dh.expose_secret().as_bytes())
            .map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push p256dh encoding".to_owned()),
                retry_after_ms: None,
            })?;
        let auth_bytes = URL_SAFE_NO_PAD
            .decode(auth.expose_secret().as_bytes())
            .map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push auth encoding".to_owned()),
                retry_after_ms: None,
            })?;
        if auth_bytes.len() != 16 {
            return Err(ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push auth length".to_owned()),
                retry_after_ms: None,
            });
        }

        let request = WebPushBuilder::new(
            endpoint.parse().map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push endpoint".to_owned()),
                retry_after_ms: None,
            })?,
            PublicKey::from_sec1_bytes(&p256dh_bytes).map_err(|_| ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("invalid Web Push p256dh key".to_owned()),
                retry_after_ms: None,
            })?,
            Auth::clone_from_slice(&auth_bytes),
        )
        .with_valid_duration(self.valid_for)
        .with_vapid(&vapid_key, &self.contact)
        .build(payload.to_vec())
        .map_err(|error| ProviderError {
            class: "invalid_payload".to_owned(),
            reason: Some(format!("web push encryption failed: {error}")),
            retry_after_ms: None,
        })?;

        let headers = request
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_ascii_lowercase(), value.to_owned()))
            })
            .collect();

        Ok(WebPushPreparedRequest {
            headers,
            body: request.into_body(),
        })
    }
}

impl WebPushDispatcher {
    pub fn new(
        _vapid_audience: impl Into<String>,
        token_provider: CachedTokenProvider,
        crypto: Arc<dyn WebPushCrypto + Send + Sync>,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            token_provider,
            crypto,
            http,
        }
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let PushRecipient::Web {
            endpoint,
            p256dh,
            auth,
        } = &job.recipient
        else {
            return Err(ProviderError {
                class: "invalid_token".to_owned(),
                reason: Some("web push recipient is missing subscription material".to_owned()),
                retry_after_ms: None,
            });
        };
        let endpoint = endpoint.expose_secret().to_owned();
        validate_webpush_target(&endpoint)?;
        let token = self
            .token_provider
            .access_token(now_ms())
            .await
            .map_err(auth_error)?;
        let rendered = render_payload_json(PushProviderKind::WebPush, job)?;
        let body = serde_json::to_vec(&rendered).map_err(|_| ProviderError {
            class: "invalid_payload".to_owned(),
            reason: Some("web push payload serialization failed".to_owned()),
            retry_after_ms: None,
        })?;
        let prepared = self
            .crypto
            .prepare_request(&endpoint, p256dh, auth, &body, token)
            .await?;
        Ok(ProviderHttpRequest {
            method: ProviderHttpMethod::Post,
            url: endpoint,
            headers: prepared.headers,
            body: prepared.body,
        })
    }
}

#[async_trait]
impl PushDispatcher for WebPushDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::WebPush
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            classify_http_result(
                job,
                self.http.send(request).await,
                classify_webpush_response,
            )
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::WebPush,
            healthy: true,
            details: "web push dispatcher configured with external RFC8291 crypto adapter"
                .to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct HmsDispatcher {
    app_id: String,
    endpoint: ProviderEndpointConfig,
    token_provider: CachedTokenProvider,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

impl HmsDispatcher {
    pub fn new(
        app_id: impl Into<String>,
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            app_id: app_id.into(),
            endpoint: ProviderEndpointConfig {
                base_url: "https://push-api.cloud.huawei.com".to_owned(),
                credential_id: "hms".to_owned(),
            },
            token_provider,
            http,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.endpoint.base_url = base_url.into();
        self
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let token = self
            .token_provider
            .access_token(now_ms())
            .await
            .map_err(auth_error)?;
        let mut rendered = render_payload_json(PushProviderKind::Hms, job)?;
        if let Some(token) = recipient_token(&job.recipient) {
            rendered["message"]["token"] = json!([token]);
        }
        Ok(json_request(
            self.endpoint
                .joined_url(&format!("/v1/{}/messages:send", self.app_id)),
            bearer_headers(token),
            rendered,
        ))
    }
}

#[async_trait]
impl PushDispatcher for HmsDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Hms
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            classify_http_result(job, self.http.send(request).await, classify_hms_response)
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Hms,
            healthy: !self.app_id.trim().is_empty(),
            details: "hms http dispatcher configured".to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct WnsDispatcher {
    token_provider: CachedTokenProvider,
    http: Arc<dyn ProviderHttpClient + Send + Sync>,
}

impl WnsDispatcher {
    pub fn new(
        token_provider: CachedTokenProvider,
        http: Arc<dyn ProviderHttpClient + Send + Sync>,
    ) -> Self {
        Self {
            token_provider,
            http,
        }
    }

    pub async fn build_request(
        &self,
        job: &DeliveryJob,
    ) -> Result<ProviderHttpRequest, ProviderError> {
        let channel_uri = recipient_token(&job.recipient).ok_or_else(|| ProviderError {
            class: "invalid_token".to_owned(),
            reason: Some("wns channel URI is missing".to_owned()),
            retry_after_ms: None,
        })?;
        validate_https_url(&channel_uri, "invalid_token")?;
        let token = self
            .token_provider
            .access_token(now_ms())
            .await
            .map_err(auth_error)?;
        let rendered = render_payload_json(PushProviderKind::Wns, job)?;
        validate_wns_payload(&rendered)?;
        let mut headers = bearer_headers(token);
        headers.insert("x-wns-type".to_owned(), wns_type(&rendered));
        Ok(json_request(channel_uri, headers, rendered))
    }
}

#[async_trait]
impl PushDispatcher for WnsDispatcher {
    fn provider(&self) -> PushProviderKind {
        PushProviderKind::Wns
    }

    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult> {
        let futures = batch.jobs.into_iter().map(|job| async move {
            let request = match self.build_request(&job).await {
                Ok(request) => request,
                Err(error) => return result_from_error(job, DeliveryOutcome::Rejected, error),
            };
            classify_http_result(job, self.http.send(request).await, classify_wns_response)
        });
        join_all(futures).await
    }

    async fn health_check(&self) -> HealthStatus {
        HealthStatus {
            provider: PushProviderKind::Wns,
            healthy: true,
            details: "wns http dispatcher configured".to_owned(),
        }
    }
}

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

fn render_payload_json(
    provider: PushProviderKind,
    job: &DeliveryJob,
) -> Result<Value, ProviderError> {
    render_provider_payload(provider, &job.payload, &[])
        .map(|rendered| rendered.payload)
        .map_err(|error| ProviderError {
            class: "invalid_payload".to_owned(),
            reason: Some(error.to_string()),
            retry_after_ms: None,
        })
}

fn json_request(
    url: String,
    mut headers: BTreeMap<String, String>,
    payload: Value,
) -> ProviderHttpRequest {
    headers
        .entry("content-type".to_owned())
        .or_insert_with(|| "application/json".to_owned());
    ProviderHttpRequest {
        method: ProviderHttpMethod::Post,
        url,
        headers,
        body: serde_json::to_vec(&payload).unwrap_or_default(),
    }
}

fn bearer_headers(token: SecretString) -> BTreeMap<String, String> {
    BTreeMap::from([(
        "authorization".to_owned(),
        format!("Bearer {}", token.expose_secret()),
    )])
}

fn recipient_token(recipient: &PushRecipient) -> Option<String> {
    match recipient {
        PushRecipient::Fcm { registration_token } | PushRecipient::Hms { registration_token } => {
            Some(registration_token.expose_secret().to_owned())
        }
        PushRecipient::Apns { device_token } => Some(device_token.expose_secret().to_owned()),
        PushRecipient::Web { endpoint, .. } => Some(endpoint.expose_secret().to_owned()),
        PushRecipient::Wns { channel_uri } => Some(channel_uri.expose_secret().to_owned()),
    }
}

fn classify_http_result(
    job: DeliveryJob,
    response: Result<ProviderHttpResponse, String>,
    classifier: ProviderResponseClassifier,
) -> DeliveryResult {
    match response {
        Ok(response) => {
            let (outcome, error, provider_message_id) = classifier(&response);
            DeliveryResult {
                app_id: job.app_id,
                publish_id: job.publish_id,
                provider: job.provider,
                batch_id: job.batch_id,
                device_id: job.device_id,
                outcome,
                provider_message_id,
                error,
                attempt: job.attempt,
            }
        }
        Err(error) => result_from_error(
            job,
            DeliveryOutcome::Retryable,
            ProviderError {
                class: "unavailable".to_owned(),
                reason: Some(error),
                retry_after_ms: None,
            },
        ),
    }
}

fn classify_fcm_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if (200..300).contains(&response.status) {
        return (
            DeliveryOutcome::Accepted,
            None,
            json_field(&response.body, &["name"]),
        );
    }
    let body = String::from_utf8_lossy(&response.body).to_ascii_uppercase();
    if body.contains("UNREGISTERED") || body.contains("NOT_FOUND") {
        rejected("invalid_token", response, None)
    } else if response.status == 400 {
        rejected("invalid_payload", response, None)
    } else if matches!(response.status, 401 | 403) {
        rejected("auth_failure", response, None)
    } else if response.status == 429 {
        retryable("quota", response)
    } else if matches!(response.status, 500 | 502 | 503 | 504) {
        retryable("unavailable", response)
    } else {
        rejected("provider_rejected", response, None)
    }
}

fn classify_apns_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if (200..300).contains(&response.status) {
        return (
            DeliveryOutcome::Accepted,
            None,
            response.headers.get("apns-id").cloned(),
        );
    }
    match response.status {
        400 => rejected("invalid_payload", response, None),
        403 => rejected("auth_failure", response, None),
        410 => rejected("invalid_token", response, Some("unregistered")),
        429 => retryable("quota", response),
        500 | 503 => retryable("unavailable", response),
        _ => rejected("provider_rejected", response, None),
    }
}

fn classify_webpush_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if matches!(response.status, 200..=202) {
        return (
            DeliveryOutcome::Accepted,
            None,
            response.headers.get("location").cloned(),
        );
    }
    match response.status {
        404 | 410 => rejected("invalid_token", response, None),
        413 => rejected("invalid_payload", response, Some("payload_too_large")),
        429 => retryable("quota", response),
        500..=599 => retryable("unavailable", response),
        401 | 403 => rejected("auth_failure", response, None),
        _ => rejected("provider_rejected", response, None),
    }
}

fn classify_hms_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if (200..300).contains(&response.status)
        && json_field(&response.body, &["code"]).is_none_or(|code| code == "80000000")
    {
        return (
            DeliveryOutcome::Accepted,
            None,
            json_field(&response.body, &["msg"])
                .or_else(|| json_field(&response.body, &["requestId"])),
        );
    }
    let body = String::from_utf8_lossy(&response.body).to_ascii_lowercase();
    if body.contains("token") && (body.contains("invalid") || body.contains("not exist")) {
        rejected("invalid_token", response, None)
    } else if response.status == 429 || body.contains("quota") {
        retryable("quota", response)
    } else if matches!(response.status, 401 | 403) {
        rejected("auth_failure", response, None)
    } else if response.status >= 500 {
        retryable("unavailable", response)
    } else {
        rejected("provider_rejected", response, None)
    }
}

fn classify_wns_response(response: &ProviderHttpResponse) -> ProviderClassification {
    if matches!(response.status, 200..=202) {
        return (
            DeliveryOutcome::Accepted,
            None,
            response.headers.get("x-wns-msg-id").cloned(),
        );
    }
    match response.status {
        404 | 410 => rejected("invalid_token", response, None),
        401 | 403 => rejected("auth_failure", response, None),
        413 => rejected("invalid_payload", response, Some("payload_too_large")),
        429 => retryable("quota", response),
        500..=599 => retryable("unavailable", response),
        _ => rejected("provider_rejected", response, None),
    }
}

fn rejected(
    class: &str,
    response: &ProviderHttpResponse,
    reason: Option<&str>,
) -> ProviderClassification {
    (
        DeliveryOutcome::Rejected,
        Some(provider_error(class, response, reason, None)),
        None,
    )
}

fn retryable(class: &str, response: &ProviderHttpResponse) -> ProviderClassification {
    (
        DeliveryOutcome::Retryable,
        Some(provider_error(
            class,
            response,
            None,
            retry_after_ms(&response.headers),
        )),
        None,
    )
}

fn provider_error(
    class: &str,
    response: &ProviderHttpResponse,
    reason: Option<&str>,
    retry_after_ms: Option<u64>,
) -> ProviderError {
    ProviderError {
        class: class.to_owned(),
        reason: reason
            .map(str::to_owned)
            .or_else(|| json_field(&response.body, &["error", "status"]))
            .or_else(|| json_field(&response.body, &["reason"]))
            .or_else(|| Some(format!("provider status {}", response.status))),
        retry_after_ms,
    }
}

fn retry_after_ms(headers: &BTreeMap<String, String>) -> Option<u64> {
    headers
        .get("retry-after")
        .or_else(|| headers.get("Retry-After"))
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(|seconds| now_ms().saturating_add(seconds.saturating_mul(1000)))
}

fn result_from_error(
    job: DeliveryJob,
    outcome: DeliveryOutcome,
    error: ProviderError,
) -> DeliveryResult {
    DeliveryResult {
        app_id: job.app_id,
        publish_id: job.publish_id,
        provider: job.provider,
        batch_id: job.batch_id,
        device_id: job.device_id,
        outcome,
        provider_message_id: None,
        error: Some(error),
        attempt: job.attempt,
    }
}

fn auth_error(error: ProviderAuthError) -> ProviderError {
    ProviderError {
        class: error.class.to_owned(),
        reason: Some(error.reason),
        retry_after_ms: None,
    }
}

fn header_string(map: &serde_json::Map<String, Value>, name: &str) -> Option<String> {
    map.get(name).and_then(|value| match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    })
}

fn json_field(body: &[u8], path: &[&str]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let mut current = &value;
    for segment in path {
        current = current.get(segment)?;
    }
    current.as_str().map(str::to_owned)
}

fn validate_webpush_target(endpoint: &str) -> Result<(), ProviderError> {
    validate_https_url(endpoint, "invalid_token")?;
    let parsed = Url::parse(endpoint).map_err(|_| ProviderError {
        class: "invalid_token".to_owned(),
        reason: Some("web push endpoint must be a URL".to_owned()),
        retry_after_ms: None,
    })?;
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    if host == "localhost" || host.ends_with(".local") {
        return Err(ProviderError {
            class: "invalid_token".to_owned(),
            reason: Some("web push endpoint host is not allowed".to_owned()),
            retry_after_ms: None,
        });
    }
    Ok(())
}

fn validate_https_url(url: &str, class: &str) -> Result<(), ProviderError> {
    let parsed = Url::parse(url).map_err(|_| ProviderError {
        class: class.to_owned(),
        reason: Some("provider URL is invalid".to_owned()),
        retry_after_ms: None,
    })?;
    if parsed.scheme() != "https" {
        return Err(ProviderError {
            class: class.to_owned(),
            reason: Some("provider URL must use https".to_owned()),
            retry_after_ms: None,
        });
    }
    Ok(())
}

fn validate_wns_payload(payload: &Value) -> Result<(), ProviderError> {
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("toast");
    if !matches!(kind, "toast" | "tile" | "raw") {
        return Err(ProviderError {
            class: "invalid_payload".to_owned(),
            reason: Some("invalid WNS notification type".to_owned()),
            retry_after_ms: None,
        });
    }
    Ok(())
}

fn wns_type(payload: &Value) -> String {
    match payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("toast")
    {
        "tile" => "wns/tile",
        "raw" => "wns/raw",
        _ => "wns/toast",
    }
    .to_owned()
}

fn redact_url(url: &str) -> String {
    Url::parse(url)
        .ok()
        .map(|mut parsed| {
            parsed.set_query(None);
            parsed.to_string()
        })
        .unwrap_or_else(|| "[REDACTED_URL]".to_owned())
}

fn redacted_headers(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(name, value)| {
            let lower = name.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
            ) {
                (name.clone(), "[REDACTED]".to_owned())
            } else {
                (name.clone(), value.clone())
            }
        })
        .collect()
}

fn result_key(result: &DeliveryResult) -> String {
    format!(
        "{}:{}:{:?}:{}:{}:{}",
        result.app_id,
        result.publish_id,
        result.provider,
        result.batch_id,
        result.device_id.as_deref().unwrap_or("[provider-target]"),
        result.attempt
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Mutex;

    use crate::domain::{PushPayload, PushRecipient};

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
        async fn fetch_token(
            &self,
            _now_ms: u64,
        ) -> Result<ProviderAccessToken, ProviderAuthError> {
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
            assert_eq!(request.headers["authorization"], "Bearer access-token");
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
                PushProviderKind::WebPush => {
                    classify_webpush_response(&response(status, json!({}))).1
                }
                PushProviderKind::Hms => classify_hms_response(&response(status, json!({}))).1,
                PushProviderKind::Wns => classify_wns_response(&response(status, json!({}))).1,
            }
            .unwrap();
            assert_eq!(error.class, class);
        }
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
            body: br#"{"token":"secret-token"}"#.to_vec(),
        };
        let debug = format!("{request:?}");
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("Bearer secret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[cfg(feature = "push-webpush")]
    #[tokio::test]
    async fn native_web_push_crypto_encrypts_and_signs_request() {
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
        assert!(prepared.headers["authorization"].starts_with("vapid t="));
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
                payload: PushPayload {
                    template_id: None,
                    template_data: json!({"k": "v"}),
                    title: Some("Hello".to_owned()),
                    body: Some("Body".to_owned()),
                    icon: None,
                    sound: None,
                    collapse_key: Some("collapse".to_owned()),
                },
                attempt: 1,
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
            payload: PushQueuePayload::DeliveryBatch(batch),
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
            body: serde_json::to_vec(&body).unwrap(),
        }
    }
}
