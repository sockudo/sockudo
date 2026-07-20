use crate::{
    AnnotationEventsParams, AnnotationEventsResponse, Channel, Config, DeleteAnnotationResponse,
    GetMessageResponse, HistoryPage, HistoryParams, ListMessageVersionsResponse,
    MessageVersionsParams, MutationResponse, PresenceHistoryPage, PresenceHistoryParams,
    PresenceSnapshot, PresenceSnapshotParams, PublishAnnotationRequest, PublishAnnotationResponse,
    RequestError, Result, SockudoError, Token, auth, events, util, webhook::Webhook,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use events::EventData;
use reqwest::{Client, Response};
use sha2::{Digest, Sha256};
use sonic_rs::{JsonValueTrait, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Main Sockudo client
#[derive(Clone)]
pub struct Sockudo {
    inner: Arc<SockudoInner>,
}

struct SockudoInner {
    config: Config,
    client: Client,
    base_id: String,
    publish_serial: AtomicU64,
    auto_idempotency_key: bool,
}

/// Generates a compact base ID: 12 random bytes, base64url-encoded (no padding) = 16 chars.
fn generate_base_id() -> String {
    let mut bytes = [0u8; 12];
    rand::fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

impl Sockudo {
    /// Creates a new Sockudo client
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;

        let client = Client::builder()
            .timeout(config.timeout())
            .pool_max_idle_per_host(config.pool_max_idle_per_host())
            .build()
            .map_err(|e| SockudoError::Config {
                message: format!("Failed to build HTTP client: {}", e),
            })?;

        let auto_idempotency_key = config.auto_idempotency_key();

        Ok(Self {
            inner: Arc::new(SockudoInner {
                config,
                client,
                base_id: generate_base_id(),
                publish_serial: AtomicU64::new(0),
                auto_idempotency_key,
            }),
        })
    }

    /// Creates a Sockudo client from URL
    pub fn from_url(url: &str, additional_config: Option<Config>) -> Result<Self> {
        let parsed_url = url::Url::parse(url).map_err(|e| SockudoError::Config {
            message: format!("Invalid Sockudo URL: {}", e),
        })?;

        let auth_parts: Vec<&str> = parsed_url.username().split(':').collect();
        if auth_parts.len() != 2 {
            return Err(SockudoError::Config {
                message: "Invalid auth format in URL. Expected KEY:SECRET".to_string(),
            });
        }

        let path_segments: Vec<&str> = parsed_url.path().split('/').collect();
        let app_id = path_segments
            .last()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| SockudoError::Config {
                message: "App ID not found in URL path".to_string(),
            })?;

        let builder = Config::builder()
            .app_id(*app_id)
            .key(auth_parts[0])
            .secret(auth_parts[1])
            .host(parsed_url.host_str().unwrap_or("api.sockudo.io"));

        let builder = if parsed_url.scheme() == "https" {
            builder.use_tls(true)
        } else {
            builder.use_tls(false)
        };

        let builder = if let Some(port) = parsed_url.port() {
            builder.port(port)
        } else {
            builder
        };

        // Apply additional config if provided
        let config = if let Some(additional) = additional_config {
            builder
                .timeout(additional.timeout())
                .pool_max_idle_per_host(additional.pool_max_idle_per_host())
                .enable_retry(additional.enable_retry())
                .max_retries(additional.max_retries())
                .auto_idempotency_key(additional.auto_idempotency_key())
                .build()?
        } else {
            builder.build()?
        };

        Self::new(config)
    }

    /// Gets the configuration
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Creates a new Sockudo client for a specific cluster
    pub fn for_cluster(&self, cluster: &str) -> Result<Self> {
        let config = Config::builder()
            .app_id(self.inner.config.app_id())
            .key(&self.inner.config.token().key)
            .secret(self.inner.config.token().secret_string())
            .cluster(cluster)
            .use_tls(self.inner.config.scheme() == "https")
            .timeout(self.inner.config.timeout())
            .pool_max_idle_per_host(self.inner.config.pool_max_idle_per_host())
            .enable_retry(self.inner.config.enable_retry())
            .max_retries(self.inner.config.max_retries())
            .auto_idempotency_key(self.inner.config.auto_idempotency_key())
            .build()?;

        Self::new(config)
    }

    /// Authorizes a channel
    pub fn authorize_channel(
        &self,
        socket_id: &str,
        channel: &Channel,
        data: Option<&Value>,
    ) -> Result<auth::SocketAuth> {
        util::validate_socket_id(socket_id)?;
        auth::get_socket_signature(
            self,
            self.inner.config.token(),
            &channel.full_name(),
            socket_id,
            data,
        )
    }

    /// Authorizes a channel by name (convenience method)
    pub fn authorize_channel_with_name(
        &self,
        socket_id: &str,
        channel_name: &str,
        data: Option<&Value>,
    ) -> Result<auth::SocketAuth> {
        let channel = Channel::from_string(channel_name)?;
        self.authorize_channel(socket_id, &channel, data)
    }

    /// Authenticates a user
    pub fn authenticate_user(&self, socket_id: &str, user_data: &Value) -> Result<auth::UserAuth> {
        util::validate_socket_id(socket_id)?;

        // Validate user data has ID
        let id = user_data
            .get("id")
            .ok_or_else(|| SockudoError::Validation {
                message: "User data must contain an 'id' field".to_string(),
            })?;
        if let Some(id_str) = id.as_str() {
            util::validate_user_id(id_str)?;
        } else {
            return Err(SockudoError::Validation {
                message: "User data ID must be a string".to_string(),
            });
        }

        auth::get_socket_signature_for_user(self.inner.config.token(), socket_id, user_data)
    }

    /// Sends an event to a user
    pub async fn send_to_user<D: Into<EventData>>(
        &self,
        user_id: &str,
        event: &str,
        data: D,
    ) -> Result<Response> {
        if event.len() > 200 {
            return Err(SockudoError::Validation {
                message: format!("Event name too long: '{}' (max 200 characters)", event),
            });
        }

        util::validate_user_id(user_id)?;

        let channel_name = format!("#server-to-user-{}", user_id);
        let channel = Channel::from_string(channel_name)?;
        events::trigger(self, &[channel], event, data, None).await
    }

    /// Terminates user connections
    pub async fn terminate_user_connections(&self, user_id: &str) -> Result<Response> {
        util::validate_user_id(user_id)?;
        let path = format!("/users/{}/terminate_connections", user_id);
        self.post(&path, &json!({})).await
    }

    /// Forces all active connections for a user to reconnect with close code 4200.
    pub async fn force_reconnect_user(&self, user_id: &str) -> Result<Response> {
        util::validate_user_id(user_id)?;
        let path = format!("/users/{}/force_reconnect", user_id);
        self.post(&path, &json!({})).await
    }

    /// Fetches durable history for a channel.
    pub async fn channel_history(
        &self,
        channel: &Channel,
        params: Option<&HistoryParams>,
    ) -> Result<HistoryPage> {
        let path = format!("/channels/{}/history", channel.full_name());
        let param_map = params.map(HistoryParams::to_map);
        let response = self.get(&path, param_map.as_ref()).await?;
        let body = response.text().await?;
        let page = sonic_rs::from_str::<HistoryPage>(&body).map_err(SockudoError::Json)?;
        Ok(page)
    }

    /// Fetches durable history for a channel name.
    pub async fn channel_history_with_name(
        &self,
        channel_name: &str,
        params: Option<&HistoryParams>,
    ) -> Result<HistoryPage> {
        let channel = Channel::from_string(channel_name)?;
        self.channel_history(&channel, params).await
    }

    /// Fetches presence history for a presence channel.
    pub async fn channel_presence_history(
        &self,
        channel_name: &str,
        params: Option<&PresenceHistoryParams>,
    ) -> Result<PresenceHistoryPage> {
        util::validate_presence_channel(channel_name)?;
        let path = format!("/channels/{}/presence/history", channel_name);
        let param_map = params.map(PresenceHistoryParams::to_map);
        let response = self.get(&path, param_map.as_ref()).await?;
        let body = response.text().await?;
        let page = sonic_rs::from_str::<PresenceHistoryPage>(&body).map_err(SockudoError::Json)?;
        Ok(page)
    }

    /// Reconstructs presence membership at a point in time.
    pub async fn channel_presence_snapshot(
        &self,
        channel_name: &str,
        params: Option<&PresenceSnapshotParams>,
    ) -> Result<PresenceSnapshot> {
        util::validate_presence_channel(channel_name)?;
        let path = format!("/channels/{}/presence/history/snapshot", channel_name);
        let param_map = params.map(PresenceSnapshotParams::to_map);
        let response = self.get(&path, param_map.as_ref()).await?;
        let body = response.text().await?;
        let snapshot = sonic_rs::from_str::<PresenceSnapshot>(&body).map_err(SockudoError::Json)?;
        Ok(snapshot)
    }

    /// Fetches the latest visible version for a logical message.
    pub async fn get_message(
        &self,
        channel_name: &str,
        message_serial: &str,
    ) -> Result<GetMessageResponse> {
        let path = format!("/channels/{}/messages/{}", channel_name, message_serial);
        let response = self.get(&path, None).await?;
        let body = response.text().await?;
        let item = sonic_rs::from_str::<GetMessageResponse>(&body).map_err(SockudoError::Json)?;
        Ok(item)
    }

    /// Fetches preserved versions for a logical message.
    pub async fn get_message_versions(
        &self,
        channel_name: &str,
        message_serial: &str,
        params: Option<&MessageVersionsParams>,
    ) -> Result<ListMessageVersionsResponse> {
        let path = format!(
            "/channels/{}/messages/{}/versions",
            channel_name, message_serial
        );
        let param_map = params.map(MessageVersionsParams::to_map);
        let response = self.get(&path, param_map.as_ref()).await?;
        let body = response.text().await?;
        let page =
            sonic_rs::from_str::<ListMessageVersionsResponse>(&body).map_err(SockudoError::Json)?;
        Ok(page)
    }

    /// Applies a versioned message update.
    pub async fn update_message(
        &self,
        channel_name: &str,
        message_serial: &str,
        body: &Value,
    ) -> Result<MutationResponse> {
        let path = format!(
            "/channels/{}/messages/{}/update",
            channel_name, message_serial
        );
        let response = self.post(&path, body).await?;
        let text = response.text().await?;
        let result = sonic_rs::from_str::<MutationResponse>(&text).map_err(SockudoError::Json)?;
        Ok(result)
    }

    /// Applies a versioned message delete.
    pub async fn delete_message(
        &self,
        channel_name: &str,
        message_serial: &str,
        body: &Value,
    ) -> Result<MutationResponse> {
        let path = format!(
            "/channels/{}/messages/{}/delete",
            channel_name, message_serial
        );
        let response = self.post(&path, body).await?;
        let text = response.text().await?;
        let result = sonic_rs::from_str::<MutationResponse>(&text).map_err(SockudoError::Json)?;
        Ok(result)
    }

    /// Applies a versioned message append.
    pub async fn append_message(
        &self,
        channel_name: &str,
        message_serial: &str,
        body: &Value,
    ) -> Result<MutationResponse> {
        let path = format!(
            "/channels/{}/messages/{}/append",
            channel_name, message_serial
        );
        let response = self.post(&path, body).await?;
        let text = response.text().await?;
        let result = sonic_rs::from_str::<MutationResponse>(&text).map_err(SockudoError::Json)?;
        Ok(result)
    }

    /// Publishes an annotation for a versioned message.
    pub async fn publish_annotation(
        &self,
        channel_name: &str,
        message_serial: &str,
        body: &PublishAnnotationRequest,
    ) -> Result<PublishAnnotationResponse> {
        let path = format!(
            "/channels/{}/messages/{}/annotations",
            channel_name, message_serial
        );
        let value = sonic_rs::to_value(body).map_err(SockudoError::Json)?;
        let response = self.post(&path, &value).await?;
        let text = response.text().await?;
        let result =
            sonic_rs::from_str::<PublishAnnotationResponse>(&text).map_err(SockudoError::Json)?;
        Ok(result)
    }

    /// Deletes an annotation from a versioned message.
    pub async fn delete_annotation(
        &self,
        channel_name: &str,
        message_serial: &str,
        annotation_serial: &str,
        socket_id: Option<&str>,
    ) -> Result<DeleteAnnotationResponse> {
        let path = format!(
            "/channels/{}/messages/{}/annotations/{}",
            channel_name, message_serial, annotation_serial
        );
        let mut params = BTreeMap::new();
        if let Some(socket_id) = socket_id {
            params.insert("socket_id".to_string(), socket_id.to_string());
        }
        let response = self.delete(&path, Some(&params)).await?;
        let text = response.text().await?;
        let result =
            sonic_rs::from_str::<DeleteAnnotationResponse>(&text).map_err(SockudoError::Json)?;
        Ok(result)
    }

    /// Lists raw annotation events for a versioned message.
    pub async fn list_annotations(
        &self,
        channel_name: &str,
        message_serial: &str,
        params: Option<&AnnotationEventsParams>,
    ) -> Result<AnnotationEventsResponse> {
        let path = format!(
            "/channels/{}/messages/{}/annotations",
            channel_name, message_serial
        );
        let param_map = params.map(AnnotationEventsParams::to_map);
        let response = self.get(&path, param_map.as_ref()).await?;
        let body = response.text().await?;
        let page =
            sonic_rs::from_str::<AnnotationEventsResponse>(&body).map_err(SockudoError::Json)?;
        Ok(page)
    }

    /// Triggers an event on channels.
    ///
    /// When `auto_idempotency_key` is enabled (default) and no explicit idempotency key
    /// is provided in `params`, a deterministic key is generated as `{base_id}:{serial}`.
    /// The serial is incremented before the request so that retries reuse the same key.
    /// Built-in retry (max 3 attempts) is applied on network/5xx errors.
    pub async fn trigger<D: Into<EventData>>(
        &self,
        channels: &[Channel],
        event: &str,
        data: D,
        params: Option<events::TriggerParams>,
    ) -> Result<Response> {
        if let Some(ref params) = params
            && let Some(ref socket_id) = params.socket_id
        {
            util::validate_socket_id(socket_id)?;
        }

        if event.len() > 200 {
            return Err(SockudoError::Validation {
                message: format!("Event name too long: '{}' (max 200 characters)", event),
            });
        }

        if channels.is_empty() {
            return Err(SockudoError::Validation {
                message: "Must specify at least one channel".to_string(),
            });
        }

        if channels.len() > 100 {
            return Err(SockudoError::Validation {
                message: format!(
                    "Can't trigger to more than 100 channels (got {})",
                    channels.len()
                ),
            });
        }

        let params = self.maybe_inject_idempotency_key(params);
        events::trigger(self, channels, event, data, params.as_ref()).await
    }

    /// Triggers an event on channel names (convenience method)
    pub async fn trigger_on_channels<D: Into<EventData>>(
        &self,
        channel_names: &[String],
        event: &str,
        data: D,
        params: Option<events::TriggerParams>,
    ) -> Result<Response> {
        let channels: Result<Vec<Channel>> =
            channel_names.iter().map(Channel::from_string).collect();
        self.trigger(&channels?, event, data, params).await
    }

    /// Triggers a batch of events.
    ///
    /// When `auto_idempotency_key` is enabled (default), each event in the batch that
    /// lacks an explicit idempotency key gets one generated as `{base_id}:{serial}:{index}`.
    /// The serial is incremented before the request so that retries reuse the same keys.
    pub async fn trigger_batch(&self, mut batch: Vec<events::BatchEvent>) -> Result<Response> {
        if self.inner.auto_idempotency_key {
            let serial = self.inner.publish_serial.fetch_add(1, Ordering::SeqCst);
            for (i, event) in batch.iter_mut().enumerate() {
                if event.idempotency_key.is_none() {
                    event.idempotency_key =
                        Some(format!("{}:{}:{}", self.inner.base_id, serial, i));
                }
            }
        }

        events::trigger_batch(self, batch, None).await
    }

    /// Triggers a batch of events with an explicit idempotency key header.
    ///
    /// Per-event auto-idempotency keys are still injected for events that lack one.
    pub async fn trigger_batch_with_idempotency_key(
        &self,
        mut batch: Vec<events::BatchEvent>,
        idempotency_key: &str,
    ) -> Result<Response> {
        if self.inner.auto_idempotency_key {
            let serial = self.inner.publish_serial.fetch_add(1, Ordering::SeqCst);
            for (i, event) in batch.iter_mut().enumerate() {
                if event.idempotency_key.is_none() {
                    event.idempotency_key =
                        Some(format!("{}:{}:{}", self.inner.base_id, serial, i));
                }
            }
        }

        events::trigger_batch(self, batch, Some(idempotency_key)).await
    }

    /// Returns whether automatic idempotency key generation is enabled.
    pub fn auto_idempotency_key(&self) -> bool {
        self.inner.auto_idempotency_key
    }

    /// Returns the base ID used for deterministic idempotency key generation.
    pub fn base_id(&self) -> &str {
        &self.inner.base_id
    }

    /// Returns the current publish serial (the next serial that will be used).
    pub fn publish_serial(&self) -> u64 {
        self.inner.publish_serial.load(Ordering::SeqCst)
    }

    /// Injects an auto-generated idempotency key into params if auto mode is enabled
    /// and no explicit key is already set. Increments the serial before the request
    /// so that retries reuse the same assigned key.
    fn maybe_inject_idempotency_key(
        &self,
        params: Option<events::TriggerParams>,
    ) -> Option<events::TriggerParams> {
        if !self.inner.auto_idempotency_key {
            return params;
        }

        let has_explicit_key = params
            .as_ref()
            .and_then(|p| p.idempotency_key.as_ref())
            .is_some();

        if has_explicit_key {
            return params;
        }

        let serial = self.inner.publish_serial.fetch_add(1, Ordering::SeqCst);
        let key = format!("{}:{}", self.inner.base_id, serial);

        let mut p = params.unwrap_or_default();
        p.idempotency_key = Some(key);
        Some(p)
    }

    /// Makes a POST request
    pub async fn post(&self, path: &str, body: &Value) -> Result<Response> {
        self.send_request("POST", path, Some(body), None, None)
            .await
    }

    /// Makes a POST request with extra headers
    pub async fn post_with_headers(
        &self,
        path: &str,
        body: &Value,
        extra_headers: &HashMap<String, String>,
    ) -> Result<Response> {
        let headers = if extra_headers.is_empty() {
            None
        } else {
            Some(extra_headers)
        };
        self.send_request("POST", path, Some(body), None, headers)
            .await
    }

    /// Makes a GET request
    pub async fn get(
        &self,
        path: &str,
        params: Option<&BTreeMap<String, String>>,
    ) -> Result<Response> {
        self.send_request("GET", path, None, params, None).await
    }

    /// Makes a DELETE request
    pub async fn delete(
        &self,
        path: &str,
        params: Option<&BTreeMap<String, String>>,
    ) -> Result<Response> {
        self.send_request("DELETE", path, None, params, None).await
    }

    /// Makes a GET request with extra headers.
    pub async fn get_with_headers(
        &self,
        path: &str,
        params: Option<&BTreeMap<String, String>>,
        extra_headers: &HashMap<String, String>,
    ) -> Result<Response> {
        let headers = if extra_headers.is_empty() {
            None
        } else {
            Some(extra_headers)
        };
        self.send_request("GET", path, None, params, headers).await
    }

    /// Makes a DELETE request with extra headers.
    pub async fn delete_with_headers(
        &self,
        path: &str,
        params: Option<&BTreeMap<String, String>>,
        extra_headers: &HashMap<String, String>,
    ) -> Result<Response> {
        let headers = if extra_headers.is_empty() {
            None
        } else {
            Some(extra_headers)
        };
        self.send_request("DELETE", path, None, params, headers)
            .await
    }

    /// Creates a webhook from request data
    pub fn webhook(&self, headers: &BTreeMap<String, String>, body: &str) -> Webhook {
        Webhook::new(self.inner.config.token(), headers, body)
    }

    /// Generates channel shared secret for encryption
    pub fn channel_shared_secret(&self, channel: &str) -> Result<[u8; 32]> {
        let master_key =
            self.inner
                .config
                .encryption_master_key()
                .ok_or_else(|| SockudoError::Encryption {
                    message: "Encryption master key not set".to_string(),
                })?;

        let mut hasher = Sha256::new();
        hasher.update(channel.as_bytes());
        hasher.update(master_key);

        let result = hasher.finalize();
        let mut secret = [0u8; 32];
        secret.copy_from_slice(&result);
        Ok(secret)
    }

    /// Creates signed query string for manual requests
    pub fn create_signed_query_string(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        params: Option<&BTreeMap<String, String>>,
    ) -> String {
        create_signed_query_string(self.inner.config.token(), method, path, body, params)
    }

    /// Internal method to send HTTP requests with retry logic
    async fn send_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
        params: Option<&BTreeMap<String, String>>,
        extra_headers: Option<&HashMap<String, String>>,
    ) -> Result<Response> {
        let full_path = self.inner.config.prefix_path(path);
        let body_str = body.map(sonic_rs::to_string).transpose()?;

        let query_string = create_signed_query_string(
            self.inner.config.token(),
            method,
            &full_path,
            body_str.as_deref(),
            params,
        );

        let url = format!(
            "{}{}?{}",
            self.inner.config.base_url(),
            full_path,
            query_string
        );

        let mut attempt = 0;
        let max_attempts = if self.inner.config.enable_retry() {
            self.inner.config.max_retries() + 1
        } else {
            1
        };

        loop {
            attempt += 1;

            let mut request = match method {
                "GET" => self.inner.client.get(&url),
                "POST" => self.inner.client.post(&url),
                "DELETE" => self.inner.client.delete(&url),
                _ => {
                    return Err(SockudoError::Request(RequestError::new(
                        format!("Unsupported HTTP method: {}", method),
                        &url,
                        None,
                        None,
                    )));
                }
            };

            if let Some(ref body_str) = body_str {
                request = request
                    .header("Content-Type", "application/json")
                    .body(body_str.clone());
            }

            if let Some(headers) = extra_headers {
                for (key, value) in headers {
                    request = request.header(key, value);
                }
            }

            let response = request
                .header("X-Sockudo-Library", "sockudo-http/1.5.0")
                .send()
                .await;

            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        return Ok(resp);
                    }

                    let status = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();

                    // Don't retry on 4xx errors (client errors)
                    if (400..500).contains(&status) {
                        return Err(SockudoError::Request(RequestError::new(
                            format!("HTTP {}", status),
                            &url,
                            Some(status),
                            Some(body),
                        )));
                    }

                    // Retry on 5xx errors if enabled
                    if attempt >= max_attempts {
                        return Err(SockudoError::Request(RequestError::new(
                            format!("HTTP {} after {} attempts", status, attempt),
                            &url,
                            Some(status),
                            Some(body),
                        )));
                    }
                }
                Err(e) => {
                    // Retry on network errors if enabled
                    if attempt >= max_attempts {
                        return Err(SockudoError::Http(e));
                    }
                }
            }

            // Exponential backoff: 100ms, 200ms, 400ms, etc.
            let delay = Duration::from_millis(100 * (1 << (attempt - 1)));
            tokio::time::sleep(delay).await;
        }
    }
}

/// Creates a signed query string for Sockudo API requests
fn create_signed_query_string(
    token: &Token,
    method: &str,
    path: &str,
    body: Option<&str>,
    params: Option<&BTreeMap<String, String>>,
) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut query_params = BTreeMap::new();
    query_params.insert("auth_key".to_string(), token.key.clone());
    query_params.insert("auth_timestamp".to_string(), timestamp.to_string());
    query_params.insert("auth_version".to_string(), "1.0".to_string());

    if let Some(body) = body {
        query_params.insert("body_md5".to_string(), util::get_md5(body));
    }

    if let Some(params) = params {
        for (key, value) in params {
            query_params.insert(key.clone(), value.clone());
        }
    }

    let query_string = util::to_ordered_array(&query_params).join("&");
    let signing_params = query_params
        .iter()
        .map(|(key, value)| (key.to_lowercase(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let signing_query_string = util::to_ordered_array(&signing_params).join("&");
    let sign_data = format!(
        "{}\n{}\n{}",
        method.to_uppercase(),
        path,
        signing_query_string
    );
    let signature = token.sign(&sign_data);

    format!("{}&auth_signature={}", query_string, signature)
}

impl std::fmt::Debug for Sockudo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sockudo")
            .field("config", &self.inner.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HistoryParams;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn test_sockudo_creation() {
        let config = Config::new("123", "key", "secret");
        let sockudo = Sockudo::new(config).unwrap();
        assert_eq!(sockudo.config().app_id(), "123");
    }

    #[tokio::test]
    async fn test_authorize_channel() {
        let config = Config::new("123", "key", "secret");
        let sockudo = Sockudo::new(config).unwrap();

        let result = sockudo.authorize_channel(
            "123.456",
            &Channel::from_string("test-channel").unwrap(),
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_for_cluster() {
        let config = Config::new("123", "key", "secret");
        let sockudo = Sockudo::new(config).unwrap();

        let eu_sockudo = sockudo.for_cluster("eu").unwrap();
        assert_eq!(eu_sockudo.config().host(), "api-eu.sockudo.io");
    }

    #[test]
    fn test_history_params_to_map() {
        let params = HistoryParams {
            limit: Some(50),
            direction: Some("newest_first".to_string()),
            cursor: Some("abc".to_string()),
            start_serial: Some(10),
            end_serial: Some(20),
            start_time_ms: Some(1000),
            end_time_ms: Some(2000),
        };

        let map = params.to_map();
        assert_eq!(map.get("limit"), Some(&"50".to_string()));
        assert_eq!(map.get("direction"), Some(&"newest_first".to_string()));
        assert_eq!(map.get("cursor"), Some(&"abc".to_string()));
        assert_eq!(map.get("start_serial"), Some(&"10".to_string()));
        assert_eq!(map.get("end_serial"), Some(&"20".to_string()));
        assert_eq!(map.get("start_time_ms"), Some(&"1000".to_string()));
        assert_eq!(map.get("end_time_ms"), Some(&"2000".to_string()));
    }

    #[test]
    fn test_signed_query_string_signs_lowercase_keys_but_keeps_request_keys() {
        let token = Token::new("key", "secret");
        let mut params = BTreeMap::new();
        params.insert("deviceId".to_string(), "device-1".to_string());
        params.insert("limit".to_string(), "10".to_string());

        let query = create_signed_query_string(
            &token,
            "GET",
            "/apps/app-id/push/channelSubscriptions",
            None,
            Some(&params),
        );
        let signature = query
            .split('&')
            .find_map(|part| part.strip_prefix("auth_signature="))
            .expect("query contains auth signature");
        let timestamp = query
            .split('&')
            .find_map(|part| part.strip_prefix("auth_timestamp="))
            .expect("query contains auth timestamp");
        let expected_to_sign = format!(
            "GET\n/apps/app-id/push/channelSubscriptions\nauth_key=key&auth_timestamp={timestamp}&auth_version=1.0&deviceid=device-1&limit=10"
        );

        assert!(query.contains("deviceId=device-1"));
        assert!(!query.contains("deviceid=device-1"));
        assert_eq!(signature, token.sign(&expected_to_sign));
    }

    #[tokio::test]
    async fn test_channel_history_requests_expected_path_and_decodes_page() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 4096];
            let read = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.contains("GET /apps/123/channels/history-room/history?"));
            assert!(request.contains("limit=50"));
            assert!(request.contains("direction=newest_first"));
            assert!(request.contains("cursor=abc"));
            assert!(request.contains("start_serial=10"));
            assert!(request.contains("end_serial=20"));
            let body = r#"{"items":[],"direction":"newest_first","limit":50,"has_more":true,"next_cursor":"abc","bounds":{"start_serial":10,"end_serial":20,"start_time_ms":1000,"end_time_ms":2000},"continuity":{"stream_id":"stream-1","oldest_available_serial":10,"newest_available_serial":20,"retained_messages":2,"retained_bytes":100,"complete":false,"truncated_by_retention":false}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let config = Config::builder()
            .app_id("123")
            .key("key")
            .secret("secret")
            .host("127.0.0.1")
            .port(addr.port())
            .use_tls(false)
            .build()
            .unwrap();
        let sockudo = Sockudo::new(config).unwrap();
        let page = sockudo
            .channel_history_with_name(
                "history-room",
                Some(&HistoryParams {
                    limit: Some(50),
                    direction: Some("newest_first".to_string()),
                    cursor: Some("abc".to_string()),
                    start_serial: Some(10),
                    end_serial: Some(20),
                    start_time_ms: Some(1000),
                    end_time_ms: Some(2000),
                }),
            )
            .await
            .unwrap();

        assert_eq!(page.direction, "newest_first");
        assert_eq!(page.next_cursor.as_deref(), Some("abc"));
        assert_eq!(page.continuity.stream_id.as_deref(), Some("stream-1"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn test_terminate_user_connections_requests_expected_path() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 4096];
            let read = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.contains("POST /apps/123/users/user-charlie/terminate_connections"));
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
            stream.write_all(response.as_bytes()).unwrap();
        });

        let config = Config::builder()
            .app_id("123")
            .key("key")
            .secret("secret")
            .host("127.0.0.1")
            .port(addr.port())
            .use_tls(false)
            .build()
            .unwrap();
        let sockudo = Sockudo::new(config).unwrap();
        let result = sockudo.terminate_user_connections("user-charlie").await;
        assert!(result.is_ok());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn test_force_reconnect_user_requests_expected_path() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 4096];
            let read = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.contains("POST /apps/123/users/user-charlie/force_reconnect"));
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
            stream.write_all(response.as_bytes()).unwrap();
        });

        let config = Config::builder()
            .app_id("123")
            .key("key")
            .secret("secret")
            .host("127.0.0.1")
            .port(addr.port())
            .use_tls(false)
            .build()
            .unwrap();
        let sockudo = Sockudo::new(config).unwrap();
        let result = sockudo.force_reconnect_user("user-charlie").await;
        assert!(result.is_ok());
        server.join().unwrap();
    }

    #[test]
    fn test_force_reconnect_user_rejects_invalid_user_id() {
        let config = Config::new("123", "key", "secret");
        let sockudo = Sockudo::new(config).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(sockudo.force_reconnect_user(""));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_message_requests_expected_path_and_decodes_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0u8; 4096];
            let read = stream.read(&mut buffer).unwrap();
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.contains("GET /apps/123/channels/chat:room-1/messages/msg:1?"));
            let body = r#"{"channel":"chat:room-1","item":{"message_serial":"msg:1","action":"update","data":"hello"}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let config = Config::builder()
            .app_id("123")
            .key("key")
            .secret("secret")
            .host("127.0.0.1")
            .port(addr.port())
            .use_tls(false)
            .build()
            .unwrap();
        let sockudo = Sockudo::new(config).unwrap();
        let response = sockudo.get_message("chat:room-1", "msg:1").await.unwrap();

        assert_eq!(response.channel, "chat:room-1");
        assert_eq!(response.item["message_serial"].as_str(), Some("msg:1"));
        server.join().unwrap();
    }
}
