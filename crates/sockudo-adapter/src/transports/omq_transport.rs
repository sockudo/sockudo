use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use async_trait::async_trait;
use bytes::Bytes;
use omq_tokio::{Context, ContextConfig, Message, Options, Socket, SocketType};
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::OmqAdapterConfig;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use tracing::{error, info, trace, warn};
use uuid::Uuid;

/// Brokerless OMQ transport implementation.
pub struct OmqTransport {
    context: Context,
    publisher: Socket,
    subscriber: Socket,
    broadcast_topic: String,
    request_topic: String,
    response_topic: String,
    inbox_prefix: String,
    config: OmqAdapterConfig,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
}

impl Clone for OmqTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            context: self.context.clone(),
            publisher: self.publisher.clone(),
            subscriber: self.subscriber.clone(),
            broadcast_topic: self.broadcast_topic.clone(),
            request_topic: self.request_topic.clone(),
            response_topic: self.response_topic.clone(),
            inbox_prefix: self.inbox_prefix.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}

impl OmqTransport {
    fn topic(prefix: &str, suffix: &str) -> String {
        format!("{prefix}.{suffix}")
    }

    fn node_topic(&self, node_id: &str) -> String {
        Self::topic(&self.config.prefix, &format!("node.{node_id}"))
    }

    async fn publish_json<T: serde::Serialize>(
        publisher: &Socket,
        topic: &str,
        value: &T,
        kind: &str,
    ) -> Result<()> {
        let payload = sonic_rs::to_vec(value)
            .map_err(|e| Error::Other(format!("Failed to serialize {kind}: {e}")))?;
        publisher
            .send(Message::multipart([
                Bytes::from(topic.to_owned()),
                Bytes::from(payload),
            ]))
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish {kind}: {e}")))?;
        Ok(())
    }

    fn parse_message(message: Message) -> Result<(Bytes, Bytes)> {
        message
            .try_as_parts::<2>()
            .map(|[topic, payload]| (topic, payload))
            .map_err(|e| Error::Other(format!("Invalid OMQ transport message: {e}")))
    }

    fn mark_dropped(metrics: &Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>) {
        if let Some(metrics) = metrics.get() {
            metrics.mark_horizontal_transport_message_dropped("omq");
        }
    }
}

impl TransportConfig for OmqAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[async_trait]
impl HorizontalTransport for OmqTransport {
    type Config = OmqAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        info!(
            adapter = "omq",
            bind_endpoint = %config.bind_endpoint,
            peer_count = config.connect_endpoints.len(),
            prefix = %config.prefix,
            request_timeout_ms = config.request_timeout_ms,
            io_threads = config.io_threads,
            "transport initializing"
        );

        let context = Context::with_config(ContextConfig {
            io_threads: config.io_threads,
        });
        let options = Options::default()
            .send_hwm(config.send_hwm)
            .recv_hwm(config.recv_hwm);
        let publisher = context.socket(SocketType::Pub, options.clone());
        let subscriber = context.socket(SocketType::Sub, options);

        let bind_endpoint = config.bind_endpoint.parse().map_err(|e| {
            Error::Other(format!(
                "Failed to parse OMQ bind endpoint '{}': {e}",
                config.bind_endpoint
            ))
        })?;
        let resolved = subscriber
            .bind(bind_endpoint)
            .await
            .map_err(|e| Error::Internal(format!("Failed to bind OMQ subscriber: {e}")))?;

        for endpoint in &config.connect_endpoints {
            let endpoint = endpoint
                .parse()
                .map_err(|e| Error::Other(format!("Failed to parse OMQ peer endpoint: {e}")))?;
            publisher
                .connect(endpoint)
                .await
                .map_err(|e| Error::Internal(format!("Failed to connect OMQ publisher: {e}")))?;
        }

        info!(
            adapter = "omq",
            bind_endpoint = %resolved,
            peer_count = config.connect_endpoints.len(),
            "transport initialized"
        );

        Ok(Self {
            context,
            publisher,
            subscriber,
            broadcast_topic: Self::topic(&config.prefix, "broadcast"),
            request_topic: Self::topic(&config.prefix, "requests"),
            response_topic: Self::topic(&config.prefix, "responses"),
            inbox_prefix: Self::topic(
                &config.prefix,
                &format!("inbox.{}.", Uuid::new_v4().as_simple()),
            ),
            config,
            metrics: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        Self::publish_json(
            &self.publisher,
            &self.broadcast_topic,
            message,
            "broadcast message",
        )
        .await?;
        trace!(adapter = "omq", "broadcast message published to transport");
        Ok(())
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        Self::publish_json(&self.publisher, &self.request_topic, request, "request").await?;
        trace!(adapter = "omq", request_id = %request.request_id, "request published to transport");
        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        Self::publish_json(&self.publisher, &self.response_topic, response, "response").await?;
        trace!(adapter = "omq", "response published to transport");
        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        let node_topic_string = self.node_topic(&handlers.node_id);
        for topic in [
            self.broadcast_topic.as_bytes(),
            self.request_topic.as_bytes(),
            self.response_topic.as_bytes(),
            self.inbox_prefix.as_bytes(),
            node_topic_string.as_bytes(),
        ] {
            self.subscriber
                .subscribe(Bytes::copy_from_slice(topic))
                .await
                .map_err(|e| Error::Internal(format!("Failed to subscribe OMQ topic: {e}")))?;
        }

        let subscriber = self.subscriber.clone();
        let publisher = self.publisher.clone();
        let broadcast_topic = Bytes::from(self.broadcast_topic.clone());
        let request_topic = Bytes::from(self.request_topic.clone());
        let response_topic = Bytes::from(self.response_topic.clone());
        let inbox_prefix = Bytes::from(self.inbox_prefix.clone());
        let node_topic = Bytes::from(node_topic_string.clone());
        let response_topic_for_publish = self.response_topic.clone();
        let broadcast_handler = handlers.on_broadcast.clone();
        let request_handler = handlers.on_request.clone();
        let response_handler = handlers.on_response.clone();
        let metrics = self.metrics.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();

        info!(
            adapter = "omq",
            broadcast_topic = %self.broadcast_topic,
            request_topic = %self.request_topic,
            response_topic = %self.response_topic,
            inbox_prefix = %self.inbox_prefix,
            node_topic = %node_topic_string,
            "transport subscriptions established"
        );

        tokio::spawn(async move {
            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }

                let message = tokio::select! {
                    _ = shutdown.notified() => break,
                    message = subscriber.recv() => message,
                };

                let message = match message {
                    Ok(message) => message,
                    Err(e) => {
                        warn!(adapter = "omq", error = %e, "subscriber receive failed");
                        break;
                    }
                };

                let (topic, payload) = match Self::parse_message(message) {
                    Ok(parts) => parts,
                    Err(e) => {
                        Self::mark_dropped(&metrics);
                        error!(adapter = "omq", error = %e, "transport message parse failed");
                        continue;
                    }
                };

                if topic == broadcast_topic {
                    match sonic_rs::from_slice::<BroadcastMessage>(&payload) {
                        Ok(broadcast) => {
                            broadcast_handler(broadcast).await;
                        }
                        Err(e) => {
                            Self::mark_dropped(&metrics);
                            error!(adapter = "omq", error = %e, "broadcast message parse failed");
                        }
                    }
                } else if topic == request_topic || topic == node_topic {
                    match sonic_rs::from_slice::<RequestBody>(&payload) {
                        Ok(request) => {
                            let reply_to = request.reply_to.clone();
                            match request_handler(request).await {
                                Ok(response) => {
                                    let response_topic = reply_to
                                        .unwrap_or_else(|| response_topic_for_publish.clone());
                                    if let Err(e) = Self::publish_json(
                                        &publisher,
                                        &response_topic,
                                        &response,
                                        "response",
                                    )
                                    .await
                                    {
                                        warn!(adapter = "omq", error = %e, "response publish failed");
                                    }
                                }
                                Err(
                                    Error::OwnRequestIgnored
                                    | Error::RequestNotForThisNode
                                    | Error::NoResponseNeeded,
                                ) => {}
                                Err(e) => {
                                    warn!(adapter = "omq", error = %e, "request handler failed");
                                }
                            }
                        }
                        Err(e) => {
                            Self::mark_dropped(&metrics);
                            error!(adapter = "omq", error = %e, "request message parse failed");
                        }
                    }
                } else if topic == response_topic || topic.starts_with(inbox_prefix.as_ref()) {
                    match sonic_rs::from_slice::<ResponseBody>(&payload) {
                        Ok(response) => {
                            response_handler(response).await;
                        }
                        Err(e) => {
                            Self::mark_dropped(&metrics);
                            error!(adapter = "omq", error = %e, "response message parse failed");
                        }
                    }
                }
            }
            warn!(adapter = "omq", "subscription ended");
        });

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        if let Some(nodes) = self.config.nodes_number {
            return Ok(nodes as usize);
        }
        Ok(self.config.connect_endpoints.len() + 1)
    }

    async fn check_health(&self) -> Result<()> {
        if self.is_running.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err(Error::Connection("OMQ transport is closed".to_string()))
        }
    }

    fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics.set(metrics);
    }

    fn new_inbox(&self) -> Option<String> {
        Some(format!(
            "{}{}",
            self.inbox_prefix,
            Uuid::new_v4().as_simple()
        ))
    }

    async fn publish_request_with_reply(
        &self,
        request: &RequestBody,
        reply_to: &str,
    ) -> Result<()> {
        let mut request = request.clone();
        request.reply_to = Some(reply_to.to_string());
        Self::publish_json(
            &self.publisher,
            &self.request_topic,
            &request,
            "request with reply",
        )
        .await?;
        trace!(adapter = "omq", request_id = %request.request_id, reply_to = %reply_to, "request with reply published to transport");
        Ok(())
    }

    async fn publish_request_to_node(
        &self,
        request: &RequestBody,
        target_node_id: &str,
    ) -> Result<()> {
        let topic = self.node_topic(target_node_id);
        Self::publish_json(&self.publisher, &topic, request, "node request").await?;
        trace!(adapter = "omq", request_id = %request.request_id, target_node_id = %target_node_id, "request published to node");
        Ok(())
    }
}

impl Drop for OmqTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::horizontal_adapter::RequestType;
    use ahash::AHashMap;
    use std::collections::{BTreeMap, HashSet};
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn reserve_endpoint() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);
        format!("tcp://127.0.0.1:{port}")
    }

    fn config(bind_endpoint: String, connect_endpoint: String) -> OmqAdapterConfig {
        OmqAdapterConfig {
            bind_endpoint,
            connect_endpoints: vec![connect_endpoint],
            prefix: format!("sockudo_test_{}", uuid::Uuid::new_v4().as_simple()),
            request_timeout_ms: 1000,
            nodes_number: Some(2),
            io_threads: 1,
            send_hwm: 1000,
            recv_hwm: 1000,
        }
    }

    fn broadcast(node_id: &str) -> BroadcastMessage {
        BroadcastMessage {
            node_id: node_id.to_string(),
            app_id: "app".to_string(),
            channel: "public-room".to_string(),
            message: "{}".to_string(),
            presence_replication: None,
            envelope: None,
            except_socket_id: None,
            timestamp_ms: None,
            compression_metadata: None,
            idempotency_key: None,
            ephemeral: false,
            trace_context: BTreeMap::new(),
        }
    }

    fn request(node_id: &str) -> RequestBody {
        RequestBody {
            request_id: "req-1".to_string(),
            node_id: node_id.to_string(),
            app_id: "app".to_string(),
            request_type: RequestType::SocketsCount,
            channel: None,
            socket_id: None,
            user_id: None,
            user_info: None,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
            channels: None,
            reply_to: None,
            trace_context: BTreeMap::new(),
        }
    }

    fn response(request: &RequestBody, node_id: &str) -> ResponseBody {
        ResponseBody {
            request_id: request.request_id.clone(),
            node_id: node_id.to_string(),
            app_id: request.app_id.clone(),
            members: AHashMap::new(),
            channels_with_sockets_count: AHashMap::new(),
            socket_ids: Vec::new(),
            sockets_count: 7,
            exists: false,
            channels: HashSet::new(),
            members_count: 0,
            responses_received: 1,
            expected_responses: 1,
            complete: true,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn omq_transport_delivers_broadcasts_and_request_responses() {
        let endpoint_a = reserve_endpoint();
        let endpoint_b = reserve_endpoint();
        let config_a = config(endpoint_a.clone(), endpoint_b.clone());
        let mut config_b = config(endpoint_b, endpoint_a);
        config_b.prefix.clone_from(&config_a.prefix);

        let node_a = OmqTransport::new(config_a).await.expect("node a");
        let node_b = OmqTransport::new(config_b).await.expect("node b");

        let (broadcast_tx, mut broadcast_rx) = mpsc::channel(1);
        let (response_tx, mut response_rx) = mpsc::channel(1);

        node_a
            .start_listeners(TransportHandlers {
                node_id: "node-a".to_string(),
                on_broadcast: Arc::new(|_| Box::pin(async {})),
                on_request: Arc::new(|_| Box::pin(async { Err(Error::NoResponseNeeded) })),
                on_response: Arc::new(move |response| {
                    let response_tx = response_tx.clone();
                    Box::pin(async move {
                        response_tx.send(response).await.expect("response send");
                    })
                }),
            })
            .await
            .expect("node a listeners");

        node_b
            .start_listeners(TransportHandlers {
                node_id: "node-b".to_string(),
                on_broadcast: Arc::new(move |broadcast| {
                    let broadcast_tx = broadcast_tx.clone();
                    Box::pin(async move {
                        broadcast_tx.send(broadcast).await.expect("broadcast send");
                    })
                }),
                on_request: Arc::new(|request| {
                    Box::pin(async move { Ok(response(&request, "node-b")) })
                }),
                on_response: Arc::new(|_| Box::pin(async {})),
            })
            .await
            .expect("node b listeners");

        node_a
            .publisher
            .wait_connected(1, Duration::from_secs(2))
            .await
            .expect("node a publisher connected");
        node_a
            .publisher
            .wait_subscribed(1, Duration::from_secs(2))
            .await
            .expect("node a publisher subscribed");
        node_b
            .publisher
            .wait_connected(1, Duration::from_secs(2))
            .await
            .expect("node b publisher connected");
        node_b
            .publisher
            .wait_subscribed(1, Duration::from_secs(2))
            .await
            .expect("node b publisher subscribed");

        node_a
            .publish_broadcast(&broadcast("node-a"))
            .await
            .expect("publish broadcast");
        let received_broadcast = tokio::time::timeout(Duration::from_secs(2), broadcast_rx.recv())
            .await
            .expect("broadcast timeout")
            .expect("broadcast");
        assert_eq!(received_broadcast.channel, "public-room");

        node_a
            .publish_request(&request("node-a"))
            .await
            .expect("publish request");
        let received_response = tokio::time::timeout(Duration::from_secs(2), response_rx.recv())
            .await
            .expect("response timeout")
            .expect("response");
        assert_eq!(received_response.request_id, "req-1");
        assert_eq!(received_response.sockets_count, 7);

        let inbox = node_a.new_inbox().expect("new inbox");
        node_a
            .publish_request_with_reply(&request("node-a"), &inbox)
            .await
            .expect("publish request with reply");
        let received_response = tokio::time::timeout(Duration::from_secs(2), response_rx.recv())
            .await
            .expect("inbox response timeout")
            .expect("inbox response");
        assert_eq!(received_response.request_id, "req-1");
        assert_eq!(received_response.sockets_count, 7);
    }
}
