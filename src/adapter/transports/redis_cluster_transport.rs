use crate::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::adapter::horizontal_transport::{
    HorizontalTransport, TransportConfig, TransportHandlers,
};
use crate::error::{Error, Result};
use crate::options::RedisClusterAdapterConfig;
use async_trait::async_trait;
use redis::AsyncCommands;
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::cluster_async::ClusterConnection;
use tracing::{debug, error, info};

/// Helper function to convert redis::Value to String
fn value_to_string(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::VerbatimString { format: _, text } => Some(text.clone()),
        _ => None,
    }
}

impl TransportConfig for RedisClusterAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Redis Cluster transport implementation.
///
/// When `use_connection_manager` is enabled, a persistent `ClusterConnection` is stored
/// and cloned per operation (cheap clone — shares the underlying multiplexed connection).
/// When disabled, a new connection is created for each operation.
#[derive(Clone)]
pub struct RedisClusterTransport {
    client: ClusterClient,
    /// Persistent connection, set when `use_connection_manager` is true
    connection: Option<ClusterConnection>,
    broadcast_channel: String,
    request_channel: String,
    response_channel: String,
    config: RedisClusterAdapterConfig,
}

impl RedisClusterTransport {
    /// Get a cluster connection — clones the persistent one if available,
    /// otherwise creates a new connection from the client.
    async fn get_connection(&self) -> Result<ClusterConnection> {
        if let Some(ref conn) = self.connection {
            Ok(conn.clone())
        } else {
            self.client
                .get_async_connection()
                .await
                .map_err(|e| Error::Redis(format!("Failed to get cluster connection: {e}")))
        }
    }
}

#[async_trait]
impl HorizontalTransport for RedisClusterTransport {
    type Config = RedisClusterAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        let client = ClusterClientBuilder::new(config.nodes.clone())
            .retries(3)
            .read_from_replicas()
            .build()
            .map_err(|e| Error::Redis(format!("Failed to create Redis Cluster client: {e}")))?;

        let connection = if config.use_connection_manager {
            let conn = client.get_async_connection().await.map_err(|e| {
                Error::Redis(format!(
                    "Failed to create persistent cluster connection: {e}"
                ))
            })?;
            info!(
                "Redis Cluster transport using persistent connection (use_connection_manager: true)"
            );
            Some(conn)
        } else {
            debug!(
                "Redis Cluster transport using per-operation connections (use_connection_manager: false)"
            );
            None
        };

        let broadcast_channel = format!("{}:#broadcast", config.prefix);
        let request_channel = format!("{}:#requests", config.prefix);
        let response_channel = format!("{}:#responses", config.prefix);

        Ok(Self {
            client,
            connection,
            broadcast_channel,
            request_channel,
            response_channel,
            config,
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let broadcast_json = serde_json::to_string(message)?;

        let mut conn = self.get_connection().await?;

        conn.publish::<_, _, ()>(&self.broadcast_channel, broadcast_json)
            .await
            .map_err(|e| Error::Redis(format!("Failed to publish broadcast: {e}")))?;

        Ok(())
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        let request_json = serde_json::to_string(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;

        let mut conn = self.get_connection().await?;

        let subscriber_count: i32 = conn
            .publish(&self.request_channel, &request_json)
            .await
            .map_err(|e| Error::Redis(format!("Failed to publish request: {e}")))?;

        debug!(
            "Broadcasted request {} to {} subscribers",
            request.request_id, subscriber_count
        );

        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        let response_json = serde_json::to_string(response)
            .map_err(|e| Error::Other(format!("Failed to serialize response: {e}")))?;

        let mut conn = self.get_connection().await?;

        conn.publish::<_, _, ()>(&self.response_channel, response_json)
            .await
            .map_err(|e| Error::Redis(format!("Failed to publish response: {e}")))?;

        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        // Clone the transport's connection source for use inside the spawned task
        let client = self.client.clone();
        let persistent_conn = self.connection.clone();
        let broadcast_channel = self.broadcast_channel.clone();
        let request_channel = self.request_channel.clone();
        let response_channel = self.response_channel.clone();
        let nodes = self.config.nodes.clone();

        // Create a separate channel for receiving PubSub messages
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // Create a new client with RESP3 protocol for PubSub
        let sub_client = ClusterClientBuilder::new(nodes)
            .use_protocol(redis::ProtocolVersion::RESP3)
            .push_sender(tx)
            .build()
            .map_err(|e| Error::Redis(format!("Failed to create PubSub client: {e}")))?;

        // Spawn the main listener task
        tokio::spawn(async move {
            // Create a connection for PubSub
            let mut pubsub = match sub_client.get_async_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Failed to get pubsub connection: {}", e);
                    return;
                }
            };

            // Subscribe to all channels
            if let Err(e) = pubsub
                .subscribe(&[&broadcast_channel, &request_channel, &response_channel])
                .await
            {
                error!("Failed to subscribe to channels: {}", e);
                return;
            }

            debug!(
                "Redis Cluster transport listening on channels: {}, {}, {}",
                broadcast_channel, request_channel, response_channel
            );

            // Process messages from the channel - PushInfo is the message type for RESP3
            while let Some(push_info) = rx.recv().await {
                // Extract channel and payload from PushInfo
                if push_info.kind != redis::PushKind::Message {
                    continue; // Skip non-message push notifications
                }

                // PushInfo.data for messages should be [channel, payload]
                if push_info.data.len() < 2 {
                    error!("Invalid push message format: {:?}", push_info);
                    continue;
                }

                let channel = match value_to_string(&push_info.data[0]) {
                    Some(s) => s,
                    None => {
                        error!("Failed to parse channel name: {:?}", push_info.data[0]);
                        continue;
                    }
                };

                let payload = match value_to_string(&push_info.data[1]) {
                    Some(s) => s,
                    None => {
                        error!("Failed to parse payload: {:?}", push_info.data[1]);
                        continue;
                    }
                };

                // Process the message in a separate task
                let broadcast_handler = handlers.on_broadcast.clone();
                let request_handler = handlers.on_request.clone();
                let response_handler = handlers.on_response.clone();
                let client_clone = client.clone();
                let persistent_conn_clone = persistent_conn.clone();
                let broadcast_channel_clone = broadcast_channel.clone();
                let request_channel_clone = request_channel.clone();
                let response_channel_clone = response_channel.clone();

                tokio::spawn(async move {
                    if channel == broadcast_channel_clone {
                        // Handle broadcast message
                        if let Ok(broadcast) = serde_json::from_str::<BroadcastMessage>(&payload) {
                            broadcast_handler(broadcast).await;
                        }
                    } else if channel == request_channel_clone {
                        // Handle request message
                        if let Ok(request) = serde_json::from_str::<RequestBody>(&payload) {
                            let response_result = request_handler(request).await;

                            if let Ok(response) = response_result
                                && let Ok(response_json) = serde_json::to_string(&response)
                            {
                                // Reuse persistent connection if available
                                let conn_result = if let Some(ref conn) = persistent_conn_clone {
                                    Ok(conn.clone())
                                } else {
                                    client_clone.get_async_connection().await
                                };
                                if let Ok(mut conn) = conn_result {
                                    let _ = conn
                                        .publish::<_, _, ()>(&response_channel_clone, response_json)
                                        .await;
                                }
                            }
                        }
                    } else if channel == response_channel_clone {
                        // Handle response message
                        if let Ok(response) = serde_json::from_str::<ResponseBody>(&payload) {
                            response_handler(response).await;
                        }
                    }
                });
            }
        });

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;

        let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg(&self.request_channel)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(values) => {
                if values.len() >= 2 {
                    if let redis::Value::Int(count) = values[1] {
                        Ok((count as usize).max(1))
                    } else {
                        Ok(1)
                    }
                } else {
                    Ok(1)
                }
            }
            Err(e) => {
                error!("Failed to execute PUBSUB NUMSUB: {}", e);
                Ok(1)
            }
        }
    }

    async fn check_health(&self) -> Result<()> {
        let mut conn = self.get_connection().await?;

        let response = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| Error::Redis(format!("Cluster health check PING failed: {e}")))?;

        if response == "PONG" {
            Ok(())
        } else {
            Err(Error::Redis(format!(
                "Cluster PING returned unexpected response: {response}"
            )))
        }
    }
}
