use async_trait::async_trait;
use sockudo_core::delta_types::ClusterCoordinator;
use sockudo_core::error::{Error, Result};
use std::sync::Arc;
use tracing::{debug, info};

/// NATS-based cluster coordinator for delta interval synchronization.
/// Uses NATS Key-Value store for atomic counter operations.
pub struct NatsClusterCoordinator {
    kv_store: Arc<async_nats::jetstream::kv::Store>,
    #[allow(dead_code)]
    prefix: String,
    #[allow(dead_code)]
    ttl_seconds: u64,
}

impl NatsClusterCoordinator {
    /// Create a new NATS cluster coordinator
    pub async fn new(nats_servers: Vec<String>, prefix: Option<&str>) -> Result<Self> {
        let client = async_nats::connect(nats_servers.join(","))
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to NATS: {}", e)))?;

        info!("Connected to NATS for cluster coordination");

        let jetstream = async_nats::jetstream::new(client);

        let bucket_name = format!("{}_delta_counts", prefix.unwrap_or("sockudo"));

        let kv_store = match jetstream.get_key_value(&bucket_name).await {
            Ok(store) => {
                info!("Using existing NATS KV bucket: {}", bucket_name);
                store
            }
            Err(_) => {
                info!("Creating NATS KV bucket: {}", bucket_name);
                jetstream
                    .create_key_value(async_nats::jetstream::kv::Config {
                        bucket: bucket_name.clone(),
                        description: "Delta compression cluster coordination counters".to_string(),
                        max_age: std::time::Duration::from_secs(300),
                        history: 1,
                        storage: async_nats::jetstream::stream::StorageType::Memory,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to create NATS KV bucket: {}", e))
                    })?
            }
        };

        Ok(Self {
            kv_store: Arc::new(kv_store),
            prefix: prefix.unwrap_or("sockudo").to_string(),
            ttl_seconds: 300,
        })
    }

    fn get_key(&self, app_id: &str, channel: &str, conflation_key: &str) -> String {
        format!("{}:{}:{}", app_id, channel, conflation_key)
    }

    fn parse_counter(&self, data: &[u8]) -> Result<u32> {
        std::str::from_utf8(data)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or_else(|| Error::Internal("Failed to parse counter value".to_string()))
    }
}

#[async_trait]
impl ClusterCoordinator for NatsClusterCoordinator {
    async fn increment_and_check(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
        interval: u32,
    ) -> Result<(bool, u32)> {
        let key = self.get_key(app_id, channel, conflation_key);

        let current = match self.kv_store.get(&key).await {
            Ok(Some(entry)) => self.parse_counter(&entry)?,
            Ok(None) => 0,
            Err(e) => {
                return Err(Error::Internal(format!(
                    "Failed to get NATS KV value: {}",
                    e
                )));
            }
        };

        let new_count = current + 1;
        let should_send_full = new_count >= interval;

        if should_send_full {
            debug!(
                "Cluster coordination (NATS): Full message triggered (count={}, interval={}) for app={}, channel={}, key={}",
                new_count, interval, app_id, channel, conflation_key
            );

            self.kv_store.put(&key, "0".into()).await.map_err(|e| {
                Error::Internal(format!("Failed to reset NATS counter: {}", e))
            })?;

            Ok((true, interval))
        } else {
            debug!(
                "Cluster coordination (NATS): Delta message (count={}/{}) for app={}, channel={}, key={}",
                new_count, interval, app_id, channel, conflation_key
            );

            self.kv_store
                .put(&key, new_count.to_string().into())
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to increment NATS counter: {}", e))
                })?;

            Ok((false, new_count))
        }
    }

    async fn reset_counter(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
    ) -> Result<()> {
        let key = self.get_key(app_id, channel, conflation_key);

        self.kv_store.delete(&key).await.map_err(|e| {
            Error::Internal(format!("Failed to delete NATS counter: {}", e))
        })?;

        debug!(
            "Cluster coordination (NATS): Reset counter for app={}, channel={}, key={}",
            app_id, channel, conflation_key
        );
        Ok(())
    }

    async fn get_counter(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
    ) -> Result<u32> {
        let key = self.get_key(app_id, channel, conflation_key);

        match self.kv_store.get(&key).await {
            Ok(Some(entry)) => self.parse_counter(&entry),
            Ok(None) => Ok(0),
            Err(e) => Err(Error::Internal(format!(
                "Failed to get NATS counter: {}",
                e
            ))),
        }
    }
}

impl Clone for NatsClusterCoordinator {
    fn clone(&self) -> Self {
        Self {
            kv_store: Arc::clone(&self.kv_store),
            prefix: self.prefix.clone(),
            ttl_seconds: self.ttl_seconds,
        }
    }
}
