// src/cluster/redis_cluster_service.rs
use super::traits::{ClusterConfig, ClusterService};
use crate::error::Result;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info};

/// Redis-based implementation of cluster service for dead node detection
pub struct RedisClusterService {
    node_id: String,
    config: ClusterConfig,
    redis_client: Client,
    // Track presence entries per node locally for efficiency
    presence_tracking: Arc<RwLock<HashMap<String, HashSet<(String, String)>>>>, // node_id -> set of (channel, user_id)
}

impl RedisClusterService {
    pub fn new(node_id: String, config: ClusterConfig, redis_client: Client) -> Result<Self> {
        Ok(Self {
            node_id,
            config,
            redis_client,
            presence_tracking: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl ClusterService for RedisClusterService {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    async fn start(&self) -> Result<()> {
        info!("Starting Redis cluster service with node_id: {}", self.node_id);

        // Start heartbeat task
        let heartbeat_task = self.start_heartbeat_task();
        
        // Start cleanup task
        let cleanup_task = self.start_cleanup_task();

        // Run both tasks concurrently
        tokio::try_join!(heartbeat_task, cleanup_task)?;

        Ok(())
    }

    async fn track_presence_join(&self, channel: &str, user_id: &str) -> Result<()> {
        let mut connection = self.redis_client.get_connection_manager().await?;
        
        // Add to channel presence
        let channel_key = format!("sockudo:presence:channel:{}", channel);
        let _: usize = connection.sadd(&channel_key, user_id).await?;
        
        // Track in node's presence set
        let node_key = format!("sockudo:presence:node:{}", self.node_id);
        let entry = format!("{}:{}", channel, user_id);
        let _: usize = connection.sadd(&node_key, &entry).await?;
        
        // Also track locally for efficiency
        let mut tracking = self.presence_tracking.write().await;
        tracking
            .entry(self.node_id.clone())
            .or_insert_with(HashSet::new)
            .insert((channel.to_string(), user_id.to_string()));
        
        Ok(())
    }

    async fn track_presence_leave(&self, channel: &str, user_id: &str) -> Result<()> {
        let mut connection = self.redis_client.get_connection_manager().await?;
        
        // Remove from channel presence
        let channel_key = format!("sockudo:presence:channel:{}", channel);
        let _: usize = connection.srem(&channel_key, user_id).await?;
        
        // Remove from node's presence set
        let node_key = format!("sockudo:presence:node:{}", self.node_id);
        let entry = format!("{}:{}", channel, user_id);
        let _: usize = connection.srem(&node_key, &entry).await?;
        
        // Remove from local tracking
        let mut tracking = self.presence_tracking.write().await;
        if let Some(node_set) = tracking.get_mut(&self.node_id) {
            node_set.remove(&(channel.to_string(), user_id.to_string()));
        }
        
        Ok(())
    }
}

impl RedisClusterService {
    async fn start_heartbeat_task(&self) -> Result<()> {
        let mut connection = self.redis_client.get_connection_manager().await?;
        let mut interval = interval(Duration::from_millis(self.config.heartbeat_interval_ms));

        loop {
            interval.tick().await;
            
            if let Err(e) = self.send_heartbeat(&mut connection).await {
                error!("Failed to send heartbeat: {}", e);
            }
        }
    }

    async fn send_heartbeat(&self, connection: &mut ConnectionManager) -> Result<()> {
        let key = format!("sockudo:nodes:heartbeat:{}", self.node_id);
        let ttl_seconds = (self.config.node_timeout_ms / 1000) as u64;
        
        let _: () = connection.set_ex(&key, "alive", ttl_seconds).await?;
        debug!("Sent heartbeat for node {}", self.node_id);
        Ok(())
    }

    async fn start_cleanup_task(&self) -> Result<()> {
        let mut connection = self.redis_client.get_connection_manager().await?;
        let mut interval = interval(Duration::from_millis(self.config.cleanup_interval_ms));

        loop {
            interval.tick().await;
            
            if let Err(e) = self.perform_cleanup(&mut connection).await {
                error!("Failed to perform cluster cleanup: {}", e);
            }
        }
    }

    async fn perform_cleanup(&self, connection: &mut ConnectionManager) -> Result<()> {
        // Get all active nodes
        let active_nodes = self.get_active_nodes(connection).await?;
        let active_set: HashSet<String> = active_nodes.into_iter().collect();

        // Deterministic leader election - only one node should perform cleanup
        if !self.am_i_cleanup_leader(&active_set) {
            debug!("Not the cleanup leader, skipping cleanup");
            return Ok(());
        }

        debug!("I am the cleanup leader, performing dead node detection");

        // Get all nodes that have presence entries
        let presence_nodes = self.get_nodes_with_presence(connection).await?;

        let mut cleaned_entries = 0;
        
        for node_id in presence_nodes {
            if !active_set.contains(&node_id) {
                info!("Cleaning up presence entries for dead node: {}", node_id);
                cleaned_entries += self.cleanup_dead_node_presence(connection, &node_id).await?;
            }
        }

        if cleaned_entries > 0 {
            info!("Cleaned up {} presence entries from dead nodes", cleaned_entries);
        }

        Ok(())
    }

    /// Deterministic leader election using consistent hashing
    /// The node with the lowest hash among active nodes becomes the leader
    fn am_i_cleanup_leader(&self, active_nodes: &HashSet<String>) -> bool {
        if active_nodes.is_empty() {
            return false;
        }

        // Find the node with the lowest hash (deterministic leader selection)
        let mut leader_node = None;
        let mut lowest_hash = u64::MAX;

        for node_id in active_nodes {
            let mut hasher = DefaultHasher::new();
            node_id.hash(&mut hasher);
            let hash = hasher.finish();
            
            if hash < lowest_hash {
                lowest_hash = hash;
                leader_node = Some(node_id.as_str());
            }
        }

        if let Some(leader) = leader_node {
            let is_leader = leader == self.node_id;
            if is_leader {
                debug!("Selected as cleanup leader (hash: {})", lowest_hash);
            }
            is_leader
        } else {
            false
        }
    }

    async fn get_active_nodes(&self, connection: &mut ConnectionManager) -> Result<Vec<String>> {
        let pattern = "sockudo:nodes:heartbeat:*";
        let keys: Vec<String> = connection.keys(pattern).await?;
        
        let node_ids = keys
            .into_iter()
            .filter_map(|key| {
                key.strip_prefix("sockudo:nodes:heartbeat:")
                    .map(|s| s.to_string())
            })
            .collect();
            
        Ok(node_ids)
    }

    async fn get_nodes_with_presence(&self, connection: &mut ConnectionManager) -> Result<Vec<String>> {
        let pattern = "sockudo:presence:node:*";
        let keys: Vec<String> = connection.keys(pattern).await?;
        
        let node_ids = keys
            .into_iter()
            .filter_map(|key| {
                key.strip_prefix("sockudo:presence:node:")
                    .map(|s| s.to_string())
            })
            .collect();
            
        Ok(node_ids)
    }

    async fn cleanup_dead_node_presence(&self, connection: &mut ConnectionManager, node_id: &str) -> Result<usize> {
        // Get all presence entries for this node
        let node_key = format!("sockudo:presence:node:{}", node_id);
        let entries: Vec<String> = connection.smembers(&node_key).await?;
        
        let mut cleaned_count = 0;
        
        for entry in entries {
            // Entry format: "channel:user_id"
            if let Some((channel, user_id)) = entry.split_once(':') {
                // Remove from channel presence
                let channel_key = format!("sockudo:presence:channel:{}", channel);
                let removed: bool = connection.srem(&channel_key, &user_id).await?;
                
                if removed {
                    cleaned_count += 1;
                    info!("Removed dead presence entry: {} from {}", user_id, channel);
                    
                    // TODO: Send pusher_internal:member_removed event
                    // This will be integrated with ConnectionManager in next step
                }
            }
        }
        
        // Remove the node's tracking set
        let _: usize = connection.del(&node_key).await?;
        
        Ok(cleaned_count)
    }
}