# Issue #87 Fix Plan: Dead Node Cleanup for Presence Channels

## Problem Summary

When a Sockudo node crashes ungracefully, existing presence channel members never receive `pusher_internal:member_removed` events for users who were connected to the crashed node. Testing confirmed that new joiners receive accurate presence member lists, but existing users retain stale presence state indefinitely.

## Solution Architecture

### 1. Generic Node Registry

Simple heartbeat system that any component can use:

```rust
pub trait NodeRegistry: Send + Sync {
    async fn register_node(&self, node_id: &str, ttl_ms: u64) -> Result<()>;
    async fn get_active_nodes(&self) -> Result<Vec<String>>;
    async fn is_node_alive(&self, node_id: &str) -> Result<bool>;
}

// Redis implementation
impl NodeRegistry for RedisAdapter {
    async fn register_node(&self, node_id: &str, ttl_ms: u64) -> Result<()> {
        let key = format!("nodes:heartbeat:{}", node_id);
        self.connection.setex(&key, ttl_ms / 1000, "alive").await?;
        Ok(())
    }
    
    async fn get_active_nodes(&self) -> Result<Vec<String>> {
        let keys: Vec<String> = self.connection.keys("nodes:heartbeat:*").await?;
        Ok(keys.iter()
            .filter_map(|k| k.strip_prefix("nodes:heartbeat:"))
            .map(|s| s.to_string())
            .collect())
    }
}
```

### 2. Generic Dead Node Cleanup Task

Extensible cleanup system that runs periodically:

```rust
pub struct DeadNodeCleaner {
    node_registry: Arc<dyn NodeRegistry + Send + Sync>,
    connection_manager: Arc<Mutex<dyn ConnectionManager + Send + Sync>>,
    webhook_integration: Option<Arc<WebhookIntegration>>,
    app_manager: Arc<dyn AppManager + Send + Sync>,
    adapter: Arc<dyn PresenceAdapterStorage + Send + Sync>,
    cleanup_interval_ms: u64, // Default: 30000
    node_id: String,
}

impl DeadNodeCleaner {
    pub async fn run_periodic_cleanup(&self) {
        loop {
            if let Err(e) = self.cleanup_dead_nodes().await {
                error!("Cleanup failed: {}", e);
            }
            sleep(Duration::from_millis(self.cleanup_interval_ms)).await;
        }
    }
    
    async fn cleanup_dead_nodes(&self) -> Result<()> {
        let active_nodes = self.node_registry.get_active_nodes().await?;
        let previously_known_nodes = self.get_previously_known_nodes().await?;
        
        let dead_nodes: Vec<String> = previously_known_nodes
            .into_iter()
            .filter(|node| !active_nodes.contains(node))
            .collect();
        
        for dead_node_id in dead_nodes {
            if self.should_i_cleanup_node(&dead_node_id, &active_nodes) {
                info!("Cleaning up dead node: {}", dead_node_id);
                
                // PRESENCE CLEANUP - add more cleanup types here in future
                self.cleanup_presence_for_dead_node(&dead_node_id).await?;
                
                // FUTURE: Add more cleanup here
                // self.cleanup_cache_for_dead_node(&dead_node_id).await?;
                // self.cleanup_metrics_for_dead_node(&dead_node_id).await?;
                
                self.mark_node_as_cleaned(&dead_node_id).await?;
            }
        }
        
        Ok(())
    }
    
    async fn cleanup_presence_for_dead_node(&self, dead_node_id: &str) -> Result<()> {
        // Get all presence data for dead node in one efficient query
        let dead_node_presence = self.adapter.get_node_presence_data(dead_node_id).await?;
        
        for (app_id, channel, user_id) in dead_node_presence {
            // Check if user has connections on other alive nodes
            let still_connected = self.adapter
                .user_has_other_connections(&app_id, &channel, &user_id, dead_node_id)
                .await?;
            
            if !still_connected {
                // User only existed on dead node - broadcast member_removed
                if let Some(app_config) = self.app_manager.find_by_id(&app_id).await? {
                    PresenceManager::handle_member_removed(
                        &self.connection_manager,
                        self.webhook_integration.as_ref(),
                        &app_config,
                        &channel,
                        &user_id,
                        None,
                    ).await?;
                }
            }
            
            // Always clean up dead node's entry
            self.adapter.remove_node_presence_entry(dead_node_id, &app_id, &channel, &user_id).await?;
        }
        
        // Clean up all presence data for dead node
        self.adapter.cleanup_node_presence_data(dead_node_id).await?;
        
        Ok(())
    }
    
    fn should_i_cleanup_node(&self, dead_node: &str, active_nodes: &[String]) -> bool {
        // Simple deterministic leader election
        let mut sorted_nodes = active_nodes.to_vec();
        sorted_nodes.sort();
        
        let hash: usize = dead_node.chars().map(|c| c as usize).sum();
        let leader_index = hash % sorted_nodes.len();
        
        sorted_nodes[leader_index] == self.node_id
    }
}
```

### 3. Presence Storage in Adapter

Store minimal presence tracking for efficient cleanup:

```rust
pub trait PresenceAdapterStorage: Send + Sync {
    async fn add_presence_entry(&self, node_id: &str, app_id: &str, channel: &str, user_id: &str) -> Result<()>;
    async fn remove_presence_entry(&self, node_id: &str, app_id: &str, channel: &str, user_id: &str) -> Result<()>;
    async fn get_node_presence_data(&self, node_id: &str) -> Result<Vec<(String, String, String)>>; // (app_id, channel, user_id)
    async fn user_has_other_connections(&self, app_id: &str, channel: &str, user_id: &str, excluding_node: &str) -> Result<bool>;
    async fn cleanup_node_presence_data(&self, node_id: &str) -> Result<()>;
}

// Redis storage schema (efficient single-query cleanup):
// "node_presence:{node_id}" -> SET of "{app_id}:{channel}:{user_id}" entries
// "presence_user:{app_id}:{channel}:{user_id}" -> SET of node_ids

impl PresenceAdapterStorage for RedisAdapter {
    async fn add_presence_entry(&self, node_id: &str, app_id: &str, channel: &str, user_id: &str) -> Result<()> {
        let node_key = format!("node_presence:{}", node_id);
        let entry = format!("{}:{}:{}", app_id, channel, user_id);
        self.connection.sadd(&node_key, &entry).await?;
        
        let user_nodes_key = format!("presence_user:{}:{}:{}", app_id, channel, user_id);
        self.connection.sadd(&user_nodes_key, node_id).await?;
        Ok(())
    }
    
    async fn get_node_presence_data(&self, node_id: &str) -> Result<Vec<(String, String, String)>> {
        let node_key = format!("node_presence:{}", node_id);
        let entries: Vec<String> = self.connection.smembers(&node_key).await?;
        
        let mut result = Vec::new();
        for entry in entries {
            let parts: Vec<&str> = entry.splitn(3, ':').collect();
            if parts.len() == 3 {
                result.push((parts[0].to_string(), parts[1].to_string(), parts[2].to_string()));
            }
        }
        Ok(result)
    }
    
    async fn user_has_other_connections(&self, app_id: &str, channel: &str, user_id: &str, excluding_node: &str) -> Result<bool> {
        let user_nodes_key = format!("presence_user:{}:{}:{}", app_id, channel, user_id);
        let nodes: Vec<String> = self.connection.smembers(&user_nodes_key).await?;
        Ok(nodes.iter().any(|node| node != excluding_node))
    }
}

// LocalAdapter implementation (no-op - no shared storage needed)
impl PresenceAdapterStorage for LocalAdapter {
    async fn add_presence_entry(&self, _: &str, _: &str, _: &str, _: &str) -> Result<()> { Ok(()) }
    async fn remove_presence_entry(&self, _: &str, _: &str, _: &str, _: &str) -> Result<()> { Ok(()) }
    async fn get_node_presence_data(&self, _: &str) -> Result<Vec<(String, String, String)>> { Ok(vec![]) }
    async fn user_has_other_connections(&self, _: &str, _: &str, _: &str, _: &str) -> Result<bool> { Ok(false) }
    async fn cleanup_node_presence_data(&self, _: &str) -> Result<()> { Ok(()) }
}
```

### 4. Integration with Existing Presence Logic

Minimal changes to existing presence management:

```rust
// In existing presence subscription logic
impl ConnectionHandler {
    async fn handle_presence_subscription_success(&self, socket_id: &SocketId, app_config: &App, channel: &str, user_id: &str) -> Result<()> {
        // ... existing logic unchanged ...
        
        // NEW: Track in adapter storage (no-op for LocalAdapter)
        if let Some(presence_storage) = &self.presence_storage {
            presence_storage.add_presence_entry(&self.node_id, &app_config.id, channel, user_id).await?;
        }
        
        Ok(())
    }
    
    async fn handle_presence_disconnect(&self, socket_id: &SocketId, app_config: &App, channel: &str, user_id: &str) -> Result<()> {
        // ... existing PresenceManager::handle_member_removed logic unchanged ...
        
        // NEW: Remove from adapter storage (no-op for LocalAdapter)
        if let Some(presence_storage) = &self.presence_storage {
            presence_storage.remove_presence_entry(&self.node_id, &app_config.id, channel, user_id).await?;
        }
        
        Ok(())
    }
}
```

## Implementation Steps

### Phase 1: Core Infrastructure
1. Add `NodeRegistry` trait and Redis implementation
2. Create `PresenceAdapterStorage` trait and implementations (Redis + Local no-op)
3. Implement `DeadNodeCleaner` with presence cleanup
4. Add heartbeat publisher to each node

### Phase 2: Integration
1. Integrate presence storage calls in existing subscription/disconnect handlers
2. Start cleanup task in main server loop
3. Add configuration for cleanup intervals

### Phase 3: Testing & Deployment
1. Test with multinode Docker setup
2. Verify no duplicate member_removed events
3. Confirm ghost users are cleaned up within configured interval
4. Deploy with feature flag

## Configuration

```json
{
  "cluster": {
    "heartbeat_interval_ms": 10000,
    "heartbeat_ttl_ms": 25000,
    "dead_node_cleanup_interval_ms": 30000,
    "cleanup_enabled": true
  }
}
```

## Files to Create/Modify

### New Files:
- `src/cluster/node_registry.rs` - NodeRegistry trait and implementations
- `src/cluster/dead_node_cleaner.rs` - DeadNodeCleaner implementation
- `src/presence/adapter_storage.rs` - PresenceAdapterStorage trait

### Modified Files:
- `src/adapter/redis_adapter.rs` - Implement NodeRegistry and PresenceAdapterStorage
- `src/adapter/local_adapter.rs` - Implement no-op versions
- `src/adapter/handler/subscription_management.rs` - Add presence storage calls
- `src/main.rs` - Start heartbeat and cleanup tasks

## Benefits

✅ **Simple and extensible** - Easy to add more cleanup types in future  
✅ **Efficient** - Single query gets all dead node presence data  
✅ **Adapter agnostic** - Works with Redis, NATS, local  
✅ **Leverages existing logic** - Reuses PresenceManager for consistent behavior  
✅ **Configurable** - Cleanup intervals can be tuned  
✅ **Multi-connection aware** - Handles users connected on multiple nodes  
✅ **Deterministic cleanup** - One leader per dead node prevents duplicates