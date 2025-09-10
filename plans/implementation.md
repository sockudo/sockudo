# Dead Node Cleanup Implementation Plan for Presence Channels

## Problem Statement
When a Sockudo node crashes or loses connectivity in a multi-node deployment, users connected to that node remain as "ghost members" in presence channels. This happens because the crashed node cannot send proper `pusher_internal:member_removed` events, leaving stale presence data visible to other nodes.

## Solution Overview
Implement distributed presence replication and automatic dead node cleanup using the existing horizontal adapter infrastructure. Each node maintains a complete replica of all presence members across the cluster, enabling any surviving node to detect and clean up orphaned members from crashed nodes.

## Architecture Design

### Core Principles
1. **Adapter Agnostic**: Works with NATS, Redis, Redis Cluster without requiring external storage
2. **Leverage Existing Infrastructure**: Use current request/response mechanism for all communication
3. **Distributed Replication**: Every node maintains complete presence data for fault tolerance
4. **Single Cleanup Leader**: Deterministic leader election prevents duplicate cleanup events
5. **Reuse Existing Logic**: Use PresenceManager for all member addition/removal validation

## Detailed Implementation

### 1. New Request Types

Add new request types to the existing `RequestType` enum in `src/adapter/horizontal_adapter.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RequestType {
    // ... existing request types ...
    
    // Presence replication requests
    PresenceMemberJoined {
        channel: String,
        user_id: String,
        user_info: Option<serde_json::Value>,
        owner_node_id: String,  // Which node owns this connection
    },
    PresenceMemberLeft {
        channel: String,
        user_id: String,
        owner_node_id: String,
    },
    
    // Node health requests
    Heartbeat {
        node_id: String,
        timestamp: u64,
        active_connections: usize, // Optional: for monitoring
    },
    NodeDead {
        dead_node_id: String,
    },
}
```

### 2. Enhanced HorizontalAdapter Structure

Add new fields to `HorizontalAdapter` in `src/adapter/horizontal_adapter.rs`:

```rust
pub struct HorizontalAdapter {
    // ... existing fields ...
    
    /// Complete cluster-wide presence registry
    /// HashMap<channel, HashMap<user_id, PresenceEntry>>
    cluster_presence_registry: Arc<RwLock<HashMap<String, HashMap<String, PresenceEntry>>>>,
    
    /// Track node heartbeats: HashMap<node_id, last_heartbeat_timestamp>
    node_heartbeats: Arc<RwLock<HashMap<String, u64>>>,
    
    /// Cleanup configuration
    heartbeat_interval_ms: u64,     // Default: 10000 (10 seconds)
    node_timeout_ms: u64,           // Default: 30000 (30 seconds)
    cleanup_interval_ms: u64,       // Default: 10000 (10 seconds)
}

#[derive(Debug, Clone)]
struct PresenceEntry {
    user_info: Option<serde_json::Value>,
    owner_node_id: String,  // Which node owns this connection
    joined_at: u64,         // Timestamp when user joined
}
```

### 3. Presence Replication Logic

#### 3.1 Member Join Flow

When a user joins a presence channel, modify the existing subscription flow:

```rust
// In src/adapter/handler/subscription_management.rs
// In handle_presence_subscription_success method

async fn handle_presence_subscription_success(
    &self,
    socket_id: &SocketId,
    app_config: &App,
    request: &SubscriptionRequest,
    subscription_result: SubscriptionResult,
) -> Result<()> {
    // ... existing code to add to local adapter ...
    
    // NEW: Broadcast presence join to all nodes for replication
    if let Some(ref user_id) = subscription_result.user_id {
        let replication_request = RequestBody {
            request_id: generate_request_id(),
            node_id: self.connection_manager.lock().await.get_node_id(), // Need to add this method
            app_id: app_config.id.clone(),
            request_type: RequestType::PresenceMemberJoined {
                channel: request.channel.clone(),
                user_id: user_id.clone(),
                user_info: subscription_result.user_info.clone(),
                owner_node_id: self.connection_manager.lock().await.get_node_id(),
            },
            channel: Some(request.channel.clone()),
            socket_id: Some(socket_id.as_ref().to_string()),
            user_id: Some(user_id.clone()),
        };
        
        // Broadcast to all nodes via horizontal adapter
        if let Some(horizontal_adapter) = self.connection_manager.lock().await.as_horizontal_adapter() {
            horizontal_adapter.broadcast_presence_replication(replication_request).await?;
        }
    }
    
    // ... existing code for member_added events ...
}
```

#### 3.2 Member Leave Flow

When a user leaves a presence channel:

```rust
// In src/adapter/handler/core.rs
// In handle_presence_member_removal method

async fn handle_presence_member_removal(
    &self,
    app_config: &App,
    channel_str: &str,
    user_id: &Option<String>,
    current_sub_count: usize,
    socket_id: &SocketId,
) -> Result<()> {
    if channel_str.starts_with("presence-") {
        if let Some(disconnected_user_id) = user_id {
            // NEW: Broadcast presence leave to all nodes BEFORE calling PresenceManager
            let replication_request = RequestBody {
                request_id: generate_request_id(),
                node_id: self.connection_manager.lock().await.get_node_id(),
                app_id: app_config.id.clone(),
                request_type: RequestType::PresenceMemberLeft {
                    channel: channel_str.to_string(),
                    user_id: disconnected_user_id.clone(),
                    owner_node_id: self.connection_manager.lock().await.get_node_id(),
                },
                channel: Some(channel_str.to_string()),
                socket_id: Some(socket_id.as_ref().to_string()),
                user_id: Some(disconnected_user_id.clone()),
            };
            
            if let Some(horizontal_adapter) = self.connection_manager.lock().await.as_horizontal_adapter() {
                horizontal_adapter.broadcast_presence_replication(replication_request).await?;
            }
            
            // Use existing centralized presence member removal logic
            PresenceManager::handle_member_removed(
                &self.connection_manager,
                self.webhook_integration.as_ref(),
                app_config,
                channel_str,
                disconnected_user_id,
                Some(socket_id),
            ).await?;
        }
    }
    
    Ok(())
}
```

### 4. HorizontalAdapter Implementation

#### 4.1 Core Methods

Add these methods to `HorizontalAdapter`:

```rust
impl HorizontalAdapter {
    /// Initialize the cluster presence system
    pub async fn start_cluster_presence_system(&self) -> Result<()> {
        // Start heartbeat broadcasting
        let heartbeat_adapter = Arc::clone(&self);
        tokio::spawn(async move {
            heartbeat_adapter.heartbeat_loop().await;
        });
        
        // Start dead node detection
        let detection_adapter = Arc::clone(&self);
        tokio::spawn(async move {
            detection_adapter.dead_node_detection_loop().await;
        });
        
        Ok(())
    }
    
    /// Broadcast presence replication request to all nodes
    pub async fn broadcast_presence_replication(&self, request: RequestBody) -> Result<()> {
        // Update local registry first
        match &request.request_type {
            RequestType::PresenceMemberJoined { channel, user_id, user_info, owner_node_id } => {
                let mut registry = self.cluster_presence_registry.write().await;
                registry
                    .entry(channel.clone())
                    .or_insert_with(HashMap::new)
                    .insert(user_id.clone(), PresenceEntry {
                        user_info: user_info.clone(),
                        owner_node_id: owner_node_id.clone(),
                        joined_at: current_timestamp(),
                    });
            }
            RequestType::PresenceMemberLeft { channel, user_id, .. } => {
                let mut registry = self.cluster_presence_registry.write().await;
                if let Some(channel_members) = registry.get_mut(channel) {
                    channel_members.remove(user_id);
                    if channel_members.is_empty() {
                        registry.remove(channel);
                    }
                }
            }
            _ => {}
        }
        
        // Broadcast to all other nodes
        self.send_request_without_response(request).await
    }
    
    /// Send request without waiting for response (for broadcasts)
    async fn send_request_without_response(&self, request: RequestBody) -> Result<()> {
        let request_json = serde_json::to_string(&request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;
        
        self.transport.publish_request(&request).await
    }
    
    /// Heartbeat broadcasting loop
    async fn heartbeat_loop(&self) {
        let interval = Duration::from_millis(self.heartbeat_interval_ms);
        
        loop {
            let active_connections = self.local_adapter.get_sockets_count("*").await.unwrap_or(0);
            
            let heartbeat_request = RequestBody {
                request_id: generate_request_id(),
                node_id: self.node_id.clone(),
                app_id: "*".to_string(), // Global heartbeat
                request_type: RequestType::Heartbeat {
                    node_id: self.node_id.clone(),
                    timestamp: current_timestamp(),
                    active_connections,
                },
                channel: None,
                socket_id: None,
                user_id: None,
            };
            
            if let Err(e) = self.send_request_without_response(heartbeat_request).await {
                warn!("Failed to send heartbeat: {}", e);
            }
            
            sleep(interval).await;
        }
    }
    
    /// Dead node detection loop
    async fn dead_node_detection_loop(&self) {
        let interval = Duration::from_millis(self.cleanup_interval_ms);
        
        loop {
            sleep(interval).await;
            
            let dead_nodes = self.detect_dead_nodes().await;
            
            for dead_node_id in dead_nodes {
                info!("Dead node detected: {}", dead_node_id);
                
                // Broadcast dead node notification
                let dead_node_request = RequestBody {
                    request_id: generate_request_id(),
                    node_id: self.node_id.clone(),
                    app_id: "*".to_string(),
                    request_type: RequestType::NodeDead {
                        dead_node_id: dead_node_id.clone(),
                    },
                    channel: None,
                    socket_id: None,
                    user_id: None,
                };
                
                if let Err(e) = self.send_request_without_response(dead_node_request).await {
                    warn!("Failed to broadcast dead node notification: {}", e);
                }
                
                // Handle the dead node (this triggers cleanup if we're the leader)
                self.handle_dead_node_detection(dead_node_id).await;
            }
        }
    }
    
    /// Detect dead nodes based on heartbeat timeouts
    async fn detect_dead_nodes(&self) -> Vec<String> {
        let current_time = current_timestamp();
        let heartbeats = self.node_heartbeats.read().await;
        
        heartbeats
            .iter()
            .filter(|(node_id, last_heartbeat)| {
                *node_id != &self.node_id && // Don't consider ourselves dead
                current_time - **last_heartbeat > self.node_timeout_ms / 1000 // Convert to seconds
            })
            .map(|(node_id, _)| node_id.clone())
            .collect()
    }
    
    /// Get list of currently active nodes
    async fn get_active_nodes(&self) -> Vec<String> {
        let current_time = current_timestamp();
        let heartbeats = self.node_heartbeats.read().await;
        
        let mut active_nodes: Vec<String> = heartbeats
            .iter()
            .filter(|(_, last_heartbeat)| current_time - **last_heartbeat < self.node_timeout_ms / 1000)
            .map(|(node_id, _)| node_id.clone())
            .collect();
        
        // Always include ourselves if we're not in the list
        if !active_nodes.contains(&self.node_id) {
            active_nodes.push(self.node_id.clone());
        }
        
        active_nodes
    }
    
    /// Determine if this node should perform cleanup for a dead node
    async fn should_i_perform_cleanup(&self, dead_node_id: &str) -> bool {
        let active_nodes = self.get_active_nodes().await;
        
        // Remove the dead node from active list
        let mut alive_nodes: Vec<String> = active_nodes
            .into_iter()
            .filter(|node| node != dead_node_id)
            .collect();
        
        // Sort for deterministic ordering
        alive_nodes.sort();
        
        // The node with the lexicographically smallest node_id becomes the cleanup leader
        alive_nodes.first() == Some(&self.node_id)
    }
    
    /// Handle dead node detection
    async fn handle_dead_node_detection(&self, dead_node_id: String) {
        info!("Processing dead node: {}", dead_node_id);
        
        // Only elected leader performs actual cleanup
        if self.should_i_perform_cleanup(&dead_node_id).await {
            info!("Acting as cleanup leader for dead node: {}", dead_node_id);
            self.cleanup_dead_node_presence(&dead_node_id).await;
        } else {
            debug!("Not the cleanup leader for dead node {}, skipping cleanup", dead_node_id);
        }
        
        // All nodes clean up their local registries and heartbeat tracking
        self.remove_dead_node_entries(&dead_node_id).await;
    }
    
    /// Cleanup presence members from dead node
    async fn cleanup_dead_node_presence(&self, dead_node_id: &str) {
        let registry = self.cluster_presence_registry.read().await;
        
        for (channel, members) in registry.iter() {
            if !channel.starts_with("presence-") {
                continue;
            }
            
            for (user_id, entry) in members.iter() {
                if entry.owner_node_id == dead_node_id {
                    info!("Cleaning up orphaned presence member: {} in channel {}", user_id, channel);
                    
                    // Extract app_config from channel name
                    if let Ok(Some(app_config)) = self.extract_app_config_for_channel(channel).await {
                        // *** CRITICAL: Use existing PresenceManager logic ***
                        // This will:
                        // 1. Check if user has OTHER connections to this channel on other nodes
                        // 2. Only send member_removed if this was their LAST connection
                        // 3. Send proper webhooks and broadcast events
                        if let Err(e) = PresenceManager::handle_member_removed(
                            &self.connection_manager,
                            self.webhook_integration.as_ref(),
                            &app_config,
                            channel,
                            user_id,
                            None, // No excluding socket since the entire node is dead
                        ).await {
                            warn!("Failed to handle member removal for dead node cleanup: {}", e);
                        }
                    } else {
                        warn!("Could not find app config for channel: {}", channel);
                    }
                }
            }
        }
    }
    
    /// Remove dead node entries from local tracking
    async fn remove_dead_node_entries(&self, dead_node_id: &str) {
        // Remove from heartbeat tracking
        {
            let mut heartbeats = self.node_heartbeats.write().await;
            heartbeats.remove(dead_node_id);
        }
        
        // Remove from presence registry
        {
            let mut registry = self.cluster_presence_registry.write().await;
            for (_, members) in registry.iter_mut() {
                members.retain(|_, entry| entry.owner_node_id != dead_node_id);
            }
            // Remove empty channels
            registry.retain(|_, members| !members.is_empty());
        }
        
        info!("Cleaned up local entries for dead node: {}", dead_node_id);
    }
    
    /// Extract app config from channel name (helper method)
    async fn extract_app_config_for_channel(&self, channel: &str) -> Result<Option<App>> {
        // This is a simplified implementation - you might need to track app_id with presence data
        // For now, we'll use a heuristic or default app
        
        // Option 1: If using default app
        if let Ok(Some(default_app)) = self.app_manager.find_by_id("default").await {
            return Ok(Some(default_app));
        }
        
        // Option 2: Extract from channel name if it includes app info
        // e.g., "presence-app123-chat" -> app_id = "app123"
        // This depends on your channel naming convention
        
        // Option 3: Query all apps and find one that has this channel
        // This is less efficient but more robust
        
        warn!("Could not determine app config for channel: {}", channel);
        Ok(None)
    }
}
```

#### 4.2 Request Handling

Extend the existing request handling to process new request types:

```rust
impl HorizontalAdapter {
    /// Handle incoming requests (extend existing handle_request method)
    pub async fn handle_request(&self, request: &RequestBody) -> Option<ResponseBody> {
        match &request.request_type {
            // ... existing request type handling ...
            
            RequestType::PresenceMemberJoined { channel, user_id, user_info, owner_node_id } => {
                // Update local registry
                let mut registry = self.cluster_presence_registry.write().await;
                registry
                    .entry(channel.clone())
                    .or_insert_with(HashMap::new)
                    .insert(user_id.clone(), PresenceEntry {
                        user_info: user_info.clone(),
                        owner_node_id: owner_node_id.clone(),
                        joined_at: current_timestamp(),
                    });
                
                debug!("Replicated presence join: {} in channel {} from node {}", 
                       user_id, channel, owner_node_id);
                
                // No response needed for replication
                None
            }
            
            RequestType::PresenceMemberLeft { channel, user_id, .. } => {
                // Update local registry
                let mut registry = self.cluster_presence_registry.write().await;
                if let Some(channel_members) = registry.get_mut(channel) {
                    channel_members.remove(user_id);
                    if channel_members.is_empty() {
                        registry.remove(channel);
                    }
                }
                
                debug!("Replicated presence leave: {} from channel {}", user_id, channel);
                
                // No response needed for replication
                None
            }
            
            RequestType::Heartbeat { node_id, timestamp, .. } => {
                // Update heartbeat tracking
                if node_id != &self.node_id {
                    let mut heartbeats = self.node_heartbeats.write().await;
                    heartbeats.insert(node_id.clone(), *timestamp);
                    
                    debug!("Received heartbeat from node: {} at timestamp: {}", node_id, timestamp);
                }
                
                // No response needed for heartbeats
                None
            }
            
            RequestType::NodeDead { dead_node_id } => {
                // Handle dead node detection
                tokio::spawn({
                    let adapter = Arc::clone(self);
                    let dead_node_id = dead_node_id.clone();
                    async move {
                        adapter.handle_dead_node_detection(dead_node_id).await;
                    }
                });
                
                // No response needed
                None
            }
            
            // ... handle other existing request types ...
        }
    }
}
```

### 5. Connection Manager Integration

#### 5.1 Add Node ID Access

Add method to `ConnectionManager` trait in `src/adapter/connection_manager.rs`:

```rust
#[async_trait]
pub trait ConnectionManager {
    // ... existing methods ...
    
    /// Get the unique node ID for this instance
    fn get_node_id(&self) -> String;
    
    /// Get reference to horizontal adapter if available
    fn as_horizontal_adapter(&self) -> Option<&HorizontalAdapterBase<T>>;
}
```

#### 5.2 Implement for HorizontalAdapterBase

In `src/adapter/horizontal_adapter_base.rs`:

```rust
impl<T: HorizontalTransport> ConnectionManager for HorizontalAdapterBase<T> {
    // ... existing implementations ...
    
    fn get_node_id(&self) -> String {
        // Access the node_id from the horizontal adapter
        // You'll need to add this field to HorizontalAdapterBase
        self.node_id.clone()
    }
    
    fn as_horizontal_adapter(&self) -> Option<&HorizontalAdapterBase<T>> {
        Some(self)
    }
}
```

### 6. Configuration

#### 6.1 Add Configuration Fields

Add to `ServerOptions` in `src/options.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerOptions {
    // ... existing fields ...
    
    pub cluster_health: ClusterHealthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterHealthConfig {
    pub enabled: bool,                    // Enable cluster health monitoring
    pub heartbeat_interval_ms: u64,       // Heartbeat broadcast interval
    pub node_timeout_ms: u64,             // Node considered dead after this timeout
    pub cleanup_interval_ms: u64,         // How often to check for dead nodes
}

impl Default for ClusterHealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,                  // Enable by default for horizontal adapters
            heartbeat_interval_ms: 10000,   // 10 seconds
            node_timeout_ms: 30000,         // 30 seconds
            cleanup_interval_ms: 10000,     // 10 seconds
        }
    }
}
```

#### 6.2 Environment Variable Support

Add to the `override_from_env` method in `ServerOptions`:

```rust
impl ServerOptions {
    pub async fn override_from_env(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // ... existing environment overrides ...
        
        // Cluster health configuration
        self.cluster_health.enabled = parse_env::<bool>(
            "CLUSTER_HEALTH_ENABLED",
            self.cluster_health.enabled,
        );
        self.cluster_health.heartbeat_interval_ms = parse_env::<u64>(
            "CLUSTER_HEALTH_HEARTBEAT_INTERVAL",
            self.cluster_health.heartbeat_interval_ms,
        );
        self.cluster_health.node_timeout_ms = parse_env::<u64>(
            "CLUSTER_HEALTH_NODE_TIMEOUT",
            self.cluster_health.node_timeout_ms,
        );
        self.cluster_health.cleanup_interval_ms = parse_env::<u64>(
            "CLUSTER_HEALTH_CLEANUP_INTERVAL",
            self.cluster_health.cleanup_interval_ms,
        );
        
        Ok(())
    }
}
```

### 7. Startup Integration

#### 7.1 Initialize Cluster Presence System

In `src/main.rs`, after creating the connection manager:

```rust
// After connection manager initialization
if config.cluster_health.enabled {
    // Only enable for horizontal adapters
    if let Some(horizontal_adapter) = connection_manager_guard.as_horizontal_adapter() {
        info!("Starting cluster presence monitoring system");
        
        // Configure the horizontal adapter with cluster health settings
        horizontal_adapter.configure_cluster_health(
            config.cluster_health.heartbeat_interval_ms,
            config.cluster_health.node_timeout_ms,
            config.cluster_health.cleanup_interval_ms,
        ).await?;
        
        // Start the cluster presence system
        horizontal_adapter.start_cluster_presence_system().await?;
        
        info!("Cluster presence monitoring system started");
    } else {
        debug!("Cluster health monitoring disabled for local adapter");
    }
}
```

### 8. Transport Layer Updates

No changes needed to individual transports! The existing `publish_request` and request handling mechanisms in each transport (Redis, RedisCluster, NATS) will automatically handle the new request types.

### 9. Utility Functions

Add utility functions to support the implementation:

```rust
/// Generate unique request ID
fn generate_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Get current timestamp in seconds
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
```

### 10. Error Handling

Add specific error types for cluster presence operations:

```rust
// In src/error/mod.rs
#[derive(thiserror::Error, Debug)]
pub enum Error {
    // ... existing error types ...
    
    #[error("Cluster presence error: {0}")]
    ClusterPresence(String),
    
    #[error("Dead node cleanup error: {0}")]
    DeadNodeCleanup(String),
}
```

### 11. Metrics Integration

Add metrics for monitoring the cluster health system:

```rust
// In src/metrics/mod.rs - add new metric methods
pub trait MetricsInterface {
    // ... existing methods ...
    
    /// Track cluster health metrics
    fn track_cluster_heartbeat_sent(&self, node_id: &str);
    fn track_cluster_heartbeat_received(&self, from_node_id: &str);
    fn track_cluster_dead_node_detected(&self, dead_node_id: &str);
    fn track_cluster_cleanup_performed(&self, dead_node_id: &str, members_cleaned: usize);
    fn track_cluster_presence_replicated(&self, operation: &str, channel: &str);
}
```

### 12. Testing Strategy

#### 12.1 Unit Tests

Create tests in `tests/cluster/` directory:

```rust
// tests/cluster/presence_replication_test.rs
#[tokio::test]
async fn test_presence_replication_across_nodes() {
    // Test that presence join/leave gets replicated to all nodes
}

#[tokio::test]
async fn test_dead_node_detection() {
    // Test that nodes are correctly detected as dead after timeout
}

#[tokio::test]
async fn test_cleanup_leader_election() {
    // Test that only one node performs cleanup
}

#[tokio::test]
async fn test_cleanup_uses_presence_manager() {
    // Test that cleanup properly uses PresenceManager::handle_member_removed
}

// tests/cluster/multi_connection_test.rs
#[tokio::test]
async fn test_user_with_multiple_connections() {
    // Test that user with connections on multiple nodes doesn't get removed
    // when one node dies
}
```

#### 12.2 Integration Tests

```rust
// tests/integration/cluster_presence_test.rs
#[tokio::test]
async fn test_full_dead_node_cleanup_flow() {
    // 1. Start 3 nodes
    // 2. Connect user to presence channel on node 1
    // 3. Kill node 1
    // 4. Verify member_removed event is sent by cleanup leader
    // 5. Verify user is removed from presence channel
}
```

### 13. Documentation Updates

#### 13.1 Environment Variables

Add to documentation:

```bash
# Cluster Health Configuration
CLUSTER_HEALTH_ENABLED=true              # Enable cluster health monitoring (default: true)
CLUSTER_HEALTH_HEARTBEAT_INTERVAL=10000  # Heartbeat interval in ms (default: 10000)
CLUSTER_HEALTH_NODE_TIMEOUT=30000        # Node timeout in ms (default: 30000)
CLUSTER_HEALTH_CLEANUP_INTERVAL=10000    # Cleanup check interval in ms (default: 10000)
```

#### 13.2 Configuration File Example

```json
{
  "cluster_health": {
    "enabled": true,
    "heartbeat_interval_ms": 10000,
    "node_timeout_ms": 30000,
    "cleanup_interval_ms": 10000
  }
}
```

## Implementation Checklist

### Phase 1: Core Infrastructure
- [ ] Add new RequestType variants
- [ ] Extend HorizontalAdapter with presence registry and heartbeat tracking
- [ ] Implement heartbeat broadcasting and dead node detection loops
- [ ] Add configuration options and environment variable support

### Phase 2: Presence Replication
- [ ] Modify subscription success handler to broadcast presence joins
- [ ] Modify unsubscribe handler to broadcast presence leaves
- [ ] Implement request handling for presence replication
- [ ] Add connection manager integration methods

### Phase 3: Dead Node Cleanup
- [ ] Implement leader election algorithm
- [ ] Implement cleanup process using PresenceManager
- [ ] Add dead node entry removal
- [ ] Integrate with startup sequence

### Phase 4: Testing and Validation
- [ ] Write unit tests for all components
- [ ] Write integration tests for multi-node scenarios
- [ ] Test with different transport backends (Redis, NATS)
- [ ] Performance testing with multiple nodes and presence channels

### Phase 5: Monitoring and Documentation
- [ ] Add metrics for cluster health monitoring
- [ ] Update documentation with configuration options
- [ ] Add logging for debugging and monitoring
- [ ] Create operational runbooks for troubleshooting

## Expected Behavior

### Normal Operation
1. User joins presence channel → broadcasted to all nodes → all nodes update their registry
2. User leaves presence channel → broadcasted to all nodes → PresenceManager validates and sends events
3. Heartbeats sent every 10 seconds → all nodes track each other's health
4. No cleanup needed when all nodes are healthy

### Node Failure Scenario
1. Node crashes or becomes unreachable
2. Other nodes detect missing heartbeats after 30 seconds
3. Multiple nodes detect the dead node and broadcast NodeDead message
4. Cleanup leader (lexicographically smallest node_id) is elected
5. Leader iterates through presence registry, finds members owned by dead node
6. For each orphaned member, calls `PresenceManager::handle_member_removed`
7. PresenceManager checks if user has connections on other nodes
8. If no other connections, sends `member_removed` events and webhooks
9. All nodes clean up dead node entries from their registries

### Multi-Connection Handling
- User connected to presence channel on Node A and Node B
- Node A dies
- Cleanup leader finds user was on dead Node A
- Calls `PresenceManager::handle_member_removed` for that user
- PresenceManager queries all nodes and finds user still connected on Node B
- No `member_removed` event sent (user remains visible in presence channel)
- User's presence is preserved correctly

This implementation provides a robust, adapter-agnostic solution for dead node cleanup that integrates seamlessly with existing Sockudo infrastructure while maintaining all existing functionality and behavior.