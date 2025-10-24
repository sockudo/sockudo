# Horizontal Adapter Architecture

This document explains the horizontal adapter architecture in Sockudo and provides guidance for developers working with or extending the system.

## Overview

The horizontal adapter architecture enables Sockudo to scale across multiple nodes by distributing WebSocket connections and coordinating communication between nodes. It implements a **transport abstraction pattern** that allows different messaging backends (Redis, NATS, Redis Cluster) to be used interchangeably while maintaining the same core functionality.

## Architecture Components

### 1. Core Architecture Pattern

The horizontal adapter uses a **generic base + transport-specific implementation** pattern:

```
HorizontalAdapterBase<T: HorizontalTransport>
├── HorizontalAdapter (core business logic)
├── LocalAdapter (local connection management)  
└── T: HorizontalTransport (messaging backend)
```

### 2. Key Components

#### `HorizontalTransport` Trait (`src/adapter/horizontal_transport.rs`)
The transport abstraction that defines how messages are sent/received across nodes:

```rust
#[async_trait]
pub trait HorizontalTransport: Send + Sync + Clone {
    type Config: Send + Sync;
    
    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()>;
    async fn publish_request(&self, request: &RequestBody) -> Result<()>;
    async fn publish_response(&self, response: &ResponseBody) -> Result<()>;
    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()>;
    async fn get_node_count(&self) -> Result<usize>;
    async fn check_health(&self) -> Result<()>;
}
```

#### `HorizontalAdapterBase<T>` (`src/adapter/horizontal_adapter_base.rs`)
The generic base implementation that:
- Manages the request/response lifecycle with timeout handling
- Coordinates between local and distributed operations
- Provides the `ConnectionManager` interface implementation
- Handles message routing and event coordination

#### Transport Implementations (`src/adapter/transports/`)
- **`RedisTransport`**: Single Redis instance with pub/sub
- **`RedisClusterTransport`**: Redis Cluster with RESP3 protocol support
- **`NatsTransport`**: NATS messaging system with subject-based routing

#### Adapter Type Aliases (`src/adapter/{redis,nats,redis_cluster}.rs`)
```rust
pub type RedisAdapter = HorizontalAdapterBase<RedisTransport>;
pub type NatsAdapter = HorizontalAdapterBase<NatsTransport>;  
pub type RedisClusterAdapter = HorizontalAdapterBase<RedisClusterTransport>;
```

## Message Flow

### 1. Broadcast Messages (Channel Events)
When a message needs to be sent to a channel across all nodes:

```
Client → WebSocket Handler → send() → HorizontalAdapterBase
                                    ├── Local delivery (immediate)
                                    └── Remote broadcast via Transport
                                                     ↓
Other Nodes ← Transport Listener ← Network ← publish_broadcast()
```

### 2. Request/Response Messages (Channel Queries)
When information is needed from all nodes (e.g., channel member count):

```
API Call → send_request() → HorizontalAdapterBase
                          ├── Generate unique request_id
                          ├── Store in pending_requests  
                          └── publish_request() via Transport
                                       ↓
Other Nodes → Transport Listener → process_request()
                                 └── publish_response() via Transport
                                              ↓
Origin Node → Response Listener → aggregate_responses()
                                └── Resolve waiting request
```

### 3. Event-Driven Response Handling
Responses use Rust's `tokio::sync::Notify` for efficient waiting:

```rust
// Wait for responses with timeout
let result = tokio::select! {
    _ = notify.notified() => {
        // Check if we have enough responses
    }
    _ = tokio::time::sleep(timeout_duration) => {
        // Handle timeout
    }
};
```

## Key Design Patterns

### 1. Generic Transport Abstraction
The `HorizontalTransport` trait provides a clean abstraction over different messaging systems:
- **Redis**: Connection pooling, retry logic, health checks
- **NATS**: Subject-based messaging, cluster discovery
- **Redis Cluster**: Cluster-aware connections, RESP3 protocol

### 2. Event-Driven Architecture
- **Broadcast events**: Fire-and-forget message distribution
- **Request/response**: Coordinated queries with timeout handling
- **Async listeners**: Non-blocking message processing

### 3. Fault Tolerance
- **Retry mechanisms**: Exponential backoff for connection failures
- **Timeout handling**: Configurable request timeouts with graceful degradation  
- **Health monitoring**: Transport-level health checks
- **Graceful failures**: Local-only operation when remote nodes are unavailable

### 4. Metrics Integration
The architecture includes comprehensive metrics tracking:
- Request/response latency
- Success/failure rates
- Message broadcast performance
- Node-level statistics

## Configuration

### Transport Configuration
Each transport implements `TransportConfig`:

```rust
pub trait TransportConfig: Send + Sync + Clone {
    fn request_timeout_ms(&self) -> u64;
    fn prefix(&self) -> &str;
}
```

### Database Pooling

Sockudo’s AppManager for SQL databases (MySQL/PostgreSQL) uses pooled connections.

- Global defaults (apply to all SQL DBs unless overridden):

```
DATABASE_POOLING_ENABLED=true
DATABASE_POOL_MIN=2
DATABASE_POOL_MAX=10
```

- Per‑database overrides:

```
# MySQL
DATABASE_MYSQL_POOL_MIN=4
DATABASE_MYSQL_POOL_MAX=32

# PostgreSQL
DATABASE_POSTGRES_POOL_MIN=2
DATABASE_POSTGRES_POOL_MAX=16
```

If pooling is disabled (`DATABASE_POOLING_ENABLED=false`), the legacy
`DATABASE_CONNECTION_POOL_SIZE` is used as the maximum connections.

Note: DynamoDB uses the AWS SDK client which manages its own connection
behavior; SQL pooling settings do not apply to DynamoDB.

### Example Configurations

#### Redis Transport
```json
{
  "adapter": {
    "driver": "redis",
    "redis_host": "localhost",
    "redis_port": 6379,
    "request_timeout_ms": 5000,
    "prefix": "sockudo"
  }
}
```

#### NATS Transport  
```json
{
  "adapter": {
    "driver": "nats", 
    "nats_servers": ["nats://localhost:4222"],
    "request_timeout_ms": 5000,
    "prefix": "sockudo"
  }
}
```

## Performance Characteristics

### Strengths
- **Efficient local operations**: Local connections bypass network entirely
- **Parallel processing**: Requests to multiple nodes handled concurrently
- **Event-driven**: No polling, uses notification system for responses
- **Connection pooling**: Reused connections for better performance

### Considerations  
- **Network latency**: Remote operations add network round-trip time
- **Node discovery**: Some transports require manual node count configuration
- **Memory usage**: Pending requests stored in memory (with cleanup)

---

# How to Add a New Transport

This section provides a step-by-step guide for adding a new messaging backend transport to the horizontal adapter system.

## Prerequisites

- Understanding of the messaging system you want to integrate (e.g., Apache Kafka, RabbitMQ, etc.)
- Knowledge of Rust async programming and the `async-trait` crate
- Familiarity with the Sockudo codebase structure

## Step-by-Step Implementation

### 1. Create the Transport Implementation

Create a new file `src/adapter/transports/your_transport.rs`:

```rust
use crate::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::adapter::horizontal_transport::{
    HorizontalTransport, TransportConfig, TransportHandlers,
};
use crate::error::{Error, Result};
use crate::options::YourTransportConfig; // Create this config struct
use async_trait::async_trait;
use tracing::{debug, error};

// Implement TransportConfig for your configuration
impl TransportConfig for YourTransportConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Your transport implementation
#[derive(Clone)]
pub struct YourTransport {
    // Your transport-specific fields (client, connection, etc.)
    client: YourClient,
    config: YourTransportConfig,
}

#[async_trait]
impl HorizontalTransport for YourTransport {
    type Config = YourTransportConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        // Initialize your transport client/connection
        let client = YourClient::new(&config)
            .await
            .map_err(|e| Error::Other(format!("Failed to create client: {e}")))?;

        Ok(Self { client, config })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let message_json = serde_json::to_string(message)
            .map_err(|e| Error::Other(format!("Failed to serialize broadcast: {e}")))?;
        
        let topic = format!("{}:broadcast", self.config.prefix);
        
        self.client
            .publish(&topic, message_json)
            .await
            .map_err(|e| Error::Other(format!("Failed to publish broadcast: {e}")))?;

        Ok(())
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        let request_json = serde_json::to_string(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;
        
        let topic = format!("{}:requests", self.config.prefix);
        
        self.client
            .publish(&topic, request_json)
            .await
            .map_err(|e| Error::Other(format!("Failed to publish request: {e}")))?;

        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        let response_json = serde_json::to_string(response)
            .map_err(|e| Error::Other(format!("Failed to serialize response: {e}")))?;
        
        let topic = format!("{}:responses", self.config.prefix);
        
        self.client
            .publish(&topic, response_json)
            .await
            .map_err(|e| Error::Other(format!("Failed to publish response: {e}")))?;

        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        // Subscribe to topics
        let broadcast_topic = format!("{}:broadcast", self.config.prefix);
        let request_topic = format!("{}:requests", self.config.prefix);
        let response_topic = format!("{}:responses", self.config.prefix);

        let mut subscriber = self.client.subscribe(&[
            &broadcast_topic,
            &request_topic, 
            &response_topic
        ]).await.map_err(|e| Error::Other(format!("Failed to subscribe: {e}")))?;

        // Start message processing loop
        tokio::spawn(async move {
            while let Ok(message) = subscriber.next_message().await {
                let topic = message.topic();
                let payload = message.payload_str();

                // Route messages to appropriate handlers
                match topic {
                    t if t == broadcast_topic => {
                        if let Ok(broadcast) = serde_json::from_str::<BroadcastMessage>(payload) {
                            tokio::spawn({
                                let handler = handlers.on_broadcast.clone();
                                async move { handler(broadcast).await }
                            });
                        }
                    }
                    t if t == request_topic => {
                        if let Ok(request) = serde_json::from_str::<RequestBody>(payload) {
                            let response_handler = handlers.on_request.clone();
                            let client_clone = self.client.clone();
                            let response_topic_clone = response_topic.clone();
                            
                            tokio::spawn(async move {
                                let response_result = response_handler(request).await;
                                
                                if let Ok(response) = response_result {
                                    if let Ok(response_json) = serde_json::to_string(&response) {
                                        let _ = client_clone
                                            .publish(&response_topic_clone, response_json)
                                            .await;
                                    }
                                }
                            });
                        }
                    }
                    t if t == response_topic => {
                        if let Ok(response) = serde_json::from_str::<ResponseBody>(payload) {
                            let handler = handlers.on_response.clone();
                            tokio::spawn(async move { handler(response).await });
                        }
                    }
                    _ => {}
                }
            }
        });

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        // Implement node discovery for your transport
        // This could be:
        // - Query your messaging system for active subscribers
        // - Use a service discovery mechanism
        // - Fall back to configuration-based count
        
        // Example implementation:
        if let Some(node_count) = self.config.node_count {
            Ok(node_count as usize)
        } else {
            // Try to discover active nodes
            match self.client.get_subscriber_count(&format!("{}:requests", self.config.prefix)).await {
                Ok(count) => Ok(count.max(1)),
                Err(_) => Ok(1), // Fallback to single node
            }
        }
    }

    async fn check_health(&self) -> Result<()> {
        self.client
            .ping()
            .await
            .map_err(|e| Error::Other(format!("Health check failed: {e}")))?;
        Ok(())
    }
}
```

### 2. Create Configuration Structure

Add your configuration to `src/options/mod.rs`:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct YourTransportConfig {
    pub servers: Vec<String>,
    pub request_timeout_ms: u64,
    pub prefix: String,
    pub node_count: Option<u32>, // Optional manual node count
    // Add other transport-specific config fields
}

impl Default for YourTransportConfig {
    fn default() -> Self {
        Self {
            servers: vec!["your://localhost:9999".to_string()],
            request_timeout_ms: 5000,
            prefix: "sockudo".to_string(),
            node_count: None,
        }
    }
}
```

### 3. Create Adapter Type Alias

Create `src/adapter/your_transport.rs`:

```rust
use crate::adapter::horizontal_adapter_base::HorizontalAdapterBase;
use crate::adapter::transports::your_transport::YourTransport;

/// Your transport adapter (type alias for easier use)
pub type YourTransportAdapter = HorizontalAdapterBase<YourTransport>;

// Re-export config for backward compatibility
pub use crate::options::YourTransportConfig;
```

### 4. Register Transport in Module System

Update `src/adapter/transports/mod.rs`:

```rust
pub mod redis_transport;
pub mod redis_cluster_transport; 
pub mod nats_transport;
pub mod your_transport; // Add your transport

pub use redis_transport::RedisTransport;
pub use redis_cluster_transport::RedisClusterTransport;
pub use nats_transport::NatsTransport;
pub use your_transport::YourTransport; // Export your transport
```

Update `src/adapter/mod.rs`:

```rust
pub mod your_transport; // Add your adapter module
```

### 5. Update Configuration Loading

In your configuration loading logic (typically in `src/config.rs` or similar), add support for your transport:

```rust
match adapter_config.driver.as_str() {
    "redis" => { /* existing redis code */ }
    "nats" => { /* existing nats code */ }
    "your_transport" => {
        let config = YourTransportConfig {
            servers: /* parse from config */,
            request_timeout_ms: adapter_config.request_timeout_ms,
            prefix: adapter_config.prefix.clone(),
            // ... other fields
        };
        
        let mut adapter = YourTransportAdapter::new(config).await?;
        // Initialize and return adapter
    }
    _ => return Err(Error::Other("Unsupported adapter driver".to_string())),
}
```

### 6. Add Environment Variable Support

Update your environment variable parsing to support the new transport:

```bash
# Example environment variables
YOUR_TRANSPORT_SERVERS=your://server1:9999,your://server2:9999
YOUR_TRANSPORT_REQUEST_TIMEOUT_MS=5000
YOUR_TRANSPORT_NODE_COUNT=3
```

### 7. Update Documentation

- Add your transport to the main README.md
- Update CLAUDE.md with configuration examples
- Add any transport-specific documentation

### 8. Add Tests

Create comprehensive tests in `tests/` directory:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_your_transport_basic_operations() {
        // Test transport creation, message publishing, etc.
    }
    
    #[tokio::test]
    async fn test_your_transport_request_response() {
        // Test request/response cycle
    }
    
    #[tokio::test]
    async fn test_your_transport_health_check() {
        // Test health check functionality
    }
}
```

## Implementation Tips

### Error Handling
- Use consistent error types (`Error::Other`, `Error::Redis`, etc.)
- Provide meaningful error messages with context
- Implement proper retry logic where appropriate

### Performance Considerations  
- Use connection pooling when possible
- Implement non-blocking message processing
- Consider message batching for high-throughput scenarios

### Monitoring and Logging
- Add appropriate `tracing` calls for debugging
- Log connection state changes
- Include metrics for message throughput and latency

### Configuration Validation
- Validate configuration at startup
- Provide sensible defaults
- Document all configuration options

## Testing Your Implementation

1. **Unit Tests**: Test individual transport methods
2. **Integration Tests**: Test with actual messaging backend
3. **Multi-node Tests**: Verify distributed functionality  
4. **Performance Tests**: Measure throughput and latency
5. **Failure Scenarios**: Test network failures, timeouts, etc.

## Example Transport Implementations

Refer to existing implementations for patterns:

- **`RedisTransport`**: Good example of connection pooling and retry logic
- **`NatsTransport`**: Shows subject-based routing and cluster support
- **`RedisClusterTransport`**: Demonstrates cluster-aware messaging and RESP3 protocol

This architecture allows you to add support for any messaging system while maintaining consistent behavior and API compatibility across all transport implementations.
