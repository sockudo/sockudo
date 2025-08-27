# Lock-Free Broadcasting Architecture Plan

## Current Architecture Analysis

### Message Flow (Current)
1. **Broadcast Initiated**: `LocalAdapter::send()` serializes message once
2. **Target Collection**: `Namespace::get_channel_socket_refs_except()` collects WebSocketRef list
3. **Parallel Send**: `send_streaming_messages()` uses `buffer_unordered(128)` for concurrent sends
4. **Lock Acquisition**: Each send calls `WebSocketRef::send_raw_message()` which:
   - Acquires `Mutex<WebSocket>` lock (LINE 535: `let ws = self.0.lock().await`)
   - Calls `WebSocket::send_raw_bytes()`
   - Sends to `MessageSender::sender` channel (unbounded)
5. **Async Write**: MessageSender's background task writes frames to socket

### Locking Points Identified
```rust
// PRIMARY BOTTLENECK - WebSocketRef::send_raw_message()
pub async fn send_raw_message(&self, bytes: Bytes) -> Result<()> {
    let ws = self.0.lock().await;  // <-- LOCK FOR EVERY MESSAGE
    ws.send_raw_bytes(bytes)
}
```

For 20,000 connections:
- 20,000 lock acquisitions per broadcast
- Up to 128 concurrent lock attempts (buffer_unordered)
- Lock contention increases exponentially

### Current Structure
```rust
pub struct WebSocketRef(pub Arc<Mutex<WebSocket>>);

pub struct WebSocket {
    pub state: ConnectionState,      // Mutable state requiring protection
    pub message_sender: MessageSender, // Already has internal channel
}

pub struct MessageSender {
    sender: mpsc::UnboundedSender<Frame<'static>>,  // Already lock-free!
    _receiver_handle: JoinHandle<()>,
}
```

## Lock-Free Design

### Core Insight
MessageSender already uses an unbounded channel internally. We can expose this channel directly for broadcasts, bypassing the Mutex entirely.

### Proposed Architecture

#### 1. Split WebSocket State
```rust
pub struct WebSocket {
    // Immutable after creation (no lock needed)
    pub socket_id: SocketId,
    pub app_config: Arc<App>,
    pub broadcast_tx: mpsc::UnboundedSender<Bytes>,  // Direct channel for broadcasts
    
    // Mutable state (still needs protection for non-broadcast ops)
    pub state: Arc<Mutex<ConnectionState>>,
    pub message_sender: MessageSender,
}

pub struct WebSocketRef {
    // Immutable references (lock-free access)
    pub socket_id: SocketId,
    pub broadcast_tx: mpsc::UnboundedSender<Bytes>,
    
    // Full reference for operations needing state
    inner: Arc<Mutex<WebSocket>>,
}
```

#### 2. Broadcast-Specific Channel
```rust
impl WebSocket {
    pub fn new(socket_id: SocketId, socket: WebSocketWrite<...>) -> (Self, WebSocketRef) {
        // Create dedicated broadcast channel
        let (broadcast_tx, mut broadcast_rx) = mpsc::unbounded_channel::<Bytes>();
        
        // Clone for WebSocketRef
        let broadcast_tx_clone = broadcast_tx.clone();
        
        // Spawn broadcast handler
        let message_sender = MessageSender::new_with_broadcast(
            socket,
            broadcast_rx
        );
        
        let websocket = WebSocket {
            socket_id: socket_id.clone(),
            broadcast_tx: broadcast_tx.clone(),
            state: Arc::new(Mutex::new(ConnectionState::new())),
            message_sender,
        };
        
        let websocket_ref = WebSocketRef {
            socket_id,
            broadcast_tx: broadcast_tx_clone,
            inner: Arc::new(Mutex::new(websocket)),
        };
        
        (websocket, websocket_ref)
    }
}
```

#### 3. Lock-Free Broadcast Path
```rust
impl WebSocketRef {
    // Lock-free for broadcasts
    pub async fn send_broadcast(&self, bytes: Bytes) -> Result<()> {
        self.broadcast_tx
            .send(bytes)
            .map_err(|_| Error::ConnectionClosed)
    }
    
    // Still uses lock for state-dependent operations
    pub async fn send_message(&self, message: &PusherMessage) -> Result<()> {
        let ws = self.inner.lock().await;
        ws.send_message(message)
    }
}
```

#### 4. Modified MessageSender
```rust
impl MessageSender {
    pub fn new_with_broadcast(
        mut socket: WebSocketWrite<...>,
        mut broadcast_rx: mpsc::UnboundedReceiver<Bytes>
    ) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<Frame<'static>>();
        
        let receiver_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Priority for broadcasts
                    Some(bytes) = broadcast_rx.recv() => {
                        let frame = Frame::text(Payload::from(bytes.to_vec()));
                        if let Err(e) = socket.write_frame(frame).await {
                            break;
                        }
                    }
                    // Regular messages
                    Some(frame) = receiver.recv() => {
                        if let Err(e) = socket.write_frame(frame).await {
                            break;
                        }
                    }
                    else => break,
                }
            }
        });
        
        Self { sender, _receiver_handle: receiver_handle }
    }
}
```

#### 5. Updated Broadcast Implementation
```rust
impl LocalAdapter {
    async fn send_streaming_messages(
        &self,
        target_socket_refs: Vec<WebSocketRef>,
        message_bytes: Bytes,
    ) -> Vec<Result<()>> {
        // Now completely lock-free!
        stream::iter(target_socket_refs.into_iter().map(|socket_ref| {
            let bytes = message_bytes.clone();
            async move { 
                socket_ref.send_broadcast(bytes).await  // No lock!
            }
        }))
        .buffer_unordered(1024)  // Can use much higher concurrency
        .collect()
        .await
    }
}
```

## Implementation Steps

### Phase 1: Refactor WebSocket Structure
1. Extract immutable fields from WebSocket
2. Create broadcast_tx channel in WebSocket::new()
3. Store broadcast_tx in both WebSocket and WebSocketRef
4. Keep existing Arc<Mutex<WebSocket>> for backward compatibility

### Phase 2: Implement Broadcast Channel
1. Add broadcast_rx to MessageSender
2. Modify MessageSender task to handle both channels
3. Implement WebSocketRef::send_broadcast() method
4. Test with small number of connections

### Phase 3: Update LocalAdapter
1. Modify send_streaming_messages to use send_broadcast()
2. Increase buffer_unordered limit (1024+)
3. Keep send_message() for non-broadcast messages

### Phase 4: Optimize Channel Selection
1. Use tokio::select! with biased priority for broadcasts
2. Consider bounded channels with backpressure
3. Add metrics for channel depth monitoring

### Phase 5: State Management Refactor
1. Move read-only state outside mutex
2. Use RwLock for read-heavy state
3. Consider atomic operations for simple state updates

## Performance Expectations

### Current Performance (with Bytes optimization)
- 20k connections: ~67-73ms latency
- Lock acquisitions: 20,000 per broadcast
- Max concurrent locks: 128

### Expected Lock-Free Performance
- 20k connections: ~10-20ms latency
- Lock acquisitions: 0 for broadcasts
- Max concurrent sends: 1024+ (no lock contention)
- Memory: Similar or slightly higher (extra channel per connection)

## Risk Mitigation

### Memory Considerations
- Each connection gets an extra channel (~100 bytes)
- 20k connections = ~2MB additional memory
- Acceptable tradeoff for performance gain

### Channel Overflow
- Unbounded channels can cause memory issues
- Solution: Monitor channel depth, add backpressure
- Fallback: Use bounded channel with try_send()

### Backward Compatibility
- Keep existing WebSocketRef structure
- Add new methods alongside existing ones
- Gradual migration path

## Testing Strategy

### Unit Tests
1. Test broadcast channel creation
2. Test message delivery via broadcast channel
3. Test channel cleanup on disconnect
4. Test backpressure handling

### Integration Tests
1. Test 1k connections with rapid broadcasts
2. Test 10k connections with sustained load
3. Test 20k+ connections for scalability
4. Test mixed broadcast/unicast messages

### Benchmark Tests
1. Baseline: Current implementation metrics
2. Measure: Latency reduction
3. Measure: CPU usage reduction
4. Measure: Memory usage increase
5. Measure: Throughput improvement

## Success Metrics

- [ ] 80%+ reduction in broadcast latency at 20k connections
- [ ] 50%+ reduction in CPU usage during broadcasts
- [ ] <5% increase in baseline memory usage
- [ ] Zero message loss under normal load
- [ ] Graceful degradation under extreme load

## Alternative Approaches Considered

### 1. Sharded Connections
- Divide connections into N shards
- Each shard has its own lock
- Rejected: Still has lock contention within shards

### 2. Read-Write Lock
- Use RwLock instead of Mutex
- Multiple readers for broadcasts
- Rejected: Still requires lock acquisition

### 3. Actor Model
- Each connection as an actor
- Message passing only
- Rejected: Too much refactoring required

### 4. Direct Socket Access
- Store raw socket in WebSocketRef
- No intermediate structure
- Rejected: Loses state management benefits

## Conclusion

The lock-free broadcast design leverages the existing MessageSender channel infrastructure while eliminating the Mutex bottleneck for broadcasts. This surgical change maintains backward compatibility while providing massive performance improvements for the critical broadcast path.

The implementation is straightforward because MessageSender already does the heavy lifting - we're just exposing a direct path to it for broadcasts, bypassing the unnecessary lock acquisition that's killing performance at scale.