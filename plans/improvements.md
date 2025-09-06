# Issue #87: State Reconciliation Failure on Node Crash - Solution Options

## Problem Summary

When a Sockudo node crashes ungracefully (kill -9, hardware failure), existing presence channel members never receive `pusher_internal:member_removed` events for users who were connected to the crashed node. While new joiners receive accurate presence member lists (confirmed by testing), existing users retain stale presence state in their client applications indefinitely.

**Root Cause**: The system has no mechanism to detect node failures and notify existing presence channel members about orphaned connections from crashed nodes.

---

## Solution 1: Redis-Stored Presence with Node Heartbeats

### Architecture
Store presence members directly in Redis using sets, combined with node health monitoring.

### Implementation
```rust
// Redis keys:
// "presence:{app}:{channel}:members" -> SET of user_ids
// "presence:{app}:{channel}:user:{user_id}" -> HASH of user info + node_id
// "nodes:{node_id}:heartbeat" -> String with TTL (30s)
```

### Flow
1. **On Subscribe**: 
   - `SADD presence:{app}:{channel}:members {user_id}`
   - `HSET presence:{app}:{channel}:user:{user_id} node_id {node_id} user_info {data}`

2. **Heartbeat System**:
   - Every node sends heartbeat every 10s: `SETEX nodes:{node_id}:heartbeat 30 "alive"`
   - Background task monitors expired heartbeats via Redis keyspace notifications

3. **On Node Death Detection**:
   - Query all presence channels for members owned by dead node
   - Remove from Redis: `SREM`, `DEL` operations
   - Broadcast `member_removed` events to all remaining nodes

4. **On New Subscription**:
   - Query Redis directly: `SMEMBERS` + `HGETALL` for each member
   - Send accurate member list in `subscription_succeeded`

### Pros
- ✅ **Authoritative source of truth** in Redis
- ✅ **Immediate consistency** for new joiners
- ✅ **Automatic cleanup** of dead node members
- ✅ **Scales horizontally** without complex distributed coordination
- ✅ **Simple to implement** and understand

### Cons
- ❌ **Redis dependency** becomes critical (single point of failure)
- ❌ **Network overhead** for every presence operation
- ❌ **Redis memory usage** scales with presence members
- ❌ **Potential race conditions** during network partitions
- ❌ **Heartbeat false positives** during temporary network issues

---

## Solution 2: Event Log with Distributed Cleanup

### Architecture
Maintain presence locally but use Redis event log to track node lifecycle and trigger cleanup.

### Implementation
```rust
// Redis keys:
// "events:{app}:stream" -> Redis Stream for node lifecycle events
// Local storage: unchanged (presence in memory per node)
// "nodes:{node_id}:owned_connections" -> SET of socket_ids
```

### Flow
1. **On Connect**: 
   - Store presence locally (current behavior)
   - `SADD nodes:{node_id}:owned_connections {socket_id}`
   - `XADD events:{app}:stream connection_added {node_id} {user_id} {channel}`

2. **Node Health Monitoring**:
   - Heartbeat system similar to Solution 1
   - On death detection: `XADD events:{app}:stream node_died {node_id} {timestamp}`

3. **Event Processing**:
   - All nodes consume event stream using Redis Streams consumer groups
   - On `node_died` event: each node checks if it has presence members that need cleanup
   - Broadcast `member_removed` for orphaned users

4. **Graceful Degradation**:
   - If event system fails, fall back to current distributed query behavior
   - Eventual consistency through periodic reconciliation

### Pros
- ✅ **Preserves current architecture** (local presence storage)
- ✅ **Event-driven consistency** across nodes
- ✅ **Fault tolerance** with fallback to current behavior
- ✅ **Audit trail** of all presence changes
- ✅ **Lower Redis memory** usage than Solution 1

### Cons
- ❌ **Complex implementation** with event processing
- ❌ **Event ordering issues** during network partitions
- ❌ **Delayed consistency** (eventual, not immediate)
- ❌ **Redis Streams dependency** (newer Redis feature)
- ❌ **More moving parts** to debug and maintain

---

## Solution 3: Hybrid Local + Distributed Reconciliation

### Architecture
Keep current local storage but add periodic reconciliation to detect and fix inconsistencies.

### Implementation
```rust
// Keep current local storage
// Add: "reconcile:{app}:{channel}:last_check" -> timestamp
// Add: "nodes:registry" -> SET of active node_ids with TTL
```

### Flow
1. **Current Behavior**: Unchanged for normal operations

2. **Node Registry**:
   - Each node registers: `SADD nodes:registry {node_id}` with periodic refresh
   - Use Redis TTL or separate heartbeat cleanup

3. **Reconciliation Process** (every 60s):
   - Each node queries: "Who should be alive?" vs "Who do I think is in this channel?"
   - Cross-reference with `nodes:registry` to find dead nodes
   - For dead nodes: remove their members from local presence cache
   - Broadcast `member_removed` events for cleaned up members

4. **On Subscription**:
   - Current distributed query behavior
   - If reconciliation detected recent changes, force fresh query

### Pros
- ✅ **Minimal changes** to current architecture
- ✅ **Self-healing** system that corrects drift over time
- ✅ **Low overhead** during normal operations
- ✅ **No critical Redis dependency** for core functionality
- ✅ **Backwards compatible** with existing deployments

### Cons
- ❌ **Delayed cleanup** (up to 60s for consistency)
- ❌ **Still possible ghost users** in the reconciliation window
- ❌ **Additional complexity** in reconciliation logic
- ❌ **Potential duplicate cleanup** if multiple nodes reconcile simultaneously
- ❌ **Doesn't solve root cause** of missing member_removed events

---

## Recommendation

**Solution 1 (Redis-Stored Presence)** is recommended for the following reasons:

1. **Solves the root cause**: Authoritative presence state prevents ghost users entirely
2. **Immediate consistency**: New joiners always get accurate state
3. **Clean architecture**: Single source of truth is easier to reason about
4. **Battle-tested pattern**: Many real-time systems use this approach
5. **Pusher compatibility**: Matches how actual Pusher likely handles presence

### Risk Mitigation
The Redis dependency concern can be addressed with:
- Redis clustering for high availability
- Graceful degradation to local-only mode if Redis unavailable
- Monitoring and alerting for Redis health

---

## Implementation Considerations

### Configuration Options
```json
{
  "cluster": {
    "heartbeat_interval_ms": 10000,
    "heartbeat_ttl_ms": 30000,
    "presence_storage": "redis", // "local" | "redis" | "hybrid"
    "node_death_detection_enabled": true,
    "reconciliation_interval_ms": 60000
  }
}
```

### Files to Modify/Create
- `src/cluster/mod.rs` - Node health monitoring
- `src/cluster/heartbeat.rs` - Heartbeat publisher/monitor
- `src/presence/redis_storage.rs` - Redis presence operations
- `src/adapter/redis_adapter.rs` - Enhanced Redis operations
- Modify `src/presence/mod.rs` - Abstract storage interface
- Update configuration structs and parsing

### Backward Compatibility
- Feature flags for gradual rollout
- Fallback mechanisms for Redis unavailability
- Migration path from local to Redis storage