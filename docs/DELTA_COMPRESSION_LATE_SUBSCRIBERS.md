# Delta Compression: Late Subscriber Handling

## Overview

When a client subscribes to a channel **after** delta compression has already started, Sockudo ensures the client receives complete data by automatically sending a **full message first**, then switching to deltas.

## How It Works

### Per-Socket State Tracking

Delta compression maintains **per-socket, per-channel state**. Each socket tracks:
- Base messages for each channel
- Sequence numbers
- Delta count
- Conflation key states

When a new socket subscribes, it has **no state** for that channel yet.

### First Message = Full Message

```
Timeline:

Existing Client (Socket A):
  Connected at T0
  ├─ Message 1: FULL (first message)
  ├─ Message 2: DELTA (based on Message 1)
  ├─ Message 3: DELTA (based on Message 2)
  └─ Message 4: DELTA (based on Message 3)

New Client (Socket B):
  Connects at T3 (late subscriber)
  ├─ Message 1: FULL ← Automatically sent as full!
  ├─ Message 2: DELTA (based on Message 1)
  └─ Message 3: DELTA (based on Message 2)
```

### Implementation Details

**File**: `src/delta_compression.rs`

```rust
// When compressing a message for a socket
match conflation_cache {
    None => {
        // First message for this cache key - create empty cache
        let cache = ConflationKeyCache::new(max_messages_per_key);
        channel_state.set_conflation_state(cache_key.clone(), cache);
        Ok(CompressionResult::FullMessage {
            sequence: 0,
            conflation_key: conflation_key_opt,
        })
    }
    Some(cache) => {
        // Has base message, can compute delta
        // ...
    }
}
```

**Key Points**:
1. When `conflation_cache` is `None` → Socket has no base message
2. Automatically returns `FullMessage` result
3. Creates cache for future deltas
4. Subsequent messages will be deltas

## Example Scenarios

### Scenario 1: Simple Late Subscriber

**Setup**:
- Channel: `ticker:AAPL`
- Conflation key: `symbol`
- 3 existing clients, 1 new client

**Timeline**:

```
T0: Broadcast Message 1 (price: $150.00)
  → Client A: FULL (first message)
  → Client B: FULL (first message)
  → Client C: FULL (first message)

T1: Broadcast Message 2 (price: $150.25)
  → Client A: DELTA (25 bytes vs 100 bytes)
  → Client B: DELTA (25 bytes vs 100 bytes)
  → Client C: DELTA (25 bytes vs 100 bytes)

T2: Client D subscribes ← Late subscriber!

T3: Broadcast Message 3 (price: $150.50)
  → Client A: DELTA (25 bytes)
  → Client B: DELTA (25 bytes)
  → Client C: DELTA (25 bytes)
  → Client D: FULL (100 bytes) ← Automatic full message!

T4: Broadcast Message 4 (price: $150.75)
  → Client A: DELTA (25 bytes)
  → Client B: DELTA (25 bytes)
  → Client C: DELTA (25 bytes)
  → Client D: DELTA (25 bytes) ← Now receiving deltas!
```

**Result**: Client D automatically catches up without any special handling needed.

---

### Scenario 2: Late Subscriber with Conflation

**Setup**:
- Channel: `prices:*`
- Conflation key: `symbol`
- Multiple symbols on same channel

**Timeline**:

```
T0: Client A subscribes to "prices"

T1: Broadcast AAPL price update
  → Client A: FULL (AAPL, first for this symbol)

T2: Broadcast GOOG price update
  → Client A: FULL (GOOG, first for this symbol)

T3: Broadcast AAPL price update
  → Client A: DELTA (AAPL, has base)

T4: Client B subscribes to "prices" ← Late subscriber!

T5: Broadcast AAPL price update
  → Client A: DELTA (AAPL, has base)
  → Client B: FULL (AAPL, first for this socket) ← Auto full!

T6: Broadcast GOOG price update
  → Client A: DELTA (GOOG, has base)
  → Client B: FULL (GOOG, first for this socket) ← Auto full!

T7: Broadcast AAPL price update
  → Client A: DELTA (AAPL, has base)
  → Client B: DELTA (AAPL, has base now) ← Now has base!
```

**Result**: Client B receives full messages for each symbol it hasn't seen yet, then switches to deltas.

---

### Scenario 3: Client Reconnects (Lost Connection)

**Setup**:
- Client loses connection and reconnects
- Treated as a new socket

**Timeline**:

```
T0: Client A connects (socket_id: abc123)
  Subscribes to "ticker:AAPL"

T1-T5: Receives messages (FULL, then DELTAs)

T6: Client A disconnects (network issue)
  → Server clears state for socket_id: abc123

T7: Client A reconnects (new socket_id: def456)
  Subscribes to "ticker:AAPL"
  → Treated as NEW socket
  → Next message will be FULL

T8: Broadcast AAPL update
  → Client A (def456): FULL ← Fresh start!
```

**Result**: Reconnected client gets a full message to resynchronize.

---

## Multi-Node Behavior

### Node-Local Intervals (Default)

Each node tracks its own state independently.

```
3-Node Cluster:
┌─────────┐  ┌─────────┐  ┌─────────┐
│ Node 1  │  │ Node 2  │  │ Node 3  │
└─────────┘  └─────────┘  └─────────┘
     ↓            ↓            ↓
Client A     Client B     Client C
(connected   (connected   (late sub
 at T0)       at T0)       at T5)

T1: Broadcast
→ Node 1 → Client A: FULL (first)
→ Node 2 → Client B: FULL (first)

T2-T5: Broadcasts
→ All clients receive DELTAs

T6: Client C subscribes to Node 3
    Next broadcast:
→ Node 1 → Client A: DELTA
→ Node 2 → Client B: DELTA
→ Node 3 → Client C: FULL ← Auto full on Node 3!
```

**Key Point**: Each node handles late subscribers independently. No coordination needed.

---

### Cluster Coordination (Optional)

With cluster coordination enabled, the behavior is the same - late subscribers still get full messages first.

```
T0: Counter = 0 (globally)
    Client A subscribes to Node 1

T1: Counter = 1 (globally)
    Broadcast → Client A: FULL (first for socket)

T2-T9: Counter increments to 10
    Broadcasts → Client A: DELTAs

T10: Counter = 10 → Reset to 0 (full message interval)
     Broadcast → Client A: FULL (interval reset)
     Client B subscribes to Node 2 ← Late subscriber!

T11: Counter = 1 (globally)
     Broadcast:
     → Client A: DELTA (has base from T10)
     → Client B: FULL (first for socket) ← Per-socket logic!
```

**Key Point**: Cluster coordination affects **when** full messages are sent cluster-wide, but late subscribers **always** get full messages first, regardless of cluster state.

---

## Edge Cases

### Edge Case 1: Subscribe During Full Message Interval

**Question**: What if a client subscribes right when the cluster is sending a full message?

**Answer**: The client still gets a full message, and it's not "wasted" - the client needs it anyway since it has no base.

```
T0-T9: Existing clients receive deltas
T10: Full message interval reached
     → All existing clients get FULL
     → New client subscribes
T11: Next broadcast
     → Existing clients: DELTA (have new base from T10)
     → New client: FULL (first message, happens to be same timing)
```

---

### Edge Case 2: Very Rapid Subscription

**Question**: What if many clients subscribe rapidly?

**Answer**: Each socket independently receives its first full message. No performance issue.

```
T0: 1000 existing clients (all receiving deltas)
T1: 100 new clients subscribe simultaneously
    Next broadcast:
    → 1000 existing: DELTA (30 bytes each = 30KB total)
    → 100 new: FULL (100 bytes each = 10KB total)
    → Total: 40KB (vs 110KB if all full messages)
```

**Bandwidth**: Still saves 63% compared to sending full messages to everyone.

---

### Edge Case 3: Subscribe to Empty Channel

**Question**: What if the first subscriber joins an empty channel?

**Answer**: First broadcast will be a full message (as expected).

```
T0: Channel "ticker:TSLA" has no subscribers
T1: Client A subscribes to "ticker:TSLA"
T2: First broadcast to "ticker:TSLA"
    → Client A: FULL (first message on channel)
T3: Second broadcast
    → Client A: DELTA (has base now)
```

---

## No Manual Synchronization Needed

### Client-Side: Zero Changes

Clients using the Pusher protocol don't need to know about late subscription handling:

```javascript
// Client code - same whether early or late subscriber
const pusher = new Pusher('app-key', {
  cluster: 'us2'
});

// Enable delta compression (optional)
pusher.connection.bind('connected', () => {
  pusher.connection.send_event('pusher:enable_delta_compression', {});
});

// Subscribe to channel - works the same regardless of timing
const channel = pusher.subscribe('ticker:AAPL');

channel.bind('price-update', (data) => {
  // Receives complete data whether it's a full message or delta
  console.log('Price:', data.price);
});
```

**The client library handles delta application transparently.**

---

### Server-Side: Automatic

No special code needed in broadcast logic:

```bash
# Broadcast to channel - works for all clients regardless of subscription timing
curl -X POST http://localhost:6001/apps/my-app/events \
  -H "Content-Type: application/json" \
  -d '{
    "name": "price-update",
    "channel": "ticker:AAPL",
    "data": "{\"symbol\":\"AAPL\",\"price\":150.50}"
  }'
```

**The server automatically determines whether to send full or delta per socket.**

---

## Performance Impact

### Bandwidth Comparison

**Scenario**: 1000 subscribers on a channel, 10 new subscribers join

| Approach | Total Bandwidth |
|----------|----------------|
| No compression | 1010 × 100 bytes = 101KB |
| Delta compression (all subscribers) | 1010 × 25 bytes = 25.25KB |
| **Delta with late subs** | **(1000 × 25) + (10 × 100) = 26KB** |

**Result**: Late subscribers add minimal overhead (0.75KB extra = 2.9% increase)

---

### CPU Impact

**Per-broadcast CPU cost**:
- Existing clients: ~5μs per delta computation
- Late subscribers: ~0μs (full message, no computation)

**Result**: Late subscribers actually reduce CPU load slightly (no delta computation needed).

---

## Best Practices

### 1. Don't Pre-send Full Messages

❌ **Don't** try to pre-send a full message on subscription:
```javascript
// BAD: Unnecessary
channel.bind('pusher:subscription_succeeded', () => {
  // Don't fetch and send initial state here
});
```

✅ **Do** trust the automatic handling:
```javascript
// GOOD: Just subscribe and receive updates
const channel = pusher.subscribe('ticker:AAPL');
channel.bind('price-update', handleUpdate);
```

---

### 2. Use Conflation Keys

With conflation keys, late subscribers get separate full messages per key:

```json
{
  "channel_delta_compression": {
    "ticker:*": {
      "enabled": true,
      "conflation_key": "symbol"
    }
  }
}
```

**Benefit**: Client receives full message only for symbols it hasn't seen yet.

---

### 3. Monitor New Subscriber Rate

Track new subscription rate to understand impact:

```bash
# Check Prometheus metrics
curl http://localhost:9601/metrics | grep channel_subscriptions_total
```

**Normal**: 5-10% of connections are late subscribers
**High churn**: >50% late subscribers → consider client-side caching

---

## Troubleshooting

### Client Not Receiving Complete Data

**Symptom**: Late subscriber receives incomplete data

**Possible Causes**:
1. Client hasn't enabled delta compression
2. Message too small (below `min_message_size`)
3. Channel not configured for delta compression

**Debug**:
```bash
# Check if delta is enabled for socket
RUST_LOG=sockudo::delta_compression=debug ./sockudo

# Look for logs like:
# "Socket XYZ has delta compression enabled for channel ABC"
# "First message for socket XYZ on channel ABC - sending FULL"
```

---

### High Bandwidth Despite Few Clients

**Symptom**: Bandwidth usage higher than expected

**Possible Cause**: High subscriber churn (many late subscribers)

**Solution**:
```bash
# Check subscription rate
curl http://localhost:9601/metrics | grep subscriptions_total

# If high churn, consider:
# 1. Client-side caching
# 2. Increase full_message_interval
# 3. Use connection pooling
```

---

## Summary

**Late subscriber handling is automatic and transparent:**

✅ **First message to any socket is always full** - No special handling needed  
✅ **Per-socket state tracking** - Each client independent  
✅ **Works with all adapters** - Local, Redis, NATS  
✅ **Works with cluster coordination** - Per-socket logic preserved  
✅ **Minimal overhead** - <3% bandwidth impact for typical churn rates  
✅ **No client changes needed** - Transparent to application code  

**You don't need to worry about late subscribers - Sockudo handles it automatically!**

---

## Related Documentation

- [DELTA_COMPRESSION.md](../DELTA_COMPRESSION.md) - Main delta compression documentation
- [DELTA_COMPRESSION_HORIZONTAL_IMPLEMENTATION.md](DELTA_COMPRESSION_HORIZONTAL_IMPLEMENTATION.md) - Multi-node implementation
- [DELTA_COMPRESSION_CLUSTER_COORDINATION.md](DELTA_COMPRESSION_CLUSTER_COORDINATION.md) - Cluster coordination details