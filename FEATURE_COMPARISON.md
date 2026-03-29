# Feature Comparison: Ably vs Centrifugo vs Sockudo

This document provides a detailed comparison of features available in **Ably**, **Centrifugo**, and **Sockudo**. It highlights what Sockudo currently has and what could potentially be implemented.

---

## Legend

- ✅ **Implemented** - Feature is fully available
- ⚠️ **Partial** - Feature is partially implemented or has limitations
- ❌ **Missing** - Feature is not available
- 🔵 **Planned** - Feature is documented or planned for implementation
- 🟢 **Easy** - Relatively straightforward to implement
- 🟡 **Medium** - Moderate complexity
- 🔴 **Hard** - High complexity or significant architectural changes required

---

## 1. Core Protocol & Transport Features

### 1.1 Wire Protocols

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **JSON Protocol** | ✅ | ✅ | ✅ | - | - |
| **Binary Protocol (Protobuf)** | ✅ | ✅ | ✅ (V2) | 🔴 High | 🟡 Medium |
| **MessagePack** | ✅ | ❌ | ✅ (V2) | 🟡 Medium | 🟢 Easy |
| **CBOR** | ❌ | ❌ | ❌ | 🟢 Low | 🟢 Easy |

**Notes:**
- **Protobuf**: Centrifugo uses Protobuf for binary protocol. Sockudo V2 now supports protobuf transport negotiation on the server and across the realtime JS, Flutter, Kotlin, and Swift SDKs.
- **MessagePack**: Ably's approach - 25-40% smaller payloads, 3-5x faster parsing. Sockudo V2 now supports explicit MessagePack transport negotiation with deterministic encoding across the server and realtime SDKs.
- **Benefits**: Combined with delta compression, could achieve 70-95% bandwidth savings vs JSON baseline.

**Implementation Considerations:**
- Add feature flags: `messagepack`, `protobuf`
- Protocol negotiation during connection handshake
- Maintain JSON as default for Pusher compatibility
- Realtime SDK support is implemented in this repository for JS, Flutter, Kotlin, and Swift

---

### 1.2 Transport Types

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **WebSocket (Bidirectional)** | ✅ | ✅ | ✅ | - | - |
| **HTTP Long-polling** | ✅ | ❌ | ❌ | 🟢 Low | 🟡 Medium |
| **SSE (Server-Sent Events)** | ✅ | ✅ (Bidirectional) | ❌ | 🟡 Medium | 🟡 Medium |
| **HTTP Streaming** | ✅ | ✅ | ❌ | 🟢 Low | 🟡 Medium |
| **WebTransport** | ❌ | ✅ (Experimental) | ❌ | 🟢 Low | 🔴 Hard |
| **GRPC (Unidirectional)** | ❌ | ✅ | ❌ | 🟡 Medium | 🔴 Hard |
| **MQTT Protocol** | ✅ | ❌ | ❌ | 🟢 Low | 🔴 Hard |

**Notes:**
- **Unidirectional Transports**: Centrifugo has excellent support for SSE, HTTP-streaming, and GRPC unidirectional streams. These don't require SDK - native browser APIs work.
- **WebTransport**: Experimental in Centrifugo. Based on HTTP/3 QUIC, very low latency.
- **MQTT**: Ably supports MQTT for IoT scenarios. Good for constrained devices.

**Implementation Considerations:**
- SSE and HTTP-streaming are most valuable for memory-constrained scenarios
- WebTransport requires HTTP/3 support
- MQTT requires separate protocol implementation

---

### 1.3 Connection Management

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Connection State Recovery** | ✅ | ✅ | ✅ (V2) | - | - |
| **Auto-reconnect with Backoff** | ✅ | ✅ | ✅ (Client) | - | - |
| **Connection Heartbeat/Ping-Pong** | ✅ (15s interval) | ✅ (25s interval) | ✅ | - | - |
| **Configurable Heartbeat Interval** | ✅ (5s-30m) | ✅ | ✅ | - | - |
| **Connection Metachannels** | ✅ `[meta]connection.lifecycle` | ❌ | ❌ | 🟢 Low | 🟡 Medium |
| **Page Unload Handling** | ✅ (configurable) | ✅ | ❌ | 🟡 Medium | 🟢 Easy |
| **Single User Connection Enforcement** | ❌ | ✅ | ❌ | 🟢 Low | 🟡 Medium |
| **Connection TTL/Expiration** | ✅ | ✅ | ✅ | - | - |
| **Connection Multiplexing** | ✅ | ✅ | ✅ | - | - |

**Notes:**
- **Connection State Recovery**: Centrifugo has sophisticated recovery with `offset` and `epoch`. Ably uses recovery keys. Sockudo V2 provides serial-based recovery with per-channel replay buffers, `message_id`, and `sockudo:resume`.
- **Connection Metachannels**: System channels for observing connection lifecycle events. Useful for debugging and monitoring.
- **Single User Connection**: Centrifugo can enforce only one connection per user using presence in personal channels.

---

## 2. Channel Features

### 2.1 Channel Types & Organization

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Public Channels** | ✅ | ✅ | ✅ | - | - |
| **Private Channels** | ✅ | ✅ | ✅ | - | - |
| **Presence Channels** | ✅ | ✅ | ✅ | - | - |
| **Encrypted Channels** | ✅ (E2E) | ✅ | ✅ | - | - |
| **Channel Namespaces** | ❌ | ✅ | ✅ (V2) | 🔴 High | 🟡 Medium |
| **User-Limited Channels** | ❌ | ✅ `channel#user_id` | ✅ (V2) | 🟡 Medium | 🟡 Medium |
| **Channel Regex Validation** | ❌ | ✅ | ✅ (V2) | 🟡 Medium | 🟢 Easy |
| **Wildcard Channel Subscriptions** | ❌ | ✅ (With NATS) | ✅ (V2) | 🟢 Low | 🟡 Medium |
| **Channel Metachannels** | ✅ `[meta]` | ❌ | ✅ (V2) | 🟡 Medium | 🟡 Medium |
| **Channel Rewind** | ✅ | ❌ | ❌ | 🟡 Medium | 🔴 Hard |

**Notes:**
- **Channel Namespaces**: Centrifugo's killer feature. Define different behaviors for channel groups. Example: `chat:*` has history, `presence:*` only has presence. Sockudo V2 now has strict namespace existence checks, namespace regex validation, namespace-level client subscribe/publish/presence flags, and namespace-specific user-limited channel toggles.
- **User-Limited Channels**: `dialog#42,43` only accessible to users 42 and 43. Sockudo V2 now enforces these channels against the signed-in connection `user_id`.
- **Channel Rewind**: Ably feature to replay from specific point in time or message. Very powerful for late joiners.
- **Wildcard Subscriptions**: Subscribe to `news.*` to get all matching public channels. Sockudo V2 now supports a single `*` wildcard per subscription pattern and matches at fanout time.
- **Channel Metachannels**: Sockudo V2 now exposes `[meta]channel` subscriptions for occupancy, subscription count, and presence member lifecycle events.

**Implementation Considerations:**
- Full namespace support still requires per-namespace overrides for history, presence, recovery, and fanout behavior
- User-limited channels are now enforced in Sockudo V2 using the authenticated connection `user_id`
- Channel rewind needs timestamp-based history indexing

---

### 2.2 Channel Options & Configuration

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Per-Channel History Size** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Per-Channel History TTL** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Per-Channel Presence** | ✅ | ✅ | ✅ | - | - |
| **Per-Channel Join/Leave Events** | ✅ | ✅ | ✅ | - | - |
| **Force Push Join/Leave** | ❌ | ✅ | ❌ | 🟢 Low | 🟢 Easy |
| **Positioning (Stream Offset Tracking)** | ✅ | ✅ `force_positioning` | ❌ | 🔴 High | 🟡 Medium |
| **Force Recovery** | ❌ | ✅ | ❌ | 🔴 High | 🟡 Medium |
| **Cache Recovery Mode** | ❌ | ✅ (PRO) | ❌ | 🟢 Low | 🔴 Hard |
| **Channel-Level Permissions** | ✅ | ✅ | ✅ | - | - |
| **Dynamic Channel Options Override** | ❌ | ✅ (via proxy) | ❌ | 🟡 Medium | 🟡 Medium |
| **Channel State Events** | ✅ | ✅ (PRO) `occupied`/`vacated` | ❌ | 🟡 Medium | 🟡 Medium |

**Notes:**
- **Force Push Join/Leave**: Automatically push join/leave to all subscribers without client opt-in.
- **Positioning**: Track exact position in stream with offset+epoch. Centrifugo can detect message loss.
- **Cache Recovery Mode**: Alternative to stream recovery. Good for channels where not all messages matter (e.g., sensor data).
- **Channel State Events**: Webhooks when channel becomes occupied (first subscriber) or vacated (last subscriber leaves).

---

### 2.3 Server-Side Subscriptions

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Server-Side Subscriptions** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Subscribe via JWT Claims** | ❌ | ✅ `channels` claim | ❌ | 🟡 Medium | 🟢 Easy |
| **Subscribe via Connect Proxy** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Dynamic Server Subscribe API** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Automatic Personal Channel** | ❌ | ✅ `#user_id` | ❌ | 🟡 Medium | 🟡 Medium |
| **Server Unsubscribe API** | ✅ | ✅ | ❌ | 🟡 Medium | 🟢 Easy |

**Notes:**
- **Server-Side Subscriptions**: Connection is automatically subscribed to channels without client calling `subscribe()`. Centrifugo has `channels` and `subs` JWT claims.
- **Automatic Personal Channel**: Config option to auto-subscribe user to `#user_id` channel. Perfect for notifications.
- Sockudo currently requires client-side subscription for all channels.

---

## 3. Message Features

### 3.1 Message Publishing & Delivery

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Publish to Channel** | ✅ | ✅ | ✅ | - | - |
| **Batch Publishing** | ✅ | ✅ (PRO) | ✅ | - | - |
| **Client-Side Publishing** | ✅ | ✅ | ✅ | - | - |
| **Message Priority** | ❌ | ❌ | ❌ | 🟢 Low | 🟡 Medium |
| **Message Ordering Guarantees** | ✅ | ✅ | ✅ | - | - |
| **Idempotent Publishing** | ✅ | ❌ | ✅ | - | - |
| **Message Deduplication** | ✅ | ❌ | ❌ | 🟢 Low | 🟡 Medium |
| **Skip History Option** | ❌ | ✅ | ❌ | 🟢 Low | 🟢 Easy |
| **Message Tags/Metadata** | ✅ `extras` | ✅ | ✅ (V2 extras + tags) | 🟡 Medium | 🟢 Easy |

**Notes:**
- **Batch Publishing**: Sockudo supports batch publishing via `POST /apps/{app_id}/batch_events` with configurable `max_event_batch_size` per-app. Centrifugo PRO supports per-channel batching with `batch_max_size`, `batch_max_delay`.
- **Idempotent Publishing**: Ably assigns message IDs client-side to prevent duplicate publishes on retry.
- **Skip History**: Don't save message to history. Useful for ephemeral data.
- **Sockudo idempotency**: Sockudo supports HTTP-level `idempotency_key` deduplication and always includes `message_id` on V2 broadcasts.
- **Message Tags**: Sockudo V2 supports both tag filtering and message `extras`, including typed headers, `ephemeral`, `idempotency_key`, and `echo`.

---

### 3.2 Message History & Recovery

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Message History** | ✅ (2min-365d) | ✅ | ⚠️ Recovery buffer only | 🟡 Medium | 🟡 Medium |
| **History Pagination** | ✅ | ✅ | ❌ | 🟡 Medium | 🟢 Easy |
| **History Iteration (Forward/Reverse)** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **History from Specific Offset** | ✅ | ✅ `since` param | ❌ | 🟡 Medium | 🟢 Easy |
| **Stream Epoch Tracking** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Automatic Message Recovery** | ✅ | ✅ | ✅ (V2) | - | - |
| **Configurable History Persistence** | ✅ | ✅ (Engine-dependent) | ❌ | 🟡 Medium | 🟡 Medium |
| **History Meta TTL** | ❌ | ✅ | ❌ | 🟢 Low | 🟢 Easy |

**Notes:**
- **Stream Epoch**: Centrifugo tracks epoch to detect when history stream was reset/lost. Critical for recovery.
- **Automatic Recovery**: On reconnect, client provides last seen offset+epoch. Server returns missed messages. Sockudo V2 supports replay-buffer-based recovery, but it is not a general-purpose long-term history system.
- **History Iteration**: Centrifugo has sophisticated API with `limit`, `since`, `reverse` parameters.
- **History Meta TTL**: Separate TTL for metadata vs messages. Prevents unbounded memory growth.

---

### 3.3 Message Compression & Optimization

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Delta Compression (Fossil)** | ✅ | ✅ | ✅ | - | - |
| **Cluster Coordination for Delta** | ❌ | ✅ | ✅ | - | - |
| **GZIP Compression** | ✅ | ❌ | ❌ | 🟢 Low | 🟢 Easy |
| **Channel Compaction** | ❌ | ✅ (PRO) | ❌ | 🟡 Medium | 🔴 Hard |
| **Message Batching (Delivery)** | ✅ | ✅ (PRO) | ❌ | 🟡 Medium | 🟡 Medium |
| **Message Flushing Strategies** | ❌ | ✅ (PRO) `flush_latest` | ❌ | 🟢 Low | 🟡 Medium |

**Notes:**
- **Delta Compression**: Both Ably and Centrifugo use fossil_delta. Sockudo supports both fossil_delta and xdelta3 (VCDIFF/RFC 3284). 60-90% bandwidth savings.
- **Channel Compaction**: Centrifugo PRO feature - negotiate compression per channel subscription.
- **Message Batching**: Collect messages and send in batches. Reduces number of events client processes.
- **Flush Latest**: Only send latest message in batch. Perfect for rapidly changing data (stock prices).

---

### 3.4 Message Annotations & Modifications

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Message Annotations** | ✅ | ❌ | ❌ | 🟢 Low | 🔴 Hard |
| **Message Updates** | ✅ | ❌ | ❌ | 🟡 Medium | 🔴 Hard |
| **Message Deletions** | ✅ | ❌ | ❌ | 🟡 Medium | 🔴 Hard |
| **Message Serial/ID** | ✅ | ✅ `offset` | ✅ (V2) | 🟡 Medium | 🟢 Easy |
| **Message Actions** | ✅ (CREATE, UPDATE, DELETE, META) | ❌ | ❌ | 🟢 Low | 🔴 Hard |

**Notes:**
- **Message Annotations**: Ably allows adding annotations to existing messages. Use case: reactions, read receipts, moderation flags.
- **Message Updates/Deletions**: Edit or delete messages after publishing. Requires message serial tracking.
- Sockudo V2 now assigns direct-message IDs as well as broadcast serial/message IDs while keeping protocol v1 deliveries stripped for Pusher compatibility.
- **Message Actions**: Enum indicating what happened to message. Clients can handle differently.

**Implementation Considerations:**
- Requires persistent message serial/ID
- History needs to store annotations
- Client SDKs need to handle update/delete events
- Consider CRDT for conflict resolution

---

## 4. Filtering & Targeting

### 4.1 Publication Filtering

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Tag-Based Filtering** | ❌ | ✅ (v6.4.0+) | ✅ | - | - |
| **Server-Side Filter Evaluation** | ❌ | ✅ | ✅ | - | - |
| **Complex Filter Expressions** | ❌ | ✅ (AND, OR, NOT, comparisons) | ✅ | - | - |
| **CEL (Common Expression Language)** | ❌ | ✅ (PRO) | ❌ | 🟢 Low | 🔴 Hard |
| **Filter on Subscribe** | ❌ | ✅ | ✅ | - | - |
| **Zero-Allocation Filtering** | ❌ | ✅ | ✅ | - | - |

**Notes:**
- **Tag Filtering**: Sockudo and Centrifugo both support this. Massive bandwidth savings (60-90%) for high-volume scenarios.
- **CEL**: Centrifugo PRO uses Common Expression Language for sophisticated filtering. Very powerful.
- Sockudo's implementation is competitive with Centrifugo's open-source version.

---

## 5. Presence & Occupancy

### 5.1 Presence Features

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Channel Presence** | ✅ | ✅ | ✅ | - | - |
| **Presence Enter/Leave/Update** | ✅ | ✅ | ✅ | - | - |
| **Presence Data/Info** | ✅ | ✅ | ✅ | - | - |
| **Presence History** | ✅ | ❌ | ❌ | 🟢 Low | 🟡 Medium |
| **Presence User Mapping** | ❌ | ✅ (Redis optimization) | ❌ | 🟢 Low | 🟡 Medium |
| **Presence Hash Field TTL** | ❌ | ✅ (Redis 7.4+) | ❌ | 🟢 Low | 🟡 Medium |

**Notes:**
- **Presence History**: Ably tracks presence changes over time. Can query who was present historically.
- **Presence User Mapping**: Centrifugo Redis optimization for presence stats with many subscribers. Reduces Redis ops from 15 to 200k/s.
- **Hash Field TTL**: Uses Redis 7.4 feature for 1.6x better memory usage.

---

### 5.2 Occupancy Features

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Channel Occupancy** | ✅ | ✅ | ✅ | - | - |
| **Occupancy Metrics** | ✅ (detailed) | ✅ | ✅ | 🟡 Medium | 🟢 Easy |
| **Global Occupancy** | ✅ | ✅ | ✅ | 🟡 Medium | 🟡 Medium |
| **Regional Occupancy** | ✅ | ❌ | ❌ | 🟢 Low | 🔴 Hard |

**Notes:**
- **Occupancy Metrics**: Ably tracks detailed metrics: connections, publishers, subscribers, presenceConnections, presenceMembers, presenceSubscribers.
- Sockudo now exposes per-app and global occupancy totals through `/stats`, including connection, authenticated connection, channel, subscription, presence subscription, presence member, and connection metadata counts.

---

## 6. Authentication & Authorization

### 6.1 Authentication Methods

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **JWT Authentication** | ✅ | ✅ | ✅ | - | - |
| **HMAC JWT (HS256/384/512)** | ✅ | ✅ | ✅ | - | - |
| **RSA JWT (RS256/384/512)** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **ECDSA JWT (ES256/384/512)** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **EdDSA JWT (EdDSA)** | ❌ | ✅ (OKP/Ed25519) | ❌ | 🟢 Low | 🟡 Medium |
| **JWK (JSON Web Key)** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **JWKS Endpoint** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Dynamic JWKS (Template)** | ❌ | ✅ | ❌ | 🟢 Low | 🟡 Medium |
| **Token Revocation** | ✅ | ✅ (PRO) | ❌ | 🟡 Medium | 🔴 Hard |
| **Token Refresh** | ✅ | ✅ | ✅ | - | - |
| **Basic Auth (API Key)** | ✅ | ✅ | ✅ | - | - |
| **Anonymous Access** | ✅ | ✅ | ✅ | - | - |
| **Custom User ID Claim** | ❌ | ✅ | ❌ | 🟢 Low | 🟢 Easy |

**Notes:**
- **RSA/ECDSA**: Asymmetric crypto for JWT. More secure, allows public key distribution.
- **JWKS**: Load public keys from endpoint. Critical for SSO integration (Keycloak, Auth0, etc.).
- **Dynamic JWKS**: Centrifugo can extract variables from `iss`/`aud` JWT claims to construct JWKS endpoint dynamically. Very flexible for multi-tenant.
- **Token Revocation**: Ably and Centrifugo PRO support revoking tokens by clientId, channel, etc.

**Implementation Considerations:**
- RSA/ECDSA support needs asymmetric crypto libraries
- JWKS needs HTTP client and key caching (1 hour in Centrifugo)
- Token revocation needs distributed state (Redis)

---

### 6.2 Authorization & Permissions

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Capabilities (Fine-grained Permissions)** | ✅ | ✅ (via JWT claims) | ✅ (V2) | 🔴 High | 🟡 Medium |
| **Channel-Level Permissions** | ✅ | ✅ | ✅ | - | - |
| **Subscription Tokens** | ✅ | ✅ | ✅ | - | - |
| **Per-Namespace Permission Flags** | ❌ | ✅ (allow_subscribe_for_client, etc.) | ✅ (V2) | 🟡 Medium | 🟡 Medium |
| **CEL Expressions for Permissions** | ❌ | ✅ (PRO) | ❌ | 🟢 Low | 🔴 Hard |
| **Connection Meta Attachment** | ✅ | ✅ | ✅ (V2) | 🟡 Medium | 🟢 Easy |
| **Dynamic Permission Override** | ❌ | ✅ (via proxy result) | ❌ | 🟡 Medium | 🟡 Medium |

**Notes:**
- **Capabilities**: Ably has granular JSON capability format. Centrifugo uses simpler boolean flags per namespace. Sockudo V2 now supports signed connection capability patterns for subscribe/publish/presence plus namespace-level client permission flags.
- **CEL Expressions**: Centrifugo PRO allows writing permission logic in CEL. Very powerful and flexible.
- **Connection Meta**: Attach arbitrary data to connection (not exposed to clients). Useful for passing backend state and internal observability.

---

## 7. Proxy & Webhooks

### 7.1 Proxy Events (Backend Hooks)

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Connect Proxy** | ❌ | ✅ | ❌ | 🔴 High | 🟡 Medium |
| **Refresh Proxy** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Subscribe Proxy** | ❌ | ✅ | ❌ | 🔴 High | 🟡 Medium |
| **Publish Proxy** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Sub Refresh Proxy** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **RPC Proxy** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Subscribe Stream Proxy** | ❌ | ✅ (Experimental) | ❌ | 🟢 Low | 🔴 Hard |
| **Cache Empty Proxy** | ❌ | ✅ (PRO) | ❌ | 🟢 Low | 🟡 Medium |
| **State Proxy** | ❌ | ✅ (PRO) `occupied`/`vacated` | ❌ | 🟡 Medium | 🟡 Medium |
| **HTTP Proxy Protocol** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **GRPC Proxy Protocol** | ❌ | ✅ | ❌ | 🟡 Medium | 🔴 Hard |
| **Per-Namespace Proxy Config** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Header/Metadata Proxying** | ❌ | ✅ | ❌ | 🟡 Medium | 🟢 Easy |
| **Binary Encoding Mode** | ❌ | ✅ (base64) | ❌ | 🟢 Low | 🟢 Easy |

**Notes:**
- **Proxy System**: Centrifugo's most powerful feature. Allows backend to control every aspect of connection/subscription lifecycle.
- **Connect Proxy**: Authenticate without JWT. Use cookies/sessions. Reduce load vs JWT refresh.
- **Subscribe Proxy**: Check permissions, return initial data, modify subscription options.
- **RPC Proxy**: Client can call backend RPC over WebSocket. No HTTP needed.
- **State Proxy**: Get notified when channel becomes occupied/vacated. Useful for "user is typing" indicators.
- **GRPC Proxy**: Use Protobuf for proxy calls. More efficient than JSON.

**Implementation Considerations:**
- Major architectural addition
- Needs HTTP client, connection pooling
- GRPC needs code generation from proto files
- Must handle proxy timeouts, failures gracefully
- Can significantly reduce JWT token generation load on backend

---

### 7.2 Webhook Features

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Webhooks** | ✅ | ✅ | ✅ | - | - |
| **Webhook Batching** | ✅ | ✅ | ✅ | - | - |
| **Webhook Retry** | ✅ | ✅ | ❌ | 🟡 Medium | 🟢 Easy |
| **Lambda Webhook Support** | ❌ | ✅ | ✅ | - | - |
| **Custom Webhook Headers** | ✅ | ✅ | ✅ | - | - |

**Notes:**
- Sockudo supports Pusher-compatible webhooks, batching, filtering, custom headers, and Lambda targets.
- Queue-backed retry support is documented in the current docs, but the behavior still depends on the configured queue backend and deployment model.

---

## 8. Scalability & Distribution

### 8.1 Horizontal Scaling

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Multi-Node Clustering** | ✅ | ✅ | ✅ | - | - |
| **Sticky Sessions Not Required** | ✅ | ✅ | ✅ | - | - |
| **Load Balancing Support** | ✅ | ✅ | ✅ | - | - |
| **Redis Adapter** | ❌ | ✅ | ✅ | - | - |
| **Redis Cluster Support** | ❌ | ✅ | ✅ | - | - |
| **Redis Sentinel Support** | ❌ | ✅ | ✅ | - | - |
| **Redis Sharding** | ❌ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **NATS Broker** | ❌ | ✅ | ✅ | - | - |
| **RabbitMQ Broker** | ❌ | ❌ | ✅ | - | - |
| **Google Cloud Pub/Sub Broker** | ❌ | ❌ | ✅ | - | - |
| **Kafka Broker** | ❌ | ❌ | ✅ | - | - |
| **NATS Raw Mode** | ❌ | ✅ | ❌ | 🟢 Low | 🟡 Medium |
| **Separate Broker & Presence Manager** | ❌ | ✅ | ❌ | 🟢 Low | 🟡 Medium |
| **Per-Namespace Engines** | ❌ | ✅ (PRO) | ❌ | 🟢 Low | 🔴 Hard |
| **Auto-Discovery** | ✅ | ✅ | ✅ | - | - |

**Notes:**
- **Redis Sentinel**: Sockudo supports Redis Sentinel with configurable sentinel nodes, sentinel password, and master name. Connection URL is automatically built in `redis+sentinel://` format. Centrifugo also supports this format.
- **Redis Sharding**: Spread channels across multiple Redis instances using consistent hashing.
- **NATS Raw Mode**: Direct channel mapping to NATS subjects without Centrifugo wrapping.
- **Separate Broker/Presence**: Use different backends for PUB/SUB vs presence. Example: NATS for broker, Redis for presence.
- **Per-Namespace Engines**: Centrifugo PRO allows different engine per namespace. Extreme flexibility.

---

### 8.2 Performance & Limits

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Rate Limiting** | ✅ | ✅ | ✅ | - | - |
| **Connection Limits** | ✅ | ✅ | ✅ | - | - |
| **Channel Subscription Limits** | ✅ | ✅ | ✅ | - | - |
| **Message Size Limits** | ✅ | ✅ | ✅ | - | - |
| **Configurable Limits** | ✅ | ✅ | ✅ | - | - |
| **Slow Client Detection** | ✅ | ✅ (disconnect code 3008) | ✅ (disconnect code 4100) | - | - |

**Notes:**
- **Slow Client Detection**: Sockudo implements bounded WebSocket buffers with three limit modes (message count, byte size, or both). Slow clients are disconnected with code 4100 or have messages dropped, configurable via `disconnect_on_buffer_full`. Centrifugo uses disconnect code 3008.

---

## 9. Developer Experience & Tooling

### 9.1 API & SDKs

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **REST API** | ✅ | ✅ | ✅ | - | - |
| **JavaScript/TypeScript SDK** | ✅ | ✅ | ✅ | - | - |
| **Native iOS SDK** | ✅ | ✅ | ✅ | - | - |
| **Native Android SDK** | ✅ | ✅ | ✅ | - | - |
| **React Hooks** | ✅ | ❌ | ❌ | 🟡 Medium | 🟡 Medium |
| **React Native SDK** | ✅ | ❌ | ❌ | 🟡 Medium | 🔴 Hard |
| **Flutter SDK** | ✅ | ✅ | ✅ | - | - |
| **Go SDK** | ✅ | ✅ | ✅ | - | - |
| **Python SDK** | ✅ | ✅ | ✅ | - | - |
| **Ruby SDK** | ✅ | ❌ | ✅ | - | - |
| **Java SDK** | ✅ | ✅ | ✅ | - | - |
| **.NET SDK** | ✅ | ✅ | ✅ | - | - |
| **PHP SDK** | ✅ | ❌ | ✅ | - | - |

**Notes:**
- Sockudo now has official V2 client SDKs for JavaScript/TypeScript, Swift, Kotlin, Flutter, Python, and C#/.NET, plus server SDKs for Node.js, PHP, Ruby, Go, Rust, Java, .NET, and Swift.
- Pusher-compatible SDKs still work in protocol V1 mode.

---

### 9.2 Monitoring & Debugging

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Prometheus Metrics** | ✅ | ✅ | ✅ | - | - |
| **Structured Logging** | ✅ | ✅ (JSON/Human) | ✅ (JSON/Human) | - | - |
| **Debug Logging** | ✅ | ✅ | ✅ | - | - |
| **Connection Inspector** | ✅ | ✅ | ❌ | 🟡 Medium | 🟡 Medium |
| **Live Event Stream** | ✅ | ❌ | ❌ | 🟢 Low | 🟡 Medium |
| **Health Check Endpoints** | ✅ | ✅ | ✅ | - | - |
| **Statistics API** | ✅ | ✅ | ✅ | 🟡 Medium | 🟡 Medium |
| **Error Codes Documentation** | ✅ | ✅ | ✅ | 🟡 Medium | 🟢 Easy |

**Notes:**
- **Structured Logging**: Sockudo supports both JSON and human-readable log formats via `LOG_OUTPUT_FORMAT`, with configurable colors and module target inclusion.
- Connection inspector would help debugging client issues.
- Sockudo now exposes a `/stats` endpoint and a dedicated error code reference page.

---

## 10. Advanced Features

### 10.1 Push Notifications

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Push Notifications** | ✅ (FCM, APNs) | ❌ | ❌ | 🟡 Medium | 🔴 Hard |
| **Push to Device** | ✅ | ❌ | ❌ | 🟡 Medium | 🔴 Hard |
| **Push to Channel** | ✅ | ❌ | ❌ | 🟡 Medium | 🔴 Hard |
| **Push Activation** | ✅ | ❌ | ❌ | 🟡 Medium | 🔴 Hard |

**Notes:**
- Push notifications are Ably-specific feature.
- Allows sending notifications even when app is closed.
- Requires integration with FCM (Firebase Cloud Messaging) and APNs (Apple Push Notification Service).

---

### 10.2 Product-Specific Features

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Chat SDK** | ✅ Ably Chat | ❌ | ❌ | 🟢 Low | 🔴 Hard |
| **Spaces (Collaboration)** | ✅ Ably Spaces | ❌ | ❌ | 🟢 Low | 🔴 Hard |
| **LiveObjects** | ✅ | ❌ | ❌ | 🟢 Low | 🔴 Hard |
| **LiveSync (DB Fan-out)** | ✅ | ❌ | ❌ | 🟢 Low | 🔴 Hard |
| **Asset Tracking** | ✅ | ❌ | ❌ | 🟢 Low | 🔴 Hard |
| **Watchlist Events** | ❌ | ❌ | ✅ | - | - |

**Notes:**
- These are Ably's higher-level product offerings built on top of core pub/sub.
- Not directly comparable as they're application-level features.
- Centrifugo and Sockudo focus on lower-level real-time infrastructure.

---

## 11. Operations & Deployment

### 11.1 Deployment Options

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **Self-Hosted** | ❌ (Cloud Only) | ✅ | ✅ | - | - |
| **Docker Support** | N/A | ✅ | ✅ | - | - |
| **Docker Compose** | N/A | ✅ | ✅ | - | - |
| **Kubernetes Ready** | N/A | ✅ | ✅ | - | - |
| **Helm Charts** | N/A | ✅ | ✅ | - | - |
| **Binary Releases** | N/A | ✅ (Multi-platform) | ✅ | - | - |
| **Unix Socket Support** | N/A | ❌ | ✅ | - | - |

**Notes:**
- Ably is cloud-only SaaS.
- Sockudo and Centrifugo are self-hosted.
- Sockudo ships production-ready Helm charts (`charts/sockudo/`) with HPA, PDB, ServiceMonitor, NetworkPolicy, and configurable probes.
- Sockudo's Unix socket support is unique - good for reverse proxy deployments.

---

### 11.2 Configuration Management

| Feature | Ably | Centrifugo | Sockudo | Priority | Difficulty |
|---------|------|------------|---------|----------|------------|
| **JSON Config** | N/A | ✅ | ✅ | - | - |
| **YAML Config** | N/A | ✅ | ❌ | 🟢 Low | 🟢 Easy |
| **TOML Config** | N/A | ✅ | ✅ | - | - |
| **Environment Variables** | N/A | ✅ | ✅ | - | - |
| **Command-Line Flags** | N/A | ✅ | ✅ | - | - |
| **Config Hierarchy** | N/A | ✅ | ✅ | - | - |
| **Hot Reload** | N/A | ⚠️ Partial | ❌ | 🟡 Medium | 🟡 Medium |

**Notes:**
- Centrifugo has comprehensive config support (JSON, YAML, TOML).
- Configuration hierarchy: defaults < config file < CLI args < env vars.
- Hot reload would be useful for updating config without restart.

---

## 12. Summary & Recommendations

### High Priority Additions for Sockudo

#### 1. **Proxy System** (🔴 High Priority, 🟡 Medium Difficulty)
Centrifugo's proxy system is incredibly powerful. Implementing even basic HTTP proxy for connect, subscribe, and publish would:
- Enable cookie/session-based authentication (reduce JWT load)
- Allow backend validation of every operation
- Enable dynamic permission management
- Support microservice architectures

**Recommended Implementation Order:**
1. Connect proxy (authentication)
2. Subscribe proxy (permissions)
3. Publish proxy (validation)
4. RPC proxy (backend calls)

#### 2. **Binary Protocols** (🔴 High Priority, 🟡 Medium Difficulty)
**MessagePack First:**
- 25-40% bandwidth savings
- Easy to implement (add `rmp-serde` crate)
- Maintains schema flexibility
- Combined with delta compression = 70-95% savings

**Protobuf Later** (if needed for performance-critical apps):
- 50-70% savings vs JSON
- Requires schema management
- Best for enterprise/gaming

#### 3. **Advanced JWT Support** (🟡 Medium Priority, 🟡 Medium Difficulty)
- RSA/ECDSA algorithms
- JWKS endpoint support
- Enables SSO integration (Keycloak, Auth0, etc.)

#### 4. **Server-Side Subscriptions** (🟡 Medium Priority, 🟡 Medium Difficulty)
- Subscribe users via JWT claims
- Automatic personal channel subscriptions
- Dynamic subscribe/unsubscribe API

### Medium Priority Additions

1. **History Improvements**
   - Durable message history beyond the current replay buffer
   - Bidirectional iteration
   - Stream epoch tracking

2. **Transport Additions**
   - SSE (Server-Sent Events) - easiest to add
   - HTTP Streaming
   - Consider WebTransport for future

3. **Connection Features**
   - Configurable heartbeat intervals
   - Page unload handling

4. **Monitoring & Ops**
   - Connection inspector/debugger
   - Expanded statistics/occupancy inspection

### Low Priority / Future Considerations

1. **MQTT Protocol** - For IoT scenarios
2. **Push Notifications** - Complex, consider if targeting mobile
3. **GRPC Transport** - If targeting microservices heavily
4. **Regional Deployments** - Multi-region awareness
5. **Product SDKs** - Native mobile SDKs (iOS, Android)

---

## Conclusion

**Sockudo's Current Strengths:**
- ✅ Solid Pusher protocol compatibility
- ✅ Dual protocol model with V2 serials, `message_id`, and connection recovery
- ✅ Delta compression with cluster coordination (fossil_delta + xdelta3)
- ✅ Tag-based filtering with zero-allocation evaluation
- ✅ Idempotent publishing via `idempotency_key`
- ✅ Good horizontal scaling (Redis, Redis Cluster, Redis Sentinel, NATS, RabbitMQ, Google Pub/Sub, Kafka)
- ✅ Multiple app-manager backends (MySQL, PostgreSQL, DynamoDB, SurrealDB 3, ScyllaDB)
- ✅ Unix socket support (unique feature)
- ✅ Slow client detection with bounded WebSocket buffers
- ✅ Batch event publishing
- ✅ Official V2 client SDKs and broad server SDK coverage
- ✅ Webhooks with batching, filtering, custom headers, and Lambda support
- ✅ TOML and JSON config support
- ✅ Structured logging (JSON/Human formats)
- ✅ Per-app origin validation with wildcard patterns
- ✅ Watchlist events and per-app connection-recovery overrides
- ✅ Well-structured codebase with feature flags

**Key Gaps vs. Competitors:**
1. **Proxy System** - Centrifugo's killer feature for backend integration
2. **Binary Protocols** - MessagePack would be quick win
3. **Advanced Auth** - RSA/ECDSA, JWKS for SSO
4. **Connection Inspector** - Better operational debugging
5. **Durable History** - Replay buffers exist, but long-term history/replay is still missing

**Strategic Recommendation:**
Focus on **Proxy System** and **MessagePack** first. These two features would:
- Dramatically improve backend integration flexibility
- Provide immediate bandwidth/performance benefits
- Open up new use cases (sessions, cookies, validation)
- Be relatively straightforward to implement

Then tackle **Advanced JWT**, a **connection inspector**, and **durable history** to match enterprise expectations.

With these additions, Sockudo would be highly competitive with both Ably and Centrifugo for self-hosted scenarios, while maintaining Pusher backward compatibility.
