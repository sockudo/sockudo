# Cache Backends in Sockudo

## Overview

Sockudo uses **multiple independent cache systems** for different purposes. Each system can be configured to use different backends based on your deployment needs.

## Cache Systems

### 1. **General Application Cache** (Primary Cache)

**Purpose**: Caching app configuration, channel data, and general application state.

**Configuration**: `cache.driver` in config file

**Supported Backends**:
- `memory` (default) - In-memory cache using Moka
- `redis` - Redis-backed cache
- `redis-cluster` - Redis Cluster-backed cache
- `none` - Disable caching

**Example**:
```json
{
  "cache": {
    "driver": "redis",
    "redis": {
      "prefix": "sockudo_cache:",
      "url_override": "redis://127.0.0.1:6379"
    }
  }
}
```

**Use Cases**:
- App configuration caching
- Channel metadata
- User session data
- Presence channel members

---

### 2. **Rate Limiter Cache**

**Purpose**: Tracking rate limit counters for API and WebSocket connections.

**Configuration**: `rate_limiter.driver` in config file

**Supported Backends**:
- `memory` (default) - In-memory rate limiting
- `redis` - Redis-backed rate limiting (shared across nodes)
- `redis-cluster` - Redis Cluster-backed rate limiting
- `none` - Disable rate limiting

**Example**:
```json
{
  "rate_limiter": {
    "enabled": true,
    "driver": "redis",
    "redis": {
      "prefix": "sockudo_ratelimit:"
    }
  }
}
```

**Use Cases**:
- API request rate limiting
- WebSocket message rate limiting
- Per-IP/per-user throttling

---

### 3. **Delta Compression Cluster Coordination**

**Purpose**: Synchronizing full message intervals across multiple nodes for delta compression.

**Configuration**: `delta_compression.cluster_coordination` + adapter type

**Supported Backends**:
- **Node-local** (default) - Each node tracks independently, no shared backend
- **Redis** - Uses Redis INCR for atomic counters (adapter: `redis` or `redis-cluster`)
- **NATS** - Uses NATS Key-Value store (adapter: `nats`)

**Example**:
```json
{
  "delta_compression": {
    "enabled": true,
    "cluster_coordination": true
  },
  "adapter": {
    "driver": "redis"  // or "nats"
  }
}
```

**Backend Used**:
- **Redis adapter** → Uses the same Redis instance as the adapter
- **NATS adapter** → Uses the same NATS cluster as the adapter
- **Local adapter** → Not applicable (single node)

**Use Cases**:
- Synchronized full message intervals across cluster
- Coordinated delta compression state

---

## Backend Comparison by Adapter

### Local Adapter (Single Node)

| System | Backend | Location |
|--------|---------|----------|
| General Cache | Memory (default) or Redis | Configurable via `cache.driver` |
| Rate Limiter | Memory (default) or Redis | Configurable via `rate_limiter.driver` |
| Delta Coordination | Node-local (N/A) | Not applicable for single node |

**Recommendation**: Use `memory` for everything (simplest, fastest)

---

### Redis Adapter (Multi-Node)

| System | Backend | Location |
|--------|---------|----------|
| General Cache | Memory/Redis | Configurable via `cache.driver` |
| Rate Limiter | Redis (recommended) | Configurable via `rate_limiter.driver` |
| Delta Coordination | Redis (optional) | Uses same Redis as adapter |
| Adapter Communication | Redis | From `database.redis` config |

**Backend Sharing**:
- Delta coordination uses the **same Redis instance** as the adapter
- General cache and rate limiter can use the **same or different** Redis instance

**Recommendation**: 
- Use Redis for rate limiter (shared across nodes)
- Use memory for general cache (if not critical to share)
- Enable delta coordination only if needed

**Example Configuration**:
```json
{
  "adapter": {
    "driver": "redis"
  },
  "database": {
    "redis": {
      "host": "127.0.0.1",
      "port": 6379
    }
  },
  "cache": {
    "driver": "memory"  // or "redis" to share cache across nodes
  },
  "rate_limiter": {
    "enabled": true,
    "driver": "redis"  // Recommended for multi-node
  },
  "delta_compression": {
    "enabled": true,
    "cluster_coordination": true  // Uses database.redis
  }
}
```

---

### Redis Cluster Adapter (Multi-Node)

| System | Backend | Location |
|--------|---------|----------|
| General Cache | Memory/Redis Cluster | Configurable via `cache.driver` |
| Rate Limiter | Redis Cluster (recommended) | Configurable via `rate_limiter.driver` |
| Delta Coordination | Redis Cluster (optional) | Uses same Redis Cluster as adapter |
| Adapter Communication | Redis Cluster | From `database.redis_cluster` config |

**Backend Sharing**:
- Delta coordination uses the **same Redis Cluster** as the adapter
- All systems can share the same Redis Cluster instance

**Recommendation**: Same as Redis adapter

---

### NATS Adapter (Multi-Node)

| System | Backend | Location |
|--------|---------|----------|
| General Cache | Memory/Redis | Configurable via `cache.driver` |
| Rate Limiter | Memory/Redis | Configurable via `rate_limiter.driver` |
| Delta Coordination | NATS KV (optional) | Uses same NATS cluster as adapter |
| Adapter Communication | NATS | From `adapter.nats` config |

**Backend Sharing**:
- Delta coordination uses the **same NATS cluster** as the adapter via JetStream KV
- General cache and rate limiter are **independent** (typically use Redis or memory)

**Note**: NATS doesn't provide a cache interface, so cache and rate limiter need separate backends.

**Recommendation**:
- Use Redis for cache and rate limiter (if sharing needed)
- Use memory for cache and rate limiter (if node-local is fine)
- Enable delta coordination if synchronized timing needed

**Example Configuration**:
```json
{
  "adapter": {
    "driver": "nats"
  },
  "nats": {
    "servers": "nats://127.0.0.1:4222",
    "prefix": "sockudo"
  },
  "cache": {
    "driver": "redis",  // Separate Redis for caching
    "redis": {
      "host": "127.0.0.1",
      "port": 6379
    }
  },
  "rate_limiter": {
    "enabled": true,
    "driver": "redis"  // Share same Redis as cache
  },
  "delta_compression": {
    "enabled": true,
    "cluster_coordination": true  // Uses NATS KV
  }
}
```

---

## Quick Decision Matrix

### When to use Memory cache:
- ✅ Single-node deployment
- ✅ Don't need to share cache across nodes
- ✅ Want fastest performance
- ✅ App config changes rarely

### When to use Redis cache:
- ✅ Multi-node deployment
- ✅ Need shared cache across nodes
- ✅ App config changes frequently
- ✅ Want centralized cache invalidation

### When to use Redis rate limiter:
- ✅ Multi-node deployment
- ✅ Need cluster-wide rate limiting
- ✅ Want consistent limits across all nodes
- ✅ Prevent per-node limit bypass

### When to enable delta coordination:
- ✅ Multi-node deployment
- ✅ Using delta compression
- ✅ Want synchronized full message intervals
- ✅ ~1ms latency overhead is acceptable
- ✅ Need predictable timing for monitoring

---

## Performance Impact

### Memory Cache
- **Latency**: ~1-10μs (fastest)
- **Overhead**: None
- **Sharing**: Per-node only

### Redis Cache
- **Latency**: ~0.5-2ms
- **Overhead**: Network + Redis operations
- **Sharing**: Cluster-wide

### Redis Rate Limiter
- **Latency**: ~0.5-2ms per check
- **Overhead**: 1-2 Redis operations per request
- **Sharing**: Cluster-wide

### Delta Coordination (Redis)
- **Latency**: +0.5ms per broadcast
- **Overhead**: 2-3 Redis operations per broadcast
- **Sharing**: Cluster-wide

### Delta Coordination (NATS)
- **Latency**: +0.7ms per broadcast
- **Overhead**: 2 NATS operations per broadcast
- **Sharing**: Cluster-wide

---

## Common Configurations

### 1. Single Node (Simplest)
```json
{
  "adapter": {
    "driver": "local"
  },
  "cache": {
    "driver": "memory"
  },
  "rate_limiter": {
    "enabled": true,
    "driver": "memory"
  }
}
```

### 2. Multi-Node with Redis (Most Common)
```json
{
  "adapter": {
    "driver": "redis"
  },
  "cache": {
    "driver": "memory"  // Per-node cache
  },
  "rate_limiter": {
    "enabled": true,
    "driver": "redis"  // Shared rate limiting
  },
  "delta_compression": {
    "cluster_coordination": false  // Node-local (default)
  }
}
```

### 3. Multi-Node with Full Sharing
```json
{
  "adapter": {
    "driver": "redis"
  },
  "cache": {
    "driver": "redis"  // Shared cache
  },
  "rate_limiter": {
    "enabled": true,
    "driver": "redis"  // Shared rate limiting
  },
  "delta_compression": {
    "cluster_coordination": true  // Synchronized intervals
  }
}
```

### 4. Multi-Node with NATS
```json
{
  "adapter": {
    "driver": "nats"
  },
  "cache": {
    "driver": "redis"  // Separate Redis for cache
  },
  "rate_limiter": {
    "enabled": true,
    "driver": "redis"  // Separate Redis for rate limiting
  },
  "delta_compression": {
    "cluster_coordination": true  // Uses NATS KV
  }
}
```

---

## Summary

**Three independent cache systems:**
1. **General Cache** - Configurable: Memory, Redis, Redis Cluster, or None
2. **Rate Limiter** - Configurable: Memory, Redis, Redis Cluster, or None
3. **Delta Coordination** - Automatic based on adapter: Node-local, Redis, or NATS

**Backend reuse:**
- Redis/NATS adapters **reuse their connection** for delta coordination
- General cache and rate limiter are **independent** and can use different backends
- All systems can share the same Redis instance if desired

**Recommendation:**
- Start with memory cache + memory rate limiter (simplest)
- Add Redis rate limiter for multi-node fairness
- Add Redis cache only if you need shared state
- Enable delta coordination only if synchronized timing is needed