# Delta Compression Bandwidth Optimization

This document describes the bandwidth optimization features available in Sockudo's delta compression system.

## Overview

Delta compression already significantly reduces bandwidth by sending only message differences. However, each delta message still includes metadata fields that can be optimized away in certain scenarios:

1. **Algorithm field** - The compression algorithm is constant for a connection, so sending it in every message is redundant
2. **Tags field** - Tags may not be needed for certain channels or applications
3. **Delta/Diff** - Cannot be removed (this is the actual compressed data)
4. **Sequence number** - Cannot be removed (required for delta chain tracking)

## Configuration Options

### 1. Global Algorithm Omission

**Config Option:** `omit_delta_algorithm`

**Location:** `delta_compression` section (global)

**Default:** `false`

**When to use:**
- Your clients don't need to know which algorithm is used
- You want to save ~20-30 bytes per delta message
- The algorithm is consistent across your deployment

**Configuration:**
```json
{
  "delta_compression": {
    "enabled": true,
    "algorithm": "fossil",
    "omit_delta_algorithm": true
  }
}
```

**Savings:** ~20-30 bytes per message (removes `"algorithm":"fossil"` field)

---

### 2. Global Tags Disabling

**Config Option:** `enable_tags`

**Location:** `tag_filtering` section (global)

**Default:** `true`

**When to use:**
- Your application doesn't use tags for client-side logic
- You want to reduce bandwidth across all channels by default
- Per-channel overrides can still enable tags where needed

**Configuration:**
```json
{
  "tag_filtering": {
    "enabled": false,
    "enable_tags": false
  }
}
```

**Savings:** 50-200 bytes per message (depending on tag size)

---

### 3. Per-Channel Tags Override

**Config Option:** `enable_tags`

**Location:** `channel_delta_compression.<pattern>` section (per-channel)

**Default:** Inherits from global `delta_compression.enable_tags`

**When to use:**
- Most channels don't need tags, but some do (e.g., for tag filtering)
- You want granular control over which channels include tags

**Configuration:**
```json
{
  "tag_filtering": {
    "enabled": false,
    "enable_tags": false  // Disable globally by default
  },
  "apps": [
    {
      "id": "app-id",
      "channel_delta_compression": {
        "ticker:*": {
          "enabled": true,
          "algorithm": "fossil",
          "conflation_key": "symbol",
          "enable_tags": false  // Explicitly disabled (inherits from global)
        },
        "filtered-channel:*": {
          "enabled": true,
          "algorithm": "fossil",
          "conflation_key": "asset",
          "enable_tags": true  // Override: enable tags for this channel
        }
      }
    }
  ]
}
```

## Configuration Priority

The system uses the following priority order for `enable_tags`:

1. **Per-channel setting** (highest priority) - If specified in `channel_delta_compression.<pattern>.enable_tags`
2. **Global setting** (fallback) - Uses `delta_compression.enable_tags`

**Example:**
```json
{
  "tag_filtering": {
    "enabled": false,
    "enable_tags": false  // Global: tags disabled by default
  },
  "apps": [{
    "channel_delta_compression": {
      "channel-a": {
        "enable_tags": true   // Override: tags ENABLED for channel-a
      },
      "channel-b": {
        // No override: inherits global setting (tags DISABLED)
      }
    }
  }]
}
```

## Complete Example Configuration

```json
{
  "delta_compression": {
    "enabled": true,
    "algorithm": "fossil",
    "full_message_interval": 10,
    "min_message_size": 100,
    "max_state_age_secs": 300,
    "max_channel_states_per_socket": 100,
    "cluster_coordination": false,
    "omit_delta_algorithm": true   // Optimization: omit algorithm field
  },
  "tag_filtering": {
    "enabled": false,
    "enable_tags": false  // Optimization: disable tags globally
  },
  "apps": [
    {
      "id": "my-app",
      "channel_delta_compression": {
        "price:*": {
          "enabled": true,
          "algorithm": "fossil",
          "conflation_key": "asset",
          "max_messages_per_key": 10,
          "max_conflation_keys": 1000,
          "enable_tags": false  // Inherits from global (explicit for clarity)
        },
        "filtered-updates:*": {
          "enabled": true,
          "algorithm": "fossil",
          "conflation_key": "id",
          "max_messages_per_key": 10,
          "max_conflation_keys": 500,
          "enable_tags": true   // Override: needs tags for filtering
        }
      }
    }
  ]
}
```

## Bandwidth Savings Summary

| Optimization | Bytes Saved per Message | Use Case |
|-------------|------------------------|----------|
| `omit_delta_algorithm: true` | ~20-30 bytes | When clients don't need algorithm info |
| `enable_tags: false` (global) | 50-200 bytes | When tags aren't used for filtering |
| Combined | 70-230 bytes | Maximum bandwidth optimization |

**Note:** Actual savings depend on:
- Tag content size (if tags are used)
- Message frequency
- Number of subscribers per channel

## Impact on Tag Filtering

**Important:** Tag filtering is a **server-side** operation. Tags are used on the server to determine which clients receive a message, but they are **NOT required** on the client side.

### You Can Safely Disable Tags Even With Filtering Enabled

```json
{
  "tag_filtering": {
    "enabled": true,  // Server-side filtering still works perfectly
    "enable_tags": false  // Safe: tags stripped AFTER server-side filtering
  }
}
```

### How It Works

The order of operations is critical to understanding why tags aren't needed on the client side:

**Processing Flow:**
```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. Publisher sends message with tags                                    │
│    POST /apps/app-id/events                                             │
│    { "channel": "scores", "data": {...}, "tags": {"type": "goal"} }     │
└────────────────────────────────┬────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. Server-side tag filtering (BEFORE tag stripping)                     │
│    - Evaluate: Filter.eq("type", "goal")                                │
│    - Result: Select matching subscribers (Client A, Client C)           │
│    - Tags are USED here for routing decisions                           │
└────────────────────────────────┬────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. Tag stripping (if enable_tags: false)                                │
│    - Remove tags from message payload                                   │
│    - Message: { "channel": "scores", "data": {...} }                    │
│    - Tags already served their purpose (filtering)                      │
└────────────────────────────────┬────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. Send to filtered clients (WITHOUT tags)                              │
│    - Client A receives message (no tags) ✓                              │
│    - Client B does NOT receive (filtered out)                           │
│    - Client C receives message (no tags) ✓                              │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key Insight:** Tags are used for **routing** on the server, not for **processing** on the client. Once the server decides who receives the message, tags are no longer needed.

### When You DO Need Tags

You only need `enable_tags: true` if:
- **Clients need tags for their own logic** (e.g., displaying metadata, client-side routing)
- **You want to inspect tags in client debugging**

You do **NOT** need tags if:
- ✅ You're using server-side tag filtering (filtering works without sending tags to clients)
- ✅ Tags are only used for routing/filtering on the server
- ✅ You want to maximize bandwidth savings

### Example: Filtering Without Client Tags

```json
{
  "tag_filtering": {
    "enabled": true,
    "enable_tags": false  // Bandwidth optimization
  },
  "apps": [{
    "channel_delta_compression": {
      "live-scores:*": {
        "enable_tags": false  // OK: filtering works, tags not sent to clients
      }
    }
  }]
}
```

**Client subscribes with filter:**
```javascript
channel.subscribe('live-scores:123', Filter.eq('event_type', 'goal'));
```

**Server publishes message with tags:**
```json
POST /apps/app-id/events
{
  "channel": "live-scores:123",
  "event": "update",
  "data": "{...}",
  "tags": {
    "event_type": "goal",
    "priority": "high"
  }
}
```

**What happens:**
1. Server evaluates: does `event_type == "goal"`? → Yes
2. Server sends to matching clients
3. Tags are stripped (because `enable_tags: false`)
4. Client receives: `{ "event": "update", "data": "{...}" }` (no tags)

**Result:** Filtering works perfectly, bandwidth saved by not sending tags to clients.

## Client Implications

### When `omit_delta_algorithm: true`

Clients must:
- Assume a consistent algorithm (e.g., always Fossil)
- Or negotiate the algorithm during connection handshake
- Handle messages without the `algorithm` field

**Standard delta message:**
```json
{
  "event": "pusher:delta",
  "channel": "price:BTC",
  "data": {
    "event": "price-update",
    "delta": "AbCdEf...base64...",
    "algorithm": "fossil",  // <-- Present
    "seq": 5,
    "base_index": 4
  }
}
```

**Optimized delta message (omit_delta_algorithm: true):**
```json
{
  "event": "pusher:delta",
  "channel": "price:BTC",
  "data": {
    "event": "price-update",
    "delta": "AbCdEf...base64...",
    // algorithm field omitted
    "seq": 5,
    "base_index": 4
  }
}
```

### When `enable_tags: false`

Clients will receive messages without tags:

**Standard message:**
```json
{
  "event": "price-update",
  "channel": "price:BTC",
  "data": "{...}",
  "tags": {
    "priority": "high",
    "region": "us"
  }
}
```

**Optimized message (enable_tags: false):**
```json
{
  "event": "price-update",
  "channel": "price:BTC",
  "data": "{...}"
  // tags field omitted
}
```

## Best Practices

1. **Start with defaults** - Test with `omit_delta_algorithm: false` and `enable_tags: true`

2. **Profile your bandwidth** - Measure actual bandwidth usage before and after optimization

3. **Use per-channel overrides** - Disable tags globally, enable for specific channels that need them

4. **Document client expectations** - Ensure clients know whether to expect algorithm/tags fields

5. **Tag filtering is server-side** - You can safely use `enable_tags: false` even with tag filtering enabled (tags are only needed for server-side routing, not client-side)

6. **Test thoroughly** - Verify client behavior with optimized messages before production deployment

## Migration Guide

### Enabling Global Tag Disabling

**Step 1:** Update server configuration
```json
{
  "tag_filtering": {
    "enabled": false,
    "enable_tags": false
  }
}
```

**Step 2:** Identify channels where clients need tags for their own logic (not for filtering)
```json
{
  "apps": [{
    "channel_delta_compression": {
      "client-needs-metadata:*": {
        "enable_tags": true  // Override: clients use tags for display/logic
      }
    }
  }]
}
```

**Note:** You do NOT need to enable tags for channels that use server-side filtering. Filtering happens before tag stripping.

**Step 3:** Deploy and monitor
- Check client logs for errors (only if clients were using tags)
- Monitor bandwidth reduction
- Verify server-side tag filtering still works (it will - filtering happens before tag stripping)

### Enabling Algorithm Omission

**Step 1:** Ensure clients can handle missing algorithm field

**Step 2:** Update server configuration
```json
{
  "delta_compression": {
    "omit_delta_algorithm": true
  }
}
```

**Step 3:** Deploy and monitor
- Verify clients decode deltas correctly
- Check for client-side errors about missing algorithm field

## Troubleshooting

### Tags Not Being Stripped

**Problem:** Tags still appear in messages despite `enable_tags: false`

**Possible causes:**
1. Per-channel override is set to `true`
2. Configuration not reloaded after change
3. Delta compression not enabled for the channel
4. Tags are being added after the stripping logic (check custom middleware)

**Solution:**
```bash
# Check effective configuration
grep -A 20 "delta_compression" config/config.json

# Verify per-channel settings
grep -A 10 "channel_delta_compression" config/config.json

# Restart server to reload configuration
```

### Tag Filtering Not Working

**Problem:** Tag filtering doesn't work after setting `enable_tags: false`

**Solution:** This is a misunderstanding. Tag filtering is **server-side** and works regardless of `enable_tags` setting:
- Tags are used for filtering on the server
- Tags are stripped AFTER filtering (if `enable_tags: false`)
- Clients receive filtered messages without tags

If filtering truly isn't working:
1. Check `tag_filtering.enabled: true` in config
2. Verify filter syntax is correct
3. Check server logs for filter evaluation messages
4. This is NOT related to the `enable_tags` setting

### Client Errors After Enabling Optimizations

**Problem:** Clients fail to process messages after enabling optimizations

**Solution:**
1. Check client library compatibility with missing fields
2. Update client code to handle optional `algorithm` field
3. Ensure clients don't depend on tags if `enable_tags: false`

## See Also

- [Delta Compression Overview](./DELTA_COMPRESSION.md)
- [Tag Filtering](./TAG_FILTERING.md)
- [Horizontal Scaling with Delta Compression](./DELTA_COMPRESSION_HORIZONTAL_IMPLEMENTATION.md)