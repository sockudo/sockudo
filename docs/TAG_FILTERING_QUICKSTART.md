# Tag Filtering Quick Start Guide

Get started with publication filtering by tags in under 5 minutes!

## What is Tag Filtering?

Tag filtering allows clients to subscribe to channels with server-side filters, so they only receive messages matching specific criteria. This dramatically reduces bandwidth usage and processing overhead.

**Example:** Subscribe to a sports channel but only receive goal events, not every pass and tackle.

**Note:** This feature is **disabled by default** for Pusher backward compatibility. You must enable it first (see Configuration section below).

## Configuration

Before using tag filtering, you must enable it:

### Environment Variable

```bash
TAG_FILTERING_ENABLED=true
```

### Configuration File

```json
{
  "tag_filtering": {
    "enabled": true
  }
}
```

### Docker Compose

```yaml
services:
  sockudo:
    image: sockudo:latest
    environment:
      - TAG_FILTERING_ENABLED=true
```

## Quick Example

### 1. Publishing with Tags (Server-Side)

Add tags to your events via the REST API:

```bash
curl -X POST http://localhost:6001/apps/my-app/events \
  -H "Content-Type: application/json" \
  -d '{
    "name": "match_event",
    "channel": "match:123",
    "data": "{\"minute\":\"23\",\"player\":\"Mbappe\"}",
    "tags": {
      "event_type": "goal",
      "team": "Real Madrid"
    }
  }'
```

### 2. Subscribing with Filter (Client-Side)

Subscribe with a filter to only receive specific events:

```typescript
import Pusher, { Filter } from 'sockudo-js';

const pusher = new Pusher('my-app-key', {
  wsHost: 'localhost',
  wsPort: 6001
});

// Only receive goal events
const channel = pusher.subscribe('match:123', Filter.eq('event_type', 'goal'));

channel.bind('match_event', (data) => {
  console.log('Goal!', data);
  // This will ONLY fire for goals, not shots, passes, etc.
});
```

That's it! The server will automatically filter messages and only send those matching your criteria.

## Common Patterns

### Filter by Event Type

```typescript
// Single event type
Filter.eq('event_type', 'goal')

// Multiple event types
Filter.in('event_type', ['goal', 'penalty', 'red_card'])
```

### Filter by Value Range

```typescript
// Only high-probability shots
Filter.and(
  Filter.eq('event_type', 'shot'),
  Filter.gte('xG', '0.8')
)
```

### Combine Multiple Conditions

```typescript
// Goals OR dangerous shots
Filter.or(
  Filter.eq('event_type', 'goal'),
  Filter.and(
    Filter.eq('event_type', 'shot'),
    Filter.gte('xG', '0.8')
  )
)
```

## Available Operators

### Comparison
- `Filter.eq(key, val)` - Equals
- `Filter.neq(key, val)` - Not equals
- `Filter.in(key, [vals])` - In set
- `Filter.nin(key, [vals])` - Not in set
- `Filter.gt(key, val)` - Greater than (numeric)
- `Filter.gte(key, val)` - Greater than or equal (numeric)
- `Filter.lt(key, val)` - Less than (numeric)
- `Filter.lte(key, val)` - Less than or equal (numeric)

### String Operations
- `Filter.startsWith(key, val)` - String starts with
- `Filter.endsWith(key, val)` - String ends with
- `Filter.contains(key, val)` - String contains

### Existence
- `Filter.exists(key)` - Key exists
- `Filter.notExists(key)` - Key doesn't exist

### Logical
- `Filter.and(...filters)` - All must match
- `Filter.or(...filters)` - At least one must match
- `Filter.not(filter)` - Negate filter

## Real-World Examples

### Stock Market

```typescript
// Only track specific tickers above a price
const filter = Filter.and(
  Filter.in('ticker', ['GOOGL', 'AAPL', 'MSFT']),
  Filter.gte('price', '100.00')
);
const channel = pusher.subscribe('stocks', filter);
```

### IoT Sensors

```typescript
// Only critical alerts
const filter = Filter.or(
  Filter.eq('severity', 'critical'),
  Filter.and(
    Filter.eq('severity', 'high'),
    Filter.gte('temperature', '100')
  )
);
const channel = pusher.subscribe('sensors', filter);
```

### Gaming

```typescript
// Only rare items and achievements
const filter = Filter.or(
  Filter.gte('item_rarity', '4'),
  Filter.eq('type', 'achievement')
);
const channel = pusher.subscribe('game-events', filter);
```

### Social Media

```typescript
// Filter by hashtags
const filter = Filter.contains('hashtags', 'breaking');
const channel = pusher.subscribe('feed', filter);
```

## Performance Benefits

### Without Filtering
```
Server: 1000 messages/sec → Network → Client: 1000 messages/sec
Client processes all 1000 messages, but only uses 50
Result: 95% wasted bandwidth and processing
```

### With Filtering
```
Server: 1000 messages/sec → Filter → Client: 50 messages/sec
Client receives and processes only 50 messages
Result: 95% bandwidth saved!
```

**Real-world savings:**
- 60-90% bandwidth reduction in high-volume channels
- Reduced battery drain on mobile devices
- Lower data costs on metered connections

## Configuration Requirements

**Before using tag filtering, you MUST enable it:**

Tag filtering is disabled by default to maintain backward compatibility with Pusher protocol.

Enable via environment variable:
```bash
TAG_FILTERING_ENABLED=true
```

Or in configuration file:
```json
{
  "tag_filtering": {
    "enabled": true
  }
}
```

When disabled:
- Filters in subscriptions are ignored
- All messages are delivered to all subscribers
- No filtering overhead

When enabled:
- Clients can specify filters
- Server-side filtering is applied
- Bandwidth is reduced

## Important Notes

### Messages Need Tags

Filters only work on messages that have tags. Messages without tags won't match any filter (even existence checks).

```typescript
// This message will be filtered
{
  "data": {...},
  "tags": {
    "event_type": "goal"
  }
}

// This message will NOT be sent to clients with filters
{
  "data": {...}
  // No tags!
}
```

### All Values are Strings

Tag values are always strings. Numeric comparisons parse strings automatically:

```typescript
// Correct
Filter.gt('xG', '0.8')  // Value as string

// Not recommended (will be converted to string anyway)
Filter.gt('xG', 0.8)
```

### Filters Don't Add Security

Filters only reduce bandwidth - they don't add permission checks. Use channel permissions for security:

```typescript
// Filters are for efficiency
const channel = pusher.subscribe('match:123', Filter.eq('event_type', 'goal'));

// Permissions are for security
const privateChannel = pusher.subscribe('private-user-data', authToken);
```

## Updating Filters

To change a filter, unsubscribe and resubscribe:

```typescript
// Initial subscription
let channel = pusher.subscribe('match:123', Filter.eq('event_type', 'goal'));

// Change filter
pusher.unsubscribe('match:123');
channel = pusher.subscribe('match:123', Filter.in('event_type', ['goal', 'penalty']));
```

## Validation

Filters are validated automatically on the server. You can also validate client-side:

```typescript
import { validateFilter } from 'sockudo-js';

const filter = Filter.eq('event_type', 'goal');
const error = validateFilter(filter);

if (error) {
  console.error('Invalid filter:', error);
} else {
  pusher.subscribe('match:123', filter);
}
```

## Best Practices

### 1. Keep Filters Simple

```typescript
// Good - simple and fast
Filter.eq('type', 'goal')

// Good - moderate complexity
Filter.in('type', ['goal', 'penalty'])

// Avoid - overly complex
Filter.and(
  Filter.or(...10 filters...),
  Filter.or(...10 filters...),
  Filter.not(Filter.and(...5 filters...))
)
```

### 2. Use Set Operations

```typescript
// Good - use 'in' for multiple values
Filter.in('type', ['goal', 'shot', 'penalty'])

// Less efficient - multiple 'or' conditions
Filter.or(
  Filter.eq('type', 'goal'),
  Filter.eq('type', 'shot'),
  Filter.eq('type', 'penalty')
)
```

### 3. Design Your Tag Schema

Plan your tags based on how clients will filter:

```javascript
// Good - filterable structure
{
  tags: {
    event_type: "goal",
    team: "Real Madrid",
    priority: "high"
  }
}

// Bad - nested data
{
  tags: {
    metadata: '{"event":"goal","team":"Real Madrid"}'
  }
}
```

## Troubleshooting

### "Not receiving any messages"

1. Check that publications have tags
2. Verify filter syntax with `validateFilter()`
3. Test without filter to confirm messages arrive
4. Check tag values match exactly (case-sensitive)

### "Filter not working as expected"

1. All comparisons are case-sensitive
2. Numeric operators require numeric strings
3. Messages without tags won't match any filter

### "Performance issues"

1. Simplify complex nested filters
2. Use `in` instead of multiple `or` conditions
3. Reduce number of tags per message

## Next Steps

- Read the [complete documentation](TAG_FILTERING.md) for advanced usage
- Check out [examples/tag_filtering.rs](../examples/tag_filtering.rs) for more examples
- Review [TAG_FILTERING_IMPLEMENTATION.md](../TAG_FILTERING_IMPLEMENTATION.md) for technical details

## Support

If you encounter issues:

1. Check the [troubleshooting section](#troubleshooting)
2. Review the [full documentation](TAG_FILTERING.md)
3. Open an issue on GitHub with a minimal reproduction

---

**Start filtering and save bandwidth today!** 🚀