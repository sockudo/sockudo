# Publication Filtering by Tags

## Overview

Publication filtering by tags is a powerful opt-in feature that allows clients to subscribe to channels with server-side filters, ensuring that only publications with matching tags are delivered to subscribers. This significantly reduces bandwidth usage and client-side processing overhead, particularly beneficial for mobile devices and applications with high-volume channels where clients only need a subset of messages.

**Note:** This feature is disabled by default to maintain backward compatibility with Pusher protocol. You must explicitly enable it via configuration.

## Design Goals

- **Zero-Allocation Performance**: Filtering uses zero allocations during evaluation in the hot broadcast path
- **Protocol Compatibility**: Filters are fully JSON compatible and easy to serialize/deserialize
- **Programmatic Construction**: Filters can be easily built programmatically without string templating
- **Security**: Filters only filter based on data that subscribers can already see in publications

## How It Works

### Server-Side

1. **Tags on Publications**: Each publication can have optional tags (key-value string pairs)
2. **Filters on Subscriptions**: Clients specify filters when subscribing to channels
3. **Evaluation During Broadcast**: Filters are evaluated with zero allocations during message broadcast
4. **Filtered Delivery**: Only messages matching the filter are sent to that subscriber

### Performance Characteristics

- **Compilation**: Filter validation happens once at subscription time (~100-200ns)
- **Evaluation**: Single filter evaluation takes ~10-95ns depending on complexity
- **Memory**: Zero allocations per evaluation (hot path optimization)
- **Scale**: 10k subscribers evaluated in ~100-900μs depending on filter complexity

## Server Implementation

### Adding Tags to Publications

Tags are added to publications in the HTTP API or via internal broadcasting:

```rust
use ahash::AHashMap;

// Via HTTP API
let mut tags = AHashMap::new();
tags.insert("event_type".to_string(), "goal".to_string());
tags.insert("team".to_string(), "Real Madrid".to_string());

let mut message = PusherMessage::channel_event("match_event", "match:123", event_data);
message.tags = Some(tags);
```

When triggering events via REST API:

```bash
POST /apps/:app_id/events
Content-Type: application/json

{
  "name": "match_event",
  "channel": "match:123",
  "data": "{\"minute\":\"23.30\",\"event_type\":\"goal\"}",
  "tags": {
    "event_type": "goal",
    "team": "Real Madrid"
  }
}
```

### Filter Structure

Filters are represented as a tree structure using `FilterNode`:

```rust
pub struct FilterNode {
    pub op: Option<String>,        // Logical operator: "and", "or", "not"
    pub key: Option<String>,        // Key for comparison (leaf nodes)
    pub cmp: Option<String>,        // Comparison operator
    pub val: Option<String>,        // Single value
    pub vals: Vec<String>,          // Multiple values (for in/nin)
    pub nodes: Vec<FilterNode>,     // Child nodes (for logical ops)
}
```

### Supported Operators

#### Comparison Operators (Leaf Nodes)

- **`eq`**: Equality (`tag_value == filter_value`)
- **`neq`**: Inequality (`tag_value != filter_value`)
- **`in`**: Set membership (`tag_value in filter_values`)
- **`nin`**: Set non-membership (`tag_value not in filter_values`)
- **`ex`**: Key exists in tags
- **`nex`**: Key does not exist in tags
- **`sw`**: String starts with
- **`ew`**: String ends with
- **`ct`**: String contains
- **`gt`**: Numeric greater than
- **`gte`**: Numeric greater than or equal
- **`lt`**: Numeric less than
- **`lte`**: Numeric less than or equal

#### Logical Operators (Branch Nodes)

- **`and`**: All child nodes must match
- **`or`**: At least one child node must match
- **`not`**: Negates the single child node

### Building Filters Programmatically

```rust
use sockudo::filter::FilterNodeBuilder;

// Simple equality filter
let filter = FilterNodeBuilder::eq("event_type", "goal");

// Complex nested filter
let filter = FilterNodeBuilder::or(vec![
    FilterNodeBuilder::eq("event_type", "goal"),
    FilterNodeBuilder::and(vec![
        FilterNodeBuilder::eq("event_type", "shot"),
        FilterNodeBuilder::gte("xG", "0.8"),
    ]),
]);
```

## Client Implementation (JavaScript/TypeScript)

### Basic Usage

```typescript
import Pusher, { Filter } from 'sockudo-js';

const pusher = new Pusher('app-key', {
  wsHost: 'localhost',
  wsPort: 6001,
});

// Subscribe with a simple filter
const channel = pusher.subscribe('match:123', Filter.eq('event_type', 'goal'));

channel.bind('match_event', (data) => {
  // Only receives goal events
  console.log('Goal!', data);
});
```

### Filter Builder API

The `Filter` object provides a clean API for building filters:

```typescript
import { Filter } from 'sockudo-js';

// Equality
Filter.eq('event_type', 'goal')

// Inequality
Filter.neq('event_type', 'pass')

// Set membership
Filter.in('event_type', ['goal', 'shot', 'penalty'])

// Set non-membership
Filter.nin('event_type', ['pass', 'tackle'])

// Existence checks
Filter.exists('xG')
Filter.notExists('penalty')

// String operations
Filter.startsWith('ticker', 'GOOG')
Filter.endsWith('ticker', 'L')
Filter.contains('description', 'important')

// Numeric comparisons
Filter.gt('xG', '0.8')
Filter.gte('price', '100.00')
Filter.lt('count', '10')
Filter.lte('score', '3')

// Logical operations
Filter.and(
  Filter.eq('event_type', 'shot'),
  Filter.gte('xG', '0.8')
)

Filter.or(
  Filter.eq('event_type', 'goal'),
  Filter.eq('event_type', 'penalty')
)

Filter.not(
  Filter.eq('event_type', 'pass')
)
```

### Complex Filter Examples

#### Football Match Events

Filter for goals OR high-probability shots:

```typescript
const filter = Filter.or(
  Filter.eq('event_type', 'goal'),
  Filter.and(
    Filter.eq('event_type', 'shot'),
    Filter.gte('xG', '0.8')
  )
);

const channel = pusher.subscribe('match:123', filter);
```

#### Stock Market Tickers

Filter for specific tickers with price threshold:

```typescript
const filter = Filter.and(
  Filter.in('ticker', ['GOOGL', 'AAPL', 'MSFT']),
  Filter.gte('price', '100.00')
);

const channel = pusher.subscribe('stock-prices', filter);
```

#### IoT Sensor Data

Filter for critical sensor readings:

```typescript
const filter = Filter.or(
  Filter.and(
    Filter.eq('sensor_type', 'temperature'),
    Filter.gt('value', '100')
  ),
  Filter.and(
    Filter.eq('sensor_type', 'pressure'),
    Filter.lt('value', '20')
  )
);

const channel = pusher.subscribe('iot-sensors', filter);
```

### Filter Validation

Filters are automatically validated on the server, but you can validate client-side:

```typescript
import { validateFilter } from 'sockudo-js';

const filter = Filter.eq('event_type', 'goal');
const error = validateFilter(filter);

if (error) {
  console.error('Invalid filter:', error);
}
```

### Helper Functions

Use pre-built filter patterns for common use cases:

```typescript
import { FilterExamples } from 'sockudo-js';

// Filter by event type
FilterExamples.eventType('goal')

// Filter by multiple event types
FilterExamples.eventTypes(['goal', 'shot'])

// Filter by numeric range
FilterExamples.range('xG', '0.5', '0.9')

// Important events pattern
FilterExamples.importantEvents('0.8')
```

### Dynamic Filter Updates

To change filters, unsubscribe and resubscribe:

```typescript
// Initial subscription with one filter
let channel = pusher.subscribe('match:123', Filter.eq('event_type', 'goal'));

// Later, change filter
pusher.unsubscribe('match:123');
channel = pusher.subscribe('match:123', Filter.in('event_type', ['goal', 'shot']));
```

## Performance Benchmarks

### Simple Filter: `event_type == "goal"`

| Operation | Time | Allocations |
|-----------|------|-------------|
| Compilation (subscription time) | 103.5ns | 3 allocs (328 bytes) |
| Single evaluation | 11.69ns | 0 allocs |
| 10k evaluations (broadcast) | 112.08μs | 0 allocs |

### Complex Filter: `(event_type == "shot" AND xG >= "0.8") AND (count > 42 OR price >= 99.5)`

| Operation | Time | Allocations |
|-----------|------|-------------|
| Compilation (subscription time) | 206.8ns | 5 allocs (664 bytes) |
| Single evaluation | 93.78ns | 0 allocs |
| 10k evaluations (broadcast) | 923.83μs | 0 allocs |

### Bandwidth Savings

Real-world scenarios show significant bandwidth reduction:

- **High-volume channels**: 60-90% bandwidth savings when filtering specific event types
- **Mobile clients**: Reduced battery drain from less processing
- **Network costs**: Lower data usage for metered connections

## Use Cases

### 1. Sports Live Scores

```typescript
// Client only interested in goals and red cards
const filter = Filter.in('event_type', ['goal', 'red_card']);
const channel = pusher.subscribe('match:live', filter);
```

### 2. Stock Market Real-Time Data

```typescript
// Only track high-value stocks
const filter = Filter.and(
  Filter.in('ticker', watchlist),
  Filter.gte('price', '50.00')
);
const channel = pusher.subscribe('stocks', filter);
```

### 3. IoT Monitoring

```typescript
// Only critical alerts
const filter = Filter.or(
  Filter.eq('severity', 'critical'),
  Filter.eq('severity', 'high')
);
const channel = pusher.subscribe('alerts', filter);
```

### 4. Gaming Events

```typescript
// Only high-value drops and achievements
const filter = Filter.or(
  Filter.gte('item_rarity', '4'),
  Filter.eq('type', 'achievement')
);
const channel = pusher.subscribe('game-events', filter);
```

### 5. Social Media Feeds

```typescript
// Filter by hashtags
const filter = Filter.contains('hashtags', 'important');
const channel = pusher.subscribe('feed', filter);
```

## Best Practices

### 1. Design Your Tag Schema

Plan your tag structure based on how clients will filter:

```rust
// Good: Filterable tags
tags: {
  "event_type": "goal",
  "team": "Real Madrid",
  "player_id": "9",
  "minute": "23"
}

// Bad: Nested or complex structures
tags: {
  "metadata": "{\"event\":\"goal\",\"team\":\"Real Madrid\"}"
}
```

### 2. Keep Filters Simple

Simpler filters evaluate faster:

```typescript
// Good: Simple filter
Filter.eq('event_type', 'goal')

// OK: Moderate complexity
Filter.or(
  Filter.eq('event_type', 'goal'),
  Filter.eq('event_type', 'penalty')
)

// Avoid: Overly complex nested filters
Filter.and(
  Filter.or(...many nodes...),
  Filter.or(...many nodes...),
  Filter.not(Filter.and(...many nodes...))
)
```

### 3. Use Set Operations Efficiently

For multiple values, use `in` instead of multiple `or` conditions:

```typescript
// Good
Filter.in('event_type', ['goal', 'shot', 'penalty'])

// Less efficient
Filter.or(
  Filter.eq('event_type', 'goal'),
  Filter.eq('event_type', 'shot'),
  Filter.eq('event_type', 'penalty')
)
```

### 4. Validate Filters Client-Side

Catch filter errors early:

```typescript
const filter = buildFilterFromUserInput();
const error = validateFilter(filter);
if (error) {
  showErrorToUser(error);
  return;
}
pusher.subscribe(channelName, filter);
```

### 5. Consider Message Volume

Filters are most beneficial when:
- Channel has high message volume (>100 messages/second)
- Clients need only a small subset (<20% of messages)
- Many concurrent subscribers with different filter needs

## Limitations

1. **Tags must exist on publication**: Messages without tags will not match any filter (even existence checks)
2. **String-based values**: All tag values are strings (numeric comparisons parse strings as needed)
3. **No regex support**: Use `sw`, `ew`, `ct` for pattern matching
4. **Filter size**: Keep filters reasonably sized (recommended <50 nodes)
5. **No security boundary**: Filters don't add permission checks, they only reduce bandwidth

## Configuration

### Enabling Tag Filtering

Tag filtering is **disabled by default** for backward compatibility with Pusher protocol. To enable it:

#### Via Environment Variable

```bash
TAG_FILTERING_ENABLED=true
```

#### Via Configuration File

```json
{
  "tag_filtering": {
    "enabled": true
  }
}
```

#### Example with Docker Compose

```yaml
services:
  sockudo:
    image: sockudo:latest
    environment:
      - TAG_FILTERING_ENABLED=true
    # ... other config
```

**Important:** When tag filtering is disabled:
- Filters in subscription requests are ignored
- Messages with tags are delivered to all subscribers
- No filtering overhead is incurred

When tag filtering is enabled:
- Clients can specify filters when subscribing
- Messages are filtered server-side before broadcast
- Only matching messages are sent to filtered subscribers

## Comparison with CEL

We initially considered using Google's CEL (Common Expression Language) but chose a custom implementation for:

- **Performance**: 3-7x faster evaluation, 280-430x faster compilation
- **Zero allocations**: Custom filters allocate zero memory during evaluation
- **Programmatic construction**: Tree-based structure is easier to build programmatically
- **Simplicity**: Covers 95% of filtering use cases without complexity

## Migration Guide

If you're adding filters to an existing application:

1. **Add tags to publications** (backward compatible - old clients ignore tags)
2. **Update clients** to use new filter API when subscribing
3. **Monitor bandwidth** to measure savings
4. **Gradually roll out** filters to different client segments

## Troubleshooting

### Messages not being received

- **Check tags exist**: Ensure publications have the tags you're filtering on
- **Validate filter**: Use `validateFilter()` to check for syntax errors
- **Check operator**: Numeric operators require parseable numeric strings
- **Test without filter**: Verify messages arrive without filter first

### Performance issues

- **Simplify filters**: Reduce nesting depth and number of nodes
- **Use appropriate operators**: `in` for sets, not multiple `or` conditions
- **Check tag count**: Fewer tags per message = faster evaluation

### Filter not working as expected

- **Case sensitivity**: All comparisons are case-sensitive
- **String vs numeric**: Numeric operators parse strings; ensure values are numeric
- **Missing tags**: Messages without tags won't match any filter

## Further Reading

- [Centrifugo Blog Post](https://centrifugal.dev/blog/2024/10/14/publication-filtering-by-tags) - Original inspiration
- [Rust Filter Implementation](../src/filter/mod.rs) - Server-side code
- [JavaScript Filter API](../sockudo-js/src/core/channels/filter.ts) - Client-side code
