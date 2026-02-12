# Filter Module

Zero-allocation publication filtering for Sockudo, inspired by Centrifugo's approach.

## Overview

This module provides a high-performance filtering system for real-time message broadcasting. It allows subscribers to specify server-side filters that determine which publications they receive, dramatically reducing bandwidth usage and client-side processing overhead.

**Key Features:**
- Zero allocations during evaluation (critical for hot broadcast path)
- Comprehensive operator support (13 comparison + 3 logical operators)
- Tree-based filter structure for complex expressions
- Programmatic construction via builder pattern
- Full JSON serialization support
- ~10-95ns per filter evaluation

## Architecture

### Core Components

- **`mod.rs`**: Main matching function with zero-allocation evaluation
- **`node.rs`**: FilterNode structure and builder pattern
- **`ops.rs`**: Comparison and logical operator enums

### Design Goals

1. **Zero-Allocation Performance**: No heap allocations during evaluation in the hot broadcast path
2. **Protocol Compatibility**: Easy serialization to/from JSON for client-server communication
3. **Programmatic Construction**: Clean API for building filters without string templating
4. **Simplicity**: Cover 95% of filtering use cases without CEL-level complexity

## Quick Start

### Basic Usage

```rust
use sockudo::filter::{FilterNodeBuilder, matches};
use ahash::AHashMap;

// Create a simple filter
let filter = FilterNodeBuilder::eq("event_type", "goal");

// Create publication tags
let mut tags = HashMap::new();
tags.insert("event_type".to_string(), "goal".to_string());

// Evaluate filter (zero allocations)
let is_match = matches(&filter, &tags);
assert!(is_match);
```

### Complex Filters

```rust
// Goals OR dangerous shots (xG >= 0.8)
let filter = FilterNodeBuilder::or(vec![
    FilterNodeBuilder::eq("event_type", "goal"),
    FilterNodeBuilder::and(vec![
        FilterNodeBuilder::eq("event_type", "shot"),
        FilterNodeBuilder::gte("xG", "0.8"),
    ]),
]);

let mut tags = HashMap::new();
tags.insert("event_type".to_string(), "shot".to_string());
tags.insert("xG".to_string(), "0.85".to_string());

assert!(matches(&filter, &tags));
```

## Filter Structure

Filters are tree structures represented by `FilterNode`:

```rust
pub struct FilterNode {
    pub op: Option<String>,        // Logical operator: "and", "or", "not"
    pub key: Option<String>,        // Tag key (leaf nodes)
    pub cmp: Option<String>,        // Comparison operator
    pub val: Option<String>,        // Single value
    pub vals: Vec<String>,          // Multiple values (for in/nin)
    pub nodes: Vec<FilterNode>,     // Child nodes (for logical ops)
}
```

### Leaf Nodes (Comparisons)

Perform comparisons on tag values:

```rust
// Equality
FilterNodeBuilder::eq("event_type", "goal")

// Numeric comparison
FilterNodeBuilder::gte("xG", "0.8")

// Set membership
FilterNodeBuilder::in_set("event_type", &["goal", "shot", "penalty"])

// String operations
FilterNodeBuilder::starts_with("ticker", "GOOG")
FilterNodeBuilder::ends_with("ticker", "L")
FilterNodeBuilder::contains("description", "urgent")

// Existence checks
FilterNodeBuilder::exists("optional_field")
FilterNodeBuilder::not_exists("removed_field")
```

### Branch Nodes (Logical Operations)

Combine multiple filters:

```rust
// AND: All children must match
FilterNodeBuilder::and(vec![
    FilterNodeBuilder::eq("event_type", "shot"),
    FilterNodeBuilder::gte("xG", "0.8"),
])

// OR: At least one child must match
FilterNodeBuilder::or(vec![
    FilterNodeBuilder::eq("event_type", "goal"),
    FilterNodeBuilder::eq("event_type", "penalty"),
])

// NOT: Negates the child
FilterNodeBuilder::not(
    FilterNodeBuilder::eq("event_type", "pass")
)
```

## Operators

### Comparison Operators

| Operator | Code | Description | Example |
|----------|------|-------------|---------|
| Equal | `eq` | Exact match | `eq("type", "goal")` |
| Not Equal | `neq` | Not equal | `neq("type", "pass")` |
| In | `in` | Set membership | `in_set("type", &["goal", "shot"])` |
| Not In | `nin` | Not in set | `nin("type", &["pass"])` |
| Exists | `ex` | Key exists | `exists("xG")` |
| Not Exists | `nex` | Key not exists | `not_exists("penalty")` |
| Starts With | `sw` | String prefix | `starts_with("ticker", "GOO")` |
| Ends With | `ew` | String suffix | `ends_with("ticker", "L")` |
| Contains | `ct` | Substring | `contains("desc", "urgent")` |
| Greater Than | `gt` | Numeric > | `gt("xG", "0.8")` |
| Greater/Equal | `gte` | Numeric >= | `gte("price", "100")` |
| Less Than | `lt` | Numeric < | `lt("count", "10")` |
| Less/Equal | `lte` | Numeric <= | `lte("score", "3")` |

### Logical Operators

| Operator | Code | Description | Example |
|----------|------|-------------|---------|
| AND | `and` | All must match | `and(vec![f1, f2])` |
| OR | `or` | At least one | `or(vec![f1, f2])` |
| NOT | `not` | Negates child | `not(f1)` |

## API Reference

### FilterNodeBuilder

Static methods for constructing filters:

```rust
// Comparison operators
pub fn eq(key: impl Into<String>, val: impl Into<String>) -> FilterNode
pub fn neq(key: impl Into<String>, val: impl Into<String>) -> FilterNode
pub fn in_set(key: impl Into<String>, vals: &[impl ToString]) -> FilterNode
pub fn nin(key: impl Into<String>, vals: &[impl ToString]) -> FilterNode
pub fn exists(key: impl Into<String>) -> FilterNode
pub fn not_exists(key: impl Into<String>) -> FilterNode
pub fn starts_with(key: impl Into<String>, val: impl Into<String>) -> FilterNode
pub fn ends_with(key: impl Into<String>, val: impl Into<String>) -> FilterNode
pub fn contains(key: impl Into<String>, val: impl Into<String>) -> FilterNode
pub fn gt(key: impl Into<String>, val: impl Into<String>) -> FilterNode
pub fn gte(key: impl Into<String>, val: impl Into<String>) -> FilterNode
pub fn lt(key: impl Into<String>, val: impl Into<String>) -> FilterNode
pub fn lte(key: impl Into<String>, val: impl Into<String>) -> FilterNode

// Logical operators
pub fn and(nodes: Vec<FilterNode>) -> FilterNode
pub fn or(nodes: Vec<FilterNode>) -> FilterNode
pub fn not(node: FilterNode) -> FilterNode
```

### FilterNode

Methods on filter instances:

```rust
// Accessors
pub fn logical_op(&self) -> Option<LogicalOp>
pub fn compare_op(&self) -> CompareOp
pub fn key(&self) -> &str
pub fn val(&self) -> &str
pub fn vals(&self) -> &[String]
pub fn nodes(&self) -> &[FilterNode]

// Validation
pub fn validate(&self) -> Option<String>
```

### Matching Function

```rust
pub fn matches(filter: &FilterNode, tags: &HashMap<String, String>) -> bool
```

Zero-allocation evaluation of a filter against publication tags.

## Performance

### Benchmarks

Simple equality filter (`event_type == "goal"`):
- Compilation: 103.5ns (3 allocations at subscription time)
- Single evaluation: 11.69ns (0 allocations)
- 10k evaluations: 112.08μs (0 allocations)

Complex nested filter:
- Compilation: 206.8ns (5 allocations at subscription time)
- Single evaluation: 93.78ns (0 allocations)
- 10k evaluations: 923.83μs (0 allocations)

### Zero-Allocation Strategy

1. **Recursive evaluation**: Uses call stack, not heap
2. **String slices**: All string operations use slice methods
3. **Inline numeric parsing**: Direct f64::parse() without allocations
4. **Early exit**: AND/OR operations short-circuit on first result

## Validation

Filters are validated at subscription time:

```rust
let filter = FilterNodeBuilder::eq("event_type", "goal");

match filter.validate() {
    None => println!("Filter is valid"),
    Some(err) => println!("Error: {}", err),
}
```

Validation checks:
- Leaf nodes have required key and value
- Set operations have non-empty vals array
- Logical operations have appropriate child count
- All child nodes are recursively valid

## JSON Serialization

Filters are fully JSON-compatible:

```rust
use sonic_rs;

let filter = FilterNodeBuilder::or(vec![
    FilterNodeBuilder::eq("event_type", "goal"),
    FilterNodeBuilder::gte("xG", "0.8"),
]);

// Serialize
let json = sonic_rs::to_string(&filter).unwrap();

// Deserialize
let parsed: FilterNode = sonic_rs::from_str(&json).unwrap();

assert_eq!(filter, parsed);
```

Example JSON:
```json
{
  "op": "or",
  "nodes": [
    {
      "key": "event_type",
      "cmp": "eq",
      "val": "goal"
    },
    {
      "key": "xG",
      "cmp": "gte",
      "val": "0.8"
    }
  ]
}
```

## Examples

### Football Match Events

Filter for goals or dangerous shots:

```rust
let filter = FilterNodeBuilder::or(vec![
    FilterNodeBuilder::eq("event_type", "goal"),
    FilterNodeBuilder::and(vec![
        FilterNodeBuilder::eq("event_type", "shot"),
        FilterNodeBuilder::gte("xG", "0.8"),
    ]),
]);

// Test with goal
let mut goal_tags = HashMap::new();
goal_tags.insert("event_type".to_string(), "goal".to_string());
assert!(matches(&filter, &goal_tags));

// Test with dangerous shot
let mut shot_tags = HashMap::new();
shot_tags.insert("event_type".to_string(), "shot".to_string());
shot_tags.insert("xG".to_string(), "0.85".to_string());
assert!(matches(&filter, &shot_tags));

// Test with low-probability shot
let mut weak_shot = HashMap::new();
weak_shot.insert("event_type".to_string(), "shot".to_string());
weak_shot.insert("xG".to_string(), "0.3".to_string());
assert!(!matches(&filter, &weak_shot));
```

### Stock Market

Filter specific tickers above a price:

```rust
let filter = FilterNodeBuilder::and(vec![
    FilterNodeBuilder::in_set("ticker", &["GOOGL", "AAPL", "MSFT"]),
    FilterNodeBuilder::gte("price", "100.00"),
]);

let mut tags = HashMap::new();
tags.insert("ticker".to_string(), "GOOGL".to_string());
tags.insert("price".to_string(), "150.25".to_string());
assert!(matches(&filter, &tags));
```

### IoT Sensors

Filter critical alerts:

```rust
let filter = FilterNodeBuilder::or(vec![
    FilterNodeBuilder::eq("severity", "critical"),
    FilterNodeBuilder::and(vec![
        FilterNodeBuilder::eq("severity", "high"),
        FilterNodeBuilder::gt("temperature", "100"),
    ]),
]);
```

## Testing

Run unit tests:
```bash
cargo test filter
```

Run benchmarks:
```bash
cargo bench filter
```

Run example:
```bash
cargo run --example tag_filtering
```

## Integration

### In Broadcast Path

```rust
// In local_adapter.rs send_to_socket_with_compression()

// Get socket's filter for this channel
let channel_filter = socket_ref.get_channel_filter(channel).await;

// Apply filter if present (zero-allocation evaluation)
if let Some(filter) = channel_filter {
    if let Some(ref tags) = base_message.tags {
        if !crate::filter::matches(&filter, tags) {
            // Message doesn't match filter, skip this socket
            return Ok(());
        }
    } else {
        // Filter present but message has no tags, skip
        return Ok(());
    }
}

// Continue with message sending...
```

### In Subscription Handler

```rust
// In subscription_management.rs

// Store filter with subscription
conn_locked.subscribe_to_channel_with_filter(
    request.channel.clone(),
    request.tags_filter.clone(),
);
```

## Best Practices

### 1. Keep Filters Simple

Simpler filters evaluate faster:

```rust
// Good
FilterNodeBuilder::eq("type", "goal")

// OK
FilterNodeBuilder::or(vec![
    FilterNodeBuilder::eq("type", "goal"),
    FilterNodeBuilder::eq("type", "penalty"),
])

// Avoid unnecessary complexity
// Use in_set instead of multiple OR conditions
FilterNodeBuilder::in_set("type", &["goal", "penalty", "red_card"])
```

### 2. Use Appropriate Operators

```rust
// For multiple values, use in_set
FilterNodeBuilder::in_set("event_type", &["goal", "shot", "penalty"])

// Not this
FilterNodeBuilder::or(vec![
    FilterNodeBuilder::eq("event_type", "goal"),
    FilterNodeBuilder::eq("event_type", "shot"),
    FilterNodeBuilder::eq("event_type", "penalty"),
])
```

### 3. Numeric Values as Strings

All tag values are strings. Numeric comparisons parse automatically:

```rust
// Correct
FilterNodeBuilder::gte("xG", "0.8")

// Also works (converted to string)
FilterNodeBuilder::gte("xG", &0.8.to_string())
```

### 4. Validate at Subscription Time

Catch errors early:

```rust
let filter = build_filter_from_request(&request);

if let Some(err) = filter.validate() {
    return Err(Error::InvalidMessageFormat(format!(
        "Invalid filter: {}", err
    )));
}

// Continue with subscription...
```

## Limitations

1. **Tags Required**: Messages without tags won't match any filter
2. **String Values**: All values are strings (parsed for numeric comparisons)
3. **No Regex**: Use string operations (sw, ew, ct) for pattern matching
4. **Filter Size**: Recommended <50 nodes for optimal performance
5. **No Security**: Filters don't add permissions, only reduce bandwidth

## Comparison with CEL

We chose a custom implementation over CEL for:

| Aspect | Custom Filter | CEL |
|--------|---------------|-----|
| Compilation | 103-207ns | 28,000-86,000ns |
| Single Eval | 12-94ns | 80-334ns |
| 10k Evals | 112-924μs | 791-3,366μs |
| Allocations | 0 | 2-7 per eval |
| Programmatic | ✅ Native tree | ❌ String parsing |

## Further Reading

- [TAG_FILTERING.md](../../docs/TAG_FILTERING.md) - Complete feature documentation
- [TAG_FILTERING_QUICKSTART.md](../../docs/TAG_FILTERING_QUICKSTART.md) - Quick start guide
- [../examples/tag_filtering.rs](../../examples/tag_filtering.rs) - Working examples
- [Centrifugo Blog Post](https://centrifugal.dev/blog/2024/10/14/publication-filtering-by-tags) - Original inspiration
