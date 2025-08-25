# Broadcast Message Latency Metrics Plan

## Current State Analysis

Currently, Sockudo does **not** have broadcast message latency metrics. The existing metrics track:
- Message counts and bytes (via `mark_api_message` and `mark_ws_message_sent`)
- Connection/disconnection counts
- Channel subscriptions
- Horizontal adapter resolve times

However, there's no end-to-end latency tracking from API call receipt to WebSocket message delivery.

## Proposed Metrics for Broadcast Latency

### 1. **Histogram Metrics (Primary Implementation)**
For broadcast latency, use **Histogram** metrics with percentile buckets (p50, p90, p95, p99):

```rust
broadcast_latency_ms: HistogramVec
```

**Labels:**
- `app_id`: Application identifier
- `channel_type`: public/private/presence/cache
- `recipient_count_bucket`: 
  - `xs`: 1-10 recipients
  - `sm`: 11-100 recipients
  - `md`: 101-1000 recipients
  - `lg`: 1001-10000 recipients
  - `xl`: 10000+ recipients

### 2. **Additional Supporting Metrics (Future Enhancement)**

**Counter Metrics:**
- `broadcast_messages_total`: Total broadcast operations
- `broadcast_recipients_total`: Total recipients across all broadcasts

**Gauge Metric:**
- `broadcast_batch_size`: Current/last broadcast batch size

### 3. **Implementation Points**

**Measurement Start:** In `http_handler.rs::events()` when API request is received
**Measurement End:** In `adapter/handler/connection_management.rs::broadcast_to_channel()` after WebSocket send

## Why These Metrics?

1. **Histograms over Averages**: Percentiles (p95, p99) better represent real user experience than averages, especially with varying recipient counts

2. **Recipient Bucketing**: Different performance characteristics for broadcasts to 10 vs 10,000 clients - bucketing helps identify scaling issues

3. **Channel Type Segmentation**: Different channel types may have different performance profiles (e.g., presence channels with member tracking)

4. **Actionable Insights**: These metrics help identify:
   - Performance degradation at scale
   - Channel-specific bottlenecks  
   - Outlier latencies affecting user experience
   - Capacity planning thresholds

## Implementation Steps

1. Add `broadcast_latency_ms` HistogramVec to PrometheusMetricsDriver
2. Pass timestamp from API handler through to broadcast function
3. Calculate and record latency at broadcast completion
4. Test with various recipient counts to verify bucketing