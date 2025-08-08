# Grafana Dashboard for Sockudo WebSocket Monitoring

This directory contains a pre-configured Grafana dashboard for monitoring your Sockudo WebSocket server infrastructure.

## Dashboard Features

The dashboard provides comprehensive monitoring across several key areas:

### 📊 Overview
- Current active connections
- Connection rate (connections/sec)
- Message throughput (messages/sec)
- Overall error rate

### 🔗 Connection Health
- Connection timeline (connections vs disconnections)
- Connection errors by type
- Connection lifecycle metrics

### 💬 Message Traffic
- WebSocket message rates (inbound/outbound)
- Byte transfer rates
- Message size distribution

### 🌐 API Performance
- HTTP request rates
- API byte transfer rates
- Average request/response sizes

### 📡 Channel Management
- Active channels by type (public, private, presence)
- Channel subscription rates
- Channel lifecycle metrics

### 🛡️ Rate Limiting
- Rate limit check frequency
- Rate limit trigger rates
- Rate limiting effectiveness

### 🔄 Horizontal Scaling (Multi-node)
- Inter-node communication latency
- Node-to-node request success rates
- Horizontal adapter performance

## Setup Instructions

### 1. Import Dashboard
1. Open Grafana → Dashboards → Import
2. Upload `websocket-dashboard-example.json`
3. Configure the required variables (see below)

### 2. Configure Dashboard Variables

After importing, you'll need to configure these dashboard variables:

#### **Data Source** (`datasource`)
- Select your Prometheus data source
- Default: `prometheus`

#### **Metrics Prefix** (`metrics_prefix`)
- Free text field to set your Sockudo metrics prefix
- Default: `sockudo_`
- Change this to match your Sockudo configuration (e.g., `wss_`, `my_app_`, etc.)
- To find your prefix, check your Sockudo config or Prometheus metrics endpoint

#### **App ID** (`app_id`)
- Select which Sockudo application to monitor
- Default: "All" (shows metrics for all applications)
- Automatically populated from your metrics' `app_id` labels
- Choose a specific app ID to focus on one application's metrics
- **Note**: Requires active connections to populate the dropdown list

### 3. Verify Metrics

Ensure your Sockudo server is configured to export metrics:

```rust
// In your Sockudo configuration
metrics_driver: "prometheus",
metrics_port: 9601,
metrics_prefix: "sockudo_", // This should match your dashboard variable
```

### 4. Prometheus Configuration

Make sure your Prometheus is scraping Sockudo metrics:

```yaml
scrape_configs:
  - job_name: 'sockudo-servers'
    static_configs:
      - targets: ['localhost:9601']  # Adjust port as needed
    metrics_path: '/metrics'
```

**Note:** The dashboard uses the `app_id` label that Sockudo automatically adds to all metrics, so no additional Prometheus labels are required.

## Customization

### Filtering by Application
The `app_id` variable automatically discovers all applications from your metrics:
1. Set to "All" to see metrics across all your Sockudo applications
2. Select a specific app ID to focus on one application
3. The list updates automatically as new applications appear in metrics

### Changing Metrics Prefix
1. Edit the `metrics_prefix` text field in dashboard variables
2. Set it to match your Sockudo configuration
3. Common prefixes: `sockudo_`, `wss_`, or custom prefixes

### Multi-Application Support
The dashboard can show:
- **All Applications**: Select "All" in the App ID dropdown
- **Single Application**: Choose a specific app ID
- **Custom Filtering**: Modify queries to add additional label filters

## Troubleshooting

### No Data Appearing
1. Check that Sockudo metrics endpoint is accessible: `curl http://localhost:9601/metrics`
2. Verify Prometheus is scraping: check Prometheus targets page
3. Ensure the metrics prefix matches your Sockudo configuration
4. Verify that metrics include `app_id` labels (this should be automatic)
5. Check that the App ID variable is populated (should auto-discover from metrics)

### App ID Variable Not Loading
If the App ID dropdown shows "Error updating options":
1. Ensure your Data Source variable is set correctly
2. Verify the metrics prefix is correct (try the default `sockudo_`)
3. Check that at least one Sockudo instance is running and has active connections
4. The query looks for `{prefix}connected` metrics, so connections must exist for discovery

### Missing Metrics
Some panels may show no data if certain features aren't enabled:
- **Horizontal Scaling panels**: Only show data when using Redis/NATS adapters
- **Rate Limiting panels**: Only active when rate limiting is enabled
- **Channel panels**: Require active WebSocket connections with channel subscriptions

## Key Metrics to Monitor

### 🚨 Critical Alerts
- **Connection Error Rate** > 5%
- **Inter-node Success Rate** < 95% (multi-node setups)
- **Rate Limit Trigger Rate** > 10%

### 📈 Performance Indicators
- **Current Connections**: Monitor capacity planning
- **Message Throughput**: Track application usage
- **Inter-node Latency**: Ensure < 100ms for good performance

### 🔍 Debugging Metrics
- **Connection Errors by Type**: Identify specific issues
- **Active Channels by Type**: Understanding usage patterns
- **Byte Transfer Rates**: Network utilization

## Dashboard Maintenance

- **Refresh Rate**: 30 seconds (configurable)
- **Time Range**: Default 1 hour (adjustable)
- **Auto-refresh**: Enabled for real-time monitoring

For more information about Sockudo metrics, see the main project documentation.