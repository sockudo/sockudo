# Automated Testing Guide

This directory contains comprehensive automated tests for Sockudo's delta compression, conflation keys, and tag filtering features.

## Prerequisites

1. **Bun runtime** installed (https://bun.sh)
2. **Sockudo server** running with features enabled
3. **Test backend server** running for authentication

## Setup

### 1. Configure Sockudo Server

Enable tag filtering (required for tag filtering tests):

```bash
# In your .env or config
TAG_FILTERING_ENABLED=true

# Start Sockudo
cargo run --release
```

### 2. Configure Test Environment

Update `test/interactive/.env` with your server credentials:

```bash
PUSHER_APP_ID="app-id"
PUSHER_APP_KEY="app-key"
PUSHER_APP_SECRET="app-secret"
PUSHER_HOST="localhost"
PUSHER_PORT="6001"
PUSHER_USE_TLS="false"
```

### 3. Start Test Backend Server

The test backend provides authentication for private/presence channels:

```bash
cd test/interactive
npm install  # or bun install
npm start    # Starts on port 3000
```

Keep this running in a separate terminal.

## Running Tests

### Run All Tests

```bash
cd test/interactive
bun test
```

### Run with Verbose Output

```bash
bun run test:verbose
```

### Run Specific Test Suites

```bash
# Delta compression only
bun test test-all.test.js --test-name-pattern "Delta Compression"

# Conflation keys only
bun test test-all.test.js --test-name-pattern "Conflation Keys"

# Tag filtering only
bun test test-all.test.js --test-name-pattern "Tag Filtering"

# Combined features
bun test test-all.test.js --test-name-pattern "Combined Features"

# Bandwidth savings
bun test test-all.test.js --test-name-pattern "Bandwidth Savings"
```

## Test Coverage

### Delta Compression Tests

- ✅ Enable delta compression
- ✅ Receive delta-compressed messages
- ✅ Decode delta messages correctly using fossil delta algorithm
- ✅ Verify message reconstruction accuracy

### Conflation Keys Tests

- ✅ Handle multiple conflation keys independently
- ✅ Maintain separate delta chains per conflation key
- ✅ Support 100+ concurrent conflation keys
- ✅ Delta compression efficiency per conflation key

### Tag Filtering Tests

- ✅ Simple equality filters (`eq`)
- ✅ Set membership filters (`in`, `nin`)
- ✅ Numeric comparison filters (`gt`, `gte`, `lt`, `lte`)
- ✅ Logical AND filters
- ✅ Logical OR filters
- ✅ Complex nested filters

### Combined Features Tests

- ✅ Delta compression + conflation keys
- ✅ Tag filtering + delta compression
- ✅ All three features working together
- ✅ Bandwidth savings measurement

## Expected Results

### Successful Test Run

```
🧪 Running comprehensive Sockudo tests...

Testing against: localhost:6001
App Key: app-key

✓ Delta Compression > should enable delta compression [1.2s]
✓ Delta Compression > should receive delta-compressed messages [3.5s]
✓ Delta Compression > should decode delta messages correctly [3.8s]
✓ Conflation Keys > should handle multiple conflation keys independently [4.2s]
✓ Conflation Keys > should use same conflation key for delta chain [3.1s]
✓ Conflation Keys > should handle many conflation keys (100+) [8.5s]
✓ Tag Filtering > should filter by simple equality [2.8s]
✓ Tag Filtering > should filter by set membership (in) [2.5s]
✓ Tag Filtering > should filter by numeric comparison (gte) [2.3s]
✓ Tag Filtering > should filter with AND logic [2.6s]
✓ Tag Filtering > should filter with OR logic [2.9s]
✓ Combined Features > should work with delta compression + conflation keys [3.2s]
✓ Combined Features > should work with tag filtering + delta compression [3.5s]
✓ Combined Features > should work with all three features together [4.1s]
✓ Bandwidth Savings > should achieve significant bandwidth savings [9.8s]

📊 Bandwidth Test Results:
   Full messages: 1
   Delta messages: 9
   Total full bytes: 5234
   Total delta bytes: 428
   Bandwidth saved: 89.2%

15 tests passed (58.1s)
```

## Troubleshooting

### Connection Errors

```
Error: Connection timeout
```

**Solution:** Ensure Sockudo server is running on the configured host/port:
```bash
cargo run --release
```

### Authentication Errors

```
Error: auth query missing
```

**Solution:** Ensure test backend server is running:
```bash
cd test/interactive
npm start
```

### Tag Filtering Tests Failing

```
Expected 2 events, received 4
```

**Solution:** Enable tag filtering on the server:
```bash
TAG_FILTERING_ENABLED=true cargo run --release
```

### Delta Compression Not Working

```
Expected delta messages, received full messages
```

**Possible causes:**
1. Messages are too small (< 100 bytes by default)
2. Messages are too different (delta isn't beneficial)
3. Delta compression not enabled on server

### Test Timeouts

Some tests have longer timeouts (20-30 seconds) due to:
- Multiple sequential WebSocket messages
- Sleep delays to ensure message delivery
- Large data set processing

If tests timeout, check:
- Server is responsive
- Network latency is reasonable
- Server isn't under heavy load

## Understanding Test Output

### Bandwidth Savings Metrics

The bandwidth test measures actual bytes sent over WebSocket:

```
📊 Bandwidth Test Results:
   Full messages: 1       ← First message (base)
   Delta messages: 9      ← Subsequent compressed messages
   Total full bytes: 5234 ← Size of base message
   Total delta bytes: 428 ← Total size of all deltas
   Bandwidth saved: 89.2% ← Percentage saved
```

**Good savings:** 60-90% for similar messages
**Excellent savings:** 90%+ for very similar messages with small changes

### Test Timing

- Fast tests: 1-3 seconds (simple features)
- Medium tests: 3-5 seconds (multiple messages)
- Slow tests: 5-10+ seconds (many messages, large datasets)

## Writing Custom Tests

You can add your own tests to `test-all.test.js`:

```javascript
test("my custom test", async () => {
  const pusher = createClient();
  await waitForConnection(pusher);
  
  const channel = pusher.subscribe("my-channel");
  await new Promise((resolve) => {
    channel.bind("pusher:subscription_succeeded", resolve);
  });
  
  // Your test logic here
  
  pusher.disconnect();
}, 10000);
```

## CI/CD Integration

Add to your CI pipeline:

```yaml
# Example GitHub Actions
- name: Run Sockudo Tests
  run: |
    # Start Sockudo server
    cargo run --release &
    SOCKUDO_PID=$!
    
    # Wait for server
    sleep 5
    
    # Start test backend
    cd test/interactive
    npm start &
    BACKEND_PID=$!
    
    # Wait for backend
    sleep 2
    
    # Run tests
    bun test
    
    # Cleanup
    kill $SOCKUDO_PID
    kill $BACKEND_PID
```

## Performance Benchmarks

Expected performance on typical hardware:

- **Connection time:** < 500ms
- **Message delivery:** < 100ms per message
- **Delta encoding:** 5-20μs per message (server-side)
- **Delta decoding:** 10-50μs per message (client-side)
- **Filter evaluation:** 10-95ns per filter (server-side)

## Test Architecture

```
test-all.test.js
├── Helper Functions
│   ├── createClient()        - WebSocket client factory
│   ├── waitForConnection()   - Connection waiter
│   ├── waitForEvent()        - Event listener
│   ├── enableDeltaCompression() - Delta enabler
│   └── Filter class          - Tag filter builder
│
├── Delta Compression Tests
│   ├── Enable/disable
│   ├── Message compression
│   └── Delta decoding
│
├── Conflation Keys Tests
│   ├── Multiple keys
│   ├── Delta chains
│   └── Scale (100+ keys)
│
├── Tag Filtering Tests
│   ├── Comparison operators
│   ├── Logical operators
│   └── Complex filters
│
├── Combined Features Tests
│   ├── Delta + Conflation
│   ├── Filtering + Delta
│   └── All three together
│
└── Bandwidth Tests
    └── Savings measurement
```

## Additional Resources

- [Delta Compression Documentation](../../DELTA_COMPRESSION.md)
- [Tag Filtering Documentation](../../docs/TAG_FILTERING.md)
- [Interactive Test Dashboard](./public/index.html)
- [Conflation Test Suite](./CONFLATION_TEST.md)

## Support

If tests fail unexpectedly:

1. Check server logs: `RUST_LOG=debug cargo run --release`
2. Check test backend logs in the terminal
3. Verify `.env` configuration matches server config
4. Try running interactive dashboard to manually verify features
5. Report issues at: https://github.com/your-repo/sockudo/issues
