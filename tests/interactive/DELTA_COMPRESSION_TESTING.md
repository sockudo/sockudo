# Delta Compression Testing Guide

## Overview

The interactive test client now supports delta compression testing with a dedicated UI panel that shows real-time statistics and allows enabling/disabling compression on the fly.

## Features

### 1. Delta Compression Toggle
- **Location**: Delta Compression panel in the UI
- **Functionality**: Enable/disable delta compression after connecting to the server
- **Behavior**: 
  - Disabled until connection is established
  - Sends `pusher:enable_delta_compression` event when checked
  - Clears compression state when unchecked

### 2. Real-time Statistics
The UI displays:
- **Status**: Whether delta compression is enabled (Yes/No)
- **Delta Messages**: Count of messages received as deltas
- **Full Messages**: Count of full messages received
- **Bandwidth Saved**: Total bytes saved with percentage

### 3. Message Handling

#### Delta Messages (`pusher:delta`)
- Received when server sends a delta-compressed message
- Format: `{ event: "event-name", delta: "base64-encoded-delta" }`
- Client decodes using fossil-delta algorithm
- Stores decoded message as base for next delta

#### Full Messages
- Regular Pusher messages received on subscribed channels
- Tracked for bandwidth comparison statistics
- Stored as base messages for future delta compression

## How to Test

### 1. Start the Server
```bash
# From project root
cargo run --release

# Or with Redis for multi-node testing
ADAPTER_DRIVER=redis cargo run --release
```

### 2. Start the Interactive Test Client
```bash
cd test/interactive
npm install  # First time only
npm start
```

### 3. Open the Dashboard
Navigate to: http://localhost:3000

### 4. Test Delta Compression

#### Basic Testing
1. Click "Connect" to establish WebSocket connection
2. Subscribe to a channel (e.g., `test-channel`)
3. Enable delta compression using the toggle switch
4. Wait for "✅ Delta compression enabled" confirmation
5. Trigger events using the "Server Events" panel
6. Watch the statistics update in real-time

#### Pattern Testing
Test different channel patterns with per-channel compression settings:

1. **Test Channel with Fossil Compression**:
   ```bash
   # Configure in config.json:
   {
     "channel_delta_compression": {
       "test-channel": "Fossil"
     }
   }
   ```
   - Subscribe to `test-channel`
   - Enable compression
   - Send multiple similar events
   - Observe delta messages and bandwidth savings

2. **Test Channel with No Compression**:
   ```bash
   {
     "channel_delta_compression": {
       "no-compress-channel": "None"
     }
   }
   ```
   - Subscribe to `no-compress-channel`
   - Enable compression globally
   - Send events
   - Verify only full messages are received (no deltas)

3. **Wildcard Pattern Testing**:
   ```bash
   {
     "channel_delta_compression": {
       "private-*": "Xdelta3",
       "*-notifications": "Fossil",
       "exact-channel": "None"
     }
   }
   ```
   - Test various channel names matching patterns
   - Verify correct algorithm is applied

### 5. Monitor Results

#### In the Events Log
- Look for events marked with `(delta)` suffix
- Check for "✅ Delta compression enabled" confirmation
- Watch for any error messages

#### In the Statistics Panel
- **Delta Messages**: Should increase with each delta-compressed event
- **Bandwidth Saved**: Should show significant savings (typically 60-90%)
- **Bandwidth calculation**: `(uncompressed - compressed) / uncompressed * 100%`

#### In Browser Console
- Open DevTools (F12)
- Check console for detailed delta decoding logs
- Look for any warnings about missing base messages

## Expected Behavior

### Successful Delta Compression
```
Statistics:
- Status: Yes
- Delta Messages: 15
- Full Messages: 1
- Bandwidth Saved: 8.45 KB (75.2%)
```

Events Log:
```
[System] 🗜️ Requesting delta compression...
[System] ✅ Delta compression enabled
[Custom] 📡 update-event on test-channel (delta)
[Custom] 📡 update-event on test-channel (delta)
...
```

### Per-Channel Configuration
- Channels configured with "None" should never send deltas
- Channels with "Fossil" or "Xdelta3" should send deltas after first full message
- Channels with "Inherit" use server default algorithm

## Troubleshooting

### No Delta Messages Received
1. **Check compression is enabled**:
   - Toggle should be checked
   - Status should show "Yes"
   
2. **Check channel configuration**:
   - Verify channel isn't configured with "None"
   - Check wildcard patterns match correctly

3. **Check base message**:
   - First message on each channel is always full
   - Subsequent similar messages should be deltas

### Decoding Errors
If you see "Failed to decode delta message" errors:
1. **Check fossil-delta library loaded**:
   - Open console and type `fossilDelta`
   - Should show the decoder object
   
2. **Check base message exists**:
   - Must have received at least one full message on the channel
   - Clear and reconnect if state is corrupted

3. **Check message format**:
   - Delta should be base64-encoded
   - Event name should match original event

### Statistics Not Updating
1. Verify events are being received (check Events Log)
2. Check browser console for JavaScript errors
3. Refresh page and reconnect

## Advanced Testing

### Batch Events
1. Select a channel in "Server Events"
2. Click "Send Batch Events"
3. Observe multiple similar events sent rapidly
4. Watch compression statistics improve

### Multi-Channel Testing
1. Subscribe to multiple channels with different compression settings
2. Trigger events on each channel
3. Verify per-channel compression works independently
4. Check statistics aggregate correctly

### Long-Running Test
1. Enable compression
2. Let the connection run for extended period
3. Periodically trigger events
4. Monitor bandwidth savings accumulate

## Configuration Examples

### Global Compression with Exceptions
```json
{
  "delta_compression": {
    "enabled": true,
    "algorithm": "Fossil"
  },
  "channel_delta_compression": {
    "realtime-*": "Fossil",
    "bulk-*": "None",
    "critical-alerts": "None"
  }
}
```

### Algorithm Comparison
```json
{
  "channel_delta_compression": {
    "fossil-test": "Fossil",
    "xdelta-test": "Xdelta3"
  }
}
```

Subscribe to both channels and compare performance.

## Performance Metrics

### Typical Results
- **Small updates** (JSON fields): 80-95% bandwidth savings
- **Medium updates** (partial object): 60-80% bandwidth savings  
- **Large changes**: 30-60% bandwidth savings
- **Completely different messages**: ~0% savings (full message sent)

### Delta Message Overhead
- Base64 encoding: ~33% overhead on delta size
- Fossil-delta metadata: ~10-20 bytes per delta
- Total overhead typically < 5% of original message

## Known Limitations

1. **First message**: Always sent as full message (establishes base)
2. **Browser support**: Requires modern browser with TextEncoder/TextDecoder
3. **Memory**: Stores one base message per channel per client
4. **Order dependency**: Messages must arrive in order for correct decoding

## Next Steps

After basic testing works:
1. Test with real application scenarios
2. Compare Fossil vs Xdelta3 performance for your use case
3. Configure per-channel compression based on message patterns
4. Monitor bandwidth savings in production
