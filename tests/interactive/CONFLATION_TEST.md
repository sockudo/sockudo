# Conflation Keys Test Suite

This test suite validates delta compression with 100+ conflation keys for market data scenarios.

## Quick Start

1. **Start Sockudo server** (in project root):
   ```bash
   cargo run --release
   ```

2. **Start the test backend server** (in test/interactive directory):
   ```bash
   cd test/interactive
   npm install
   npm start
   ```

3. **Open the test page** in your browser:
   ```
   http://localhost:3000/conflation-test.html
   ```

## How It Works

The conflation test uses the Pusher server SDK through an Express backend to send authenticated events to Sockudo. This avoids authentication errors that would occur when calling Sockudo's HTTP API directly.

**Architecture:**
```
Browser (conflation-test.html)
  ↓ WebSocket (Pusher client)
  ↓ HTTP POST /trigger-event (for sending events)
  ↓
Express Backend (server.js)
  ↓ Pusher SDK with authentication
  ↓
Sockudo Server (WebSocket + HTTP API)
```

## Test Suite

### Test 1: 100 Conflation Keys
Sends messages for 100 different assets (BTC, ETH, ADA, etc.) to verify delta compression works with many conflation keys.

### Test 2: Interleaved Messages
Sends interleaved updates for multiple assets to verify delta grouping by conflation key.

### Test 3: Cache Sync
Verifies initial cache sync on subscription with existing state.

### Test 4: Bandwidth Comparison
Compares bandwidth usage with and without conflation keys.

## Configuration

The test uses your environment variables from `.env`:
- `PUSHER_APP_KEY` - App key for WebSocket connection
- `PUSHER_APP_SECRET` - App secret for authentication
- `PUSHER_HOST` - Sockudo server host (default: localhost)
- `PUSHER_PORT` - Sockudo server port (default: 6001)

## Troubleshooting

### "auth query missing" error
Make sure you're running the test through the Express backend (`npm start`) and accessing it at `http://localhost:3000/conflation-test.html`, not opening the HTML file directly.

### Connection refused
Ensure Sockudo is running on the configured host and port.

### Delta compression not working
1. Verify delta compression is enabled in Sockudo config
2. Check browser console for delta decoding errors
3. Ensure `fossil-delta.min.js` is loaded in the HTML
