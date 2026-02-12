# Quick Test Guide - Delta Compression Fix

## 3-Step Quick Test

### Step 1: Rebuild Client
```bash
cd test/interactive
./rebuild-client.sh
```

### Step 2: Start Servers
```bash
# Terminal 1 - Sockudo Server (from project root)
cargo run --release

# Terminal 2 - Test Server (from test/interactive)
npm start
```

### Step 3: Test in Browser
Open: http://localhost:3000/delta-test-local.html

**Actions:**
1. Click "Connect" → Should see green "Connected"
2. Click "Enable Delta" → Should see confirmation
3. Click "Subscribe to Channel" → Should see success
4. Click "Send Test Message" 5-10 times

**Expected Results:**
- ✅ NO `pusher:delta_sync_error` messages
- ✅ Delta Messages counter increases (after 1st message)
- ✅ Bandwidth Saved shows percentage (60-90%)
- ✅ Console shows sequence numbers and keys

**If It Fails:**
- See DELTA_FIX_TESTING.md for detailed troubleshooting
- Check server logs with: `DEBUG=true cargo run`
- Verify pusher-local.js exists in public/ directory

## What Was Fixed

The client now properly extracts `sequence` and `conflation_key` from messages so it can strip them before storing base messages for delta compression, matching what the server stores.

**Files Changed:**
- `sockudo-js/src/core/connection/protocol/protocol.ts`
- `sockudo-js/src/core/connection/protocol/message-types.ts`
