# Sockudo AI Transport Conformance

Raw protocol conformance harness for the AIT-S SDK executable spec.

This suite intentionally has no Sockudo SDK dependency. It uses Node `fetch` plus the built-in
WebSocket client to exercise the server wire surface directly.

## Run

Start an AI-enabled Sockudo server, then:

```bash
cd test/ai-conformance
node src/run.mjs
```

Defaults:

- `SOCKUDO_BASE_URL=http://127.0.0.1:6001`
- `SOCKUDO_WS_URL=ws://127.0.0.1:6001/app/app-key?protocol=2&client=ait-conformance&version=0`
- `SOCKUDO_APP_ID=app-id`
- `SOCKUDO_APP_KEY=app-key`
- `SOCKUDO_APP_SECRET=app-secret`

Offline fixture validation:

```bash
AIT_CONFORMANCE_OFFLINE=1 node src/run.mjs
```

## Golden Transcripts

Golden files live in `fixtures/golden/`. Runtime serials, timestamps, UUID-like IDs, and socket IDs
are normalized before comparison. The golden transcripts are the SDK-facing executable spec for
canonical AI Transport sequences.

Forward-compat fixtures live in `fixtures/forward-compat/` and are consumed by SDK tolerance lanes.
