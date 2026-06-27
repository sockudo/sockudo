# Existing SDK Ecosystem — Compatibility & Enablement Prompt Plan

> **Goal:** Guarantee that the AI Transport server work ([01-server-parity-prompts.md](01-server-parity-prompts.md)) and the new `@sockudo/ai-transport` SDK ([02-sdk-prompts.md](02-sdk-prompts.md)) **cannot break any existing SDK**, and then bring the existing SDKs up to the new platform primitives — deliberately, additively, and versioned.
>
> **The ecosystem (in-tree, audited 2026-06-27 — absent from CLAUDE.md):**
>
> | Kind | SDKs |
> |---|---|
> | Client packages | `client-sdks/sockudo-js` (**`@sockudo/client`** v2.0.0, with react / vue / react-native / nativescript / worker subpaths), `sockudo-ai-transport-js` (**`@sockudo/ai-transport`** v2.0.0 adapter package), `sockudo-python`, `sockudo-dotnet`, `sockudo-flutter`, `sockudo-kotlin`, `sockudo-swift` |
> | Server (HTTP) | `server-sdks/sockudo-http-{node, php, python, ruby, go, java, dotnet, rust, swift}` |
> | Third-party (V1 canary) | vanilla `pusher-js` + official Pusher server libs (must keep working forever — V1 byte-compat) |
>
> `sockudo-js` already implements V2 resume, rewind (+`rewind_complete`), a `channel_history` action, and mutable-message reducers (`reduceMutableMessageEvent(s)`). Several others implement subsets, and `@sockudo/ai-transport` is an adapter layered on the realtime client rather than an independent socket implementation — **E0 establishes the truth.**

---

## 1. Guiding principles (apply to every prompt)

1. **Baseline before change.** No server protocol PR (plan 01, S1+) merges until E0's baseline + E1's harness exist. The harness is the tripwire; without it "additive-only" is a promise, not a guarantee.
2. **Tolerant reader, conservative writer.** Every SDK must *ignore* unknown event names, unknown JSON fields, and unknown message actions without crashing, corrupting state, or dropping the connection (log-and-skip). Every SDK must *send* only what it has always sent unless explicitly upgraded.
3. **Additive-only, semver-disciplined.** Existing public APIs and wire behavior are frozen per SDK. New capabilities = minor releases; nothing is removed or reshaped. Each SDK's existing test suite must pass **unmodified** after any change.
4. **Server defaults preserve status quo** (plan 01 §2.8): behavior-changing features (presence ungraceful grace, AI validation) default off; `idempotency_key` semantics frozen; rollup must reduce byte-identically through `sockudo-js`'s existing reducer.
5. **One source of truth for fixtures.** Forward-compat fixtures (golden "future" frames) live in the server repo (`test/ai-conformance/fixtures/forward-compat/`) and are consumed by every SDK suite — never hand-copied per SDK.

## 2. Execution order & dependencies

```
E0 (audit+baseline) → E1 (harness, CI tripwire) → E2 (client tolerance) ┬→ [server S1–S6 land]
                                                  E3 (server-SDK tolerance) ┘        │
[02-P1 = sockudo-js upgrade] ←───────────────────────────────────────────────────────┤
E4a–E4e (dotnet/flutter/kotlin/swift/python upgrades) ←───────────────────────────────┤
E5 (server HTTP SDK enablement, per-SDK) ←────────────────────────────────────────────┘
E6 (versioning, compatibility matrix, release gates) — starts with E1, finalizes last
```

**Critical sequencing rule:** E0–E3 are guard rails — run them **before or in parallel with** server S1; they protect against the server work. E4/E5 are enablement — run them **after** the server features they expose exist (S1–S6). The `sockudo-js` upgrade is **file 02's P1** (not duplicated here).

## 3. Standard Preamble — paste at the top of EVERY prompt in this file

```text
You are working in the Sockudo ecosystem (~/Desktop/Code/Rust/sockudo). The repo contains the
server (crates/) AND the full SDK ecosystem in-tree: client-sdks/{sockudo-js,
sockudo-ai-transport-js, sockudo-python, sockudo-dotnet, sockudo-flutter, sockudo-kotlin,
sockudo-swift} and server-sdks/sockudo-http-{node, php, python, ruby, go, java, dotnet, rust,
swift}. SDK directories are imported monorepo package directories, not git submodules in the
audited checkout; detect and respect each package's toolchain, CI, and conventions (read its
README/package metadata/root CI config first).
WARNING: the root CLAUDE.md is stale; trust the code. Context docs to read first:
plans/ai-transport/03-existing-sdks-prompts.md (§1 principles + the prompt you're executing),
plans/ai-transport/01-server-parity-prompts.md §1–§2 (audited server reality + compat contract),
and docs/specs/ai-transport-wire-protocol.md if it exists.

Non-negotiable rules for ALL work in this plan:

DO-NO-HARM
- Existing public APIs and wire behavior are FROZEN per SDK. Additive-only. Each SDK's existing
  test suite must pass UNMODIFIED (you may add tests, never edit existing assertions to make
  them pass — if an existing test fails, the change is wrong, not the test).
- Never hand-edit generated/vendored code; never bump major versions; never change default
  behavior of any existing option.

TOLERANT-READER REQUIREMENT (the core of this plan)
- Unknown event names → routed to generic handlers / ignored without error.
- Unknown JSON fields on known frames → ignored (no strict-decoding failures).
- Unknown message actions / enum values → log-and-skip the message, never kill the connection
  or corrupt channel state.
- Oversized-but-valid extras/metadata → passed through or ignored, never thrown on.

QUALITY
- Follow each SDK's existing lint/format/test commands (discover via its CI config; record the
  exact commands you ran in the PR description). All green before done.
- Every behavioral claim verified by a test, not by reading code alone.
```

---

## 4. Common Upgrade Spec (referenced by E4a–E4e — paste along with those prompts)

```text
COMMON UPGRADE SPEC — new platform primitives for existing client SDKs (server plan S1–S6).
All items ADDITIVE (minor release). Wire details: docs/specs/ai-transport-wire-protocol.md.
Skip any item whose server feature is not yet merged — never ship client support for
nonexistent server behavior; record skipped items in the PR description.

U1. CAPABILITY-TOKEN AUTH (server S5): connection options token | authUrl | authCallback
    (Ably-style (tokenParams, callback) signature where idiomatic); proactive refresh at 80%
    of token lifetime; in-place re-auth frame (sockudo:auth); token_expired (40142) → refresh
    and retry; revoked → surface typed auth error. Verified clientId exposed from the
    connection-established payload. Existing Pusher key/HMAC auth untouched.
U2. PRESENCE UPDATE (server S6): presence.update(data) on presence channels; handle
    sockudo_internal:presence_update events; presence snapshot/get() exposes each member's
    current data. Document the ordering caveat (presence not strictly ordered vs messages).
U3. HISTORY + UNTILATTACH (server S4): extend the SDK's channel-history call (or add one if
    absent) with until_attach, serial bounds, limit, direction, cursor pagination → typed
    paginated result (items, hasNext, next). Surface the subscription's attach_serial
    (additive field on subscription_succeeded).
U4. MUTATION PUBLISH ACKS (server S1/S2): client APIs for versioned-message operations —
    create / append / update / delete addressed by message_serial, optional client-minted
    message_serial + message_id (idempotent create) + op_id (idempotent mutation); acks return
    { message_serial, history_serial, version_serial? } with a 10s correlation timeout →
    typed error. If the SDK already has consume-side mutable-message support, align types with
    it; if it has publish-side, only add the missing fields.
U5. ROLLUP PARAM (server S3): connection/transport param append_rollup_window ∈
    {0,20,40,100,500} validated locally; document that delivery cadence of appends may change
    while reduced state is identical.
U6. EXTRAS PASSTHROUGH: verify extras (incl. an `ai` tier) survives publish→deliver→state
    untouched and is readable on delivered messages; fix only if dropped.
U7. SERIAL SAFETY: history_serial/delivery_serial are u64 — verify no numeric truncation in
    the language (JS: BigInt/string-compare; Dart/Kotlin/Swift/C#: 64-bit ints fine but check
    JSON decoding paths); test the >2^53 boundary regardless of language.

TESTING BAR (per SDK): new unit tests per item (fake timers for refresh/ack timeout; pagination
walking; boundary serials); integration against the AI-enabled server (pinned SHA) covering:
token auth + refresh, mutation round-trip with acks, gapless until_attach (seeded randomized
runs), presence update flow, rollup-on vs rollup-off reduced-state equality. Existing suite
unmodified and green. API diff reviewed as purely additive.
```

---

## 5. The prompts

---

### E0 — SDK ecosystem audit & compatibility baseline

**Depends on:** nothing. Run **first**, before any server protocol work merges.

```text
Produce the authoritative audit of the in-tree SDK ecosystem and record the compatibility
baseline that all later work is measured against. No production code changes. Output:
docs/specs/sdk-ecosystem-audit.md + committed baseline artifacts.

SCOPE
1. INVENTORY — for each of the 7 client packages and 9 server SDKs: name, package id + published
   version, language/toolchain, repo/import status (branch, last commit, CI setup), exact
   test/lint/build commands, and runtime targets (browser/node/mobile/etc.).
2. PROTOCOL COVERAGE MATRIX — for each client SDK, determine (from code, with file refs) which
   protocol features it implements: V1 Pusher, V2 (which prefix handling), connection recovery/
   resume, rewind, channel_history, versioned/mutable messages (consume side? publish side?),
   delta compression, tag filtering, presence (incl. any update support), extras handling,
   push device registration, auth modes. For each server SDK: trigger/batch endpoints used,
   signing implementation, webhook validation/parsing helpers (CRITICAL — these parse event
   arrays and may be strict), channel-info queries, any V2-specific fields.
3. PARSER-STRICTNESS ASSESSMENT (the risk register) — for each SDK, locate every wire-decoding
   path and classify it: tolerant (ignores unknowns) vs strict (throws/fails on unknown fields,
   events, enum values). Typed-language SDKs (dotnet/kotlin/swift/flutter, and Go/Java server
   libs) are the prime suspects: check JSON decoder configs (e.g., Kotlin serialization
   ignoreUnknownKeys, Swift Decodable behavior, System.Text.Json/Newtonsoft settings, Go struct
   decoding, enum decode failure modes). Output a table: SDK → decode site (file:line) →
   strictness → break risk vs the planned additive changes (new event names ai-*, new optional
   fields on subscription_succeeded/acks, new actions, new presence event, new webhook event
   types member_updated/ai_*) → required E2/E3 fix.
4. BASELINE RUNS — run every SDK's full test suite against the CURRENT server build (pin the
   server SHA); record pass/fail + flake notes in the audit doc. Any pre-existing red is
   documented as out-of-scope-but-known (issue filed per SDK).
5. THIRD-PARTY CANARY — verify vanilla pusher-js (latest) and one official Pusher server lib
   connect/publish/subscribe against the current server; record as the V1 canary baseline.
6. GAPS feeding later prompts: list per-SDK which Common Upgrade Spec items (U1–U7) are
   relevant/absent, and which strictness fixes E2/E3 must make. Update
   plans/ai-transport/03-existing-sdks-prompts.md per-SDK notes (E4/E5 tables) if reality
   differs from assumptions.

Definition of Done: audit doc committed with file-referenced claims; baseline results recorded
(machine-readable JSON + human summary); risk register complete; per-SDK issues filed for
pre-existing failures; the E4/E5 tables in this plan corrected against findings.
```

---

### E1 — Permanent cross-SDK compatibility harness (the CI tripwire)

**Depends on:** E0. This is the gate that lets server work proceed safely.

```text
Build the permanent cross-SDK compatibility harness: a one-command rig that proves every
existing SDK keeps working against any server build — current main, the AI-enabled branch, and
every future protocol PR. This implements the cross-SDK suite that server plan S13 references;
S13 consumes this harness rather than rebuilding it.

SCOPE
1. RIG (test/sdk-compat/ in the server repo): docker-compose (or the repo's preferred tooling)
   that starts: (a) the server under test (parameterized: image/SHA + feature set + config
   profile: default-config vs ai-enabled), (b) an AI traffic generator (reuse/extend the S13
   conformance client when it exists; until then a thin script speaking raw V2) that produces
   on shared channels: canonical turn sequences (create/appends/terminal/turn events with
   extras.ai tiers), versioned-message mutations, annotations, presence enter/update/leave,
   high-rate append streams (rollup on and off), and plain V1/V2 traffic.
2. SDK LANES — one lane per SDK, each runnable standalone and in CI:
   a) SUITE LANE: run the SDK's own full test suite against the server under test (commands
      from E0's audit).
   b) TOLERANCE LANE: a small fixture app per client SDK (lives in that SDK's repo under
      test/compat/ or its idiomatic location) that: connects, subscribes to public/private/
      presence channels that the traffic generator is actively flooding with AI traffic,
      runs its normal operations (publish client events, presence, disconnect/reconnect), and
      asserts: no thrown errors, no dropped connection (beyond scripted ones), existing-feature
      behavior intact (its own messages round-trip), and — where the SDK exposes raw events —
      unknown events were delivered/ignored per the tolerant-reader rule.
   c) SERVER-SDK LANE: per HTTP lib — trigger/triggerBatch/channel-info calls behave
      byte-identically against both server profiles; webhook VALIDATION helpers accept payloads
      containing the NEW event types (member_updated, ai_turn_started, ai_turn_ended,
      message_version_created, annotation events) without rejection (signature still validates;
      unknown event names pass through parsing).
   d) V1 CANARY LANE: vanilla pusher-js + one official Pusher server lib smoke (connect, auth,
      presence, client events) — must be green against the ai-enabled profile forever.
3. FORWARD-COMPAT FIXTURES (test/ai-conformance/fixtures/forward-compat/): golden frames
   containing synthetic FUTURE constructs (unknown event names, unknown fields on every known
   frame type, unknown action values, unknown webhook event types) — the tolerance lanes replay
   these directly into each SDK's decode path where feasible (unit-level adapter per SDK,
   added in E2/E3). Single source of truth; SDK packages consume via path/workspace reference,
   never copy.
4. CI WIRING (server repo): sockudo-js suite+tolerance lanes on every PR touching protocol-
   relevant paths (path filter: crates/sockudo-protocol/**, crates/sockudo-adapter/src/handler/**,
   crates/sockudo-server/src/{ws_handler,http_handler,history*}*, webhook types) and on a
   nightly full matrix (all SDK lanes × both profiles). Failures block merge for PR lanes;
   nightly failures page/file automatically. Document runtimes; budget the PR lanes ≤ 10 min.
5. REPORTING: one summary artifact (JSON + markdown) per run: SDK × lane × profile → status,
   diffable against E0's baseline.

Definition of Done: `make sdk-compat` (or equivalent) runs the full matrix locally; CI wired
with the path filters; baseline run green and committed; server plan 01 S13 §4 updated to point
here; a deliberately-broken test PR (remove a field from a frame in a scratch branch) is
demonstrated to trip the harness (include evidence in the PR description, then revert).
```

---

### E2 — Client SDK forward-compat hardening (tolerance fixes)

**Depends on:** E0 (risk register), E1 (fixtures). One PR per realtime client SDK; this prompt is run once per client SDK — `sockudo-js`, `sockudo-python`, `sockudo-dotnet`, `sockudo-flutter`, `sockudo-kotlin`, `sockudo-swift` — substituting the SDK name. `@sockudo/ai-transport` gets adapter-level tolerance verification after `sockudo-js`, but is not an independent socket decoder.

```text
Target SDK: client-sdks/<SDK_NAME>   (run this prompt once per client SDK)

Make <SDK_NAME> a fully tolerant reader per the risk register in
docs/specs/sdk-ecosystem-audit.md (E0) — so the AI Transport server changes (new ai-* event
names, new optional fields on existing frames, new message actions, sockudo_internal:
presence_update, extras.ai tiers) can NEVER crash it, corrupt its state, or kill its
connection. Fix ONLY strictness/tolerance; no new features (those are E4/02-P1).

SCOPE
1. From E0's risk register, fix every decode site classified strict:
   - JSON decoding: configure/replace decoders to ignore unknown fields everywhere wire data
     is parsed (e.g., ignoreUnknownKeys / appropriate JsonSerializerOptions / custom Decodable
     leniency / fromJson guards) — scoped to wire-decoding types only, not the SDK's own config
     parsing.
   - Event routing: unknown event names (incl. sockudo:/sockudo_internal:-prefixed ones the SDK
     doesn't know) route to the generic/global binder if one exists, else are dropped with a
     debug log — never an exception path.
   - Enum/action decoding: unknown action / status / type values → log-and-skip that message;
     channel and connection state untouched; subsequent messages processed normally.
   - Numeric width: any path decoding serials into 32-bit or float types fixed to 64-bit-safe
     handling (language-appropriate; tests at the >2^53 and >2^31 boundaries).
   - Presence: receiving an unknown presence event type or member-data shape must not corrupt
     the member map.
2. Wire the forward-compat fixtures (E1 §3) into this SDK's unit suite via a small replay
   adapter (feed each fixture frame through the real decode path; assert: no throw, connection/
   channel state unchanged, known traffic before/after still processes). Keep the adapter tiny
   and maintained in this SDK's package; fixtures come from the server repo (path dependency or
   workspace reference —
   match how this SDK already consumes shared assets, or vendor-with-checksum if it must, with
   a CI check that detects drift).
3. Add the E1 tolerance-lane fixture app for this SDK (test/compat/ or idiomatic location) if
   E1 stubbed it.
4. Changelog entry + patch/minor release prep per this SDK's release process (tolerance fixes
   are behavior-preserving for all valid current traffic — assert via the SDK's full existing
   suite, unmodified).

Definition of Done: every risk-register entry for this SDK resolved or explicitly waived with
rationale; fixture replay suite green; existing suite green unmodified; E1 lanes green for this
SDK against BOTH server profiles; risk register updated to "tolerant" with new file refs.
```

---

### E3 — Server HTTP SDK forward-compat verification & hardening

**Depends on:** E0, E1. One PR per SDK; run once per server SDK — `sockudo-http-{node, php, python, ruby, go, java, dotnet, rust, swift}`.

```text
Target SDK: server-sdks/<SDK_NAME>   (run this prompt once per server HTTP SDK)

Guarantee <SDK_NAME> is unaffected by the AI Transport server work. Server HTTP SDKs have two
exposure surfaces: (1) REQUESTS they send (trigger/batch/channel queries — must behave
byte-identically; additive server response fields must be ignored), and (2) WEBHOOK payloads
they validate/parse (new event types arrive: member_updated, ai_turn_started, ai_turn_ended,
message_version_created, annotation events — must validate and pass through, never reject).

SCOPE
1. From E0's audit, fix any strict decode path:
   - Response parsing of /events, /batch_events, /channels[...]: unknown fields in responses
     ignored (incl. the future { serials } / ai-stats additions from server S7) — no strict DTO
     failures.
   - Webhook helpers: signature validation must operate on the raw body (verify it does — a
     helper that re-serializes parsed JSON before HMAC would break on unknown fields);
     event-array parsing must surface unknown event names as generic events (name + payload)
     rather than erroring or silently dropping the whole batch.
   - 64-bit-safe numeric handling for any serial fields surfaced.
2. Forward-compat fixtures: consume E1's webhook + response fixtures in this SDK's unit suite
   via a small replay adapter (same sourcing rules as E2 §2).
3. E1 server-SDK lane for this SDK implemented (trigger/batch/info against both server
   profiles byte-identical from the SDK-API perspective; webhook fixtures accepted).
4. Existing suite unmodified and green; changelog + patch release prep per the SDK's process.

Definition of Done: strict paths fixed or waived with rationale; fixture suite green; E1 lane
green on both profiles; audit risk register updated.
```

---

### E4a–E4e — Client SDK platform-primitive upgrades (enablement)

**Depends on:** server S1–S6 merged (each U-item gated on its server feature), E2 done for that SDK, and file 02 P1 (`sockudo-js`) done first — it's the reference implementation the others port from. Paste the **Standard Preamble (§3) + Common Upgrade Spec (§4) + the per-SDK prompt below**.

> **Note:** the `sockudo-js` upgrade is **file 02, prompt P1** — already specified there; do not duplicate. `@sockudo/ai-transport` is the adapter package and must be checked after `sockudo-js` lands, but it is not an independent realtime socket port. These five bring the rest of the realtime client fleet to parity. Run in any order after sockudo-js.

#### E4a — `sockudo-dotnet`

```text
Target SDK: client-sdks/sockudo-dotnet (run after 02-P1; use sockudo-js's landed implementation
+ its integration tests as the reference for wire shapes and edge cases).

Apply the COMMON UPGRADE SPEC (U1–U7) to sockudo-dotnet, idiomatically:
- U1 auth: TokenProvider/Func<CancellationToken, Task<string>> option alongside existing auth;
  refresh via background timer honoring 80%-lifetime; thread-safe credential swap.
- U4 acks: Task-returning mutation APIs with CancellationToken support; ack correlation via the
  SDK's existing request/response pattern (audit first); strongly-typed results.
- U3 history: IAsyncEnumerable or paginated-result idiom matching the SDK's existing patterns.
- U7: long throughout; System.Text.Json/Newtonsoft (whichever it uses) configured for 64-bit
  fidelity; boundary tests.
- Concurrency review: all new shared state thread-safe per the SDK's existing locking model.
Testing bar from the Common Spec + .NET specifics: async deadlock checks (no .Result/.Wait on
new paths), integration suite against the AI-enabled server in CI (E1 rig), API diff via
PublicApiAnalyzer (or equivalent) proving additive-only. Existing suite unmodified and green.
```

#### E4b — `sockudo-flutter`

```text
Target SDK: client-sdks/sockudo-flutter (run after 02-P1; reference sockudo-js's implementation).

Apply the COMMON UPGRADE SPEC (U1–U7) to sockudo-flutter, idiomatically:
- U1 auth: token/authCallback as Future<String> Function(); refresh via Timer respecting app
  lifecycle (background/foreground — document mobile token-refresh behavior); secure storage
  NOT in scope (document that token storage is the app's concern).
- U2/U4: Streams for presence updates and mutation events consistent with the SDK's existing
  Stream-based API; mutation acks as Futures with timeout.
- U3 history: paginated result with hasNext/next() Futures.
- U7: Dart ints are 64-bit on VM but NOT on web (dart2js → doubles past 2^53!) — if the SDK
  supports Flutter web, serials must be BigInt or String there; decide per existing platform
  support (audit pubspec + conditional imports) and test both compile targets.
- The proto/ dir suggests protobuf usage (audit): if wire decode is proto-based, extend schemas
  additively (field numbers append-only, reserved respected).
Testing bar from the Common Spec + Flutter specifics: unit tests with fake_async for refresh/
timeout; integration against the AI-enabled server (E1 rig) on VM; web-target compile + serial
boundary test if web is supported. Existing suite unmodified and green.
```

#### E4c — `sockudo-kotlin`

```text
Target SDK: client-sdks/sockudo-kotlin (run after 02-P1; reference sockudo-js's implementation).

Apply the COMMON UPGRADE SPEC (U1–U7) to sockudo-kotlin, idiomatically:
- U1 auth: suspend fun token provider + existing callback style if the SDK has one; refresh via
  the SDK's existing coroutine scope (audit structured-concurrency conventions; no GlobalScope).
- U2/U4: Flow-based presence/mutation events if the SDK is Flow-idiomatic (audit), suspend
  mutation APIs with ack timeout via withTimeout; results as data classes.
- U3 history: suspend pagination (hasNext/next()).
- U7: Long end-to-end; kotlinx.serialization configured with ignoreUnknownKeys on wire types
  (E2 should have done this — verify) and Long fidelity; boundary tests.
- Android specifics if applicable (audit targets): main-thread safety for callbacks, Doze-aware
  refresh documented.
Testing bar from the Common Spec + Kotlin specifics: runTest/virtual-time for refresh+timeouts;
integration against the AI-enabled server (E1 rig); binary/API compatibility check via
binary-compatibility-validator (or equivalent) proving additive-only. Existing suite unmodified
and green.
```

#### E4d — `sockudo-swift`

```text
Target SDK: client-sdks/sockudo-swift (run after 02-P1; reference sockudo-js's implementation).

Apply the COMMON UPGRADE SPEC (U1–U7) to sockudo-swift, idiomatically:
- U1 auth: async token provider closure (() async throws -> String) alongside existing auth;
  refresh via Task respecting the SDK's threading model (audit: delegate queues vs async/await;
  match what exists). Sendable-correctness for new types if the SDK has strict concurrency on.
- U2/U4: presence updates + mutation events through the SDK's existing delegate/callback or
  AsyncSequence pattern (audit and match); mutation acks async with timeout.
- U3 history: paginated async API.
- U7: Int64 end-to-end; Decodable wire types lenient to unknown keys (E2 verify) with explicit
  Int64 decoding (JSONDecoder default is fine but test the boundary); watch for NSNumber
  bridging pitfalls.
- Platforms: respect the SDK's declared platforms (iOS/macOS/watchOS/tvOS from its package
  manifest); CI builds all of them.
Testing bar from the Common Spec + Swift specifics: async tests with controlled clocks where
the toolchain allows; integration against the AI-enabled server (E1 rig) on macOS CI; API diff
(swift-api-digester or equivalent) proving additive-only. Existing suite unmodified and green.
```

#### E4e — `sockudo-python`

```text
Target SDK: client-sdks/sockudo-python (run after 02-P1; reference sockudo-js's implementation).

Apply the COMMON UPGRADE SPEC (U1–U7) to sockudo-python, idiomatically:
- U1 auth: async token/authCallback support alongside existing channel/user auth helpers;
  proactive refresh on the asyncio event loop with clear cancellation on disconnect.
- U2: presence.update(data) on presence channels, presence_update handling, and current member
  data projection.
- U3: add channel history if still absent, including until_attach, serial bounds, direction,
  cursor pagination, and attach_serial from subscription_succeeded.
- U4: coroutine mutation APIs with 10s ack timeout and typed acknowledgement objects.
- U5: append_rollup_window connection/transport param validation.
- U6: preserve unknown extras tiers, including extras.ai, across JSON/messagepack/protobuf paths.
- U7: Python ints are arbitrary precision, but JSON/messagepack/protobuf boundary tests must cover
  history_serial and delivery_serial above 2^53.
Testing bar from the Common Spec + Python specifics: pytest-asyncio fake/controlled timers where
practical; integration against the AI-enabled server (E1 rig); existing suite unmodified and green.
```

---

### E5 — Server HTTP SDK enablement (additive AI-publishing support)

**Depends on:** server S1, S2, S7 merged; E3 done for that SDK. Run once per server SDK, starting with `sockudo-http-node` (the reference port), then the other eight.

```text
Target SDK: server-sdks/<SDK_NAME>   (start with sockudo-http-node as the reference; later runs
port from it)

Add OPTIONAL, additive support for the AI-relevant server HTTP surface (server S1/S2/S7) so
backend agents in this language can publish AI traffic without the realtime SDK. Existing API
frozen; everything below is new, optional parameters/methods.

SCOPE (gate each item on the server feature being merged; skip + record otherwise)
1. Trigger extensions: optional per-event fields on the existing trigger/batch APIs —
   message_id (idempotent create), extras (incl. ai.transport/ai.codec tiers), action +
   target message_serial + op_id for mutations IF server S7 exposed mutations via /events;
   otherwise typed helpers for the dedicated mutation endpoints (update/append/delete/get/
   list-versions — exact paths from the S0 spec/S7 contract tests). Result types surface
   { message_serial, history_serial, version_serial } from acks.
2. History helper: typed wrapper for GET /apps/{id}/channels/{ch}/history (cursor pagination).
3. Webhook additions: typed (but tolerant) representations for the new event types
   (member_updated, ai_turn_started, ai_turn_ended, message_version_created, annotations) on
   top of E3's pass-through behavior — unknown types must STILL pass through generically.
4. Idempotency-Key support on publish calls if the SDK exposes per-request headers (else add).
5. Docs/examples: an "AI agent publishing" example (create → appends → terminal → turn events)
   in this SDK's README/examples, mirroring the canonical sequence.

TESTING: contract tests against the AI-enabled server (E1 rig) — the pure-HTTP canonical turn
(matching server S7's test) executed through THIS SDK's public API; idempotent retry returns
identical serials; existing-suite unmodified and green; API diff additive-only; signature
verification unchanged (raw-body discipline from E3 preserved).

Definition of Done: green locally + in the E1 lane for this SDK; changelog + minor release
prep; for the first run (node): document the porting checklist the other 8 runs follow.
```

---

### E6 — Versioning, compatibility matrix, and release gates

**Depends on:** E1 (starts then), finalizes after E4/E5. Lives in the server repo + each SDK repo's release process.

```text
Institutionalize the compatibility guarantees: versioning policy, a living compatibility
matrix, and release gates that make it impossible to ship a protocol change that breaks an SDK.

SCOPE
1. COMPATIBILITY MATRIX: extend docs/content/docs/reference/compatibility.mdx (it exists —
   audit its current shape) with a generated table: server version/feature-flag × SDK ×
   capability (V1, V2 core, recovery, rewind, history+untilAttach, mutable messages, presence
   update, capability tokens, rollup param, AI conventions) → supported-since version.
   Generation: a small script reading a machine-readable registry (compat-matrix.toml/json in
   the server repo) that E4/E5/02-P1 PRs must update; doc-drift test asserts the rendered table
   matches the registry.
2. VERSIONING POLICY (documented in CONTRIBUTING or docs): wire protocol changes additive-only
   under wire-protocol v1 (per server S16); SDK releases semver with API-diff gates (the
   per-language tools wired in E4); release ORDER rule: server (defaults off) → sockudo-js →
   @sockudo/ai-transport → other clients → server-http SDKs; rollback guidance per layer.
3. RELEASE GATES (CI enforcement):
   - Server repo: protocol-path-filtered PRs require the E1 sockudo-js lanes green (already
     wired in E1) + a "Protocol Change" PR-template checklist (fixtures updated? matrix registry
     updated? S0 spec updated? additive-only confirmed?). Add CODEOWNERS for
     crates/sockudo-protocol/** and the handler/webhook-type paths so protocol changes get
     deliberate review.
   - SDK repos: release workflows run their E1 lane against BOTH the latest released server and
     server main before publish; API-diff job blocks non-additive changes without a major bump.
4. SUPPORT WINDOW: document which SDK versions are supported against which server versions
   (minimum: current and previous minor), and the deprecation process (never within this plan's
   scope — AI transport ships with zero deprecations).
5. DRY-RUN: execute one full release-order rehearsal on release candidates (local registries /
   dry-run publishes) proving the order rule and gates work end-to-end; record in the doc.

Definition of Done: matrix generated + drift-tested; gates live and demonstrated (one synthetic
violation caught per gate, then reverted); policy docs merged; rehearsal recorded.
```

---

## 6. Definition of "nothing goes wrong" (this plan's acceptance checklist)

1. **E0 baseline recorded** — every SDK's suite status against current server is known and pinned.
2. **E1 harness is a merge-blocking tripwire** for protocol-touching server PRs (sockudo-js lanes per-PR; full matrix nightly; V1 pusher-js canary included).
3. **Every strict decode path** in all 14 SDKs is fixed or explicitly waived (E2/E3), with forward-compat fixtures replayed in each SDK's own unit suite from a single fixture source.
4. **Server behavior-changing defaults verified off** by the harness's default-config profile (presence grace, AI validation) — default-profile runs are byte/timing-identical to the E0 baseline.
5. **Enablement is additive-only by construction** — per-language API-diff gates prove it (E4/E5), and each SDK's pre-existing test suite passes unmodified at every step.
6. **The release order rule + compatibility matrix** make version skew safe in both directions (old SDK ↔ new server always works; new SDK ↔ old server degrades cleanly with typed "server too old" errors where a feature is absent).
