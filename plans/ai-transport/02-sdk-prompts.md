# `@sockudo/ai-transport` — TypeScript SDK Prompt Plan

> **Goal:** A production-grade TypeScript SDK with **1:1 API parity** to `@ably/ai-transport` (docs + source studied in full), targeting the Sockudo AI Transport wire protocol defined in [01-server-parity-prompts.md](01-server-parity-prompts.md) and `docs/specs/ai-transport-wire-protocol.md`. The AI-Transport *semantics* — turns, conversation trees, views, codecs, cancellation, optimistic updates, Vercel `useChat` integration — all live here.
>
> **Repo:** new repository `sockudo-ai-transport-js` (sibling of the `sockudo` repo). License Apache-2.0. Package `@sockudo/ai-transport`.

---

## 1. API parity target (what "about the same API" means, exactly)

Mirror Ably's documented public surface with Sockudo branding. Studied sources: all `ably.com/docs/ai-transport/**` pages (concepts, 15 features, full JS + React API references, internals: wire protocol / codec architecture / conversation tree / transport patterns, production, troubleshooting, errors) + the complete `ably/ably-ai-transport-js@0.1.0` source.

| Surface | Ably | Sockudo (this SDK) |
|---|---|---|
| Entry points | `@ably/ai-transport`, `/react`, `/vercel`, `/vercel/react` | `@sockudo/ai-transport`, `/react`, `/vercel`, `/vercel/react` |
| Realtime substrate | `ably` (ably-js ≥2.21) as **peer dependency** | **`@sockudo/client`** (sockudo-js, in-tree at `client-sdks/sockudo-js`, v1.3+) as **peer dependency** — it already speaks V2 resume, rewind, `channel_history`, and mutable-message reduction; P1 closes its gaps |
| Client factory | `createClientTransport({ channel, codec, api, clientId?, headers?, body?, credentials?, fetch?, messages?, logger? })` | identical (`channel` is a `@sockudo/client` channel; providers accept `channelName`; see §2.3) |
| Server factory | `createServerTransport({ channel, codec, logger?, onError? })` | identical |
| Turn (server) | `transport.newTurn({ turnId, clientId?, parent?, forkOf?, onMessage?, onAbort?, onCancel?, onError?, signal? })` → `Turn { turnId, abortSignal, start(), addMessages(), streamResponse(), addEvents(), end(reason) }` | identical |
| Turn reasons | `'complete' \| 'cancelled' \| 'error'` (+ `'suspended'` on the wire/source) | `'complete' \| 'cancelled' \| 'error' \| 'suspended'` |
| Client surface | `transport.tree`, `transport.view`, `createView()`, `cancel(filter?)`, `waitForTurn(filter?)`, `stageEvents()`, `stageMessage()`, `on('error')`, `close({ cancel? })` | identical |
| View | `getMessages, flattenNodes, hasOlder, loadOlder, select, getSelectedIndex, getSiblings, hasSiblings, getNode, send, regenerate, edit, update` (+ message-anchored sibling nav from source) | identical |
| ActiveTurn | `{ stream: ReadableStream<TEvent>, turnId, cancel(), optimisticMsgIds }` | identical (+ `invocationId`, `inputEventId` like the source) |
| CancelFilter | `{ turnId? } \| { own? } \| { clientId? } \| { all? }`, default `{ own: true }` | identical |
| Codec | `Codec<TEvent,TMessage>`: `createEncoder/createDecoder/createAccumulator/isTerminal` + `createEncoderCore/createDecoderCore/createLifecycleTracker` + `ChannelWriter { publish, appendMessage, updateMessage }` | identical names; generics follow the source's richer `Codec<TInput,TOutput,TProjection,TMessage>` with docs-compatible aliases |
| Vercel | `UIMessageCodec`, pre-bound factories, `createChatTransport` → `ChatTransport { sendMessages, reconnectToStream, close, streaming, onStreamingChange }`, `prepareSendMessagesRequest(SendMessagesRequestContext)` | identical |
| React | `TransportProvider`, `useClientTransport`, `useView`, `useCreateView`, `useTree`, `useActiveTurns`, `useAblyMessages` → **`useSockudoMessages`**, `ChatTransportProvider`, `useChatTransport`, `useMessageSync` | identical shapes (skip/stub/error-field contracts included) |
| Errors | `ErrorInfo { code, message, statusCode, cause?, detail? }`, `ErrorCode` 104000–104010 block, `errorInfoIs()` | identical codes & names |

**Locked protocol decisions** (must match the server spec; do not re-litigate). ⚠️ Corrected after a code audit of Sockudo (2026-06-02): the server **already has** mutable/versioned messages, durable history, rewind, and two-tier recovery — the SDK targets those existing frames, not invented ones:
- **Content operations ride Sockudo's existing versioned-message protocol:** wire events `sockudo:message.create | message.append | message.update | message.delete` (`MessageAction` in `sockudo-protocol`). The logical message identity is **`message_serial` (string ≤128) — which IS the codec-message-id** (use the same value end-to-end; appends are addressed by `message_serial`, simpler than Ably's append-by-create-serial). Total ordering = `history_serial` (u64, per-channel publish order); recovery position = `delivery_serial` (u64); mutation versions carry `version.serial`.
- **AI lifecycle/codec events are plain channel events:** `ai-input`, `ai-output`, `ai-turn-start`, `ai-turn-end`, `ai-cancel`, with header tiers `extras.ai.transport` / `extras.ai.codec` (a convention over Sockudo's existing `extras` field; key registry per the server plan §1: `turn-id`, `turn-client-id`, `turn-reason`, `turn-continue`, `invocation-id`, `event-id`, `codec-message-id`, `stream`, `stream-id`, `status`, `discrete`, `role`, `parent`, `fork-of`, `msg-regenerate`, `error-code`, `error-message`).
- Stream status values on wire: `streaming | complete | cancelled`. Turn-end reasons: `complete | cancelled | error | suspended`.
- Streaming mechanics: `message.create` (ack returns `{ message_serial, history_serial }`) → fire-and-forget `message.append` addressed by `message_serial` (collect promises; **flush barrier** on close/cancel) → closing append with terminal status; recovery on failed appends = `message.update` with full accumulated content (failure of that = `EncoderRecoveryFailed` 104000). Late joiners/history readers receive the server-side **aggregated** message (existing `VersionStore` behavior).
- Send pipeline is **publish-then-poke**: optimistic tree insert → channel publish of turn-start + user input events → HTTP POST to the customer's `api` endpoint. POST body **always** carries the invocation pointer `{ turnId, invocationId, inputEventId, sessionName }` and **by default also** the documented fat shape `{ id, messages, history, clientId, parent, forkOf, trigger, messageId? }` (Ably docs' default: "sends all previous messages as history"); `prepareSendMessagesRequest` / `body` options can reshape it. HTTP response is `200` empty; tokens flow over the channel.
- Cancel is a **published channel message** (`ai-cancel` carrying the serialized `CancelFilter` in transport headers), never HTTP. Matching + `onCancel` authorization happen agent-side. Cancels arriving before a turn registers must still fire (early-cancel registration in `newTurn`, which is synchronous).
- `TMessage.id === codec-message-id` end-to-end (load-bearing for tree indexing and optimistic reconciliation).
- Client-side determinism invariant: two participants folding the same serial-ordered operations **must** materialize identical state.

---

## 2. Architecture & module map

```
src/
  constants.ts errors.ts event-emitter.ts logger.ts utils.ts version.ts index.ts
  realtime/            # THIN adapter over @sockudo/client (P2): ChannelLike/ClientLike seam,
    adapter.ts types.ts mocks.ts          # typed acks, history/untilAttach, presence helpers
  core/
    codec/             # P4: encoder-core, decoder-core, lifecycle-tracker, types
    transport/         # P5–P8: tree, view, stream-router, client-transport,
                       #         agent/server-transport, turn-manager, pipe-stream,
                       #         invocation, headers, decode-history
  react/               # P11
  vercel/              # P9–P10 (codec/, transport/, run-end-reason)
    react/             # P12
```

- **Two-layer split (sacred):** the transport layer manages turns/lifecycle/cancel/history/sync and never inspects domain payloads; the codec maps domain events ↔ channel operations and never manages turns.
- **Realtime substrate = `@sockudo/client` (peer dependency), exactly Ably's model** (`@ably/ai-transport` peers on `ably`). The repo already ships sockudo-js in-tree (`client-sdks/sockudo-js`, `@sockudo/client` v1.3+) with V2 resume, rewind + `rewind_complete`, a `channel_history` action, and mutable-message reducers (`reduceMutableMessageEvent(s)`) — **P1 extends sockudo-js itself** (in its own repo/dir) with the missing primitives; **P2 builds this SDK's thin adapter seam** over it so core/ stays mockable and never imports `@sockudo/client` directly outside `realtime/`.
- **Build:** Vite library mode ×4 configs (ESM + UMD + d.ts per entry point), `sideEffects: false`, peer deps `@sockudo/client ^1.x` (required), `react` (optional), `ai` (optional).
- §2.3 Channel handle: where Ably passes `channel`, our options accept a `@sockudo/client` channel (or `client` + `channelName`) — and the React providers hide even that, composing with sockudo-js's existing `framework-react` bindings where they fit (P11 decides reuse-vs-wrap per hook).

## 3. How to use this plan

One prompt = one session = one PR, in order (P9/P10 depend on P4–P8; P11/P12 on P7/P10). Paste the Standard Preamble below, then the prompt. Definition of Done must be fully green before moving on. The Sockudo server with the `ai-transport` feature (server plan S1–S7 minimum) must be running for integration tests from P2 onward — pin the server git SHA in CI.

**Coordination with the existing-SDK plan ([03-existing-sdks-prompts.md](03-existing-sdks-prompts.md)):** run 03's E2 (tolerance hardening) for `sockudo-js` before or together with P1, so P1's additive work lands on a tolerant-reader baseline; P1's landed implementation then serves as the reference for 03's E4a–E4d (dotnet/flutter/kotlin/swift upgrades). Every P1 change must keep 03-E1's sockudo-js harness lanes green.

## 4. Standard Preamble — paste at the top of EVERY prompt

```text
You are working in sockudo-ai-transport-js, the TypeScript SDK for Sockudo AI Transport
(@sockudo/ai-transport). It is a 1:1 API-parity port of @ably/ai-transport targeting the Sockudo
server's AI Transport wire protocol. Its realtime substrate is the EXISTING @sockudo/client SDK
(in-tree in the sockudo repo at client-sdks/sockudo-js) as a peer dependency — exactly like
@ably/ai-transport peers on ably-js. Never duplicate connection/protocol logic that belongs in
@sockudo/client; only the src/realtime/ adapter (P2) may import it. (EXCEPTION: prompt P1's
working directory is client-sdks/sockudo-js itself.) THE source of truth for wire format and
API surface is: plans/ai-transport/02-sdk-prompts.md (§1 parity table + locked decisions) in
the sockudo repo, and the server spec docs/specs/ai-transport-wire-protocol.md. Read both
before coding. Conform exactly — wire names, header keys, error codes, defaults, and public
type names are frozen.

Engineering standards (non-negotiable):

QUALITY & API
- TypeScript strict (no any leaks in public types; exactOptionalPropertyTypes on). Public API
  documented with TSDoc on every exported symbol, including @defaultValue and error behavior.
- ESM-first with .js extension imports; subpath exports map (., /react, /vercel, /vercel/react);
  tree-shakeable (sideEffects:false; verify with a bundle test).
- Every async public method rejects with ErrorInfo (never raw Error); sync methods throw
  ErrorInfo with InvalidArgument for misuse.

PERFORMANCE
- Hot paths (decode → fold → view update, on every token) must not allocate per token beyond
  the delta itself: no array spreads of message lists per chunk, no JSON round-trips of opaque
  payloads, reuse trackers. Long conversations (10k messages, 100k ops) must stay responsive:
  document and meet the budgets in each prompt.
- React: re-renders only when visible content changes (reference-stable handles, scoped events).

SECURITY
- Never log payloads or tokens; redact auth material in errors. Treat all inbound wire data as
  hostile: defensive header access, bounded buffers, no prototype-pollution vectors (null-proto
  maps for header/dynamic keys). The clientId is whatever the server verified — never trust
  local assertions.

TESTING (every prompt)
- Vitest. Unit tests colocated; integration tests under test/integration (real Sockudo server,
  env SOCKUDO_URL/SOCKUDO_APP_KEY..., skipped when unset). Deterministic: inject ids/clocks
  (no raw crypto.randomUUID()/Date.now() in core logic — injectable providers with defaults).
- pnpm test, pnpm lint (eslint flat config), pnpm typecheck, pnpm build all green.
- Public-API snapshot test (api-extractor or rollup-dts diff) — any surface change must be
  intentional and reviewed.
```

---

## 5. The prompts

---

### P0 — Repository scaffolding, tooling, CI

```text
Create the sockudo-ai-transport-js repository skeleton for @sockudo/ai-transport, mirroring the
build/packaging architecture of @ably/ai-transport (studied: single package, four subpath
exports, Vite library mode per entry, ESM+UMD+d.ts).

SCOPE
1. pnpm workspace (package + demo/ folder placeholder + test/integration). package.json:
   type module, sideEffects false, engines node >= 20, exports map for ., ./react, ./vercel,
   ./vercel/react (each: types, import, require→UMD), peerDependencies @sockudo/client ^1
   (required — the realtime substrate, in-tree at client-sdks/sockudo-js), react ^18||^19
   (optional), ai ^6 (optional). NO dependency on ably or pusher-js.
2. Tooling: TypeScript strict config (NodeNext), Vite 4-config build (core/react/vercel/
   vercel-react with correct externals), Vitest (unit + integration configs), ESLint flat +
   import rules enforcing .js extensions and layer boundaries (core must not import vercel/react;
   realtime must not import core/transport — use eslint-plugin-import zones), prettier, api
   snapshot tooling, size-limit with budgets (core ≤ 30 KB min+gzip placeholder, tightened in P15).
3. CI (GitHub Actions): lint, typecheck, unit, build, bundle-size, api-snapshot on PR;
   integration job spins up the Sockudo server (docker image or cargo run pinned SHA, with
   ai-transport feature config) + runs test/integration; release workflow stub (changesets).
4. Repo hygiene: README skeleton with the parity statement + quickstart placeholders, LICENSE
   Apache-2.0, CONTRIBUTING with the layer-architecture diagram from the plan §2, .editorconfig.
5. src/version.ts (build-injected), src/index.ts empty-export placeholder so build passes.

Definition of Done: pnpm install/build/test/lint green in CI on a trivial placeholder export;
exports map verified by a node smoke script importing all four entries (ESM + CJS).
```

---

### P1 — Extend `@sockudo/client` (sockudo-js) with the missing platform primitives

```text
This prompt's working directory is client-sdks/sockudo-js (the existing @sockudo/client SDK,
v1.3+), NOT the AI-transport repo. sockudo-js already implements V2 resume
(resume/resume_success/resume_failed), rewind (+rewind_complete), a channel_history action, and
mutable-message client reducers (src/core/versioned_messages.ts: MutableMessageAction,
MutableMessageState, reduceMutableMessageEvent(s)). AUDIT all of that first (src/core/sockudo.ts,
channels/channel.ts, connection/protocol/*) — then ADD ONLY what the AI transport SDK needs and
the server plan provides (S1–S6). Everything is ADDITIVE: existing public API and wire behavior
are frozen (semver minor); the full existing sockudo-js test suite must stay green untouched.

SCOPE (each item: characterize current behavior first, extend second)
1. Mutation publish path with typed acks: verify/complete client APIs for versioned-message
   operations — publish-create, append, update, delete addressed by message_serial, with
   ack-correlated results { messageSerial, historySerial, versionSerial? } and optional opId
   (server S1 idempotency) + client-minted messageSerial/messageId on create. If publish-side
   mutation APIs already exist, align their result types; if only the consume-side reducers
   exist, add the publish side per the server spec envelope. Ack correlation timeout (10s) →
   typed error.
2. Capability-token auth mode (server S5): token | authUrl | authCallback options with Ably's
   (tokenParams, callback) signature; proactive refresh at 80% lifetime; in-place re-auth frame
   (sockudo:auth); token_expired (40142) → refresh-and-retry; verified clientId surfaced from
   the connection-established payload. Existing Pusher-style key/HMAC auth untouched.
3. History: extend the existing channel_history call with until_attach, serial bounds, limit,
   direction, and cursor pagination (server S4 extends the same action server-side — coordinate
   field names via the spec); expose a typed PaginatedResult { items, hasNext(), next() }.
   Surface the subscription's attach_serial from subscription_succeeded (additive field).
4. Presence update (server S6): presence.update(data) + member-data in presence snapshots +
   sockudo_internal:presence_update event handling; presence.get() includes current data.
5. Connection params: transportParams { append_rollup_window } validated client-side
   ({0,20,40,100,500}); document the suspended/disconnected publish contract as it ACTUALLY is
   today (characterize: does sockudo-js queue, reject, or buffer?) and align docs — only change
   behavior if it contradicts the AI SDK's needs, and then behind an option.
6. Extras passthrough: verify extras (incl. an ai tier) round-trips publish→deliver→reducers
   untouched and exposed on delivered messages; fix only if dropped. Serial handling: historySerial
   may exceed 2^53 — ensure no Number truncation anywhere (BigInt or string-compare; test the
   boundary).
7. Error surfaces: typed error objects carrying { code, message } compatible with the AI SDK's
   ErrorInfo mapping (40142/40160/40009/93002-equivalent passthrough from server responses).

TESTING: sockudo-js's own suite green (zero modifications to existing tests); new unit tests per
feature (fake timers for refresh, ack timeout, pagination walking, BigInt boundary, malformed
frames never throw out of the read loop); integration against the AI-enabled Sockudo server
(pinned SHA): token auth + refresh + revocation close, mutation round-trip with acks, gapless
until_attach (the money test: union(history(untilAttach), live-since-attach) == store content,
1000 seeded runs), rewind mid-stream join converging through reduceMutableMessageEvents,
presence update flow, resume short-gap (hot) and long-gap (position_expired → history backfill).

Definition of Done: sockudo-js minor release candidate with changelog; API snapshot diff is
purely additive; integration suite green; the AI SDK repo can now peer-depend on this version.
```

---

### P2 — Realtime adapter seam over `@sockudo/client`

```text
Implement src/realtime/ in the AI-transport repo: a THIN, fully-typed adapter over the
@sockudo/client peer dependency (extended in P1). Purpose: (a) give core/ a minimal, mockable
seam (ChannelLike/ClientLike) so nothing outside src/realtime/ imports @sockudo/client directly
(lint-enforced); (b) normalize sockudo-js's surface into the exact shapes the transport layer
needs. NO protocol logic here — sockudo-js owns the wire; this layer adapts and types.

SCOPE
1. Interfaces (consumed by P4–P8): ChannelLike { publish(msg): Promise<{ messageSerial,
   historySerial }>, appendMessage/updateMessage/deleteMessage by messageSerial with typed acks,
   subscribe(listener) (unfiltered firehose — REQUIRED for rewind/recovery delivery; name
   filtering only as client-side convenience), history({ limit?, direction?, start?, end?,
   untilAttach? }): PaginatedResult<InboundMessage> with the gaplessness guarantee
   (union(history(untilAttach:true), live-since-attach) gap- and duplicate-free), attachSerial,
   presence { enter/update/leave/get/subscribe }, on('continuity_lost' | state events) } and
   ClientLike { channels.get(name, { params? }), connection state + clientId (verified), close }.
2. InboundMessage normalization: { name, data, action ('create'|'append'|'update'|'delete'|
   'summary'), messageSerial, historySerial, deliverySerial?, version?, clientId, timestamp,
   messageId, extras } with defensive getTransportHeaders()/getCodecHeaders() (null-proto,
   missing-safe) — mapped from whatever sockudo-js delivers (audit first; reuse its
   getMutableMessageInfo/reducer types rather than re-parsing). historySerial comparisons
   BigInt/string-safe (>2^53 boundary test).
3. Recovery normalization: sockudo-js resume events pass through — resume_success { recovered:
   [{ channel, source: 'hot'|'durable', replayed }] } is seamless (no action); resume_failed
   { code: 'stream_reset' | 'position_expired' } and continuity loss map to ONE channel
   'continuity_lost' event carrying ChannelContinuityLost (104006) ErrorInfo — upper layers (P7)
   hydrate via history/untilAttach. Verify sockudo-js already tracks per-channel stream_id +
   serial for resume (audit); only add tracking here if it does not.
4. Rewind passthrough: subscribe-with-rewind (number | { count } | { seconds }, matching
   sockudo-js's existing shapes), replayed-then-live ordering preserved through the seam.
5. appendRollupWindow: pass transportParams { appendRollupWindow?: 0|20|40|100|500 } through to
   the client; reject invalid values locally with InvalidArgument.
6. Error mapping: every sockudo-js failure → ErrorInfo (P3) with correct codes (40142/40160/
   40009/93002-equivalent passthrough; ack timeout → TransportSendFailed-family).
7. Mocks: a full in-memory ChannelLike/ClientLike mock (scripted serials, controllable timing,
   recovery-event injection) — the unit-test backbone for P4–P8.

PERFORMANCE: zero copies on the delivery path (pass sockudo-js message objects through with lazy
header views); history page normalization of 100 messages < 2 ms; rewind backlog of 1k messages
must not block the event loop > 50 ms (chunked dispatch if the seam batches).

TESTING
- Unit: normalization mapping table (every action/event shape), error-mapping matrix, BigInt
  boundary, untilAttach boundary math, header getters vs hostile extras (prototype-pollution
  keys), mock-fidelity contract tests.
- Integration (real server + real @sockudo/client): the gaplessness money-test (1000 seeded
  randomized runs under concurrent publishing); rewind mid-stream join converging to the store
  aggregate; presence enter/update/leave across two clients incl. snapshot-with-data for late
  joiner; recovery: short gap → seamless resume (source hot/durable); long gap →
  continuity_lost fires and history backfill reconstructs exactly.

Definition of Done: standards; api-snapshot freezes the seam; lint rule (no @sockudo/client
imports outside src/realtime/) active; the mock and the real adapter pass the SAME contract-test
suite (run it against both).
```

---

### P3 — Shared infrastructure: errors, events, logging, header utilities, constants

```text
Implement the SDK-wide infrastructure exactly matching Ably's shapes.

SCOPE
1. src/errors.ts: ErrorInfo class { code, message, statusCode, cause?, detail? } with message
   format "unable to <operation>; <reason>"; ErrorCode enum:
   BadRequest=40000, InvalidArgument=40003, TokenExpired=40142, InsufficientCapability=40160,
   EncoderRecoveryFailed=104000, TransportSubscriptionError=104001, CancelListenerError=104002,
   TurnLifecycleError=104003, TransportClosed=104004, TransportSendFailed=104005,
   ChannelContinuityLost=104006, ChannelNotReady=104007, StreamError=104008,
   TurnStartDeadlineExceeded=104009, InputEventNotFound=104010.
   errorInfoIs(errorInfo, code): boolean. statusCode derivation: Math.floor(code/100) for
   10000–59999 else 500; 104009/104010 → 504 (source parity). HTTP 401/403 publish rejections
   map to InsufficientCapability.
2. src/event-emitter.ts: tiny typed emitter (EventsMap generic), synchronous dispatch, listener
   exceptions caught + logged (never propagate to the emit site), on() returns unsubscribe.
3. src/logger.ts: Logger interface trace/debug/info/warn/error(message, context?) +
   withContext(); LogLevel enum incl. Silent (default); makeLogger({ logHandler?, logLevel });
   consoleLogger. Format "[ISO] LEVEL sockudo-ai-transport: msg, context: {json}". Context
   values pass through a redactor (token/authorization/secret keys masked).
4. src/constants.ts: the five EVENT_* names + every HEADER_* transport key from the locked
   registry; src/utils.ts: getTransportHeaders/getCodecHeaders (defensive, null-proto),
   headerWriter() (str/bool/json builders skipping undefined; bools "true"/"false"),
   headerReader() (typed getters), mergeHeaders, stripUndefined, buildTransportHeaders({ role,
   turnId, codecMessageId, turnClientId?, parent?, forkOf?, regenerates?, invocationId?,
   inputClientId?, inputEventId?, turnContinue? }) — exact key mapping per the registry.
5. Export all of it from the core entry point (parity: these are public in Ably's SDK).

TESTING: exhaustive unit coverage — every ErrorCode statusCode mapping, errorInfoIs, emitter
error isolation + unsubscribe semantics, every header builder/reader round-trip incl. unicode,
oversized values, undefined-skipping, redaction. Property test: buildTransportHeaders ∘
headerReader is identity for valid inputs.

Definition of Done: standards; api snapshot updated intentionally; 100% line coverage on this
module (it's small and foundational — enforce via vitest coverage thresholds scoped to src/
{errors,event-emitter,logger,utils,constants}.ts).
```

---

### P4 — Codec contract, encoder core, decoder core, lifecycle tracker

```text
Implement the codec layer (src/core/codec/) — the framework-agnostic seam, matching Ably's
documented Codec architecture and the source's proven mechanics.

SCOPE
1. types.ts — the contracts (public):
   Codec<TInput, TOutput, TProjection, TMessage> extends Reducer<TInput|TOutput, TProjection>:
     init(): TProjection
     fold(state, event, meta: ReducerMeta { serial, messageId? }): TProjection   // idempotent by
       serial/conflict-key; MAY mutate state (document); deterministic
     createEncoder(channel: ChannelWriter, options?: EncoderOptions): Encoder<TInput,TOutput>
     createDecoder(): Decoder<TInput,TOutput>
     getMessages(projection): TMessage[]
     createUserMessage(message: TMessage): UserMessage<TMessage>
     createRegenerate(target: string, parent: string): Regenerate
     resolveToolTarget(output, projection): string | undefined
     isTerminal(output): boolean
   Docs-compat aliases: export type Codec2<TEvent,TMessage> mapping onto the 4-generic form, and
   createAccumulator()-style adapter (MessageAccumulator { messages, completedMessages,
   hasActiveStream, processOutputs, updateMessage, initMessage, completeMessage }) implemented
   over fold/getMessages so the documented Ably Codec API shape works verbatim.
   Encoder { publishInput(input, opts?), publishOutput(output, opts?), cancel(reason?), close() }.
   Decoder { decode(msg: InboundMessage): { inputs: DecodedEvent<TInput>[],
   outputs: DecodedEvent<TOutput>[] } } with DecodedEvent carrying messageId (codec-message-id).
   ChannelWriter { publish, appendMessage, updateMessage } — the realtime Channel satisfies it
   structurally (assert with a type test). WriteOptions { clientId?, extras?, messageId? }.
   EncoderOptions { clientId?, extras?, onMessage?, messageId? }.
2. encoder.ts — createEncoderCore(writer, options): EncoderCore with
   publishDiscrete, publishDiscreteBatch, startStream, appendStream, closeStream, cancelStream,
   cancelAllStreams, close. Mechanics (source-parity, all REQUIRED):
   - startStream(streamId, payload, opts): headers stream:"true", status:"streaming", stream-id;
     publish create; capture serial = result.serials[0] (throw BadRequest if absent); track
     StreamState { serial, accumulated, persistentTransport, persistentCodec, cancelled }.
   - appendStream: FIRE-AND-FORGET — accumulate locally, push append promise to pending list,
     repeat ALL persistent headers on every append (server replaces extras wholesale).
   - closeStream/cancelStream: append terminal status (complete/cancelled), then FLUSH BARRIER:
     Promise.allSettled(pending); on any rejection → recovery via updateMessage(serial, full
     accumulated, terminal headers); recovery failure → throw EncoderRecoveryFailed(104000).
     Concurrent closes serialize through a single flush (re-entrancy guard).
   - onMessage hook fires before every publish (mutate-in-place), exception-isolated.
3. decoder.ts — createDecoderCore(hooks: DecoderCoreHooks { buildStartEvents(tracker),
   buildDeltaEvents(tracker, delta), buildEndEvents(tracker, closingCodecHeaders),
   decodeDiscrete(payload) }, options?): per-serial tracker map; dispatch on action:
   - create+stream:"true" → new tracker + start events; create+stream:"false" → decodeDiscrete.
   - append: known serial → accumulate + delta events; terminal status → end events / cancelled
     → mark closed. UNKNOWN serial → first-contact: synthesize start + delta(full data) (+ end
     if terminal) — this is how mid-stream join, rewind-miss, and history compaction work.
   - update: known serial → prefix-match (data.startsWith(accumulated) ⇒ delta = suffix) else
     replacement (reset + resync callback); unknown → first-contact.
   - delete → stream-delete signal + cleanup.
   Bounded tracker map (LRU cap, default 1024 streams) with metrics hooks.
4. lifecycle-tracker.ts — createLifecycleTracker(phases: PhaseConfig[]): per-scope (turnId)
   phase synthesis: ensurePhases(scopeId, ctx) emits missing phase events in order before the
   triggering event; markEmitted; resetPhase (repeating phases); clearScope. (Vercel codec will
   configure ['start','start-step'].)

PERFORMANCE: appendStream ≤ 1 alloc per call beyond delta; decoder delta path zero-copy
(substring only on update prefix-match); 10k-append stream encode+decode round-trip < 50 ms
in a vitest bench.

TESTING
- Unit: every dispatch branch incl. first-contact variants, prefix-match vs replacement,
  flush-barrier recovery (mock writer failing appends N..M → updateMessage called once with
  exact accumulated content), recovery-failure → 104000, re-entrancy, header repetition on
  appends, tracker LRU eviction, lifecycle synthesis ordering (real event always AFTER
  synthesized preamble), accumulator adapter parity (initMessage idempotent, completeMessage
  no-op-when-inactive — documented Ably semantics).
- Property tests (fast-check): random op interleavings → decoder output folded == encoder
  accumulated state (round-trip determinism); random join points mid-stream → late decoder
  converges to identical final state as a from-start decoder.

Definition of Done: standards; this layer has zero imports from transport/ (lint-enforced).
```

---

### P5 — Conversation tree

```text
Implement src/core/transport/tree.ts — the turn-keyed conversation tree, per Ably's
internals/conversation-tree doc + the source's actual data structures.

SCOPE
1. Data model: Tree keyed by TURN (TurnNode), not message:
   TurnNode<TProjection> { turnId, parentTurnId?, forkOf?, regeneratesCodecMessageId?, clientId,
   status: 'active' | TurnEndReason, projection: TProjection, invocationId, startSerial?,
   endSerial? }. Indexes: _turnIndex (turnId→node), _codecMessageIdToTurnId, _sortedTurns (by
   startSerial; optimistic/serial-less sort LAST via placeholder-serial that compares greater
   than any real serial), _parentIndex (parent→children), _regenerateByMsgId, _siblingCache
   keyed by a structuralVersion counter (bump on insert/promote/status change, NOT on
   content-only folds).
2. Mutation paths (single upsert discipline — live and history use THE SAME path):
   - applyMessage(decodedEvents, transportHeaders, serial): route per source rules —
     fresh user input (create turn from headers if absent), continuation amend
     (turn-continue:"true" → route via codec-message-id), assistant output (route by turn-id);
     fold each event into the turn's projection with ReducerMeta{serial, messageId};
     wire-only metadata carriers decoding to zero events must NOT create phantom turns.
   - applyTurnLifecycle(event): ai-turn-start → create-or-activate, promote startSerial, backfill
     parent/forkOf/regenerates if the wire raced ahead (NEVER for continuations — self-cycle
     guard); record latest continuation invocation. ai-turn-end → status=reason, endSerial.
     reason=suspended keeps the turn live (status 'suspended', stream stays open upstream).
   - Serial promotion: optimistic turn gets real startSerial when the relay arrives → re-sort +
     structuralVersion++.
   - delete(codecMessageId) and reachability cleanup.
3. Sibling semantics (CRITICAL, source-parity):
   - EDITS are turn-level forks: sibling group = turns sharing parentTurnId chained via forkOf
     (transitive, cycle-guarded).
   - REGENERATES are continuations carrying msg-regenerate: regenerate group = owner of the
     anchored codec-message-id + every turn whose regeneratesCodecMessageId points at it; owner
     first, then by serial. getSiblings/fork-resolution: fork-of resolves to a SIBLING of the
     forked node (new node's parent = forked node's parent).
4. Query API (public via transport.tree): getTurnNode, getTurnByCodecMessageId, getSiblingTurns,
   hasSiblingTurns, getRegenerateGroup, getLatestContinuationInvocation, getActiveTurnIds():
   Map<clientId, Set<turnId>>, getHeaders(codecMessageId), plus docs-compat upsert/delete/
   getNode/getSiblings/hasSiblings aliases operating at message granularity.
5. Events: on('update' | 'ably-message'→'message' (emit both names, 'ably-message' deprecated
   alias for drop-in compat) | 'turn' | 'turn-projection-updated', handler) → unsubscribe.
   'turn-projection-updated' fires on streaming folds (projection mutated in place — document
   that ref-equality is NOT a change signal); 'update' on structural change.

PERFORMANCE: fold path O(1) index lookups, zero array copies; structural queries served from
caches keyed by structuralVersion; 100k-op replay into a fresh tree < 250 ms (bench, fixture
generated from golden transcripts).

TESTING
- Unit: every mutation path; out-of-order arrival (assistant events before turn-start; appends
  before create handled in P4 but tree must tolerate orphan folds via pending bucket or create-
  on-headers — match source behavior); promotion re-sort; fork sibling transitivity + cycle
  detection + descendant exclusion; regenerate owner-first ordering + backfill + continuation
  no-self-cycle; suspended keeps active; phantom-turn guard.
- Property test: ANY permutation of a valid op log (constrained: per-message ops in serial
  order) folds to the identical final tree (determinism invariant — the cornerstone).

Definition of Done: standards; tree has zero realtime/ imports (pure data structure + codec).
```

---

### P6 — Views: projection, branch navigation, pagination

```text
Implement src/core/transport/view.ts + decode-history.ts — the paginated, branch-aware
projection with Ably's exact public semantics.

SCOPE
1. View public API (per parity table): getMessages(), flattenNodes(), hasOlder(),
   loadOlder(limit=100): Promise<void>, select(id, index) (clamped [0, len-1]),
   getSelectedIndex, getSiblings (serial-chronological), hasSiblings, getNode,
   getMessageMetadata(msgId) → { codecMessageId, turnId, clientId, status:
   'streaming'|TurnEndReason }, message-anchored nav (hasMessageSiblings, getMessageSiblings,
   getSelectedMessageSiblingIndex, selectMessageSibling), send/sendInput/regenerate/edit/update
   (wired to the transport in P7 — define the interfaces now, inject the send executor),
   on('update'|'message'|'turn') scoped events, close().
2. Flatten algorithm (source-parity): linear scan of _sortedTurns with parent-reachability
   filtering + selected-sibling-at-each-fork (default = LATEST sibling); then view filters:
   withheld turns (pagination), regen-hidden turns, shadow filter (regenerator whose anchor is
   in a truncated tail of its owner is dropped). _extractMessages: per visible owner turn emit
   codec.getMessages(projection) substituting regenerator content IN PLACE at its anchor
   (recursive for nested regens), skipping the owner's post-anchor messages — output order
   [u1, a1', u2, a2], NOT raw serial order.
3. Branch-selection intents: 'user' (explicit select), 'auto' (this view created the fork),
   'pinned' (external fork arrived → pin current visible leaf so remote edits don't yank the
   user's view), 'pending' (this view's edit/regen in flight → auto-select when it lands).
   Regen selections default to latest (auto-roll-forward) unless pinned. Re-pin pass on every
   structural update.
4. Pagination: unit = TURN. loadOlder drains a withheld buffer first, then loads channel history
   backward (untilAttach on first load) via decode-history: pages decoded through the codec
   decoder (aggregated messages → first-contact synthesis from P4 makes this uniform), folded
   through the SAME tree upsert path; count complete messages per page with a turn↔message fetch
   factor (3) and wireLimit = limit*10; loop until ≥ limit new visible turns or history exhausted.
   loading flag + loadError surfaced; concurrent loadOlder → no-op (guard). hasOlder reflects
   both withheld buffer and server hasNext.
5. Scoped events: view emits ONLY when visible output changes ('update'), and 'message'/'turn'
   only for visible turns. Multiple views over one tree are independent (selection + pagination
   state per view); transport.view is the default; createView() makes more (P7).

PERFORMANCE: token-streaming re-projection must be O(visible tail), not O(history): cache
flattened nodes; on 'turn-projection-updated' only re-extract the affected turn's messages and
patch the cached arrays (no full re-flatten). Budget: 10k-message conversation, 100 tok/s
stream → getMessages() stable references for untouched messages, < 0.5 ms per token update
(bench).

TESTING
- Unit: flatten with every filter (withheld/regen-hidden/shadow), in-place regen substitution
  incl. nested, default-latest + clamp + intents matrix (user/auto/pinned/pending transitions),
  message-anchored nav resolution (edit anchors = fork sibling's first user msg; regen anchors =
  assistant slot), pagination loop math + concurrent-load guard + error surfacing, scoped-event
  discipline (mutation on hidden branch emits nothing — assert via spy).
- Integration: against the real server with a scripted multi-branch fixture (edits + regens +
  concurrent turns): loadOlder reconstructs the exact tree from history; two views select
  different branches independently; late-join + loadOlder + live tail converge to golden
  transcript state.

Definition of Done: standards; view perf bench committed; public View type matches the parity
table verbatim (api snapshot).
```

---

### P7 — ClientTransport: send pipeline, stream router, optimistic updates, cancel, active turns

```text
Implement src/core/transport/client-transport.ts (+ stream-router.ts, invocation.ts) — the
client half, with Ably's exact public surface and the source's proven internals.

SCOPE
1. createClientTransport(options: ClientTransportOptions { client | channel, channelName?,
   codec, api, clientId?, headers? (static|fn), body? (static|fn), credentials?, fetch?,
   messages? (seed → linear chain of single-message turns), logger?, turnStartDeadlineMs?=30000 })
   → ClientTransport { tree, view, createView(), cancel(filter?), waitForTurn(filter?),
   stageEvents(msgId, events), stageMessage(msgId, message), on('error'), close(options?) }.
   Lazy attach on first use (parity). connect() internal + idempotent.
2. SEND PIPELINE (_internalSend — the heart; exact order):
   a) Build turn context: turnId/invocationId/inputEventId (injectable id provider), parent
      auto-computed from the view's selected branch (or SendOptions.parent), forkOf from
      SendOptions/edit/regenerate logic.
   b) OPTIMISTIC INSERT: codec.createUserMessage per message → fold into tree as serial-less
      turn (sorts last); optimisticMsgIds captured (TMessage.id === codec-message-id).
   c) CHANNEL PUBLISH (before the poke): every input event as ai-input with full transport
      headers (event-id per event; primary inputEventId = last); for continuations
      (SendOptions.turnId) stamp turn-continue:"true" and REUSE the existing turn/stream.
      Publish failure → remove optimistic nodes, map 401/403 → InsufficientCapability, reject.
   d) STREAM REGISTRATION: StreamRouter.createStream(turnId, invocationId) → ReadableStream
      handed to the caller IMMEDIATELY (before POST resolves).
   e) POKE: HTTP POST to api with the locked body contract (§1): invocation pointer 4-tuple +
      fat default fields; headers/body option merging (option fns evaluated per send; invocation
      fields spread LAST so they always win); credentials/fetch honored. Fire-and-forget for the
      caller, but failures: error the turn's stream, reject the pending turn-start wait, emit
      transport 'error' with TransportSendFailed(104005, statusCode from response), LEAVE
      optimistic nodes in the tree (Ably-documented behavior — app decides).
   f) TURN-START WAIT: resolve the returned promise when ai-turn-start with matching
      invocation-id arrives (or immediately if deadline=0); timeout → TurnStartDeadlineExceeded
      (104009, 504).
   Returns ActiveTurn { stream, turnId, invocationId, inputEventId, cancel(), optimisticMsgIds }.
3. RECONCILIATION (inbound own-message): match by codec-message-id → serial promotion (P5);
   non-matching messages = observer messages, folded normally. A relayed message without a
   codec-message-id header is a NEW message (documented Ably edge).
4. StreamRouter: (turnId, invocationId)-keyed controllers; route decoded outputs of own turns
   into per-turn ReadableStreams; close on ai-turn-end (reason≠suspended) / terminal isTerminal
   output; SUSPENDED: keep stream open, rebindStream(turnId, newInvocationId) on continuation;
   stale-invocation turn-ends dropped (resolve expected invocation via own-turn registry →
   router active invocation → tree's latest continuation). Observer turns: tracked in tree only
   (no stream). Backpressure: bounded controller queue (default 1024 chunks) — overflow errors
   the stream with StreamError rather than ballooning memory.
5. cancel(filter = { own:true }): publish ai-cancel with the serialized filter in transport
   headers; also locally close matching own streams (caller sees end-of-input promptly); tree
   state untouched (late events still fold). waitForTurn(filter={own:true}): resolve when
   matching active turns (from tree.getActiveTurnIds) complete; immediate if none.
6. stageEvents(msgId, events): apply via fold locally (one 'update'), queue for the NEXT send's
   POST body (staged array, drained on send); no-op + warn if msgId unknown. stageMessage
   (msgId, message): replace tree copy preserving headers/serial, NOT queued (tree is
   authoritative for POSTed history); no-op + warn if unknown.
7. Errors: continuity_lost from realtime → error own-turn streams with ChannelContinuityLost
   (104006), emit 'error', do NOT clear own-turn registry (turn-end is cleanup); subscription
   callback exceptions → TransportSubscriptionError(104001) on 'error'. close({ cancel? }):
   optional cancel publish first, then local teardown only (server keeps streaming — parity).

PERFORMANCE: send pipeline ≤ 5 ms local work p50 (excluding network); router route() O(1);
no per-token allocations beyond the chunk object.

TESTING
- Unit (mock realtime channel + fetch): the full send order (publish BEFORE poke — assert call
  order), every failure leg (publish fail rollback, POST fail stream-error + optimistic-stays,
  deadline timeout, continuation reuse + rebind, suspended keep-alive, stale turn-end drop,
  cancel filter serialization for all four shapes, waitForTurn immediate/eventual, stage*
  semantics incl. unknown-id warn path, backpressure overflow, close-with-cancel ordering).
- Integration (real server + a stub agent harness — build test/harness/stub-agent.ts that
  speaks the server-side wire protocol directly): send → stream tokens → for-await the
  ActiveTurn.stream sees deltas; refresh-and-resume mid-stream converges; second client observes
  everything (observer path); cancel from the second client stops the stub agent (it honors
  ai-cancel); optimistic+echo converge to ONE node; concurrent turns route to the right streams.

Definition of Done: standards; public ClientTransport/ActiveTurn/CancelFilter/SendOptions match
the parity table verbatim; golden-transcript replay test (from server plan S13 fixtures) folds
to expected view output.
```

---

### P8 — ServerTransport (agent side): turns, input-event lookup, cancel routing, pipeStream

```text
Implement src/core/transport/agent-transport.ts (+ turn-manager.ts, pipe-stream.ts) — the agent
half: createServerTransport + Turn lifecycle, with both the documented API and the source's
hydration mechanics.

SCOPE
1. createServerTransport(options { client | channel, channelName?, codec, logger?, onError?,
   inputEventLookupTimeoutMs?=30000, inputEventBufferLimit?=200, rewindWindow?='2m' }) →
   ServerTransport { newTurn(options): Turn, close(): void }. connect(): attach with
   params.rewind = rewindWindow; ONE unfiltered subscription routing: ai-cancel → cancel router;
   ai-input → input-event dispatcher (+ bounded buffer keyed by invocation-id for events that
   arrive before their turn registers — FIFO eviction, drained on registration, post-lookup
   stragglers warned+dropped with a bounded seen-set).
2. newTurn(options { turnId, clientId?, parent?, forkOf?, onMessage?, onAbort?, onCancel?,
   onError?, signal?, invocationId?, inputEventId? }): SYNCHRONOUS; registers for cancel routing
   immediately (early cancels must fire); no channel activity until start().
   Turn { turnId, abortSignal, view: { messages }, messages,
     start(): Promise<void>            // input-event lookup (if inputEventId): await the
       triggering ai-input on the channel (buffer + live), capture its headers (parent/forkOf/
       msg-regenerate/turn-continue) + publisher clientId (input-client-id), fold user messages;
       timeout → InputEventNotFound(104010, 504) and NO channel publish (never emit turn-end
       without turn-start). Then publish ai-turn-start with full headers. Idempotent.
     addMessages(nodes: MessageNode[], opts?: { clientId? }): Promise<{ msgIds }>   // discrete
       publishes via codec encoder; per-node header overrides win (optimistic reconciliation);
       returns codec-message-ids in order.
     streamResponse(stream: ReadableStream<TOutput>, opts?: { parent?, forkOf?,
       resolveWriteOptions? }): Promise<StreamResult { reason, error? }>   // pipeStream; does
       NOT call end().
     addEvents(nodes: EventsNode[]): Promise<void>   // target existing messages by
       codec-message-id; one encoder per node, closed after (cross-turn tool results).
     loadProjection(): Promise<TProjection>          // channel.history full scan for this turn,
       merged with live-observed lookup messages (dedup by serial, history wins).
     loadConversation(opts?: { pageLimit?=200, maxMessages?=2000 }): Promise<TMessage[]>
       // walk ancestor parentTurnId chain with truncation at fork/regenerate points,
       cycle-guarded, single history scan; caches projection for pipe.
     end(reason: TurnEndReason): Promise<void>       // publish ai-turn-end (+ error-code/
       error-message headers when reason='error'); publish BEFORE deregistering (failed publish
       keeps the turn retryable); reason='suspended' keeps registration alive for continuation. }
3. Cancel routing: ai-cancel arrives → parse CancelFilter from headers → match registered turns
   ({own} ⇒ owner clientId == cancel publisher's verified clientId; {turnId}; {clientId};
   {all}) → per-turn onCancel(request: CancelRequest { message, filter, matchedTurnIds,
   turnOwners }) if provided (false ⇒ skip; throw ⇒ CancelListenerError to turn onError, skip)
   → fire that turn's AbortController. Idempotent per turn; isolation between turns. External
   signal composed via AbortSignal.any([internal, options.signal]).
4. pipeStream(stream, encoder, abortSignal, hooks): read → encoder.publishOutput per chunk
   (with resolveWriteOptions + resolveToolTarget redirection for tool outputs belonging to a
   prior assistant message); on abort: cancel reader, onAbort(write) hook may publish final
   events, encoder.cancel('cancelled') → return { reason:'cancelled' }; natural end →
   encoder.close() → { reason:'complete' }; thrown (non-abort) → encoder recovery already
   handled in P4; wrap as StreamError(104008) to turn onError, return { reason:'error', error
   (original provider error preserved) }.
5. Error delivery matrix (exact parity): publish failures in start/addMessages/addEvents/end →
   REJECT the promise (TurnLifecycleError family), NOT onError; stream failures → onError
   (wrapped) + StreamResult.error (raw); onCancel throw → turn onError; channel-level
   (continuity, attach, cancel-listener subscribe) → transport-level onError; in-flight turns
   NOT auto-cancelled on continuity loss (documented).
6. close(): void (sync) — unsubscribe, abort all registered turns, cleanup.

PERFORMANCE: input-event buffer + registries O(1) per message; loadConversation 2000 messages
< 500 ms against local server (bench in integration).

TESTING
- Unit (mock channel): newTurn-before-cancel ordering (early cancel fires after registration),
  lookup success/timeout/buffered/straggler paths, idempotent start, header stamping matrix
  (parent/forkOf/regenerates/continue/input-client-id from a DIFFERENT continuation publisher),
  addMessages override precedence + ordered ids, addEvents encoder-per-node, every cancel filter
  × onCancel outcome, abort composition with external signal, suspended keep-alive + resume via
  a second agent session (loadProjection merge), error matrix exhaustively, end-publish-before-
  deregister.
- Integration: real server, real ClientTransport from P7 driving it: full golden-transcript
  turns (normal/cancel/abort-partial/error/suspended-continuation/regenerate/edit/concurrent);
  serverless-shape test: one process per POST (spawn fresh transport per turn), multi-turn
  conversation correctness via loadConversation.

Definition of Done: standards; the P7 stub agent is replaced by this real implementation in the
integration suite; docs-parity check: the documented Next.js route pattern (turn.start →
addMessages → streamResponse → end in after()) works verbatim in a demo test.
```

---

### P9 — Vercel codec: UIMessage chunks, reducer, tool transitions

```text
Implement src/vercel/codec/ — UIMessageCodec: Codec<VercelInput, AI.UIMessageChunk,
VercelProjection, AI.UIMessage>, the reference codec (Vercel AI SDK v6).

SCOPE
1. events.ts: VercelInput = UserMessage<UIMessage> | Regenerate | ToolResult | ToolResultError |
   ToolApprovalResponse; VercelOutput = AI.UIMessageChunk (pass-through).
2. encoder.ts (over createEncoderCore):
   - STREAMED chunk families → stream tracker ops: text-start/delta/end (codec type "text"),
     reasoning-start/delta/end ("reasoning"), tool-input-start/delta + tool-input-available
     close ("tool-input"; tool-input-available falls back to discrete when no active stream).
   - DISCRETE chunks → publishDiscrete: start (carries messageId + messageMetadata), start-step,
     finish-step, finish (finishReason + metadata), error (data = errorText), abort (cancels all
     streams), message-metadata, tool-input-error, tool-output-available, tool-output-error,
     tool-approval-request, tool-output-denied, file (data=url), source-url, source-document,
     data-* (transient:true ⇒ ephemeral, not persisted — set the envelope ephemeral flag if the
     server supports it, else skip-persist header per spec).
   - USER messages: fan UIMessage.parts into per-part ai-input discretes (discrete:"true",
     role:user); empty parts ⇒ one empty text part. Tool resolutions as ai-input: tool-result
     { output }, tool-result-error { message }, tool-approval-response (headers approved/reason),
     regenerate (wire-only, empty data, parent/target in transport headers).
   - Codec headers via headerWriter: type, id, messageId, toolCallId, toolName, dynamic, title,
     providerExecuted, preliminary, approvalId, finishReason, sourceId, mediaType, filename,
     transient, providerMetadata (json), messageMetadata (json).
3. decoder.ts (over createDecoderCore + lifecycle tracker with phases ['start','start-step']):
   inverse mapping, synthesizing start/start-step preambles on mid-stream join.
4. reducer.ts — fold into VercelProjection { messages: UIMessage[], conflictSerials,
   trackers, pendingToolResolutions }:
   - Idempotency per CONFLICT KEY with highest-serial-wins: tool-output:<toolCallId> (all
     tool-output-ish + client tool results compete), text-start:<msgId>:<id>, finish:<msgId>,
     user-msg:<id>; additive content (deltas, file/source, data-*) never deduped.
   - Stream accumulation via MessageTrackers (text/reasoning msgId→partIndex; tools toolCallId→
     { partIndex, inputText incremental-JSON }); finish-step clears text/reasoning trackers
     (stream-id reuse across steps).
   - Tool parts normalized to dynamic-tool via tool-transitions.ts (transitionToolPart, toolBase;
     states: input-streaming → input-available → approval-requested/approval-responded →
     output-available | output-error | output-denied) — exact UIMessage part shapes
     { type:'dynamic-tool', toolName, toolCallId, state, input/output/errorText/approval }.
   - Out-of-order tool resolutions buffered in pendingToolResolutions, retried after every fold.
   - start chunk: WIRE msgId stays authoritative over the LLM-provided messageId (orphan guard).
   - getMessages(projection) returns the UIMessage list; createUserMessage/createRegenerate/
     resolveToolTarget (scan dynamic-tool parts in approval-responded/approval-requested for the
     toolCallId → that message's id) / isTerminal (finish|error|abort).
5. index.ts: export UIMessageCodec; type-level conformance test against the docs-compat
   Codec2<UIMessageChunk, UIMessage> alias.

TESTING
- Unit: per-chunk-type encode→decode round-trip (every chunk in the registry — table-driven);
  reducer conflict-key matrix (duplicate serials, out-of-order, client-result vs server-output
  races); pending-resolution retry; tool state machine transitions (every legal + illegal edge);
  transient data-* exclusion; multi-step streams (finish-step tracker reset); reasoning +
  text multiplexed in one turn (chain-of-thought parity); incremental tool-input JSON parsing.
- Property test: random valid chunk streams → reducer(messages) equals Vercel's own
  reference accumulation (use ai package's readUIMessageStream over the same chunks as oracle
  where feasible) — this is the compatibility keystone.
- Golden: encode the canonical turn fixtures → wire transcripts match the server plan's golden
  files (normalized).

Definition of Done: standards; chunk-mapping table documented in TSDoc on the codec (the table
Ably never published — ours is explicit).
```

---

### P10 — Vercel ChatTransport adapter + turn-end reason mapping

```text
Implement src/vercel/transport/chat-transport.ts + run-end-reason.ts + vercel factories — the
useChat bridge with Ably's exact adapter contract.

SCOPE
1. Factories (src/vercel/index.ts): createClientTransport / createServerTransport pre-bound with
   UIMessageCodec (client api default '/api/chat'); createChatTransport(transport, chatOptions?:
   { prepareSendMessagesRequest?(ctx: SendMessagesRequestContext { chatId?, trigger:
   'submit-message'|'regenerate-message', messageId?, history, messages, forkOf?, parent? }) →
   { body?, headers? } }) → ChatTransport.
2. ChatTransport (structurally satisfies Vercel useChat's transport):
   - sendMessages({ trigger, chatId, messageId, messages, abortSignal, … }): Promise<
     ReadableStream<UIMessageChunk>> with THREE modes:
     a) trigger=regenerate-message → view.regenerate(messageId) (forkOf/parent derived).
     b) trigger=submit-message + last message is ASSISTANT and in tree → CONTINUATION:
        deriveContinuationInputs (diff useChat's overlay vs tree → synthesize tool-result /
        tool-result-error / tool-approval-response inputs carrying the assistant's tree
        codec-message-id) → view.sendInput(inputs, { turnId: that turn }) — atomic: channel
        publish precedes POST so agent hydration sees them.
     c) trigger=submit-message + last is USER → fresh send; messageId set ⇒ EDIT (forkOf=
        messageId, parent=predecessor). FORK-ON-UNRESOLVED-TOOL: if messages.at(-2) is an
        assistant with an unresolved tool part (input-streaming|input-available|
        approval-requested) → fork the user message off it (forkOf) and drop it from history
        (dangling tool call stays dormant on a sibling branch).
     History split: regenerate/continuation ⇒ newMessages=[], history=all; submit ⇒
     newMessages=[last], history=rest (minus dropped). Default POST body per the locked contract;
     prepareSendMessagesRequest merges/overrides.
   - Abort wiring: check abortSignal.aborted FIRST (useChat enables Stop before awaiting), then
     addEventListener('abort', () => transport.cancel({ turnId })).
   - reconnectToStream({ chatId }): Promise.resolve(null) — observer mode handles in-progress
     streams (document why; parity with source).
   - streaming flag + onStreamingChange(cb): wrap the returned stream in a passthrough
     TransformStream resolving a done promise on flush/error; true on own-turn start, false on
     end — the useMessageSync gate.
   - close(options?): delegate to ClientTransport.close.
3. vercelTurnEndReason(pipeResult: StreamResult, finishReason: Promise<…>): Promise<
   TurnEndReason>: pipeResult.reason !== 'complete' ⇒ return it (and silently observe
   finishReason to avoid unhandled rejection); else finishReason 'tool-calls' ⇒ 'suspended',
   other ⇒ 'complete'; AbortError-shaped rejection ⇒ 'cancelled'; else ⇒ 'error'. Export it —
   route handlers NEED it or observers hang on "streaming".

TESTING
- Unit: the 3-mode decision table exhaustively (incl. edit metadata, fork-on-unresolved for all
  3 unresolved states and NOT for resolved/approval-responded, continuation input derivation
  against overlay/tree fixtures, default-vs-custom prepareSendMessagesRequest, already-aborted
  signal, streaming flag transitions on complete/error/cancel).
- Integration: real Next.js-style route harness (node http) + real server + real useChat
  (happy-dom + @ai-sdk/react): type a message → tokens render; stop button cancels; edit forks;
  regenerate creates sibling; tool approval round-trip (suspended → approve → continuation)
  passes end-to-end; POST failure surfaces through useChat status='error' with "unable to send".

Definition of Done: standards; the documented Ably quickstart route code, renamed to sockudo
imports, runs UNCHANGED in the integration harness (drop-in parity proof).
```

---

### P11 — React core: providers + hooks

```text
Implement src/react/ — the generic React layer with Ably's exact hook contracts.

SCOPE
1. TransportProvider (props = ClientTransportOptions minus client/channel + channelName;
   children): creates the ClientTransport ONCE per channelName (recreate on change; old →
   disposal list); construction errors CAUGHT and surfaced via useClientTransport().
   transportError (children still render); context registry keyed by channelName (nested
   providers merge; nearest wins when unnamed). STRICT-MODE SAFETY: schedule close as a
   microtask so mount→unmount→remount cancels it (source-parity). The outer client provider:
   sockudo-js ships framework-react bindings (client-sdks/sockudo-js/src/framework-react) —
   AUDIT them first and REUSE its client provider/context as the outer SockudoProvider (parity
   with Ably's AblyProvider from ably/react), wrapping only where its contracts don't fit;
   document the final provider stack.
2. Hooks (all options-object, all SSR-safe returning stable empties pre-mount):
   - useClientTransport({ channelName?, skip?, onError? }) → { transport, transportError? }:
     thin context reader; THROWING-STUB pattern: when skip/no-provider/construction-failed,
     transport is a Proxy stub throwing InvalidArgument on any access; transportError set unless
     skip (skip ⇒ undefined). onError subscribes on resolve, unsubscribes on change/unmount,
     callback in a ref.
   - useView({ transport?, view?, limit?, skip? }) → ViewHandle { messages, nodes, hasOlder,
     loading, loadError?, loadOlder, select, getSelectedIndex, getSiblings, hasSiblings,
     getNode, send, regenerate, edit, update } — view prop wins over transport; subscribes to
     view 'update'; limit ⇒ auto-load first page once per view instance (SWR-style); loadOlder
     no-op while loading; ALL methods stable useCallbacks; re-render ONLY on view updates.
   - useCreateView({ transport?, limit?, skip? }) → same handle, OWNS the view (createView on
     mount, close on unmount/transport change).
   - useTree({ transport? }) → { getSiblings, hasSiblings, getNode } stable callbacks, NO
     re-render on tree changes (documented contrast with useView).
   - useActiveTurns({ transport? }) → reactive Map<clientId, Set<turnId>> (new reference per
     change), driven by tree 'turn' events; cross-client consistent.
   - useSockudoMessages({ transport?, skip? }) → InboundMessage[] raw firehose, append-only,
     reset on transport change.
3. createTransportHooks<…>() factory baking generics (source's createSessionHooks parity) —
   export it; the Vercel layer reuses it.

PERFORMANCE: a 100-token/s stream re-renders ONLY components subscribed to the affected view;
useTree/useClientTransport components: zero re-renders during streaming (assert with react
test-renderer commit counts in tests).

TESTING: @testing-library/react: provider registry resolution (nearest/named/missing), stub
throw + error-field contract for every hook with skip/no-provider/failed-construction,
strict-mode double-mount survives (no premature close — fake timers + microtask flush),
useView auto-load-once + view-prop priority, useCreateView ownership lifecycle, useActiveTurns
map identity changes, commit-count assertions for the perf contract, SSR render (renderToString)
crash-free.

Definition of Done: standards; hook API parity verified against the documented Ably React
reference (snapshot of public types reviewed in PR).
```

---

### P12 — Vercel React: ChatTransportProvider, useChatTransport, useMessageSync

```text
Implement src/vercel/react/ — the useChat-facing React layer.

SCOPE
1. ChatTransportProvider (props = Vercel client options + channelName + chatOptions?):
   internally wraps children with TransportProvider (codec=UIMessageCodec, api default
   '/api/chat'); creates the ChatTransport via useMemo([transport, chatOptions]) — document
   that chatOptions must be referentially stable; does NOT close the ChatTransport on unmount
   (underlying TransportProvider owns lifecycle). Nested named providers merge into the same
   registry so generic hooks work inside it.
2. useChatTransport({ channelName?, skip? }) → { chatTransport, transport, chatTransportError?,
   transportError? } with the dual throwing-stub/error-field contract.
3. useMessageSync({ setMessages, channelName?, skip? }) → void:
   - Subscribes to the view's 'update'; calls setMessages(overlay => mergeMessages(
     view.getMessages(), overlay)).
   - GATED by chatTransport.streaming (own-turn stream active ⇒ suppress; on transition to
     false ⇒ immediate sync) — prevents useChat's push/replace ID mismatch.
   - mergeMessages = PER-MESSAGE merge, not replace: for assistant messages, overlay tool
     resolutions (output-available/output-error/approval-responded/output-denied) win over the
     tree's still-unresolved tool parts (match dynamic-tool vs tool-${name} by toolCallId, keep
     the tree's type) — preserves locally-resolved tools before the echo lands.
4. Re-export the generic hooks pre-bound to Vercel types (createTransportHooks).

TESTING: unit — gating truth table (streaming on/off transitions, immediate post-stream sync,
skip), mergeMessages matrix (every tool-resolution overlay case + non-assistant passthrough +
unknown messages appended), provider nesting; integration — two simulated clients on one channel
with useChat: client B's sends appear in client A's useChat state only via useMessageSync, with
no flicker during A's own streams (commit-count + state assertions); branch switch on the tree
reflects into useChat after sync.

Definition of Done: standards; the documented multi-device quickstart (useChatTransport +
useChat + useMessageSync + useView({limit:30}) + useActiveTurns stop button) runs verbatim in
the integration harness.
```

---

### P13 — Cross-SDK conformance & integration suite (golden transcripts, chaos-lite)

```text
Build the SDK's permanent conformance suite: prove wire compatibility against the server's
golden transcripts and the full feature matrix end-to-end. This suite is the release gate.

SCOPE
1. Golden-transcript conformance: consume the server repo's test/ai-conformance golden files
   (pin the sockudo SHA): (a) REPLAY: feed each golden inbound transcript through
   decoder→tree→view and snapshot-assert the materialized state (messages, branches, statuses,
   active turns); (b) PRODUCE: drive ClientTransport+ServerTransport through each scenario
   against the live server and assert the captured wire transcript matches golden (normalized
   serials/ids/timestamps via the injectable providers).
2. Feature-matrix e2e (every documented Ably feature, one spec each, real server + real agent
   harness with a scripted fake LLM): token streaming (incl. rollup window content-equality at
   0 and 100), cancellation (all 4 filters + onCancel reject + cancel-before-turn), reconnection
   (mid-stream socket kill → converge; beyond-window → history fallback), multi-device (2
   clients, observer rendering + active-turns sync), history/replay (loadOlder paging deep
   multi-branch), branching (edit/regenerate/sibling nav/two views), interruption (cancel-then-
   send + send-alongside), concurrent turns (parallel streams routed correctly + waitForTurn),
   tool calling (server-executed inline; client-executed end+continuation via view.update),
   human-in-the-loop (suspended → approval from a SECOND device → continuation; first-response-
   wins double-submit), optimistic updates (POST-fail leaves node + error event; reconcile-
   before-POST-returns race), agent presence (status flow via realtime presence), chain of
   thought (reasoning parts multiplexed + cancelled-turn partial reasoning), double texting
   (all three patterns).
3. Chaos-lite (local, deterministic): drop WS at every pipeline stage (pre-publish, post-publish
   pre-POST, mid-stream, pre-turn-end) via a proxy (toxiproxy or in-process), assert documented
   recovery behavior for each; clock-skew on serials (client clock irrelevant — assert);
   slow-consumer stream backpressure.
4. CI wiring: matrix {node 20, 22} × {server local-adapter, server redis-adapter}; flaky policy
   zero; suite runtime budget ≤ 10 min (parallelize).

Definition of Done: every feature spec green; transcript PRODUCE/REPLAY both green; a
FEATURE_PARITY.md generated from the suite (feature → spec file → status) committed — this is
the "not one feature missing" receipt, reviewed line-by-line against the Ably feature list.
```

---

### P14 — Demos, documentation, examples

```text
Ship the documentation + demo apps proving drop-in parity with Ably's getting-started paths.

SCOPE
1. demo/nextjs-usechat: the Vercel-path quickstart — Next.js app: auth endpoint issuing Sockudo
   capability JWTs, /api/chat route (createServerTransport + streamText + after() +
   vercelTurnEndReason), ChatTransportProvider/useChatTransport/useMessageSync/useView/
   useActiveTurns chat UI with stop button. Mirrors Ably's getting-started/vercel-ai-sdk page
   1:1 (same file layout, same behaviors: tab-close-resume, second-tab sync, stop).
2. demo/nextjs-core-sdk: the core-path quickstart (TransportProvider + useView send/edit/
   regenerate/branch-nav UI) mirroring getting-started/core-sdk, plus a branch-comparison
   split-pane (useCreateView ×2).
3. demo/node-agent: standalone Node agent (no Next.js) — express poke endpoint + fake-LLM
   streamer; shows presence status (idle/thinking/streaming), onCancel ownership authorization,
   suspended/HITL approval flow. Each demo: README + one-command run against a docker-compose
   (sockudo + demo) — make demo target.
4. docs/ in-repo: README (install, quickstart both paths, parity statement + link to
   FEATURE_PARITY.md), guides mirroring Ably's structure: concepts (sessions/turns/transport/
   codec/tree), per-feature pages (15 — adapt the studied content to Sockudo, each with working
   snippets imported from compiled demo code so they can't rot — use a snippet-extraction test),
   API reference (typedoc with TSDoc — publishable), errors page (the 104xxx table + recovery
   guidance), custom-codec guide (the createEncoderCore/createDecoderCore walkthrough),
   troubleshooting (adapted Ably table: empty-assistant-message ⇒ namespace mutable_messages /
   93002; missing history capability; turn-never-ends ⇒ end() in finally + vercelTurnEndReason;
   duplicate turns ⇒ strict-mode/edit-mid-stream; clientId sharing; suspended-state publishes).
5. Wire the demos into CI as build-only checks (full e2e covered by P13).

Definition of Done: both quickstarts run end-to-end via make demo; docs build (typedoc + lint);
snippet-extraction test green; a reviewer following the README cold reaches a working chat in
< 15 minutes (have the session actually verify the steps).
```

---

### P15 — Performance, memory, and bundle audit

```text
Dedicated performance pass with enforced budgets — the SDK must stay smooth at
million-user-product scale (long conversations, fast models, many tabs).

SCOPE & BUDGETS (all enforced as repeatable benchmarks/tests committed to bench/):
1. Bundle: core entry ≤ 35 KB min+gzip; /react ≤ +8 KB; /vercel ≤ +15 KB; /vercel/react ≤ +6 KB;
   zero dependencies in the runtime graph beyond optional peers (verify); tree-shake test: an
   app importing only createClientTransport excludes all vercel/react code (rollup test).
2. Throughput: decode→fold→view-update pipeline ≥ 50k tokens/s single stream in node bench;
   16 concurrent streams × 200 tok/s in a browser-like env (happy-dom) with < 30% of frame
   budget consumed (measure with synthetic rAF accounting).
3. Memory: 10k-message conversation tree ≤ 50 MB retained (heap snapshot assertion); streaming
   1M tokens through one turn shows flat memory after close (tracker/pending cleanup — leak
   test with --expose-gc); router/tracker/buffer caps verified under hostile input (P3 bounds).
4. Latency: send() local pipeline ≤ 5 ms p50 / 20 ms p99 (mocked network); token wire→view
   ≤ 0.5 ms p50.
5. React: streaming re-render scope verified by commit-count tests (from P11) promoted into
   bench assertions; getMessages reference stability audited.
6. Profile + fix top findings (clinic.js / chrome devtools); document before/after in the PR.
   Known suspects: header object churn per inbound message (pool or lazy-parse), conflict-key
   string building per fold (precompute/intern), array copies in getMessages (cached arrays +
   patch), JSON.parse of codec headers on hot path (lazy).
7. CI: size-limit + bench-regression job (compare against stored baselines, flag >10%).

Definition of Done: every budget met with committed evidence; CI guards live; no public API
changes (api snapshot unchanged).
```

---

### P16 — Release engineering & production readiness

```text
Final GA gate for @sockudo/ai-transport.

SCOPE
1. Release mechanics: changesets-driven versioning (start 0.1.0); provenance-enabled npm publish
   workflow (OIDC) for @sockudo/ai-transport; CDN/UMD artifact upload step (parity with Ably's
   CDN distribution) or documented skip; tag + GitHub release with generated notes.
2. Compatibility matrix in README (tested in CI): node 20/22, browsers (last 2 evergreen via
   playwright smoke of demo/nextjs-usechat), react 18/19, ai SDK v6 range, sockudo server
   version range (wire protocol v1) — add a runtime handshake check: SDK logs a clear error if
   the server lacks the ai-transport feature (probe via the connect payload/feature flags;
   coordinate with the server spec).
3. Supply-chain hardening: lockfile audit in CI (pnpm audit + osv-scanner), pinned action SHAs,
   minimumReleaseAge-style dependency policy documented, SECURITY.md with disclosure process.
4. Final sweeps: api-snapshot frozen as the 0.1 public API (review every exported symbol against
   the parity table one last time — produce the diff table in the PR); TSDoc coverage 100% on
   exports (lint rule); FEATURE_PARITY.md regenerated and cross-checked against the full Ably
   feature list (the 15 features + concepts + API surfaces) — every row must be ✅ or have an
   approved waiver; LICENSE/NOTICE headers.
5. Dry-run release to a local registry (verdaccio) + install-and-run the quickstart from the
   packed tarball (not the workspace) — catches exports-map/file-list bugs.
6. Post-release issue templates + a roadmap doc for post-GA items (AI-transport bindings for
   the other in-tree client SDKs — sockudo-dotnet/flutter/kotlin/swift; additional codecs —
   LangChain/OpenAI raw; React Native validation via sockudo-js's react-native subpath).

Definition of Done: dry-run release verified; all CI gates green on the release candidate;
FEATURE_PARITY.md fully ✅; version 0.1.0 published (or release PR ready for human approval —
do not publish without explicit maintainer sign-off).
```

---

## 6. Dependency graph

```
P0 → P1 → P2 ─┐
P0 → P3 ──────┼→ P4 → P5 → P6 → P7 → P8 → P9 → P10 → P12 → P13 → P14 → P15 → P16
              │                  └──────→ P11 ──────────┘
```

P3 can run parallel to P1/P2. P11 needs P7; P12 needs P10+P11. P13–P16 are strictly sequential gates.

## 7. Combined definition of "absolute parity" (the final checklist)

The project is done when **all** of the following hold:

1. Server plan S0–S16 complete (file 01), SDK plan P0–P16 complete (this file), and existing-SDK plan E0–E6 complete ([03-existing-sdks-prompts.md](03-existing-sdks-prompts.md)).
2. `FEATURE_PARITY.md` shows ✅ for every Ably AI Transport feature page: token streaming, cancellation, reconnection & recovery, multi-device sessions, history & replay, conversation branching, interruption, concurrent turns, tool calling, human-in-the-loop, optimistic updates, agent presence, push notifications, chain of thought, double texting — plus all concepts (sessions, conversation tree, turns, transport, authentication, infrastructure), both framework integrations (Vercel AI SDK UI + Core), the full JS + React API surfaces, and the error registry.
3. The documented Ably quickstart code, with only import/name substitutions, runs unchanged against Sockudo (P10/P14 drop-in proofs).
4. Scale evidence: server S14 headline runs (1M connections / 50k streams, chaos, 24h soak) and SDK P15 budgets, all green with committed results.
5. Security evidence: server S11 report + SDK P16 supply-chain gates, all green.
6. Ecosystem evidence: 03-E1's full cross-SDK harness matrix (5 client SDKs, 9 server-HTTP SDKs, pusher-js V1 canary) green on both server profiles, default-profile diff-clean vs the E0 baseline — nothing about the existing client/server SDKs changed except deliberate, additive, semver-minor upgrades.
