# Sockudo AI Transport — Server Feature-Parity Prompt Plan

> **Goal:** Absolute feature parity with [Ably AI Transport](https://ably.com/docs/ai-transport) on the Sockudo server — not one feature missing — with an API surface that mirrors Ably's, engineered for ultra-high performance, security, and production scale (millions of concurrent connections).
>
> **Companion files:** [02-sdk-prompts.md](02-sdk-prompts.md) — prompts for `@sockudo/ai-transport`, the TypeScript client/server SDK (the AI-Transport *semantics* — turns, trees, views, codecs — live in the SDK; this file closes the remaining *platform-primitive* gaps the SDK rides on). [03-existing-sdks-prompts.md](03-existing-sdks-prompts.md) — compatibility guard rails + enablement for the existing in-tree SDK ecosystem (5 client + 9 server-HTTP SDKs). **Sequencing rule: 03's E0+E1 (audit + compat harness) must be in place before S1 merges** — they are the tripwire that enforces §2.8.
>
> **⚠️ CLAUDE.md is stale** (audited 2026-06-02 against code): it documents 12 crates but there are 13 — the entire `sockudo-push` crate, versioned messages, durable history, annotations, and rewind are undocumented. Every prompt below is **verify-first**: trust the code, not the docs. S15 includes fixing CLAUDE.md.

---

## 1. Research basis

All 51 pages of Ably's AI Transport docs (fetched as raw markdown) plus the full source of `github.com/ably/ably-ai-transport-js` (`@ably/ai-transport@0.1.0`), cross-checked against a **code audit of Sockudo as it exists today**. Key architectural takeaway:

**Ably's service knows nothing about turns, trees, or codecs.** All AI-Transport semantics live in the TypeScript SDK. The platform supplies nine primitives — and Sockudo already has most of them:

| # | Platform primitive | What Ably provides | Sockudo today (audited against code) |
|---|---|---|---|
| 1 | **Message ordering serials** | Lexicographic string serial per message, total order per channel; the ordering + resume key | ✅ **Exists, different shape:** `history_serial: u64` (monotonic per-channel publish order) + `delivery_serial: u64` (recovery position) + `message_serial: String ≤128` (logical mutable-message identity). u64 ordering is strictly stronger than lexicographic strings — the SDK maps onto these |
| 2 | **Mutable messages** | `create`/`append`/`update`/`delete` actions on one logical message; appends aggregate server-side into the persisted message | ✅ **Exists:** `MessageAction { Create, Update, Delete, Append, Summary }` (`sockudo-protocol/src/versioned_messages.rs`), wire events `sockudo:message.*`, `VersionStore` trait with 6 backends (Memory/PG/MySQL/DynamoDB/Scylla/Surreal), server-side aggregation (`get_latest`, `latest_by_history`), per-action authz (`message_{create,update,delete,append}_{any,own}`), HTTP mutation endpoints. **Gaps:** per-mutation idempotency keys; streaming-rate hardening (caps, accumulation perf) unverified |
| 3 | **Message `extras` headers persisted per message** | `extras.ai.transport` + `extras.ai.codec` string maps on every message, persisted to history | 🟡 **Partially:** `PusherMessage` carries `extras` (plus `message_id`, `idempotency_key`, `tags`) and `VersionedMessage` persists extras per version. **Gap:** the `ai.transport`/`ai.codec` two-tier *convention* + server-side validation/limits + anti-spoof rules |
| 4 | **Append rollup** | Server-side coalescing of high-rate appends (`appendRollupWindow`: 0/20/40/100/500 ms, default **40 ms ≈ 25 msg/s**); first append delivered immediately | ❌ **Missing** — every append fans out individually today. The genuine biggest build |
| 5 | **History + `untilAttach` + `rewind`** | Gapless serial-keyed backward pagination joined to the live subscription; `rewind=2m` channel param | 🟡 **Mostly exists:** durable `HistoryStore` (cursors, direction, serial/time bounds, retention, purge, HTTP `GET /apps/{app}/channels/{ch}/history`) + **rewind on subscribe** (`rewind: {count|seconds}` with a rewind gate). **Gaps:** `untilAttach` bounding; a *client-accessible* history path (WS frames or token-auth HTTP — today's endpoint is app-key signed) |
| 6 | **Connection resume, two-tier** | Serial-based gapless replay in a live window, then history fallback | ✅ **Exists:** `pusher:resume` with `channel_positions {stream_id, serial}` → `resume_success {recovered:[{channel, source: "hot"\|"durable", replayed}]}` — hot replay buffer with automatic **durable-history fallback** already implemented; `stream_reset`/`position_expired` failure codes |
| 7 | **Capability tokens + verified clientId** | JWT claims `x-ably-capability` (channel-pattern → ops map) + `x-ably-clientId`; ops `publish`/`subscribe`/`history`/`presence`; service-verified | ❌ **Missing:** auth is Pusher HMAC + `pusher:signin` only. No JWT, no per-channel capability scoping, no expiry/refresh/revocation |
| 8 | **Presence with `update` + timeout** | `enter`/`update`/`leave` with arbitrary data; ~15 s timeout removal on ungraceful disconnect | ❌ **Missing both:** presence is add/remove only (TOCTOU-safe, webhooks, presence-history retention exist); no member-data update op, no ungraceful-disconnect grace window |
| 9 | **Idempotent publish with client-supplied message id** | Client mints the id; duplicates deduped; id echoed | 🟡 **Partially:** `PusherMessage.idempotency_key` dedupes publish; `message_id` exists (server-minted for late joiners). **Gap:** unify into client-supplied-id idempotent create + per-append `op_id` |

**Bonus subsystems that already exceed the plan's earlier assumptions:**
- **Annotations** (= Ably's message-annotations feature): `annotation.create`/`annotation.delete` with serials, summarizers (Total/Flag/Distinct/Unique/Multiple), `sockudo_internal:annotation` + `sockudo_internal:message` summary events.
- **Push notifications**: full `sockudo-push` crate — FCM/APNs/WebPush/HMS/WNS, device registrations (PBKDF2-hashed secrets), per-channel subscriptions, publish/batch/scheduled, feedback + device lifecycle, circuit breakers, sharded fanout, templates, AES-256-GCM credential envelopes, `/apps/{appId}/push/*` HTTP surface, ~10 queue backends.
- **Webhooks** with channel filtering; **metrics**; **rate limiting** (API + per-connection WS).

**Wire-protocol ground truth from the Ably SDK source** (supersedes their docs where they differ):
- Five message names: `ai-input` (client codec events), `ai-output` (agent codec events), `ai-run-start`/`ai-run-end`/`ai-cancel` (docs' newer naming: *turn* — we adopt `ai-turn-start`/`ai-turn-end`/`ai-cancel`).
- Transport headers (bare keys in a transport tier): `stream`, `status` (`streaming|complete|cancelled`), `stream-id`, `discrete`, `turn-id`, `invocation-id`, `event-id`, `codec-message-id`, `turn-client-id`, `input-client-id`, `role`, `parent`, `fork-of`, `msg-regenerate`, `turn-reason` (`complete|cancelled|error|suspended`), `turn-continue`, `error-code`, `error-message`. Codec-tier headers are opaque to the server.
- Streaming = one create + N fire-and-forget appends (flush barrier on close) + closing append with terminal status; failed-append recovery = full `update` with accumulated content. Late joiners read the **aggregated** message.
- The HTTP "poke" body is minimally `{ turnId, invocationId, inputEventId, sessionName }`; the channel is the record.
- Error codes: AI block `104000–104010` + platform codes (`40000`, `40003`, `40160`, `40142`, `93002` mutable-not-enabled, `40009` too-large). Limits: 64 KiB messages (256 KiB pro), ~2 min live-recovery window, ~15 s presence timeout.

---

## 2. Locked design decisions

Fixed across all prompts. **Build on what exists** — do not re-invent subsystems the audit found.

1. **Reuse the versioned-message protocol as the streaming substrate.** AI streaming rides the EXISTING `sockudo:message.create|append|update|delete` events and `VersionStore`. The logical message identity is the existing `message_serial` — which the SDK uses directly as Ably's `codec-message-id` (appends are addressed by `message_serial`, simpler than Ably's append-by-create-serial). Ordering = `history_serial` (u64); recovery position = `delivery_serial` (u64).
2. **AI lifecycle events are plain channel events** named `ai-turn-start`, `ai-turn-end`, `ai-cancel`, `ai-input`, `ai-output`, published through the normal V2 publish path with AI headers in `extras`. The server stays domain-blind: it validates/limits the transport tier and passes the codec tier through opaquely.
3. **Header tiers are a convention over the existing `extras` field:** `extras.ai.transport` + `extras.ai.codec` (string→string, limits enforced server-side when `ai-transport` is enabled). JWT claims: `x-sockudo-capability`, `x-sockudo-client-id`.
4. **Feature gating:** new work lands behind a Cargo feature `ai-transport` on `sockudo-server` (in `full`, **not** `default` until GA) + runtime `[ai_transport] enabled`. V2-only; V1 Pusher byte-compatibility is a hard gate on every prompt. Versioned messages/history/push keep their existing flags and defaults — we do not move them.
5. **New code goes where the audit says the subsystem lives:** rollup engine + AI validation/observability in a new `crates/sockudo-ai-transport/`; everything else extends existing crates (`sockudo-protocol`, `sockudo-core`, `sockudo-adapter`, `sockudo-server`, `sockudo-push`, `sockudo-webhook`).
6. **The poke endpoint is customer-owned.** Sockudo never calls LLMs and never hosts `/api/chat`.
7. **Config:** TOML-first under `[ai_transport]`, `[auth.capability_tokens]`, `[presence]`, extending existing `[history]`/`[version_store]`/`[push]` sections; env overrides per existing conventions. No config for features that don't exist yet.
8. **Existing-SDK compatibility contract (hard gate on every prompt).** This repo ships a real SDK ecosystem in-tree: `client-sdks/` (`@sockudo/client` (sockudo-js) v1.3+, sockudo-dotnet, sockudo-flutter, sockudo-kotlin, sockudo-swift) and `server-sdks/` (9 HTTP libraries). `sockudo-js` already implements V2 resume, rewind (+`rewind_complete`), a `channel_history` action, and mutable-message client reducers (`reduceMutableMessageEvent(s)`). Therefore:
   - **Additive-only wire changes:** never change an existing frame/field/event shape; new fields are optional, new events are new names. Before adding any frame, grep the existing SDKs for name collisions (e.g., S4 must extend the existing `channel_history` action, not invent a parallel one).
   - **Behavioral defaults preserve the status quo:** anything that changes timing/semantics visible to current clients (e.g., the presence ungraceful-disconnect grace window) defaults **off** and is opt-in per app/channel-pattern.
   - **Invariant over cadence:** rollup may change append *delivery cadence*, but coalesced appends must reduce to byte-identical state through `sockudo-js`'s existing `reduceMutableMessageEvents` — tested against the actual JS reducer, not just Rust logic (S13).
   - **Existing `idempotency_key` semantics are frozen;** the S1 message-id story is additive.
   - The contract is **enforced by machinery, not promises:** [03-existing-sdks-prompts.md](03-existing-sdks-prompts.md) builds the cross-SDK compatibility harness (E1) that runs every SDK's suite + tolerance lanes (+ a vanilla pusher-js V1 canary) against both default-config and AI-enabled server profiles, merge-blocking on protocol-touching PRs. E0's baseline + E1's harness are prerequisites for S1; S13 consumes the harness.

---

## 3. Feature-parity matrix (Ably feature → where it lands)

| Ably AI Transport feature | Server status / prompts | SDK prompts (file 02) |
|---|---|---|
| Token streaming (appends + aggregation) | ✅ exists — S2 hardens; S3 adds rollup | P4, P8 |
| Append rollup / rate protection | ❌ → S3 | P1 (transportParams) |
| Cancellation (signal + filters + auth hook) | rides pub/sub — S1 conventions; S5 capability gate | P7, P8 |
| Reconnection & recovery (2-tier) | ✅ exists (hot→durable) — S4 verifies + polishes | P1, P7 |
| Multi-device sessions | ✅ native fan-out | P7 |
| History & replay | 🟡 S4 (untilAttach + client access) | P5, P6 |
| Conversation branching (parent/fork-of headers) | extras persisted ✅ — S1 validates | P5, P6 |
| Interruption / double texting / concurrent turns | turn-id multiplexing via headers — S1 | P7, P8 |
| Tool calling / human-in-the-loop (`suspended`) | passthrough — S1 | P9, P10 |
| Optimistic updates (client msg-id reconciliation) | 🟡 S1 (idempotent client-supplied ids) | P7 |
| Agent presence (enter/update/leave + timeout) | ❌ → S6 | P1, P14 example |
| Push notifications | ✅ exists (beyond parity) — S10 adds rules bridge + recipe | P14 example |
| Chain of thought | codec-tier opaque ✅ | P9 |
| Auth (capabilities, verified clientId, revocation) | ❌ → S5 | P1 |
| Errors (104xxx, 93002, 40009…) | S0 registry, S1/S2 mapping | P2 |
| Annotations (bonus: Ably annotations parity) | ✅ exists — S9 webhooks | post-GA SDK roadmap |
| Infrastructure (ordering, idempotency, scale) | S8, S12, S14 | — |
| Production / troubleshooting parity | S9, S13–S16 | P13, P16 |

---

## 4. How to use this plan

- Run prompts **in order** (graph in §7). One prompt = one Claude Code session = one PR.
- Paste the **Standard Preamble** (§5), then the prompt body.
- Every prompt **starts by auditing current behavior** (CLAUDE.md is stale; code is truth). A prompt is done only when its Definition of Done is fully green.
- **Before S1:** run [03-existing-sdks-prompts.md](03-existing-sdks-prompts.md) E0 (SDK audit + baseline) and E1 (cross-SDK compat harness). From S1 onward, every protocol-touching PR must keep the harness's sockudo-js lanes green (merge-blocking) and must not regress the default-config profile vs E0's baseline.

---

## 5. Standard Preamble — paste at the top of EVERY prompt

```text
You are working on Sockudo (~/Desktop/Code/Rust/sockudo), a Pusher-compatible, high-performance
WebSocket server in Rust (Cargo workspace, 13 crates under crates/). WARNING: CLAUDE.md is STALE
— it omits the sockudo-push crate, versioned messages (MessageAction create/update/delete/append/
summary + VersionStore), durable history (HistoryStore + rewind), and annotations. Trust the code.
Before implementing anything, READ the relevant existing subsystems and the plan at
plans/ai-transport/01-server-parity-prompts.md (§1 audited reality + §2 locked decisions) and
docs/specs/ai-transport-wire-protocol.md once it exists. We are closing the remaining platform
gaps for Ably AI Transport parity — build ON the existing versioned-message/history/recovery/push
subsystems, never beside them.

Non-negotiable engineering standards:

PERFORMANCE
- Millions of concurrent connections; the broadcast/fan-out path is sacred. No per-message heap
  allocations on hot paths where avoidable; no locks held across .await; bound every buffer/queue.
- bytes::Bytes for payload passing; no serde round-trips on opaque passthrough data.
- Any new hot-path code needs a criterion benchmark with a stated budget, and the budget met.

SECURITY
- Validate every external input (lengths, header counts, charsets, UTF-8) with precise error
  codes. All identity (client_id) comes from verified auth state — never client-asserted fields.
- No panics reachable from network input. Constant-time comparison for secrets/signatures.

TESTING (every prompt)
- Unit tests for every new public function/branch incl. malformed-input + limit cases.
- Integration tests covering the full wire flow in the owning crate's tests/.
- cargo test --workspace, cargo clippy --workspace -- -D warnings, cargo fmt --check green.
- V1 (Pusher) regression suite passes unchanged; V1 wire output byte-identical.

CONVENTIONS
- Feature-gate new AI work with `ai-transport` (propagated through sockudo-server/Cargo.toml);
  runtime-gate with config. tracing logs, sockudo_core::error::{Error, Result}, no println!.
- Wire names/headers/claims must match the plan §2 and the spec exactly.
- Update docs/ for every new config key or endpoint.
```

---

## 6. The prompts

---

### S0 — Code audit + canonical wire-protocol spec (no production code)

**Depends on:** nothing. **Output:** `docs/specs/ai-transport-wire-protocol.md` + ADR.

```text
Produce the canonical AI Transport wire-protocol + architecture spec for Sockudo as
docs/specs/ai-transport-wire-protocol.md plus a short ADR (docs/specs/adr/0001-ai-transport-
architecture.md). CLAUDE.md is stale — this spec must be derived from a CODE AUDIT, then extended
with the gap designs. No production code in this PR.

PART A — DOCUMENT WHAT EXISTS (from code, with file references; verify every claim by reading):
1. The versioned-message protocol: MessageAction variants + wire events (sockudo:message.create/
   update/delete/append/summary), VersionedRealtimeMessage full JSON shape (action,
   message_serial, history_serial, delivery_serial, version{serial, client_id, timestamp_ms,
   description, metadata}, flattened PusherMessage fields incl. extras/message_id/
   idempotency_key/tags), serial semantics (message_serial: opaque string ≤128 = logical
   identity; history_serial/delivery_serial: u64 monotonic), aggregation behavior (what
   get_latest/latest_by_history return), authorization policies (message_*_any/own), the HTTP
   mutation endpoints and their exact request/response shapes.
2. History: HistoryStore API, cursor encoding, direction/bounds, retention/purge, the HTTP
   history endpoint, rewind-on-subscribe ({count|seconds} + rewind gate semantics).
3. Recovery: pusher:resume / resume_success / resume_failed exact shapes, hot-buffer →
   durable-history fallback, stream_id continuity, failure codes (stream_reset,
   position_expired).
4. Annotations: actions, events (sockudo_internal:annotation, sockudo_internal:message),
   summarizer outputs.
5. Push: the /apps/{appId}/push/* surface relevant to AI recipes (device registration, channel
   subscriptions, publish targets incl. Channel, scheduling).
6. Extras/idempotency today: PusherMessage.extras, message_id, idempotency_key — exact current
   semantics (who mints what, what dedupes where).
Record discrepancies found vs CLAUDE.md in an appendix (feeds S15's CLAUDE.md fix).

PART B — SPECIFY THE GAPS (design, Ably-parity):
7. AI EVENT + HEADER CONVENTIONS: the five AI event names (ai-input, ai-output, ai-turn-start,
   ai-turn-end, ai-cancel) as plain channel events; extras.ai.transport + extras.ai.codec tiers
   over the existing extras field with the full transport-key registry from the plan §1 (turn-id,
   turn-client-id, turn-reason ∈ complete|cancelled|error|suspended, codec-message-id, parent,
   fork-of, msg-regenerate, stream, stream-id, status ∈ streaming|complete|cancelled, discrete,
   invocation-id, event-id, input-client-id, role, turn-continue, error-code, error-message);
   value domains, per-tier limits (≤32 keys, key ≤64 [a-z0-9-], value ≤256 UTF-8), which keys the
   server validates vs passes through, anti-spoof rules (any *-client-id must match verified
   identity unless full app key). Canonical turn sequence diagrams (normal/cancel/abort-partial/
   error/suspended-continuation/regenerate/edit) expressed in EXISTING wire events.
8. IDEMPOTENCY UNIFICATION: client-supplied message_serial/message_id idempotent create
   (relationship to existing idempotency_key — pick one story), per-mutation op_id for
   append/update/delete dedupe, replay-safe semantics.
9. APPEND ROLLUP: coalescing window semantics (first append immediate; window 0|20|40|100|500ms,
   default 40; flush-before-terminal ordering), connection param (?append_rollup_window=), per-app
   clamps, store-receives-everything invariant (persistence never rolled up), rate-limit interplay.
10. UNTILATTACH + CLIENT HISTORY: attach-serial capture per subscription; WS frames
    sockudo:history / sockudo:history_result (limit ≤1000, direction, start/end, until_attach)
    returning aggregated messages with gaplessness guarantee (history ≤ attach_serial; live >
    attach_serial); capability gating; relationship to existing rewind + HTTP endpoint.
11. CAPABILITY TOKENS: JWT (HS256, kid=app key) claims x-sockudo-capability (JSON map pattern →
    [publish|subscribe|history|presence]) + x-sockudo-client-id; pattern rules (exact, "ns:*",
    "*", case-sensitive); enforcement matrix per operation incl. all mutation actions and push;
    refresh (sockudo:auth frame), expiry (40142 flow), revocation (jti); coexistence with HMAC
    auth (additive, V2-only).
12. PRESENCE UPDATE + TIMEOUT: sockudo:presence_update frames, member-data mutation semantics,
    snapshot-includes-current-data, ungraceful-disconnect grace window (default 15s) interacting
    with V2 connection recovery (resume cancels removal; no member flap).
13. ERROR REGISTRY: map existing error paths + add Ably-compatible numerics (40000, 40003, 40142,
    40160, 40009, 93002-equivalent for "mutations not permitted on this channel" — decide: reuse
    existing authz error or add the 93002 code for SDK compat; document the exact message string),
    reserve 104000–104010 for SDK use. Full table: code, name, HTTP status, when, recovery.
14. LIMITS & DEFAULTS table (message size 64 KiB default/256 max per-app, accumulated-stream cap
    1 MiB, max appends/message 4096, header limits, rollup bounds, history page 1000, rewind caps,
    presence timeout 15s, retention defaults — reconcile with EXISTING config keys; never invent a
    second knob for an existing one).
15. SCALE-OUT notes: how history_serial/delivery_serial reservation behaves across nodes today
    (reserve_delivery_position), rollup-at-egress design, orphaned-stream janitor.
16. CONFORMANCE CHECKLIST: numbered testable assertions (AIT-S1..n, ≥60) tagged [EXISTS-VERIFY]
    vs [NEW] — later suites implement these.

Definition of Done: spec + ADR committed; every PART-A claim carries a file:line reference;
checklist complete; a reviewer can implement any later prompt from the spec alone.
```

---

### S1 — AI conventions, header validation, idempotency unification, identity stamping

**Depends on:** S0. **Crates:** `sockudo-protocol`, `sockudo-core`, `sockudo-adapter`, `sockudo-server`, new `sockudo-ai-transport`.

```text
Implement spec §7–§8: the AI event/header conventions over the EXISTING extras field, server-side
validation, idempotency unification, and verified-identity stamping. Audit-first: read the
existing extras/message_id/idempotency_key/versioned-message code paths before changing anything.

SCOPE
1. sockudo-protocol: typed, zero-copy accessors for extras.ai.{transport,codec} over the existing
   extras Value (TransportHeaders view: turn_id(), status(), parent(), fork_of(), etc. with value-
   domain validators); AI event-name constants (ai-input, ai-output, ai-turn-start, ai-turn-end,
   ai-cancel) + is_ai_event(); serde must NOT change any existing wire shape — V1 byte-stability
   snapshot tests required.
2. Validation at the edge (ws_handler + HTTP publish), active only when [ai_transport].enabled
   and the channel matches [[ai_transport.channels]] config (prefix patterns):
   - Tier limits: ≤32 keys/tier, key ≤64 bytes [a-z0-9-], value ≤256 bytes UTF-8; exact error
     codes from the spec registry.
   - Event-name policy: clients may publish ai-input + ai-cancel only; ai-output/ai-turn-* require
     a full app key OR (post-S5) an agent-capability token. Until S5 lands, gate by key type.
   - Anti-spoof: any transport-tier *-client-id must equal the connection's verified identity
     (socket auth user / signin), else reject; HTTP app-key publishes are exempt (trusted).
   - status/turn-reason/role value-domain enforcement.
3. Idempotency unification (spec §8) — BACKWARD-COMPATIBLE (plan §2.8): existing
   idempotency_key behavior is frozen; everything here is additive:
   - Client-supplied idempotent create: accept message_id (≤64 chars) on V2 client publishes +
     HTTP events; dedupe via existing idempotency machinery (set_if_not_exists) keyed
     (app, channel, message_id); duplicates return the ORIGINAL serials, not an error. Document
     the relationship to idempotency_key (both honored; message_id is the SDK-facing story) —
     zero behavior change for clients using idempotency_key today (regression test).
   - Per-mutation op_id: add optional op_id to append/update/delete mutation requests
     (protocol + HTTP); replayed op_ids are no-ops returning current version. Extend
     AppendMessageRequest/UpdateMessageRequest etc. — verify current request shapes first.
4. Serial exposure: ensure V2 publish/mutation acks (WS and HTTP) return { message_serial,
   history_serial, version_serial } so the SDK encoder can address appends and order the tree —
   audit what acks exist today and add only what's missing.
5. Metrics hooks (consumed in S9): count validated/rejected AI messages by code.

PERFORMANCE: header validation on the AI hot path < 2 µs p50, zero allocs beyond the parsed
envelope (criterion bench); non-AI channels and V1 path: 0 added cost (bench before/after).

TESTING: unit (every validation rule, every idempotency branch, anti-spoof matrix); integration
(full canonical turn sequence from the spec §7 published over WS by a key-authed "agent"
connection + a client connection, received intact by a second client with extras preserved and
serials assigned; duplicate create with same message_id deduped returning original serials;
replayed append op_id no-op); V1 isolation (V1 subscriber on the same channel sees plain Pusher
events, no extras/actions); fuzz target for the header validator.

Definition of Done: standards; spec assertions for §7–§8 checked off; docs page stub
"AI Transport conventions" added.
```

---

### S2 — Streaming-grade hardening of versioned messages (verify + gap-close)

**Depends on:** S1. **Crates:** `sockudo-core`, `sockudo-server` (history/version stores), `sockudo-adapter`.

```text
The mutable-message subsystem EXISTS (MessageAction, VersionStore ×6 backends, aggregation,
authz policies, HTTP mutation endpoints). This prompt verifies it is streaming-grade for AI
token traffic (one create + hundreds of appends per response at 150+/s) and closes the gaps the
audit flagged. DO NOT rebuild it. Start by writing characterization tests for current behavior.

SCOPE
1. CHARACTERIZE (tests first, fix what fails):
   a) Late-joiner/aggregated-read invariant: after create + N appends (+ optional terminal
      update), get_latest / the GET message endpoint / history reads return the exact accumulated
      state; a rewind subscriber mid-stream receives aggregated-then-live converging to identical
      final content. Run for memory + postgres + (CI-available) backends.
   b) Ordering: per-message_serial mutation order strictly preserved under 100 concurrent tasks
      appending to 100 distinct message_serials on one channel; history_serial strictly
      monotonic per channel.
   c) Determinism: replaying the version/history log into a fresh reader reproduces identical
      aggregated state (property test, arbitrary interleavings).
2. GAP-CLOSE:
   a) Caps (config under existing sections; verify none exist before adding): accumulated size
      per message (default 1 MiB) and max appends per message (default 4096) → reject with the
      spec's 40009-family/StreamError-compatible codes; per-channel open-streaming-message cap.
   b) Append accumulation performance: audit how append aggregation stores/concatenates data
      (O(n²) copy risk on long token streams). If naive, move to segment/rope accumulation with
      fold-on-read + cached folded Bytes (invalidate on write). Criterion: 64-byte append on a
      100 KiB message < 10 µs p50 (memory backend); aggregated read of 2000-append message
      < 50 µs warm.
   c) WS mutation ergonomics: confirm V2 clients/agents can perform append/update/delete over
      WS (not just HTTP) with acks carrying serials (S1 §4); add the missing WS frames if any,
      reusing the existing mutation authz path.
   d) Error-code parity: map "mutations not permitted on this channel/app policy" to the spec's
      93002-equivalent (exact code+message per S0 decision) so the SDK's error mapping works.
   e) Terminal-status semantics: closing append/update with status complete|cancelled must
      persist the terminal status in the aggregated record (verify version metadata carries it;
      add a typed field if it's only conventionally in extras).
3. Document (spec appendix): exactly where aggregation happens per backend, deletion/tombstone
   behavior in history reads, retention interplay (version_store purge vs history retention).

PERFORMANCE BUDGETS: above + mutation fan-out overhead ≤ +10% vs plain V2 broadcast (bench).
SECURITY: re-run mutation authz matrix (any/own × create/update/delete/append × owner/non-owner/
app-key) as a permanent table-driven test; cross-channel/cross-app message_serial confusion
attempts must fail safely.

Definition of Done: characterization + gap tests green on memory & postgres backends in CI;
budgets met; spec assertions [EXISTS-VERIFY] for §2 flipped to verified; zero behavior change
for non-AI users of versioned messages (existing tests untouched and green).
```

---

### S3 — Append rollup engine (the genuine new build)

**Depends on:** S2. **Crates:** new `sockudo-ai-transport`, `sockudo-adapter`, `sockudo-server`.

```text
Implement Ably-parity append rollup: server-side coalescing of high-rate message.append fan-out
so a token stream never blows per-connection delivery rates, per spec §9. This subsystem is
genuinely absent today. LLMs emit 150+ token events/s; default delivery ≤ 25 msg/s per stream.

SCOPE
1. crates/sockudo-ai-transport: RollupEngine on the EGRESS side of the existing versioned-message
   fan-out (the VersionStore receives every append untouched — persistence is never rolled up;
   characterize S2's invariant still holds):
   - Per (channel, message_serial) state: pending delta chain (Bytes), latest extras, deadline.
     First append for a stream: deliver immediately, arm fixed window. Within window: concatenate
     deltas, latest-extras-wins. On deadline: flush ONE coalesced append. Terminal op (status
     complete|cancelled, update, delete): synchronously flush pending FIRST, then the terminal op
     (ordering invariant).
   - Sharded timer wheel (hashed wheel over tokio time) — NOT a task per stream. 10k active
     streams/node < 1% CPU on the wheel; zero per-append timer allocation.
   - Janitor for orphaned states (publisher died mid-window): flush + TTL sweep; metrics.
2. Configuration: connection param ?append_rollup_window= ∈ {0,20,40,100,500} ms (default 40),
   rejected at connect otherwise; per-app clamp [ai_transport.rollup] min/max + enabled.
   Window applies per RECEIVING connection (each subscriber's choice), so rollup state is keyed
   per (subscriber-class) — design note: implement per-channel coalescing at the configured
   server default with per-connection override only if the fan-out architecture allows it
   cheaply; otherwise document server-wide-per-channel behavior as the v1 semantics (decide in
   S0; implement what the spec locked).
3. Rate-limit interplay: window=0 → appends count individually against existing WS rate limits;
   rolled-up flushes count once. Exceeding limits surfaces as a failed append so the SDK encoder
   falls back to update — verify that path end-to-end.
4. Metrics: appends_received_total, appends_delivered_total, rollup_ratio histogram,
   active_streams gauge, flush_latency histogram (Prometheus, per app, low-cardinality).

PERFORMANCE: rollup decision+concat < 500 ns p50/append; e2e load test (script committed):
200 tok/s synthetic stream × 60 s on a node with 10k idle conns delivers ≤ 25.5 msg/s with
content-identical accumulation, p99 added latency ≤ window+5 ms.

TESTING: unit (window math, first-immediate, terminal-flush ordering, clamps, window=0 bypass);
proptest (random token sizes/timings → subscriber accumulation == store aggregate ALWAYS);
integration (subscribers at windows 0 and 100 converge identically; recovery replay mid-stream
has no gap/dup vs aggregate); chaos (publisher drop mid-window → flush + janitor, state map
empty after sweep).

Definition of Done: standards + budgets; metrics live at /metrics; docs "Token streaming &
rollup" page with the 0/20/40/100/500 table.
```

---

### S4 — untilAttach, client-accessible history, recovery polish

**Depends on:** S2 (parallel-safe with S3). **Crates:** `sockudo-adapter`, `sockudo-server`, `sockudo-protocol`.

```text
History, rewind, and two-tier recovery EXIST (HistoryStore + rewind gate + hot→durable resume).
Close the three client-facing gaps: untilAttach bounding, a client-accessible history API (the
SDK's loadOlder), and recovery polish. Audit the existing flows first (handler/recovery.rs,
message_handlers.rs rewind gate, http_handler history endpoint) and reuse them.

SCOPE
1. Attach-serial capture: on V2 subscribe, record the channel head history_serial as that
   subscription's attach_serial (cheap read; verify a head accessor exists on the store, add if
   missing). Expose it in subscription_succeeded (V2 only) for SDK use.
2. WS history frames (V2): FIRST audit the existing `channel_history` action — sockudo-js
   (client-sdks/sockudo-js/src/core/channels/channel.ts:528) already sends it, so a server-side
   handler and response shape likely exist. EXTEND that existing request/response contract
   (do not invent a parallel sockudo:history frame) with: limit ≤1000 (default 100), direction
   (backwards default), start?/end? serial bounds, until_attach?: bool, cursor pagination —
   keeping every existing field/response shape byte-stable for current sockudo-js versions
   (additive-only, plan §2.8; characterization test of the current contract first).
   until_attach=true bounds results to history_serial ≤ attach_serial — gapless with the live
   stream (live began at attach_serial+1). Items are aggregated messages in the V2 envelope incl.
   extras + serials. Reuse HistoryStore::read_page + existing cursor machinery; concurrency cap
   per connection (2); capability hook for S5 (`history`).
3. Recovery polish (verify-first): the hot→durable fallback already exists — characterize it,
   then ensure the SDK contract: resume_failed carries a recovery hint (code position_expired →
   document "fallback to history" client guidance; add a hint field only if the spec locked one).
   Make the hot-buffer window configurable per app if it isn't (default aligned ~2 min parity)
   and verify replay interleaves mutation OPERATIONS in delivery_serial order within the window.
4. Rewind polish: verify rewind returns AGGREGATED messages for mutable messages (not raw op
   logs) — fix if not; add optional per-channel-pattern default rewind config ONLY if the spec
   locked it (else skip).
5. HTTP: keep the existing app-key history endpoint; no client-token HTTP history in v1 (clients
   use WS frames). Document the split.

PERFORMANCE: history page of 100 aggregated messages (memory backend) < 1 ms server-side p50;
rewind/history backlog streaming chunked (≤64 KiB bursts) under existing socket backpressure;
non-history subscribers: zero added subscribe latency (bench).

TESTING (the money tests):
- GAPLESSNESS: subscribe → receive live 1..N → history(until_attach) → union == exact store
  content, no dup/gap, under concurrent publishing, 1000 seeded-random iterations.
- Mid-stream join: rewind + live tail converges to aggregate (with S3 rollup on).
- Two-tier recovery: short gap → hot replay exactly-once; long gap (force eviction) →
  position_expired → WS history backfill completes the picture exactly.
- Cursor stability under live writes; pagination property test (any (cursor,limit) walk
  reconstructs the identical sequence).

Definition of Done: standards + budgets; spec §10 assertions green; docs "History & replay" /
"Reconnection & recovery" updated with the exact client contract.
```

---

### S5 — Capability tokens, verified clientId, revocation (auth layer)

**Depends on:** S1 (parallel to S2–S4 except enforcement hooks). **Crates:** `sockudo-core`, `sockudo-server`, `sockudo-adapter`.

```text
Add Ably-parity capability-token authentication alongside existing Pusher HMAC auth (which stays
untouched). Genuinely missing today: auth is HMAC + pusher:signin only. Per spec §11. Security-
critical code — highest test bar.

SCOPE
1. Token format (HS256, audited JWT lib): claims x-sockudo-capability (stringified JSON map
   { "<pattern>": ["publish"|"subscribe"|"history"|"presence", ...] }, ≤100 patterns),
   x-sockudo-client-id (≤128), iat, exp (≤24h, guidance 1h), jti; header kid = app key; signed
   with app secret. Validation: constant-time, 30s skew, alg pinned, exp required, ≤8 KiB.
2. Connection auth (V2): ?token=<jwt> (or first-frame auth per existing handshake) alternative
   to Pusher key auth → AuthContext { client_id (verified), capabilities (compiled matcher),
   jti, exp }. In-place refresh via sockudo:auth { token } (atomic swap); expiry → sockudo:
   token_expired (40142) + 30s grace close. Verified client_id feeds the S1 anti-spoof check and
   presence identity.
3. Enforcement matrix (exact codes on deny): subscribe→subscribe; presence ops→presence;
   history/rewind/untilAttach→history; any publish (ai-input, ai-cancel, client events) →
   publish; versioned-message mutations → publish + existing message_*_own/any policies (token
   layer composes with, never replaces, the per-app mutation policies); agent-only events
   (ai-output/ai-turn-*) → publish + an `agent` marker decided in S0 (capability op or key type).
   Pattern match: exact > "ns:*" prefix > "*", case-sensitive, compiled matcher O(patterns),
   zero-alloc hot check. HMAC-authed connections keep status-quo implicit capabilities.
4. Revocation: jti/client_id revocation set (sockudo-cache, TTL = remaining life); admin endpoint
   POST /apps/{app}/revocations (app-key signed) → matching connections get token_expired +
   close; propagation guarantee documented.
5. Docs: customer token-endpoint guide (Node + Rust JWT snippets, "never ship app secrets to
   clients"), three-layer auth model page (token / customer-endpoint HTTP auth / SDK onCancel).

PERFORMANCE: verify+compile < 50 µs p50; per-op capability check < 100 ns p50 zero-alloc (bench).

SECURITY TESTING: every claim violation (alg confusion, exp/nbf/skew, oversized, malformed map,
pattern edges: "ns:*" not matching "ns", case sensitivity), constant-time comparison, refresh
race (in-flight ops use a consistent snapshot), revocation; full op×principal integration matrix
(anon / V1 HMAC / V2 HMAC / token per capability subset / expired / revoked / wrong-app);
cancel-without-publish silently fails at channel level (Ably parity); JWT + capability-map fuzz
targets 10 min clean; threat-model section in PR (theft, replay, escalation-via-refresh,
cross-app kid confusion).

Definition of Done: standards; S2/S4 enforcement hooks wired and their suites re-green with
capabilities on; docs + config reference complete.
```

---

### S6 — Presence parity: `update` operation + ungraceful-disconnect timeout

**Depends on:** S1 (parallel-safe). **Crates:** `sockudo-adapter` (presence.rs), `sockudo-protocol`, `sockudo-server`, `sockudo-webhook`.

```text
Presence today is add/remove only (TOCTOU-safe, webhooks, presence-history) — genuinely missing:
member-data UPDATE and an ungraceful-disconnect grace window. Implement both per spec §12,
extending crates/sockudo-adapter/src/presence.rs (audit its locking + webhook flow first).

SCOPE
1. Presence update (V2-only): client→server sockudo:presence_update { channel, data ≤1 KiB } —
   requires membership + presence capability (S5 hook) + rate limit (default 10/s/member);
   fan-out sockudo_internal:presence_update { channel, user_id, data } to V2 subscribers; V1
   subscribers see NOTHING new (frozen Pusher semantics — test). Latest data stored on the member
   and included in the presence snapshot at subscribe (late joiner sees current agent status
   without waiting). presence-history records updates if that subsystem is enabled (verify how
   add/remove are recorded and mirror it).
2. Ungraceful-disconnect grace: abnormal socket termination (no clean unsubscribe/disconnect) →
   member marked pending-removal for [presence.ungraceful_timeout]; successful V2 resume within
   the window cancels removal (no member_removed/added flap — integrate with the existing
   recovery path); window lapse → removal + webhook + broadcast exactly once. Clean disconnects
   keep immediate removal (status quo). Respect the existing multi-connection-per-user rule
   (removal only when the LAST connection is gone — characterize first).
   ⚠️ BACKWARD COMPAT (plan §2.8): this changes member_removed TIMING visible to every existing
   client SDK and webhook consumer — the default MUST be 0/off (status quo); enable per app or
   per channel-pattern config (AI docs recommend 15s for agent channels). Regression test that
   default-config behavior is byte/timing-identical to today.
3. Horizontal: updates + pending-removal correct across redis/nats adapters; pending-removal
   owned by the connection's node with reconciliation takeover on node death (reuse existing
   horizontal presence machinery — audit it; build the minimal missing piece + node-kill test).
4. Webhooks: add member_updated event type (payload mirrors member_added + data) to
   sockudo-webhook with filtering parity; metrics: presence ops counted as messages.

PERFORMANCE: update fan-out budget = member_added ±10%; 1k-member channel, 100 updates/s storm →
p99 < 10 ms local delivery, zero-alloc hot lookup (bench).

TESTING: unit (size/rate/capability gates, V1 isolation, snapshot-includes-data); integration
(idle→thinking→streaming→idle flow; mid-flight joiner sees current status; abrupt kill → stays
15s → exactly one member_removed; resume at 10s → no removal, no flap; multi-connection rules);
horizontal (2-node: update visible cross-node; node kill → reconciled removal within
timeout+sweep).

Definition of Done: standards; docs "Agent presence" server page (status convention
{idle,thinking,streaming}, ordering caveat vs channel messages); webhook docs updated.
```

---

### S7 — HTTP & WS API surface verification for the SDK (gap-close only)

**Depends on:** S1, S2, S4, S5. **Crates:** `sockudo-server`.

```text
Verify the complete server API surface the @sockudo/ai-transport SDK needs, and add ONLY what's
missing. The audit found mutation endpoints, history endpoint, and push surface already exist —
this prompt is a contract-driven sweep, not a build.

SCOPE — for each SDK need below: write the contract test FIRST against the running server; where
it fails, implement minimally; where it passes, record the verified shape in the spec appendix:
1. Agent publish path (HTTP, app-key): POST /apps/{app_id}/events accepting AI events with
   extras tiers + client-supplied message_id (S1) — and per-event mutation fields if the spec
   chose HTTP parity for mutations beyond the existing dedicated endpoints; acks return
   { message_serial, history_serial, version_serial } per S1 §4. Batch ordering + deterministic
   partial-failure documented.
2. Existing mutation endpoints (update/delete/append/get/list_message_versions — audit exact
   paths in http_handler.rs): verify request/response shapes vs spec, op_id support (S1),
   error-code parity (S2), authz composition (key vs policies vs S5 tokens).
3. History endpoint: verify pagination/cursor/bounds vs spec; channel stats endpoint: add an
   ai block { active_streams, last_history_serial, message_count } gated on [ai_transport]
   (cheap metadata reads only) if absent.
4. Idempotency: Idempotency-Key header / per-event message_id on the events endpoint → duplicate
   POSTs return original serials with no duplicate fan-out (verify S1 machinery over HTTP).
5. OpenAPI: generate/extend the HTTP API doc for every touched endpoint with examples.

PERFORMANCE: HTTP publish-with-append reuses the same store+rollup+fanout path as WS (no
divergent logic — verify by code inspection + identical-behavior test); HTTP overhead ≤ 200 µs
p50 over the WS-internal path (bench).

TESTING: the agent-over-pure-HTTP turn: create user msg → ai-turn-start → assistant create →
100 appends → terminal close → ai-turn-end, all via HTTP; WS subscribers receive the identical
canonical sequence; history matches; retried POSTs dedupe. Negative matrix: bad target serial,
policy-denied mutation (93002-equivalent), oversized (40009), missing capability/key, malformed
extras — exact codes. Load: 1k HTTP appends/s × 60 s, p99 < 20 ms, zero errors (script committed).

Definition of Done: contract tests green and kept as the permanent HTTP conformance suite;
OpenAPI/docs updated; spec appendix records every verified shape.
```

---

### S8 — Horizontal scale-out verification + gaps

**Depends on:** S2, S3, S4. **Crates:** `sockudo-adapter` (adapters), `sockudo-ai-transport`, stores.

```text
Versioned messages/history are DB-backed and clustered tests exist (e.g.
clustered_durable_history_redis_test) — so the cluster story partially exists. Verify it under
AI streaming load and close the gaps: serial-reservation contention, egress rollup, orphan
handling, cross-node recovery. Audit reserve_delivery_position + the clustered tests first.

SCOPE
1. Serial authority under load: characterize how history_serial/delivery_serial reservation
   behaves with multiple nodes publishing to ONE channel (contention on
   reserve_delivery_position). Budget: amortized < 0.1 store round-trips per append at 200
   appends/s/channel — if the current implementation can't, add block-leasing (lease 128
   positions per node per channel with monotonic handoff) behind the store trait. Strict
   monotonicity audit across 100k cross-node publishes.
2. Operation fan-out ordering: per-message_serial mutation order preserved on every node
   (bounded per-target reorder buffer 64 ops/250 ms if the adapter bus can reorder — measure
   first; don't build it if ordering is already guaranteed). Metrics for reorder/drop.
3. Rollup placement: S3 runs at egress per node — verify subscriber-accumulation == store-
   aggregate invariant cross-node (publisher on A, subscribers on B/C, windows 0 and 100).
4. Cross-node recovery/history: resume and untilAttach correct when the client reconnects to a
   DIFFERENT node (attach serials + durable fallback are store-backed — verify; fix anything
   node-local).
5. Failure semantics: ingest node dies mid-stream → orphaned streaming message closed as
   status=cancelled by a janitor after [ai_transport.stream_orphan_timeout] (default 60s) +
   webhook event; hard-kill test.
6. Adapter matrix: document supported deployments for ai-transport (which adapters + which
   store backends); reject unsupported combos at startup with a clear config error.

PERFORMANCE (3-node compose bench, scripts committed): cross-node append fan-out added latency
p99 ≤ 5 ms; scaled-down CI soak (50k conns / 1k streams) with bounded rollup/reorder state.

TESTING: multi-node integration (identical sequences + aggregates on all nodes; gapless
untilAttach from any node; resume-on-different-node; node-kill orphan; serial monotonicity
audit); jepsen-lite partition scripts (pause/unpause) → no serial regression, no divergent
aggregates after heal; documented accepted behaviors.

Definition of Done: standards + budgets; supported-matrix documented; spec §15 assertions green.
```

---

### S9 — AI observability: metrics, webhook event types, dashboards

**Depends on:** S1 (better after S8). **Crates:** `sockudo-ai-transport`, `sockudo-metrics`, `sockudo-webhook`.

```text
Webhooks/metrics exist but have NO AI, versioned-message, or annotation event types (audited).
Add turn-aware observability by reading the well-known transport headers — domain-blind,
feature-gated, zero cost when off.

SCOPE
1. Metrics (Prometheus, per-app, low-cardinality — no per-channel/turn labels):
   ai_turns_started_total, ai_turns_ended_total{reason}, ai_cancel_signals_total,
   ai_active_streams gauge, ai_stream_duration_seconds histogram, ai_stream_bytes_total,
   ai_messages_rejected_total{code} (+ S3's rollup metrics). Parsing infallible: malformed AI
   headers increment ai_messages_unparseable_total, never affect delivery (validation lives in
   S1 only).
2. Webhook event types (extend sockudo-webhook + filtering): ai_turn_started, ai_turn_ended
   {reason, error_code?}, ai_stream_orphaned (S8), ai_cancel_requested — plus the missing
   subsystem events the audit flagged: message_version_created (versioned-message mutations,
   action-filtered) and annotation events (create/delete), each opt-in per app config.
3. The persistence recipe (Ably going-to-production parity): document end-to-end "persist
   completed turns to your own store on ai_turn_ended via the history endpoint" with a worked
   example.
4. Grafana dashboard JSON committed (turn outcomes, stream rates, rollup efficiency, rejects);
   docs "Observability for AI Transport" page incl. the four-signals guidance and the support
   escalation checklist (channel, time, clientId, first error).

PERFORMANCE: ≤1% publish-path overhead when enabled (bench on/off); exactly 0 when off
(compile + runtime gates verified by bench).

TESTING: unit (header-parse fuzz safety, cardinality bounds); integration (full turn → every
metric increments + webhooks delivered signed with retry-on-500 per existing semantics;
config-off → zero calls).

Definition of Done: standards; metrics at /metrics; webhook docs updated for ALL new event types.
```

---

### S10 — Push: channel→push rules bridge + AI completion recipe

**Depends on:** S9. **Crates:** `sockudo-push`, `sockudo-server`, docs.

```text
The push subsystem already EXCEEDS the original plan (5 providers, device registrations,
channel subscriptions, publish/batch/scheduled, feedback, circuit breakers — audit
crates/sockudo-push + push_http.rs first). Two parity gaps remain: (1) Ably-style CHANNEL RULES
that auto-trigger a push when a matching message is published on a channel (today push requires
an explicit publish API call), and (2) the documented AI "agent finished while you were away"
recipe.

SCOPE
1. Channel push rules: [[push_rules]] config { channel_pattern (e.g. "notifications:*"),
   event_filter (name allowlist e.g. "agent-complete"), payload_mapping } — on matching channel
   publish, enqueue a push PublishIntent through the EXISTING pipeline (reuse PublishTarget::
   Channel + the channel-subscription fanout; payload extracted by documented convention
   { title, body, ...data } from message data, with redaction config honored). Per-rule rate
   limit; rule matching on the publish path O(rules), < 200 ns when nothing matches (bench);
   all delivery async via the existing queue stages (zero publish-path I/O).
2. AI recipe end-to-end (docs + example): long task → user leaves → agent publishes
   'agent-complete' { title, body, sessionId } on notifications:{userId} → rule fires → devices
   subscribed via the EXISTING channelSubscriptions API receive FCM/APNs push → user taps →
   client loads session + history (S4). Alternative path documented: ai_turn_ended webhook (S9)
   → customer backend calls the existing push publish API.
3. Capability/auth: who may publish to notifications:* (capability pattern guidance with S5);
   rule config validated at startup.
4. Verify (characterization tests) the existing pieces the recipe relies on: device
   registration, channel subscription fanout, scheduled publish, feedback transitions —
   document verified shapes in the spec appendix; fix only what the recipe surfaces as broken.

TESTING: unit (pattern/filter/payload mapping, redaction); integration (publish on matching
channel → push job → mock FCM/APNs receive correct wire payloads; non-matching → zero overhead
verified; rule + explicit-API paths both green; retry/dead-letter per existing semantics);
config-off zero-overhead test.

Definition of Done: standards; docs "Push notifications for AI Transport" page with both
recipes; existing push tests untouched and green.
```

---

### S11 — Security hardening & adversarial pass (whole AI surface)

**Depends on:** S1–S10. **Crates:** all touched.

```text
Adversarial security pass over the entire AI Transport surface (new AND the pre-existing
subsystems it newly exposes: versioned messages, history, push rules) before perf/GA. Assume a
hostile multi-tenant internet. Output: fixes + docs/specs/ai-transport-security-review.md.

SCOPE — audit, exploit-test, fix:
1. AuthZ matrix wall: one table-driven integration test enumerating EVERY operation (subscribe,
   publish ai-input/ai-output/ai-turn-*/ai-cancel, client events, append/update/delete own &
   other's message_serial, history WS/HTTP, rewind, untilAttach, presence enter/update, mutation
   endpoints, push rule publishes, push admin, revocation admin) × principal (anon, V1 HMAC,
   V2 HMAC, token per capability subset, expired, revoked, server key, wrong-app key) → exact
   allow/deny/error-code. Permanent regression suite.
2. Identity: spoof attempts via every identity-bearing field (envelope client_id, transport-tier
   *-client-id, presence data, version metadata client_id, HTTP bodies); cross-channel/cross-app
   message_serial + history-cursor confusion (tampered base64 cursors!); consistent
   existence-vs-permission error policy.
3. Resource exhaustion: bounds + metric + exact error + test for: open streaming messages per
   channel/app, rollup states, reorder buffers, history query concurrency, presence update rate,
   header/extras sizes, append rate at window=0, version-store growth (caps × retention),
   slow-loris appends (idle-stream timeout), pending-removal presence entries, push-rule fanout
   amplification (one message → N devices: per-rule caps).
4. Parser robustness: cargo-fuzz targets (extras/header validator, JWT, capability map, history
   params + cursor decode, push payload mapping, mutation requests) committed with corpora; CI
   smoke ≤60s; zero panics, size caps before OOM on 10 MB inputs.
5. Crypto: constant-time everywhere; JWT alg pinning; existing push credential envelopes
   (AES-256-GCM) and device-secret hashing (PBKDF2) reviewed for the new exposure paths; webhook
   signing; no secrets in logs/Debug (redaction tests).
6. Supply chain: cargo-deny/audit clean; no new unsafe (justify exceptions); miri on protocol
   unit tests if feasible.
7. Cross-version: V1 client can never elicit V2-only data (extras/serials/actions) on an
   AI-enabled channel via any path incl. error responses — test.

Definition of Done: report committed (threat model, mitigations, accepted risks, the matrix);
fixes landed with tests; fuzz corpora + CI smoke wired; audits green.
```

---

### S12 — Performance optimization & benchmark suite

**Depends on:** S1–S8 (after S11 fixes). **Crates:** hot-path crates.

```text
Dedicated optimization pass + permanent benchmark suite for AI hot paths, budgets enforced as
CI regression alarms.

SCOPE
1. benches/ai/ criterion suites: extras/header validate, serial reservation (local + clustered
   store), versioned append (memory/redis-backed store), aggregated fold/read, rollup
   decision+flush, capability check, e2e intra-node publish→fanout (discrete ai-input; streaming
   append rollup on/off; 1k-subscriber fan-out of one append).
2. Profile (cargo flamegraph + tokio-console) the S3 streaming scenario + S8 cluster scenario;
   fix top findings. Suspects to check explicitly: serde round-trips on opaque payload
   passthrough (must be zero), Bytes copies on append fan-out (one copy ingest, zero per
   subscriber), per-channel store-entry lock contention (shard/ArcSwap snapshots for reads),
   timer-wheel wakeup storms (coalesce per tick), extras Value re-parsing (parse once at edge,
   pass typed view).
3. Capacity targets on one 8-core node (scripts + results in docs/specs/ai-transport-
   capacity.md): 500k idle V2 conns baseline within +5% memory/conn of main; 10k concurrent
   active streams at 100 tok/s, rollup 40 ms → ≥99.9% flushes within window+5 ms, fan-out p99
   ≤ 10 ms, CPU < 70%, no growth over 30 min; 50k history queries/min concurrent with the above,
   p99 ≤ 25 ms.
4. Memory budgets: ≤ 4 KiB rollup+tracking overhead per active stream (excl. payload); verify
   with heaptrack/dhat under the load run.
5. CI: criterion baselines stored; >10% regressions flagged on PRs.

Definition of Done: budgets met with raw numbers + hardware notes; before/after flamegraphs in
PR; CI regression job live.
```

---

### S13 — Protocol conformance & integration test harness (the "AIT-S" suite)

**Depends on:** S1–S10. **Location:** `crates/sockudo-ai-transport/tests/` + `test/ai-conformance/` (Node).

```text
Build the permanent conformance suite asserting every numbered spec point (AIT-S assertions,
both [EXISTS-VERIFY] and [NEW]) against a real server — the wall that keeps wire compatibility
honest for the SDK forever.

SCOPE
1. Rust harness (in-process server per existing patterns): every AIT-S assertion as a test named
   ait_sNNN_*. Minimum coverage: envelope/extras/action rules, aggregation determinism, rollup
   table (0/20/40/100/500), untilAttach gaplessness, rewind, two-tier recovery (hot + durable),
   capability spot-checks (full matrix lives in S11), presence update/timeout, idempotency
   (message_id, op_id, HTTP Idempotency-Key), mutation authz parity codes, V1 isolation.
2. Node conformance suite (test/ai-conformance, raw ws + fetch, NO SDK dependency — what the SDK
   team codes against): thin TS protocol client (≤500 lines) speaking V2 frames; scenario
   scripts replaying the spec's canonical turn sequences (normal, cancel, abort-partial, error,
   suspended/continuation, regenerate, edit, concurrent turns, late-join, recovery) asserting
   full wire transcripts against golden JSON (serials/timestamps normalized).
3. Golden transcripts committed — the SDK's executable spec.
4. Cross-SDK tolerance verification (plan §2.8 gate): CONSUME the cross-SDK compatibility
   harness from plans/ai-transport/03-existing-sdks-prompts.md E1 (test/sdk-compat/) — do not
   rebuild it. This prompt's job: run the full matrix (all 5 client SDKs' suite + tolerance
   lanes, all 9 server-HTTP-SDK lanes, the vanilla pusher-js V1 canary) against the AI-enabled
   server at this milestone, plus one S13-specific scenario: rollup-coalesced appends reduce to
   byte-identical state through sockudo-js's reduceMutableMessageEvents while the golden-
   transcript traffic runs. Every red is either a server bug (fix here) or an SDK strictness
   bug (file per-SDK, referencing 03's E2/E3).
5. CI: both suites on local-adapter always + redis service container; the 03-E1 sockudo-js
   lanes stay per-PR (path-filtered), full SDK matrix nightly; zero flaky tolerance (quarantine
   label + issue required).

Definition of Done: every AIT-S assertion implemented or explicitly waived with rationale;
suites green on both matrices; golden transcripts hand-reviewed against the spec once (PR note).
```

---

### S14 — Chaos, soak, and scale validation (millions-of-users readiness)

**Depends on:** S8, S12, S13. **Location:** `test/load/`, `tools/chaos/`, docs.

```text
Prove production readiness at scale on a real multi-node deployment: chaos + soak + headline
load. Deliver runbooks + the capacity model for millions of concurrent customers.

SCOPE
1. Scale rig (extend existing docker/make scale tooling): 3–5 nodes + redis(-cluster) + load
   generator fleet simulating AI traffic: N sessions × (1 streaming agent at 100–200 tok/s,
   mean 800-token responses; 1–5 subscriber devices; cancel probability; reconnect probability;
   history-on-join), configurable to 1M+ connections.
2. Headline runs (configs + results in docs/specs/ai-transport-capacity.md):
   - 1M concurrent connections, 50k active streams cluster-wide, 30-min soak: zero message loss
     (sampled transcript audits), serial monotonicity audit clean, p99 append→delivery < 25 ms
     intra-region, flat memory after warm-up, no fd/task leaks.
   - Reconnect storm: 20% simultaneous drop → all resumed within 60 s, recovery correctness
     sampled, no thundering herd (connection-rate limiting verified).
   - Cancel storm: 10k cancels in 10 s routed correctly.
3. Chaos (scripted, tools/chaos/): node kill mid-streams (janitor at scale), redis failover
   during streams (bounded stall, zero corruption), inter-node partition (S8 semantics hold),
   slow-subscriber pathology (existing slow-consumer policy verified on hot channels), clock
   skew (u64 serial reservation unaffected — verify).
4. Soak: 24h at 20% headline load; automated leak pass/fail (RSS, tasks, store size vs
   retention math).
5. Runbooks (docs production-ops): scaling formulae (conns/node, streams/node, store sizing),
   alert thresholds tied to S9 metrics, incident playbooks from chaos findings, and the
   Sockudo-flavored troubleshooting table (mutations-not-permitted policy errors, history
   retention too short, turn-never-ends client guidance, suspended-state publishes, reconnect
   loop on bad tokens, rollup mis-tuning).

Definition of Done: all runs executed with committed configs + results; every chaos finding
fixed (with test) or documented as accepted; runbooks merged; one-page capacity model signed off.
```

---

### S15 — Documentation parity + fix stale CLAUDE.md

**Depends on:** prior prompts (draftable alongside). **Location:** `docs/content/`, `CLAUDE.md`.

```text
Ship the complete server documentation set for AI Transport AND fix the stale project docs the
audit exposed.

SCOPE
1. FIX CLAUDE.md (high priority — it currently omits entire subsystems): 13 crates incl.
   sockudo-push; versioned messages (MessageAction, VersionStore, mutation endpoints, authz
   policies); durable history + rewind + two-tier recovery; annotations; push (providers,
   device/channel-subscription model, feature flags); presence-history; the new ai-transport
   feature + crates. Update the dependency DAG, crate table, feature-flag list, key-files list,
   test locations. Verify every claim against code.
2. New/updated pages under docs/content/2.server/: "AI Transport overview" (the 9 primitives,
   session=channel model, what exists vs what `ai-transport` adds); "Mutable messages" (actions,
   aggregation, policies, limits, error parity — document the EXISTING subsystem properly for
   the first time); "Token streaming & rollup" (the window table, billing/metrics counting, edge
   cases); "History & replay" / "Reconnection & recovery" (untilAttach, rewind, hot→durable,
   retention); "Authentication for AI Transport" (capability tokens, claims, patterns,
   refresh/revocation, three-layer model, JWT examples Node+Rust); "Agent presence"; "Push for
   AI" (both recipes); "Observability" (metrics table + dashboard); "Production checklist";
   "Troubleshooting" (S14 table); "Limits & quotas" (one canonical table reconciling EXISTING
   config keys with new ones).
3. Config reference: every new key + every pre-existing-but-undocumented key touched by this
   plan ([version_store], [history], [presence_history], [push], [ai_transport],
   [auth.capability_tokens], [[push_rules]]). Add a permanent doc-drift test: dump default
   config from code and diff against documented defaults.
4. Link the wire-protocol spec as the formal reference; verify every snippet runs.

Definition of Done: pages merged; doc-drift test green; docs build clean; CLAUDE.md accurate
(spot-check by a fresh-session smoke: ask the agent to find versioned messages/push from
CLAUDE.md alone).
```

---

### S16 — GA readiness review & release engineering

**Depends on:** all. **Location:** repo-wide.

```text
Final gate to production GA for Sockudo AI Transport. No new features — verification, polish,
release mechanics.

SCOPE
1. Full-matrix CI: cargo test/clippy/fmt across feature combos: default (NO ai-transport — zero
   trace of the feature), ai-transport alone, +redis, +redis-cluster, +nats (with documented
   support matrix), full, --no-default-features (pure Pusher server still builds + passes).
2. Rolling-upgrade safety: old node + new node on shared redis — V1/V2 non-AI traffic unaffected;
   AI features activate only when all nodes upgraded OR degrade cleanly (min-cluster-version gate
   if needed); config migration notes for operators already using versioned messages/history/push.
3. API stability: wire names/headers/codes frozen against the spec; S13 golden transcripts final
   run; spec tagged wire-protocol v1 with a compatibility-promise section.
4. Performance sign-off: S12 benches + scaled S14 smoke on the RC; numbers in release notes.
5. Security sign-off: S11 matrix + fuzz smoke + cargo audit/deny re-run; advisory review.
6. Ecosystem sign-off: the FULL cross-SDK harness matrix (03-existing-sdks-prompts.md E1 — all
   5 client SDKs, all 9 server-HTTP SDKs, pusher-js V1 canary) green on the release candidate
   against BOTH config profiles; default-profile results diff-clean vs the E0 baseline; the
   release-order rule from 03-E6 honored (server ships with defaults off before any SDK release
   that depends on it).
7. Release notes + operator migration guide; version bump per workspace convention; CHANGELOG;
   Docker images build with/without the feature; make quick-start works on the release artifact
   with config/ai-transport.example.toml included.

Definition of Done: checklist green, linked from release notes; remaining dependencies for full
product parity are the SDK plans (plans/ai-transport/02-sdk-prompts.md and
03-existing-sdks-prompts.md E4/E5 enablement) — state their status.
```

---

## 7. Dependency graph

```
S0 → S1 → S2 → S3 ─┬→ S8 → S12 → S14 → S16
              S4 ──┤         ↑
        S1 → S5 ───┤   S11 ──┘ (S11 after S1–S10)
        S1 → S6 ───┤
S1,S2,S4,S5 → S7 ──┤
        S9 → S10 ──┘   S13 after S1–S10; S15 alongside; S16 last
```

Parallel tracks after S1: {S2→S3, S2→S4}, {S5}, {S6}. S7 needs S1+S2+S4+S5. S9/S10 anytime after S1 (better after S8).

**Effort note vs the original draft of this plan:** the audit removed roughly a third of the originally-planned build (mutable store, history backends, recovery fallback, push subsystem all exist). The critical path is now: S3 (rollup) + S5 (capability tokens) + S6 (presence) + S4 (untilAttach/client history) + hardening/verification of what exists.
