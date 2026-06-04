# AGENTS.md

This file is the top-level operating contract for coding agents working in
`/Users/radudiaconu/Desktop/Code/Rust/sockudo`. It applies to the whole repository unless a
deeper `AGENTS.md` overrides a subset of directories.

## Role And Intent

You are working on Sockudo, a Rust realtime server with:

- strict Protocol V1 Pusher compatibility
- Protocol V2 native Sockudo features
- horizontal fanout across multiple nodes
- durable history, rewind, two-tier recovery, presence-history, annotations, versioned mutable
  messages, push notifications, metrics, webhooks, delta compression, tag filtering, and optional
  AI Transport

Default posture: execute directly, finish the task, verify the result, and keep diffs reviewable.

## Autonomy

- Proceed automatically on clear, reversible next steps.
- Ask only when the choice is materially ambiguous, destructive, or changes product scope.
- If blocked, try the next reasonable path before escalating.
- Use bounded subagents only when that materially improves throughput or correctness.

## Repository Map

Production crates under `crates/`:

- `sockudo-protocol`: protocol message types, V1/V2 prefixing, extras, versioned wire payloads
- `sockudo-filter`: tag filtering
- `sockudo-core`: shared traits, config, errors, auth, history, version store, annotations,
  presence-history, common types
- `sockudo-app`: app-manager backends
- `sockudo-cache`: cache backends
- `sockudo-queue`: queue backends
- `sockudo-rate-limiter`: rate limiting and middleware
- `sockudo-metrics`: Prometheus metrics
- `sockudo-webhook`: webhook delivery
- `sockudo-delta`: delta compression
- `sockudo-push`: push providers, device registrations, channel subscriptions, publish pipeline,
  durable status, feedback, scheduler
- `sockudo-ai-transport`: AI Transport validation, rollup, and conformance helpers
- `sockudo-adapter`: connection handling, presence, horizontal scaling, replay/recovery, fanout
- `sockudo-server`: binary, HTTP/WS handlers, bootstrap, durable history/version stores, push API

Other workspace member:

- `benches/ai`: permanent Criterion benchmark suite and budgets for AI hot paths

High-value docs and reference surfaces:

- `CLAUDE.md`
- `docs/content/docs/server/*`
- `docs/content/docs/reference/*`
- `docs/specs/ai-transport-wire-protocol.md`
- `docs/specs/ai-transport-capacity.md`
- `config/config.toml`
- `docker-compose*.yml`
- `Makefile`

## Core Operating Principles

- Preserve existing behavior unless the task explicitly changes it.
- Prefer deletion over addition.
- Reuse current abstractions before inventing new ones.
- No new dependencies without explicit need.
- Keep V1 compatibility and V2 semantics clearly separated.
- Verify before claiming completion.
- When behavior changes, update tests and docs in the same change.

## Execution Protocol

1. Inspect the relevant code paths first.
2. State the working understanding briefly in progress updates.
3. For cleanup/refactor work, write a cleanup plan and lock behavior with tests first when not
   already protected.
4. Make the smallest coherent change that solves the task.
5. Run the narrowest meaningful verification first, then widen as needed.
6. Read test and diagnostic output before deciding the task is done.

## Search And Read Strategy

- Prefer `rg` and `rg --files`.
- Read crate boundaries plus immediate call sites before changing shared code.
- For protocol, adapter, versioned messages, recovery, history, presence, annotations, push, or AI
  Transport work, inspect runtime code and docs before editing.
- Do not assume a feature is single-node only. Check horizontal and failure-path code.

## Feature Flag Discipline

Sockudo relies on feature propagation.

- Check touched crate `Cargo.toml` and `crates/sockudo-server/Cargo.toml`.
- Keep AI Transport behind Cargo feature `ai-transport` and runtime `[ai_transport]`.
- Keep push behind feature/config surfaces already used by `sockudo-push` and `sockudo-server`.
- Do not accidentally expose V2-only features to V1 clients.
- Keep targeted feature combinations compiling; do not assume `full` is the only build.

## Protocol And Compatibility Rules

- Preserve Protocol V1 Pusher-compatible behavior unless explicitly changing it.
- Keep V2-only fields gated and stripped from V1 delivery.
- Treat wire payload shape as a compatibility contract.
- Add or update tests for any wire-visible change.
- Link significant protocol changes to `docs/specs/ai-transport-wire-protocol.md` when AI
  Transport is involved.

## Stateful Subsystem Rules

### Versioned Messages

- Build on `MessageAction` and `VersionStore`; do not add parallel mutable-message state.
- Preserve `message_serial`, `version_serial`, `history_serial`, and `delivery_serial`
  continuity.
- Keep mutation authorization parity across HTTP and WebSocket actor contexts.
- Own-scoped mutations require verified actor identity and matching original creator identity.

### Durable History And Recovery

- Keep durable history distinct from the hot replay buffer.
- Preserve continuity semantics for `stream_id`, `serial`, and replay.
- Fail closed when continuity cannot be proven.
- Use bounded retention and opaque cursors.
- For late-join/history work, respect `until_attach` gaplessness.

### Presence And Presence History

- Preserve first-join and last-leave correctness.
- Distinguish current membership from historical transition records.
- Consider reconnect races, duplicate disconnects, multi-connection users, and degraded durable
  history state.

### Annotations

- Preserve raw annotation delivery gating and summary projection semantics.
- Keep annotation publish/delete auth separate from message mutation auth.

### Push

- Preserve device registration, channel subscription, provider credential, publish status,
  idempotency, queue, retry, feedback, and scheduler contracts.
- Do not log device secrets, provider tokens, raw credentials, or unredacted payloads unless a
  debug-only option explicitly permits it.

### AI Transport

- Build on versioned messages, durable history, recovery, presence, annotations, and push.
- Validate external AI extras/headers at the edge with precise errors.
- Treat append rollup as egress-only; persistence, history, recovery, webhooks, and push see every
  original mutation.
- Protect fanout hot paths: no unbounded buffers, no locks held across `.await`, no avoidable
  per-subscriber payload copies.

## Observability Requirements

For stateful/distributed changes, consider metrics and logs for:

- success/failure counters
- degraded/reset-required state
- duplicate suppression
- bounded queue/backlog depth
- recovery source and failure code
- push provider outcome and status retention
- AI rollup ratio, flush latency, active streams, turn outcomes

## Documentation Sync

Update docs when changing:

- HTTP API request or response shapes
- protocol-visible payloads
- feature availability or comparison claims
- scaling semantics
- versioned messages, history, recovery, presence-history, annotations, push, or AI Transport
- configuration or feature-flag behavior

Likely docs:

- `docs/content/docs/server/http-api.mdx`
- `docs/content/docs/server/history-recovery.mdx`
- `docs/content/docs/server/mutable-messages.mdx`
- `docs/content/docs/server/ai-transport-*.mdx`
- `docs/content/docs/reference/configuration.mdx`
- `docs/content/docs/reference/environment-variables.mdx`
- `docs/content/docs/reference/http-endpoints.mdx`
- `docs/specs/ai-transport-wire-protocol.md`

## Verification Standard

Small local change:

- run focused tests for the touched crate or module
- run formatting if Rust changed

Standard backend/API/docs-with-guard change:

- `cargo fmt --all`
- focused tests
- docs type/build checks when docs changed

High-risk protocol, presence, scaling, recovery, history, annotations, push, or AI Transport:

- `cargo fmt --all`
- focused subsystem tests
- relevant integration/conformance tests
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`

If full verification is too expensive or blocked, say exactly what was not run and why.

## Useful Commands

```bash
cargo fmt --all
cargo test --workspace
cargo test -p sockudo-core
cargo test -p sockudo-adapter
cargo test -p sockudo-ai-transport
cargo test -p sockudo-push
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p sockudo --features "v2,ai-transport,redis,postgres,push"
scripts/ai-transport-bench-guard.sh
AIT_CONFORMANCE_OFFLINE=1 scripts/ai-conformance-node.sh
cd docs && npm run types:check && npm run build
```

## Safety Constraints

- Never run destructive commands like `git reset --hard`, `git checkout --`, or broad `rm -rf`
  unless explicitly requested.
- Do not rewrite history unless explicitly requested.
- Do not amend commits unless explicitly requested.
- Treat production-facing config, migrations, and destructive history operations as high-risk.

## Final Response Contract

When closing a task, report what changed, how it was verified, and remaining risks/gaps.
