# AGENTS.md

This is the top-level operating contract for coding agents working in
`sockudo`. It applies to the whole monorepo unless a deeper
`AGENTS.md` overrides a subset of directories.

## Mission

Sockudo is a Rust realtime server and SDK monorepo. The server must preserve strict Protocol V1
Pusher compatibility while advancing Protocol V2 Sockudo-native features:

- horizontal fanout across multiple nodes
- durable history, rewind, and two-tier recovery
- versioned mutable messages
- presence and presence history
- annotations
- push notifications
- metrics, webhooks, rate limits, and operator APIs
- delta compression and tag filtering
- optional AI Transport built on the same durable primitives

Default posture: execute clear tasks directly, keep diffs reviewable, verify before claiming
completion, and preserve existing behavior unless the task explicitly changes it.

## Autonomy

- Proceed automatically on clear, reversible next steps.
- Ask only when the choice is materially ambiguous, destructive, or changes product scope.
- If blocked, try the next reasonable path before escalating.
- Use bounded subagents only when they materially improve throughput or correctness.
- Never revert unrelated work. The tree may already contain user changes.

## Repository Map

Server workspace:

- `crates/sockudo-protocol`: protocol message types, V1/V2 prefixing, extras, versioned wire payloads
- `crates/sockudo-filter`: tag filtering
- `crates/sockudo-core`: shared traits, config, errors, auth, history, version store, annotations,
  presence history, common types
- `crates/sockudo-app`: app-manager backends
- `crates/sockudo-cache`: cache backends
- `crates/sockudo-queue`: queue backends
- `crates/sockudo-rate-limiter`: rate limiting and middleware
- `crates/sockudo-metrics`: Prometheus metrics
- `crates/sockudo-webhook`: webhook delivery
- `crates/sockudo-delta`: delta compression
- `crates/sockudo-push`: push providers, device registrations, channel subscriptions, publish
  pipeline, durable status, feedback, scheduler
- `crates/sockudo-ai-transport`: AI Transport validation, rollup, and conformance helpers
- `crates/sockudo-adapter`: connection handling, presence, horizontal scaling, replay/recovery,
  fanout
- `crates/sockudo-server`: binary, HTTP/WS handlers, bootstrap, durable history/version stores,
  push API

Other permanent areas:

- `benches/ai`: Criterion benchmark suite and budgets for AI hot paths
- `client-sdks`: official realtime client SDKs plus `sockudo-ai-transport-js`
- `server-sdks`: official HTTP/server SDKs
- `dashboard`: operator API and Vue UI
- `docs`: documentation site
- `config`, `ops`, `charts`, `docker-compose*.yml`: configuration, migrations, Helm, Compose
- `tests`, `tools`: conformance, load, dashboard, chaos, probes, and support tooling

High-value reference surfaces:

- `CLAUDE.md`
- `docs/content/docs/server/*`
- `docs/content/docs/clients/*`
- `docs/content/docs/server-sdks/*`
- `docs/content/docs/reference/*`
- `docs/specs/ai-transport-wire-protocol.md`
- `docs/specs/ai-transport-capacity.md`
- `config/config.toml`
- `Makefile`

## Core Principles

- Preserve V1 compatibility unless explicitly changing it.
- Keep V2-only features gated and stripped from V1 delivery.
- Reuse current abstractions before adding new ones.
- Prefer deletion over addition.
- Avoid new dependencies unless the need is explicit and justified.
- Keep behavior changes, tests, and docs in the same change.
- Treat wire payloads, HTTP response shapes, config keys, and SDK public APIs as compatibility
  contracts.
- Do not log secrets: app secrets, provider credentials, device tokens, private keys, or raw push
  payloads unless a debug-only option explicitly permits it.

## Execution Protocol

1. Inspect relevant code, docs, and package metadata first.
2. State the working understanding briefly while working.
3. For cleanup/refactor work, write a cleanup plan and lock behavior with tests when not already
   protected.
4. Make the smallest coherent change that solves the task.
5. Run narrow verification first, then widen as risk increases.
6. Read diagnostics before deciding the task is done.
7. End with what changed, what was verified, and any remaining risks.

## Search And Reading

- Prefer `rg` and `rg --files`.
- Read crate boundaries plus immediate call sites before changing shared Rust code.
- For SDK changes, read the package manifest, public README, tests, and release/build scripts before
  editing public API.
- For protocol, adapter, versioned messages, recovery, history, presence, annotations, push, or AI
  Transport work, inspect runtime code and docs before editing.
- Do not assume a feature is single-node only. Check horizontal and failure-path code.

## Monorepo And SDK Rules

- Keep server internals under `crates/`; do not move production Rust crates into SDK folders.
- Keep realtime clients under `client-sdks/<repo-name>`.
- Keep HTTP/server SDKs under `server-sdks/<repo-name>`.
- Keep `client-sdks/sockudo-ai-transport-js` with client SDKs; it is a client-side AI Transport SDK
  layered on `@sockudo/client`.
- Treat the old per-SDK GitHub repositories as archived/legacy mirrors. New source changes,
  examples, and install docs should point at this monorepo and package-local paths until registry
  publishing is fixed.
- Preserve imported SDK package names, manifests, README files, and release metadata unless the task
  is explicitly about changing them.
- Do not accidentally add SDK crates to the root Cargo workspace. Independently released Rust SDKs
  belong in `Cargo.toml` `workspace.exclude`.
- Do not use submodules for SDK imports unless explicitly requested.
- When refreshing imported SDKs, record source repository and commit in the relevant SDK group
  README.

## Feature Flag Discipline

- Check touched crate `Cargo.toml` files and `crates/sockudo-server/Cargo.toml`.
- Keep AI Transport behind Cargo feature `ai-transport` and runtime `[ai_transport]`.
- Keep push behind feature/config surfaces already used by `sockudo-push` and `sockudo-server`.
- Keep V2 feature propagation explicit; do not assume `full` is the only supported build.
- Compile targeted feature combinations when behavior crosses feature boundaries.

## Protocol And Compatibility

- Preserve Pusher-compatible Protocol V1 wire behavior.
- Keep V2 fields gated and stripped from V1 clients.
- Add or update tests for any wire-visible change.
- Keep HTTP auth, channel auth, user auth, webhook validation, and idempotency compatible with
  server SDK expectations.
- Link significant AI Transport protocol work to
  `docs/specs/ai-transport-wire-protocol.md`.

## Stateful Subsystems

### Versioned Messages

- Build on `MessageAction` and `VersionStore`; do not add parallel mutable-message state.
- Preserve `message_serial`, `version_serial`, `history_serial`, and `delivery_serial` continuity.
- Keep mutation authorization parity across HTTP and WebSocket actor contexts.
- Own-scoped mutations require verified actor identity and matching original creator identity.

### Durable History And Recovery

- Keep durable history distinct from the hot replay buffer.
- Preserve continuity semantics for `stream_id`, `serial`, cursors, and replay.
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
- Keep provider outcome and durable status retention observable.

### AI Transport

- Build on versioned messages, durable history, recovery, presence, annotations, and push.
- Validate external AI extras/headers at the edge with precise errors.
- Treat append rollup as egress-only; persistence, history, recovery, webhooks, and push see every
  original mutation.
- Protect fanout hot paths: no unbounded buffers, no locks held across `.await`, no avoidable
  per-subscriber payload copies.

## Observability

For stateful/distributed changes, consider metrics and logs for:

- success/failure counters
- degraded or reset-required state
- duplicate suppression
- bounded queue/backlog depth
- recovery source and failure code
- push provider outcome and status retention
- AI rollup ratio, flush latency, active streams, turn outcomes

## Documentation Sync

Update docs when changing:

- HTTP API request or response shapes
- protocol-visible payloads
- SDK public APIs or install instructions
- feature availability or comparison claims
- scaling semantics
- versioned messages, history, recovery, presence history, annotations, push, or AI Transport
- configuration or feature-flag behavior

Likely docs:

- `README.md`
- `docs/content/docs/server/http-api.mdx`
- `docs/content/docs/server/history-recovery.mdx`
- `docs/content/docs/server/mutable-messages.mdx`
- `docs/content/docs/server/ai-transport-*.mdx`
- `docs/content/docs/clients/*.mdx`
- `docs/content/docs/server-sdks/*.mdx`
- `docs/content/docs/reference/configuration.mdx`
- `docs/content/docs/reference/environment-variables.mdx`
- `docs/content/docs/reference/http-endpoints.mdx`
- `docs/specs/ai-transport-wire-protocol.md`

## Verification Standard

Small local change:

- run focused tests for the touched crate, package, or module
- run formatting if Rust or SDK source changed

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

SDK changes:

- use the package-local manager and scripts from that SDK README/manifest
- run the narrow language-specific test first, then add live Sockudo integration tests when behavior
  depends on the server

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
- Treat production-facing config, migrations, credentials, and destructive history operations as
  high risk.

## Final Response Contract

When closing a task, report what changed, how it was verified, and remaining risks or gaps.
