# AGENTS.md

This file is the top-level operating contract for coding agents working in `/Users/radudiaconu/Desktop/Code/Rust/sockudo`.
It applies to the entire repository unless a deeper `AGENTS.md` overrides a subset of directories.

## Role And Intent

You are working on Sockudo, a Rust real-time server with:

- strict Protocol V1 Pusher compatibility
- Protocol V2 native Sockudo features
- horizontal fanout across multiple nodes
- optional durable history, recovery, delta compression, filtering, metrics, and webhooks

Default posture: execute directly, finish the task, verify the result, and keep diffs reviewable.
Do not stop at analysis when the requested task is implementable and low-risk.

## Autonomy

- Proceed automatically on clear, reversible next steps.
- Ask only when the choice is materially ambiguous, destructive, or changes product scope.
- If blocked, try the next reasonable path before escalating.
- Use Codex native subagents for bounded parallel subtasks when that improves throughput.
- Do not delegate trivial work or work you have not scoped precisely.

## Repository Map

Cargo workspace crates:

- `crates/sockudo-protocol`: protocol message types, versioning, wire payloads
- `crates/sockudo-filter`: tag filtering
- `crates/sockudo-core`: shared traits, config, errors, common types
- `crates/sockudo-app`: app-manager backends
- `crates/sockudo-cache`: cache backends
- `crates/sockudo-queue`: queue backends
- `crates/sockudo-rate-limiter`: rate limiting and middleware
- `crates/sockudo-metrics`: Prometheus metrics
- `crates/sockudo-webhook`: webhook delivery
- `crates/sockudo-delta`: delta compression
- `crates/sockudo-adapter`: connection handling, presence, horizontal scaling, recovery
- `crates/sockudo-server`: binary, HTTP handlers, bootstrap, durable history stores

High-value docs and reference surfaces:

- `README.md`
- `FEATURE_COMPARISON.md`
- `docs/content/2.server/*`
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
3. For cleanup or refactor work:
   - write a cleanup plan before editing
   - lock behavior with regression tests first when not already protected
4. Make the smallest coherent change that solves the task.
5. Run the narrowest meaningful verification first, then widen as needed.
6. Read test and diagnostic output before deciding the task is done.

## Search And Read Strategy

- Prefer `rg` and `rg --files` for discovery.
- Read the crate boundary plus immediate call sites before changing shared code.
- For adapter, protocol, recovery, presence, or history work, inspect both runtime code and user-facing docs before editing.
- Do not assume a feature is single-node only. Check horizontal and failure-path code.

## Editing Rules

- Use ASCII unless the file already requires Unicode.
- Keep comments rare and high-signal.
- Preserve public API names unless a breaking change is explicitly requested.
- Avoid broad mechanical rewrites unless the task is explicitly a cleanup pass.
- Never revert unrelated user changes.
- If the worktree is dirty, isolate your edits and work around existing changes.

## Rust Standards

- Favor straightforward ownership and explicit data flow over clever abstractions.
- Prefer existing error types and `Result` plumbing used by the surrounding crate.
- Keep feature-gated code consistent across crates.
- Avoid hidden allocations or cloning in hot paths unless justified.
- Treat adapter, recovery, and fanout code as latency-sensitive paths.

## Feature Flag Discipline

Sockudo relies heavily on feature propagation.

- Check `Cargo.toml` in the touched crate and the server crate before adding feature-specific code.
- If you add a capability in an implementation crate, confirm whether it must be propagated through `crates/sockudo-server/Cargo.toml`.
- Do not accidentally make a V2-only feature visible to V1 clients.
- Do not assume `full` is the only supported build. Keep targeted feature combinations compiling.

## Protocol And Compatibility Rules

When touching protocol, connection, or event code:

- Preserve Protocol V1 Pusher-compatible behavior unless the task explicitly changes it.
- Keep Protocol V2-only fields gated appropriately.
- Maintain canonical event-name handling and prefix translation behavior.
- Treat wire payload shape as a compatibility contract.
- Add or update tests for any wire-visible change.

## Presence, Recovery, History, And Scaling Rules

These are the highest-risk areas in this repo. Be conservative.

### Presence

- Preserve first-join and last-leave correctness.
- Distinguish current presence membership from historical or diagnostic state.
- Consider reconnect races, duplicate disconnects, and multi-connection users.
- Do not introduce duplicate lifecycle events for one authoritative transition.

### Connection recovery

- Preserve continuity semantics for `stream_id`, `serial`, and replay behavior.
- Fail closed when continuity cannot be proven.
- Never silently turn a degraded stream into an apparently authoritative one.

### Durable history

- Keep durable history distinct from the in-memory replay buffer.
- Prefer append-only models, bounded retention, and opaque pagination cursors.
- If a history change affects stream continuity or authoritative state, update both runtime logic and docs.

### Horizontal scaling

- Design for duplicate delivery, retries, node death, late messages, and partial failure.
- Check fanout, deduplication, heartbeats, orphan cleanup, and aggregation paths together.
- Do not assume single-node ordering or perfect transport reliability.

## Observability Requirements

If you change behavior in a stateful or distributed path, consider whether metrics, logs, or inspection endpoints need updates.

At minimum, think about:

- success and failure counters
- degraded-state visibility
- duplicate suppression visibility
- bounded queue or backlog visibility
- operator-facing diagnostics for resets or repair-required states

## Documentation Sync

Update docs when changing:

- HTTP API request or response shapes
- protocol-visible payloads
- feature availability or comparison claims
- scaling semantics
- history, presence, recovery, or webhook behavior
- configuration or feature-flag behavior

Likely docs to update:

- `docs/content/2.server/3.http-api.md`
- `docs/content/2.server/4.scaling.md`
- `docs/content/2.server/12.webhooks.md`
- `docs/content/2.server/15.connection-recovery.md`
- `docs/content/2.server/21.channel-history.md`
- `docs/content/2.server/23.multinode-history-ops.md`
- `FEATURE_COMPARISON.md`
- `README.md`

## Verification Standard

Match verification depth to risk.

Small local change:

- run focused tests for the touched crate or module
- run formatting if you changed Rust code

Standard backend or API change:

- `cargo fmt --all`
- focused `cargo test` for touched crates
- `cargo test --workspace` if the change crosses crate boundaries or shared contracts

High-risk protocol, presence, scaling, recovery, or history change:

- `cargo fmt --all`
- focused tests around the touched subsystem
- relevant integration tests
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets` when practical

If full verification is too expensive or blocked, say exactly what was not run and why.

## Useful Commands

Core Rust workflow:

- `cargo fmt --all`
- `cargo test --workspace`
- `cargo test -p sockudo-adapter`
- `cargo test -p sockudo-core`
- `cargo test -p sockudo`
- `cargo clippy --workspace --all-targets`
- `cargo build -p sockudo`
- `cargo build -p sockudo --features full`

Docker and environment workflow:

- `make setup`
- `make dev`
- `make test`
- `make benchmark`
- `make health`

Use the narrowest command that proves the claim before escalating to a full workspace run.

## Commit Quality

Commits should explain why, not just what.

Preferred commit shape:

1. intent line focused on motivation
2. short rationale body if the change is non-trivial
3. optional trailers for constraints, rejected approaches, confidence, scope risk, and testing

Example trailers:

- `Constraint:`
- `Rejected:`
- `Confidence:`
- `Scope-risk:`
- `Directive:`
- `Tested:`
- `Not-tested:`

## Safety Constraints

- Never run destructive commands like `git reset --hard`, `git checkout --`, or broad `rm -rf` unless explicitly requested.
- Do not rewrite history unless explicitly requested.
- Do not amend commits unless explicitly requested.
- Treat production-facing config, migrations, and destructive history operations as high-risk.

## Final Response Contract

When closing a task, report:

- what changed
- how it was verified
- any remaining risks, gaps, or unrun checks

For larger changes, also mention:

- changed files
- docs updated
- notable compatibility or operational considerations

## Decision Heuristics

When in doubt:

- prefer compatibility over cleverness
- prefer explicit state over inferred state
- prefer bounded behavior over unbounded behavior
- prefer operator-visible failure over silent corruption
- prefer a smaller correct change over a larger speculative one
