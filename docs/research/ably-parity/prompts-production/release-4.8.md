# Release 4.8 Production Prompt Pack

## Prompt 4.8-01: Architecture and public API contract

```text
You are implementing Sockudo release 4.8: SDK, Framework, And Protocol Surface.

Read and follow all AGENTS.md instructions.

Primary context:
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-ai-transport-research.md
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md

Before coding, produce the public API contract for:
- client transport API
- server transport API
- codec API
- React hooks
- Vercel integration
- error codes
- public versus internal boundary

The contract must explicitly state which surfaces are stable public API in this release.
```

## Prompt 4.8-02: Codec architecture and extensibility

```text
Design and implement the codec architecture for release 4.8.

Scope:
- encoder contract
- decoder contract
- accumulator contract
- terminal detection contract
- default codec behavior
- extension and compatibility rules

Requirements:
- framework-neutral transport core
- no hard dependency on one provider's event grammar
- enough flexibility for future adapters without leaking internal complexity
```

## Prompt 4.8-03: Client transport API implementation

```text
Implement the public client transport API for release 4.8.

Scope:
- initialization/config
- send APIs
- view/history APIs
- active turn APIs
- events and callbacks
- reconnect behavior exposure
- branch-aware or transcript-aware views

Tests and docs must treat this as a real public developer surface.
```

## Prompt 4.8-04: Server transport API implementation

```text
Implement the public server transport API for release 4.8.

Scope:
- session access
- turn creation and management
- stream publication
- cancellation hooks
- tool/HITL integration points
- error handling
- configuration model

Keep the API ergonomic but faithful to the transport's durable-session semantics.
```

## Prompt 4.8-05: React hooks implementation

```text
Implement the React hook surface for release 4.8.

Scope:
- session connection hook
- transcript/history hook
- active turn hook
- send/cancel hook surfaces
- optimistic update behavior
- branch-aware hooks if supported in this release

Requirements:
- reconnect-safe behavior
- correct hydration behavior
- examples and tests
```

## Prompt 4.8-06: Vercel AI SDK integration

```text
Implement the Vercel AI SDK integration for release 4.8.

Scope:
- transport adapter
- default codec alignment
- end-to-end example
- docs for adoption
- tests verifying the integration path

Do not produce a shallow wrapper. Make it a credible first-class integration.
```

## Prompt 4.8-07: Error code system and recovery guidance

```text
Implement the AI transport error code system for release 4.8.

Scope:
- stable error identifiers
- descriptions
- HTTP status mapping where relevant
- client recovery guidance
- server/operator remediation guidance
- docs and tests

The error model must be specific enough for production use, not generic catch-all failures.
```

## Prompt 4.8-08: Wire protocol and internals documentation

```text
Publish the technical docs for release 4.8.

Required docs:
- wire protocol
- codec architecture
- conversation tree
- transport patterns
- internal lifecycle notes where needed to support advanced adopters

These docs must match implementation exactly.
```

## Prompt 4.8-09: Examples, onboarding, and developer documentation

```text
Complete the developer-facing release 4.8 package.

Required outputs:
- official examples
- quickstart
- framework guide
- API reference
- migration or adoption notes
- troubleshooting notes

Examples must be runnable and realistic.
```

## Prompt 4.8-10: Final verification and release audit

```text
Run the final release 4.8 verification and audit.

Prove:
- the public APIs are coherent
- codec extension points are viable
- React hooks work
- Vercel integration works
- docs and examples are trustworthy

Deliver:
- findings first if any
- verification evidence
- go/no-go
- blockers if no-go
- release note draft
- explicit readiness statement for final hardening in 4.9
```
