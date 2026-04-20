# Release 4.8 Prompt Pack

## Prompt 4.8-A: Architecture and acceptance

```text
You are implementing Sockudo release 4.8: SDK, Framework, And Protocol Surface.

Context:
- Repository: /Users/radudiaconu/Desktop/Code/Rust/sockudo
- Follow all AGENTS.md instructions.
- Use:
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-ai-transport-research.md
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md
- Assume releases 4.6 and 4.7 exist.

Before coding, produce the public-surface plan for release 4.8.

It must define:
- public client transport API
- public server transport API
- codec API shape
- default codec behavior
- React hook surface
- Vercel AI SDK integration strategy
- error code system
- AI transport wire protocol publication approach
- internal versus public contract boundaries

Acceptance criteria must cover:
- framework-neutral core transport
- at least one blessed Vercel integration path
- React hooks that cover durable sessions, active turns, and history
- published error codes with recovery guidance
- documented wire protocol and conversation-tree semantics
- runnable examples that prove the public API is usable

Do not start implementation until the team can explain exactly which parts are stable public API and which remain internal.
```

## Prompt 4.8-B: Implementation

```text
Implement Sockudo release 4.8: SDK, Framework, And Protocol Surface.

Ship the full public surface:
- client transport API
- server transport API
- codec API and default codec implementation
- React hooks
- Vercel AI SDK integration
- AI transport error code catalog
- wire protocol documentation
- codec architecture documentation
- conversation tree documentation
- transport-pattern documentation
- runnable examples and developer docs

Standards:
- keep the transport core framework-neutral
- treat public APIs as versioned contracts
- make error handling explicit and actionable
- ensure examples are production-shaped, not toy-only

Minimum verification:
- examples build and run
- public APIs are exercised by tests
- error codes are triggered by at least representative tests
- Vercel integration works end to end
- React hooks behave correctly for reconnect, history loading, and active turn tracking

Required docs:
- overview for developers
- API reference
- framework guide
- wire protocol reference
- internals reference
```

## Prompt 4.8-C: Verification and docs hardening

```text
Audit and harden Sockudo release 4.8.

Verify:
- the public API is coherent and consistent
- Vercel integration is not overfitted to one narrow example
- React hooks correctly model durable-session behavior
- codec extension points are sufficient but not overly leaky
- wire protocol docs match implementation exactly
- error codes are complete, accurate, and actionable
- examples are trustworthy

If defects or doc mismatches exist, fix them before reporting.

Final report:
- findings first if any
- verification evidence
- remaining risks
- readiness statement for GA hardening in release 4.9
```

## Prompt 4.8-D: Release readiness audit

```text
Perform a release-readiness audit for Sockudo release 4.8.

Determine whether Sockudo now exposes a credible AI Transport developer platform.

Audit:
- API stability
- docs quality
- example quality
- framework completeness
- operational clarity
- suitability for external adopters

Deliver:
- go/no-go
- blockers if any
- release note draft
- explicit statement of what must still happen in 4.9 before the platform can be considered parity-grade
```
