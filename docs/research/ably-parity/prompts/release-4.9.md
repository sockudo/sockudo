# Release 4.9 Prompt Pack

## Prompt 4.9-A: Hardening plan and acceptance

```text
You are implementing Sockudo release 4.9: Hardening, Operability, And Final Parity Audit.

Context:
- Repository: /Users/radudiaconu/Desktop/Code/Rust/sockudo
- Follow all AGENTS.md instructions.
- Use the complete research pack under /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/
- Assume releases 4.3 through 4.8 are implemented.

Before editing code, produce the 4.9 hardening plan and final acceptance package.

It must define:
- observability gaps across mutable messages, annotations, push, and AI Transport
- operational/debug surfaces still missing
- multi-node and failover test matrix
- load/soak test matrix
- doc synchronization requirements
- final parity audit checklist against all three research dossiers

Acceptance criteria must cover:
- metrics
- logs
- meta/debug channels or equivalent
- admin inspection surfaces
- cluster correctness
- recovery correctness under load
- push reliability visibility
- transcript repair visibility
- documentation completeness

Do not proceed without an explicit parity checklist that maps every researched Ably capability to either:
- shipped Sockudo behavior
- intentionally different Sockudo-native behavior that still satisfies capability parity
```

## Prompt 4.9-B: Implementation and hardening

```text
Implement Sockudo release 4.9: Hardening, Operability, And Final Parity Audit.

Ship the complete hardening scope:
- metrics for message mutations, annotations, push, turns, cancellations, branching, tool calls, and recovery
- structured logs for high-risk state transitions
- inspection/debug/admin surfaces for session state, turn state, push device state, and transcript health
- multi-node validation fixes
- load and soak test improvements
- doc synchronization across product docs, protocol docs, and examples
- final parity audit document inside the repo

Standards:
- do not add noisy metrics that operators cannot act on
- do not leave critical failure modes invisible
- ensure every stateful distributed feature has at least one operator-facing diagnostic path

Minimum verification:
- focused tests for each newly instrumented subsystem
- relevant cluster/integration tests
- broader workspace tests where contracts changed
- explicit evidence for parity checklist completion
```

## Prompt 4.9-C: Final verification and parity audit

```text
Run the final verification and parity audit for Sockudo release 4.9.

You must prove:
- mutable messages are production-credible
- annotations are correct and observable
- push is manageable and debuggable
- AI Transport is durable, recoverable, and operator-visible
- SDK/docs/examples are in sync
- the parity checklist is complete

If gaps remain, fix them. Then provide:
- findings first if any
- verification evidence
- final parity matrix
- remaining deliberate differences from Ably, if any
```

## Prompt 4.9-D: Release sign-off

```text
Perform final release sign-off for Sockudo release 4.9.

Deliver:
- go/no-go recommendation
- final blocker list if no-go
- polished release note draft
- short executive summary explaining why Sockudo can now claim full future feature parity in behavior and capability for:
  - mutable messages and annotations
  - push notifications
  - AI Transport

Be precise. If any capability is not truly complete, call it out explicitly rather than overstating readiness.
```
