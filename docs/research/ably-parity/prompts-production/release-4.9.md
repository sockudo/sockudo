# Release 4.9 Production Prompt Pack

## Prompt 4.9-01: Hardening architecture and parity contract

```text
You are implementing Sockudo release 4.9: Hardening, Operability, And Final Parity Audit.

Read and follow all AGENTS.md instructions.

Primary context:
- the full research pack under /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/

Before coding, produce the hardening contract defining:
- parity checklist
- observability checklist
- cluster/failover test matrix
- load/soak matrix
- operational tooling goals
- release blockers

This contract must map every researched capability to a concrete validation target.
```

## Prompt 4.9-02: Metrics and instrumentation

```text
Implement the metrics and instrumentation layer for release 4.9.

Scope:
- mutable messages
- annotations
- push notifications
- AI sessions
- turns
- cancellations
- branching
- tool calling
- approvals
- recovery and hydration

Requirements:
- metrics must be actionable
- names and labels must be stable and documented
- instrumentation must cover both success and failure paths
```

## Prompt 4.9-03: Logs, diagnostics, and debug surfaces

```text
Implement the logging and diagnostics layer for release 4.9.

Scope:
- structured logs for high-risk state transitions
- debug/admin surfaces for session state
- debug/admin surfaces for turn state
- debug/admin surfaces for device/subscription push state
- transcript integrity diagnostics
- mutation/version diagnostics

Document operator workflows for using these surfaces.
```

## Prompt 4.9-04: Cluster, failover, and recovery hardening

```text
Harden the distributed behavior for release 4.9.

Scope:
- cross-node mutable-message convergence
- annotation projection convergence
- push state consistency
- AI session continuity across nodes
- replay and hydration after failover
- degraded-state signaling

Build the necessary tests and fix defects before moving on.
```

## Prompt 4.9-05: Load, soak, and stress validation

```text
Build and run the load/soak validation plan for release 4.9.

Scope:
- mutation-heavy channels
- annotation-heavy workloads
- push fanout workloads
- AI token streaming workloads
- reconnect storms
- concurrent-turn sessions

If certain tests cannot be fully executed locally, still implement the harnesses and document the external execution plan precisely.
```

## Prompt 4.9-06: Admin and operator UX completion

```text
Complete the operator/admin experience for release 4.9.

Scope:
- inspectability of mutable messages and versions
- inspectability of annotation summary state
- inspectability of device and channel push state
- inspectability of AI sessions, turns, and pending HITL tasks
- actionable troubleshooting guidance

This prompt is about production operability, not end-user UI polish.
```

## Prompt 4.9-07: Docs synchronization and migration set

```text
Synchronize the entire documentation set for release 4.9.

Required outputs:
- product docs
- protocol docs
- API reference docs
- examples
- troubleshooting docs
- migration guidance
- release notes
- parity audit docs

Every shipped feature must be documented in the correct place with no major contradictions.
```

## Prompt 4.9-08: Final parity matrix

```text
Produce the final parity matrix for release 4.9.

For every researched Ably capability in:
- message mutation and annotations
- push notifications
- AI Transport

mark it as one of:
- parity achieved
- parity achieved with Sockudo-native surface differences
- not achieved

If any item is not achieved, explain exactly why and what remains.
```

## Prompt 4.9-09: Final verification and QA sweep

```text
Run the final verification and QA sweep for release 4.9.

You must prove:
- implementation correctness
- operational visibility
- documentation correctness
- parity matrix honesty
- release readiness

Findings must come first. Fix what is fixable before reporting.
```

## Prompt 4.9-10: Release sign-off

```text
Perform final sign-off for Sockudo release 4.9.

Deliver:
- go/no-go
- blocker list if no-go
- final release note draft
- executive summary of parity status
- explicit statement of any deliberate Sockudo-native differences from Ably

Do not overstate readiness. If parity is incomplete anywhere, say so precisely.
```
