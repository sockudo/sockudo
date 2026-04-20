# Release 4.5 Production Prompt Pack

## Prompt 4.5-01: Architecture and release contract

```text
You are implementing Sockudo release 4.5: Push Notification Platform.

Read and follow all AGENTS.md instructions.

Primary context:
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-push-notifications-research.md
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md

Before coding, produce the release contract for:
- FCM
- APNs
- Web Push
- direct publish
- batch publish
- channel push
- device/browser registration
- server-assisted activation
- admin surfaces
- operator tooling

The contract must explicitly define release goals, non-goals, acceptance criteria, and blockers.
```

## Prompt 4.5-02: Storage and registry model

```text
Design and implement the storage and registry model for release 4.5.

Scope:
- device registration schema
- browser registration schema
- client-to-device association model
- channel push subscription schema
- provider credential storage model
- state model for active/failing/failed registrations
- token rotation/update behavior

Requirements:
- secure storage of sensitive credential material
- efficient admin listing/filtering
- clean deregistration semantics
```

## Prompt 4.5-03: Publish payload and transformation layer

```text
Implement the push payload transformation layer for release 4.5.

You must support:
- generic notification fields
- generic data payload
- provider-specific override sections
- mapping to FCM
- mapping to APNs
- mapping to Web Push

Requirements:
- explicit transformation logic
- preservation of provider-specific advanced fields
- documented behavior when generic fields do not map cleanly to a provider

Include tests for payload mapping correctness.
```

## Prompt 4.5-04: Auth, capabilities, and security model

```text
Implement release 4.5 auth and security rules.

Scope:
- admin versus self-service capabilities
- device identity tokens or equivalent
- direct activation versus server-assisted activation trust boundaries
- restriction of secret-bearing operations to trusted paths
- authorization checks on admin APIs and channel push subscription APIs

Deliver:
- implementation
- threat-focused design note
- tests for privilege boundaries
```

## Prompt 4.5-05: Server/runtime implementation

```text
Implement the push runtime for release 4.5.

Ship:
- direct publish runtime
- batch publish runtime
- channel push runtime
- provider dispatchers
- retry/failure handling
- registration and deregistration runtime
- subscription save/remove runtime

Do not stop at API handlers. Complete the delivery pipeline and failure-state transitions.
```

## Prompt 4.5-06: Client/device/browser activation flows

```text
Make release 4.5 actually usable from clients.

Scope:
- device activation flow guidance
- browser/Web Push activation flow guidance
- server-assisted activation examples
- channel subscribe/unsubscribe usage
- any SDK or example adjustments needed to demonstrate realistic activation paths

The release is not complete without practical browser and device onboarding flows.
```

## Prompt 4.5-07: Test matrix and automation

```text
Build the full verification matrix for release 4.5.

Coverage must include:
- registration save/get/list/remove
- subscription save/list/remove/removeWhere equivalents
- direct publish by device
- direct publish by client
- direct publish by recipient attributes if supported
- batch publish
- channel push
- provider payload transforms
- failure state transitions
- auth boundary tests

If full live-provider testing is not possible in-repo, still build the strongest local and mocked verification possible and document the external verification gaps precisely.
```

## Prompt 4.5-08: Observability, inspection, and operator tooling

```text
Add production observability and operator tooling for release 4.5.

Scope:
- push delivery metrics
- provider failure metrics
- registration state metrics
- structured logs for provider errors
- push meta-log or equivalent inspection surface
- admin diagnostics for device and subscription state

Document how operators should debug:
- failed registrations
- failed deliveries
- stale subscriptions
- credential misconfiguration
```

## Prompt 4.5-09: Docs, examples, and setup guides

```text
Complete the release 4.5 documentation set.

Required docs:
- operator setup for FCM
- operator setup for APNs
- operator setup for Web Push
- device activation guide
- browser activation guide
- server-assisted activation guide
- direct publish guide
- channel push guide
- admin API reference
- troubleshooting guide

Include examples realistic enough for production adopters, not just toy snippets.
```

## Prompt 4.5-10: Final verification and release audit

```text
Run the final release 4.5 verification and audit.

Prove:
- Sockudo now has a real push platform
- admin and ops surfaces are sufficient
- provider integration model is coherent
- docs are usable

Deliver:
- findings first if any
- verification evidence
- go/no-go
- blockers if no-go
- release note draft
- explicit readiness statement for AI Transport background notification use in later releases
```
