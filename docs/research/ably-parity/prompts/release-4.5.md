# Release 4.5 Prompt Pack

## Prompt 4.5-A: Architecture and acceptance

```text
You are implementing Sockudo release 4.5: Push Notification Platform.

Context:
- Repository: /Users/radudiaconu/Desktop/Code/Rust/sockudo
- Follow all AGENTS.md instructions.
- Target full behavioral parity with the Ably push capability set researched in:
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-push-notifications-research.md
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md
- Do not implement Ably API compatibility.

Before writing code, create the release 4.5 architecture and acceptance package.

It must define:
- operator credential model for FCM, APNs, and Web Push
- device/browser registration model
- direct activation versus server-assisted activation model
- channel push subscription model
- direct publish, batch publish, and channel-based publish flows
- provider-specific payload transformation rules
- push device/browser state model
- observability and debugging surfaces

Acceptance criteria must cover:
- FCM setup and publish
- APNs setup and publish
- Web Push setup and publish
- per-device registration and deregistration
- per-client subscriptions
- channel push fanout
- admin list/get/save/remove flows
- failure state transitions and operator visibility

Do not hand-wave credential storage, device identity, or payload mapping. They must be concrete in the design.
```

## Prompt 4.5-B: Implementation

```text
Implement Sockudo release 4.5: Push Notification Platform.

Ship the complete release scope:
- credentials/config support for FCM, APNs, and Web Push
- device and browser registration APIs
- direct activation and server-assisted activation support
- admin APIs for device registrations and channel push subscriptions
- direct publish by device, client, and recipient attributes
- batch push publish
- channel-based push via Sockudo-native extras/rules
- provider-specific payload transformation layer
- push device state tracking and error storage
- meta-log or equivalent debugging/inspection surface
- end-to-end documentation and examples

Standards:
- keep provider-specific concerns explicit rather than flattening everything into a lowest-common-denominator schema
- do not leak secrets to clients
- make failure states operator-visible
- make browser/Web Push service worker requirements explicit in docs

Minimum verification:
- at least one end-to-end test path per provider
- channel push subscription and unsubscribe behavior
- direct publish by deviceId and clientId equivalent Sockudo-native targeting
- admin list/get/remove flows
- provider override payload mapping
- failure-state surfacing and logs

Update docs comprehensively. This release is not complete without operator-facing setup guidance.
```

## Prompt 4.5-C: Verification and docs hardening

```text
Audit and harden Sockudo release 4.5: Push Notification Platform.

Verify:
- configuration setup is coherent and secure
- device/browser identity lifecycle is correct
- admin APIs are sufficient for operations
- push payload mapping behaves correctly across FCM, APNs, and Web Push
- channel-based push does not rely on ordinary realtime connection state
- observability is sufficient to debug failed deliveries
- docs are usable for operators and SDK consumers

If the repo cannot run full provider integration tests locally, say exactly what was validated and what remains external, but still verify everything possible inside the repo.
```

## Prompt 4.5-D: Release readiness audit

```text
Perform a release-readiness audit for Sockudo release 4.5.

Decide whether Sockudo now has a credible push platform, not merely a thin provider adapter.

Audit dimensions:
- operator setup quality
- device/browser lifecycle completeness
- admin/manageability
- direct and channel publish completeness
- debugging capability
- fit for later AI background-notification use

Deliver:
- go/no-go
- blocker list if any
- release note draft
- explicit downstream-readiness statement for AI Transport integration
```
