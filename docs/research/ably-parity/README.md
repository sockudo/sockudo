# Ably Parity Research Pack

This folder contains a research-driven roadmap for bringing Sockudo to behavioral and capability parity with the Ably features requested for study.

Files:

- `ably-message-annotations-research.md`: Deep research on Ably message annotations, updates, deletes, appends, summaries, history semantics, and edge cases.
- `ably-ai-transport-research.md`: Deep research on Ably AI Transport, including sessions, turns, transport architecture, authentication, features, API surfaces, and internals.
- `ably-push-notifications-research.md`: Deep research on Ably Push Notifications, including configuration, activation, devices, browsers, publishing, and admin APIs.
- `sockudo-ably-parity-release-plan.md`: Release-ordered Sockudo roadmap for full feature parity in behavior and capability, without requiring Ably API compatibility.
- `master-execution-index.md`: Recommended global execution order, release gates, dependency gates, and cadence guidance.
- `prompts/release-4.3.md` through `prompts/release-4.9.md`: Copy-paste prompt packs for executing each release cleanly and professionally.
- `prompts-production/release-4.3.md` through `prompts-production/release-4.9.md`: Expanded production-grade prompt packs with deeper decomposition across architecture, storage, protocol, auth, runtime, tests, observability, docs, and release audit.
- `prompts-ultra/release-4.3.md` through `prompts-ultra/release-4.9.md`: Ultra-granular execution packs with explicit migration/backfill, cluster correctness, and framework/release-engineering prompts.

Positioning:

- The roadmap targets feature parity, not wire/API cloning.
- Sockudo should preserve its own naming, configuration model, and V1/V2 boundaries unless a release explicitly changes them.
- Releases are ordered to land foundational storage, mutation, transport, and operational guarantees before higher-level UX features.
- Two prompt systems are included:
- Three prompt systems are included:
  - `prompts/`: compact release-complete packs
  - `prompts-production/`: exhaustive production-decomposed packs for full implementation control
  - `prompts-ultra/`: maximum-control packs for production-grade end-to-end execution

Primary source set used:

- `https://ably.com/docs/messages/annotations`
- `https://ably.com/docs/messages/updates-deletes`
- `https://ably.com/docs/ai-transport`
- `https://ably.com/docs/ai-transport/why`
- `https://ably.com/docs/ai-transport/how-it-works/sessions-and-turns`
- `https://ably.com/docs/ai-transport/how-it-works/transport`
- `https://ably.com/docs/ai-transport/how-it-works/authentication`
- `https://ably.com/docs/ai-transport/getting-started/vercel-ai-sdk`
- `https://ably.com/docs/ai-transport/framework-guides/vercel-ai-sdk`
- `https://ably.com/llms.txt`
- `https://ably.com/docs/push`
- `https://ably.com/docs/push/configure/device`
- `https://ably.com/docs/push/configure/web`
- `https://ably.com/docs/push/publish`
- `https://ably.com/docs/api/realtime-sdk/push-admin`
- `https://ably.com/docs/platform/account/app/notifications`
