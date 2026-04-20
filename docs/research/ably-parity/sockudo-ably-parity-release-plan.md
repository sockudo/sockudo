# Sockudo Release Plan For Full Behavioral Parity With Ably Features

## Planning principles

- Target behavioral and capability parity, not Ably API compatibility.
- Preserve Sockudo's own V1/V2 product line: these parity features should be Sockudo-native and V2-first unless there is a compelling reason otherwise.
- Land storage and protocol substrate before SDK ergonomics.
- Treat history, ordering, identity, authorization, and clustered convergence as release blockers, not polish.
- Do not ship advanced UX features until the underlying mutation, session, and replay models are stable.

## Current Sockudo baseline

Grounded in the current repository docs and feature comparison:

- Sockudo already has V2 serials, `message_id`, connection recovery, extras, delta compression, tag filtering, webhook support, clustered adapters, and growing durable-history capabilities.
- Sockudo does not yet have Ably-equivalent:
  - mutable versioned messages
  - annotation summaries and raw annotation event streams
  - push notification platform
  - AI Transport session/turn transport
  - branching/edit-regenerate transcript semantics
  - framework-specific AI adapters and APIs

## Release map

### Release 4.3: Versioned Durable Messages

Goal:

- Establish the message mutation substrate required by both Ably-style mutable messages and AI Transport.

Scope:

- stable top-level message identity separate from version identity
- durable version store and version ordering
- `message.create`, `message.update`, `message.delete`, `message.append`, `message.summary` action model
- REST and realtime mutation flows for update/delete/append
- shallow-mixin semantics for update/delete
- latest-version replacement in history at original history position
- `getMessage` and `getMessageVersions` equivalents in Sockudo-native APIs
- mutation capabilities and identified-client ownership rules
- clustered winner selection and convergence rules
- mutation conflation guarantees for update/delete/append
- initial documentation and tests for all message actions

Why first:

- annotations, AI token streaming, edit/regenerate, and transcript branching all depend on this substrate.

Exit criteria:

- latest-version history is correct under racing updates
- append streams converge correctly under realtime and reconnect scenarios
- full version history is queryable
- identity-based own/any authorization is enforced

Prompt pack:

- `prompts/release-4.3.md`
- `prompts-production/release-4.3.md`
- `prompts-ultra/release-4.3.md`

### Release 4.4: Annotation Engine And Summary Model

Goal:

- Deliver Ably-equivalent annotation capability on top of the 4.3 message substrate.

Scope:

- annotation persistence model
- annotation types using Sockudo-native type schema with namespace + summarizer + version
- `total`, `flag`, `distinct`, `unique`, `multiple` summary engines
- annotation publish/delete APIs
- summary rollup generation
- `message.summary` delivery semantics
- individual annotation subscription mode
- clipping behavior for oversized summaries
- annotation payload storage separate from summary projection
- moderation/read-receipt/reaction examples and docs

Why second:

- it is a natural extension of versioned durable messages
- it avoids entangling annotation complexity into the already large AI Transport releases

Exit criteria:

- summaries remain correct under concurrent annotation churn
- clipped summary signaling is deterministic
- individual annotation event subscriptions and summary subscriptions both work

Prompt pack:

- `prompts/release-4.4.md`
- `prompts-production/release-4.4.md`
- `prompts-ultra/release-4.4.md`

### Release 4.5: Push Notification Platform

Goal:

- Build Sockudo's push platform across devices, browsers, and channels.

Scope:

- operator credential configuration for FCM, APNs, and Web Push
- device/browser registration and identity-token model
- direct activation and server-assisted activation flows
- admin APIs for device registrations and channel subscriptions
- direct publish by device, client, and recipient attributes
- batch publish
- channel-based push using channel rules and message extras
- payload transformation layer for FCM/APNs/Web Push
- APNs header support for background notifications
- per-device state model: active/failing/failed
- push log / inspection surface
- docs, examples, and operational guidance

Why here:

- push is a standalone product surface with its own operational complexity
- AI Transport later depends on this for background completion notifications

Exit criteria:

- end-to-end verified delivery for FCM/APNs/Web Push
- admin registry surfaces are complete and debuggable
- channel push works independently of ordinary realtime subscription state

Prompt pack:

- `prompts/release-4.5.md`
- `prompts-production/release-4.5.md`
- `prompts-ultra/release-4.5.md`

### Release 4.6: AI Transport Core

Goal:

- Introduce the durable session and turn transport that makes AI interactions resilient and multi-device.

Scope:

- session-as-channel model
- turn lifecycle model
- transport core for server and client
- authenticated session access using Sockudo tokens/capabilities
- token streaming patterns:
  - message-per-response
  - message-per-token
- turn-scoped cancellation and authorization hooks
- reconnection and recovery
- history hydration and replay for longer disconnects
- multi-device session consistency
- optimistic user message insertion and reconciliation
- agent presence states
- initial wire contract for AI turns and transport metadata

Why here:

- this is the minimum viable AI Transport parity layer
- advanced UX features should not precede durable session correctness

Exit criteria:

- streams survive disconnect/reconnect
- multiple devices remain in sync
- turn-scoped cancellation does not kill whole sessions
- optimistic inserts reconcile without duplicates

Prompt pack:

- `prompts/release-4.6.md`
- `prompts-production/release-4.6.md`
- `prompts-ultra/release-4.6.md`

### Release 4.7: AI Interaction Control And Collaboration

Goal:

- Add the advanced interaction semantics that make AI Transport feel like a product, not just a transport.

Scope:

- conversation branching
- edit and regenerate
- interruption and barge-in
- double texting policies
- concurrent turns
- tool calling
- human-in-the-loop approval flows
- chain-of-thought/reasoning stream channels or message classes
- background completion hooks that can trigger push re-engagement

Why here:

- these features all depend on the transcript tree, mutation model, and turn transport already being stable

Exit criteria:

- transcript tree remains coherent under edit/regenerate and concurrent turns
- tool calls and approvals persist and survive reconnect
- user interruption policies are explicit and testable

Prompt pack:

- `prompts/release-4.7.md`
- `prompts-production/release-4.7.md`
- `prompts-ultra/release-4.7.md`

### Release 4.8: SDK, Framework, And Protocol Surface

Goal:

- Turn the core transport into a usable developer product with explicit SDK surfaces and framework integration.

Scope:

- public client transport API
- public server transport API
- React hooks
- Vercel AI SDK integration
- codec API and default codec(s)
- error code catalog and recovery guidance
- AI transport protocol documentation
- wire protocol publication
- codec architecture docs
- conversation tree docs
- transport-pattern internals docs
- first-class examples for Vercel and at least one non-Vercel server flow

Why here:

- platform parity is incomplete without a usable SDK and framework story
- internal contracts should be explicit before GA hardening

Exit criteria:

- documented and versioned transport APIs exist
- official examples run cleanly
- error handling and recovery guidance are documented and enforced in SDK behavior

Prompt pack:

- `prompts/release-4.8.md`
- `prompts-production/release-4.8.md`
- `prompts-ultra/release-4.8.md`

### Release 4.9: Hardening, Operability, And Final Parity Audit

Goal:

- Close the gap from "feature present" to "production-credible parity".

Scope:

- cluster/failover validation across all new mutable-message and AI session flows
- metrics and logs for mutations, annotations, push, turns, cancellation, replay, and branching
- admin/debug surfaces for session state, turn state, device state, and transcript repair
- load and soak tests
- multi-backend validation
- docs synchronization across server reference and product docs
- migration guides
- parity audit against the three research dossiers
- release notes and compatibility caveats

Why last:

- this is where parity becomes trustworthy
- Ably-grade features without Ably-grade operability will fail in production

Exit criteria:

- all scoped parity items are either shipped or explicitly documented as intentionally different Sockudo-native behavior
- observability exists for every high-risk stateful path
- docs and examples match implementation

Prompt pack:

- `prompts/release-4.9.md`
- `prompts-production/release-4.9.md`
- `prompts-ultra/release-4.9.md`

## Dependency graph

Release dependencies:

- `4.3 -> 4.4`
- `4.3 -> 4.6`
- `4.5 -> 4.7` for AI background re-engagement via push
- `4.6 -> 4.7`
- `4.6 -> 4.8`
- `4.7 -> 4.9`
- `4.8 -> 4.9`

## Prompt execution order

Use each release prompt pack in this sequence:

1. architecture and acceptance
2. implementation
3. verification and docs
4. release-readiness audit

Do not skip prompt 1 on releases `4.3`, `4.6`, `4.7`, and `4.8`; they contain the highest architectural risk.

## What not to do

- Do not clone Ably endpoint names or SDK names unless Sockudo explicitly wants compatibility.
- Do not ship AI Transport features on top of ephemeral in-memory transcript state.
- Do not treat push, annotation summaries, or branching as UI-only concerns.
- Do not postpone clustered convergence semantics to a later "cleanup" release.
- Do not ship Vercel integration before the codec and wire contract are stable enough to support it.

## Deliverable index

- Master roadmap: this file
- Prompt packs:
  - `prompts/release-4.3.md`
  - `prompts/release-4.4.md`
  - `prompts/release-4.5.md`
  - `prompts/release-4.6.md`
  - `prompts/release-4.7.md`
  - `prompts/release-4.8.md`
  - `prompts/release-4.9.md`
  - `prompts-production/release-4.3.md`
  - `prompts-production/release-4.4.md`
  - `prompts-production/release-4.5.md`
  - `prompts-production/release-4.6.md`
  - `prompts-production/release-4.7.md`
  - `prompts-production/release-4.8.md`
  - `prompts-production/release-4.9.md`
  - `prompts-ultra/release-4.3.md`
  - `prompts-ultra/release-4.4.md`
  - `prompts-ultra/release-4.5.md`
  - `prompts-ultra/release-4.6.md`
  - `prompts-ultra/release-4.7.md`
  - `prompts-ultra/release-4.8.md`
  - `prompts-ultra/release-4.9.md`
