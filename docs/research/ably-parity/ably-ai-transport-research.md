# Ably AI Transport Research

## Scope

This research covers Ably AI Transport using the official documentation corpus indexed at:

- `https://ably.com/docs/ai-transport`
- `https://ably.com/docs/ai-transport/why`
- `https://ably.com/docs/ai-transport/how-it-works/sessions-and-turns`
- `https://ably.com/docs/ai-transport/how-it-works/transport`
- `https://ably.com/docs/ai-transport/how-it-works/authentication`
- `https://ably.com/docs/ai-transport/getting-started/vercel-ai-sdk`
- `https://ably.com/docs/ai-transport/framework-guides/vercel-ai-sdk`
- `https://ably.com/docs/ai-transport/internals`
- `https://ably.com/llms.txt`

The AI Transport docs are explicitly marked by Ably as new and actively being written. That matters for parity planning:

- behavior is directional and product-defining
- surface area is still expanding
- Sockudo should target the capability model, not page-by-page doc shape

## Executive summary

Ably AI Transport is not "chat over WebSockets". It is a durable, channel-oriented session layer for AI applications. The transport decouples:

- the HTTP request that starts work
- the session that persists conversation state
- the stream that carries model output
- the control plane that allows cancellation, interruption, tool approval, and multi-device participation

Its defining themes are:

- durable sessions
- turn-scoped lifecycle
- branch-aware conversation state
- resilient token streaming
- multi-device continuity
- bidirectional control
- codec-driven framework neutrality
- SDK surfaces for Vercel AI SDK, React, and custom integrations

## Overview

Ably describes AI Transport as a drop-in infrastructure layer for AI applications that upgrades direct HTTP streaming into a stateful, durable, multi-device interaction model.

The overview page highlights:

- durable sessions between agents and users
- reliable token streaming
- multi-device continuity
- bidirectional control
- compatibility with different model stacks and frameworks
- background processing and agent presence

Ably positions the product on top of its global pub/sub platform rather than as a standalone AI orchestration engine.

## Why AI Transport

Ably's "why" page frames the product against the limitations of HTTP/SSE streaming.

### Problems Ably is solving

#### Streams fail on disconnection

- direct streamed HTTP ties response lifetime to a single connection
- network changes, refreshes, or suspended devices break the stream
- resumable SSE would require custom sequencing, buffering, and replay infrastructure

#### Sessions do not span devices

- a second tab or phone cannot join the same in-progress stream
- users cannot move between surfaces without application-specific plumbing

#### Clients cannot reach the agent through the same session

- SSE is effectively one-way after request initiation
- closing the request to "stop" the model destroys resumability
- even WebSocket point-to-point designs still do not solve shared-session upstream control

#### Multi-agent systems become proxy-heavy

- with exclusive client-to-orchestrator pipes, subagent visibility and user control must be proxied through one orchestrator

### Durable session answer

Ably's answer is a durable shared session:

- the agent writes events to the session
- clients connect independently to that session
- the session survives disconnections
- any participant can publish control signals into the session

This is the conceptual heart of the product and should drive Sockudo's design.

## How it works

## Sessions and turns

Ably makes two concepts central:

- session: the conversation
- turn: one prompt/response cycle inside that conversation

### Sessions

- a session is an Ably channel
- it contains the ongoing conversation history
- all participants on the channel see the same prompts, responses, and control messages
- because the session is channel-based, not connection-based, it survives disconnects
- reconnecting clients resume from the last received point or hydrate from history if the gap is longer

### Turns

- each prompt/response unit is a turn
- turns create structure around otherwise flat token streams
- turns can be tracked, cancelled, replayed, and run concurrently

### Turn lifecycle

Server-side lifecycle from the sessions-and-turns page:

1. `transport.newTurn({ turnId, clientId })`
2. `turn.addMessages(messages)`
3. `turn.streamResponse(stream)`
4. `turn.end(reason)`

End reasons:

- `complete`
- `cancelled`
- `error`

On the client side:

- `view.send(messages)` creates a turn
- the caller receives an `ActiveTurn`
- that turn exposes a stream and a turn-scoped cancel handle

## Transport

Ably documents a two-layer architecture:

- core transport
- pluggable codec

### Server transport

The server transport:

- publishes to an Ably channel
- manages turns
- listens for control signals such as cancellation
- decouples HTTP request completion from token delivery

Ably's documented single-turn flow:

1. client sends HTTP POST
2. server creates turn and publishes user message
3. server invokes LLM and pipes output through codec to channel
4. server ends turn

Important behavior:

- the HTTP response is immediate
- token flow travels through the Ably channel, not the HTTP response body

### Client transport

The client transport:

- subscribes before attach so no messages are missed
- decodes inbound Ably messages through the codec
- builds a branch-aware conversation tree
- provides views for pagination and branch navigation
- tracks active turns across clients
- handles optimistic user message insertion

### Codec

Ably describes four codec responsibilities:

1. encoder
2. decoder
3. accumulator
4. terminal detection

The default Vercel-oriented codec is `UIMessageCodec`.

This is a strong design signal for Sockudo:

- transport core and framework adaptation should be separate layers
- the platform should not hard-wire one AI framework's event grammar into the wire protocol

## Authentication

Ably documents three layers of authentication/authorization in AI Transport:

- Ably token authentication for channel access
- HTTP headers for server endpoint authorization
- cancel authorization for deciding who may stop turns

### Documented flow

1. app auth server authenticates the user
2. app auth server issues an Ably-compatible token, with JWT recommended
3. client fetches tokens via `authCallback` and auto-refreshes before expiry
4. authenticated Pub/Sub client provides channels to AI Transport

### Important identity rules

- never use API keys in client-side code
- `clientId` is used throughout AI Transport to identify message senders, turn initiators, and cancellation requesters
- Ably expects the client ID to be embedded in the signed token so it cannot be spoofed

### AI Transport-specific operational dependency

- Ably explicitly states AI Transport requires the `Message annotations, updates, deletes, and appends` channel rule to be enabled on conversation channels
- because these features force persistence, AI Transport inherits that storage and billing implication

### Capability model

The auth page and documentation index indicate AI Transport uses Ably capability controls to govern publish, subscribe, history, and cancel-related operations. Sockudo parity needs a similarly explicit capability model at the channel/session level.

## Getting started

Ably's overview and Vercel quickstart show:

- Vercel AI SDK is the primary first-party getting-started path today
- OpenAI, Anthropic, Vercel AI SDK, and LangGraph are all framed as valid integration targets
- the quickstart builds a streaming chat app with durable sessions and multi-tab continuity

The Vercel getting-started flow includes:

- installing `@ably/ai-transport`, `ably`, Vercel AI SDK packages, and app dependencies
- creating an Ably token endpoint
- wrapping the app with an authenticated Ably provider
- wiring AI Transport into a chat UI

This implies Sockudo's eventual parity story should include:

- framework-neutral core
- at least one blessed end-to-end integration path
- runnable examples, not just API primitives

## Framework guides

Ably's documentation index confirms a dedicated framework guide for:

- Vercel AI SDK

The overview page also lists guided getting-started flows for:

- OpenAI
- Anthropic
- Vercel AI SDK
- LangGraph

That means framework parity is not just transport parity. Sockudo will need:

- transport core
- canonical adapters/examples
- documentation for multiple model stacks

## Feature research

## Token streaming

Opened documentation confirms:

- token streaming is a first-class feature
- streams are durable and survive tab changes, refreshes, device switches, and temporary network loss
- Ably applies append rollup to reduce cost while keeping realtime delivery

Ably also explicitly supports two token patterns from the documentation index and overview:

- message-per-response
- message-per-token

Implication for Sockudo:

- parity requires both a correctness model and a cost-control model
- append rollup or equivalent conflation is part of the feature, not an optional optimization

## Cancellation

The sessions-and-turns page plus documentation index establish the core behavior:

- cancellation is scoped to turns, not connections
- filters may target:
  - a specific turn
  - all turns from a client
  - your own turns
  - all turns
- the server may authorize or reject cancellation through an `onCancel` hook
- the dedicated feature page is titled around scoped cancel signals, server-side authorization, and graceful abort handling

Parity implication:

- cancellation must be a message-level control plane, not a TCP disconnect side effect

## Reconnection and recovery

Opened documentation confirms:

- when a client disconnects, the agent continues streaming to the session channel
- Ably SDK reconnects automatically
- the client transport uses `untilAttach` to load missed messages
- state is restored without manual retry logic
- longer gaps fall back to loading conversation state from history

This is not equivalent to plain pub/sub recovery. It is AI conversation hydration with in-progress-stream continuity.

## Multi-device sessions

Opened overview content and the dedicated feature page establish:

- sessions are shared channels
- any device that joins sees prompts, responses, and control signals
- multiple users can participate in one session
- late joiners load history and become current

Parity implication:

- session identity must be decoupled from connection identity
- single-session multi-subscriber consistency is mandatory

## History and replay

The sessions-and-turns page plus documentation index define this feature as:

- loading conversation history from channels
- paginated history
- gapless continuity
- scroll-back patterns

The reconnection documentation also says:

- short disconnects resume via connection recovery
- longer gaps hydrate from channel history

Sockudo parity therefore needs both:

- realtime recovery
- branch-aware durable transcript replay

## Conversation branching

The documentation index describes conversation branching as:

- branching conversations
- sibling branch navigation
- preserved full history

This means the underlying transcript is not a linear list; it is a tree with alternate futures.

## Interruption and barge-in

The documentation index describes this page as:

- cancel-then-send
- send-alongside patterns
- responsive AI interactions

Interpretation:

- users can interrupt a streaming agent and redirect the conversation
- the framework supports both replacement and overlap behaviors
- interruption is more than cancellation because it defines what the next user message does relative to the active turn

## Concurrent turns

Sessions-and-turns and the index establish:

- multiple turns may be active simultaneously
- each has independent streams and independent cancellation
- multi-agent support is explicitly part of the feature

This is a major capability difference from single-stream chat systems.

## Edit and regenerate

The documentation index defines this as:

- editing user messages
- regenerating AI responses
- forking conversations
- navigating between branches

This feature depends on conversation branching and durable message versioning.

## Tool calling

The documentation index and overview state:

- tool invocations and tool results are streamed through AI Transport
- both server-executed and client-executed tools are supported
- tool state is persistent

This is important:

- tools are not treated as out-of-band implementation details
- tool calls become durable conversational artifacts

## Human-in-the-loop

Opened documentation confirms:

- HITL is built on tool calling primitives
- agent publishes a pending tool call requiring approval
- the agent pauses until a connected client approves or rejects
- because the approval request is an Ably message, it persists across reconnect and device switch

Parity implication:

- approval workflows are transport-visible state machines, not ephemeral in-memory callbacks

## Optimistic updates

The documentation index describes:

- user messages appear instantly
- optimistic insertion
- automatic reconciliation when the server confirms

The transport architecture page also says client transport handles optimistic insertion before server confirmation.

Sockudo parity needs:

- provisional local message nodes
- stable reconciliation rules
- no duplicate or re-ordered transcript artifacts after confirmation

## Agent presence

Opened documentation confirms:

- agent presence is built on Ably Presence
- agents publish statuses such as thinking, streaming, idle, and offline
- clients can render typing indicators, streaming indicators, and offline state
- presence can also drive cost controls when no users are connected

This is both UX and operational control surface.

## Push notifications

The documentation index defines this AI Transport feature as:

- notifying users when background tasks complete
- using Ably Push Notifications
- reaching users while offline

Combined with the overview's background-processing section, this implies:

- sessions may outlive user presence
- push is a re-engagement bridge back into the durable session

## Chain of thought

The documentation index defines this as:

- streaming reasoning/thinking content alongside responses
- displaying chain-of-thought in realtime

Even if product teams do not expose full reasoning to end users, the transport model supports a separate reasoning stream category. Sockudo should account for policy controls here.

## Double texting

The documentation index defines this as:

- users sending multiple messages while AI is still streaming
- queue or concurrent execution strategies

This is effectively productized overlap policy for user prompts.

## API reference research

The documentation index confirms these API reference surfaces:

- `client transport`
- `server transport`
- `react hooks`
- `vercel integration`
- `codec`
- `error codes`

### Client transport

Index description:

- options
- methods
- events
- `View` interface

The transport page adds expected behavior:

- active turn tracking
- branch-aware projections
- pagination
- send/loadOlder APIs

### Server transport

Index description:

- turn lifecycle
- cancel routing
- configuration

### React hooks

Index description:

- generic hooks
- Vercel-specific hooks
- chat UI building

### Vercel integration

Index description:

- `UIMessageCodec`
- `ChatTransport`
- pre-bound factories

The framework guide also frames AI Transport as the durable-session layer complementing Vercel's UI/model abstractions.

### Codec API

Index description plus transport page:

- custom codec support
- encoder
- decoder
- accumulator
- terminal detection

### Error codes

Index description:

- codes
- descriptions
- HTTP status
- recovery guidance

Parity implication:

- Sockudo needs a dedicated AI Transport error model, not generic pub/sub errors alone

## Internals research

Ably exposes dedicated internals docs for:

- `wire protocol`
- `codec architecture`
- `conversation tree`
- `transport patterns`

### Internals overview

The internals landing page positions these as the implementation substrate of AI Transport rather than external optional details. That is a strong design signal: the product's behavior depends on explicit protocol and tree semantics.

### Wire protocol

The documentation index states this covers:

- Ably channel wire format for AI Transport
- headers
- lifecycle events
- content messages
- message identity

This confirms AI Transport is a layered protocol over pub/sub, not just ad hoc message naming.

### Codec architecture

The index says this covers:

- encoder
- decoder
- accumulator
- lifecycle tracker internals

### Conversation tree

The index says this covers:

- serial ordering
- sibling groups
- fork chains
- flatten algorithm

This is the strongest evidence that branching/edit/regenerate are structural transcript features, not UI tricks.

### Transport patterns

Opened search snippet confirms internal components such as:

- `StreamRouter`
- `TurnManager`
- `pipeStream`
- cancel routing

The snippet specifically states `StreamRouter` routes flat channel events into per-turn streams using `x-ably-turn-id`.

## Behavioral conclusions relevant to Sockudo

To match Ably AI Transport behaviorally, Sockudo needs all of the following:

- session-as-channel semantics
- durable history-backed transcripts
- turn-scoped state and scoped cancellation
- client recovery plus longer-gap hydration
- branch-aware transcript model
- explicit optimistic reconciliation
- presence-aware agent status
- an AI-specific wire protocol
- a codec extension system
- framework adapters
- operational auth/capability boundaries for users, agents, and cancellation
- push re-engagement for offline continuation

Anything less than that is only partial parity.

## Recommended implementation posture for Sockudo

- Keep Sockudo-native APIs and naming.
- Preserve V1/V2 boundaries; AI Transport should be V2-native.
- Build the protocol and storage substrate first, then SDKs and framework adapters, then advanced UX features.
- Treat branching, tool calling, and optimistic updates as first-class transcript semantics, not client-side conveniences.

## Where this research used direct Ably statements versus indexed evidence

Directly opened pages provided concrete detail for:

- overview
- why AI Transport
- sessions and turns
- transport architecture
- authentication
- getting started with Vercel AI SDK
- agent presence
- reconnection and recovery

The Ably documentation index (`llms.txt`) provided authoritative page inventory and topic scopes for:

- cancellation
- history and replay
- branching
- edit and regenerate
- interruption and barge-in
- concurrent turns
- tool calling
- optimistic updates
- push notifications
- chain of thought
- double texting
- API reference pages
- internals pages

For those indexed-only sections, this document stays inside the scope stated by Ably's own documentation titles and descriptions and avoids inventing unpublished behavior.

## Source links

- [AI Transport overview](https://ably.com/docs/ai-transport)
- [Why AI Transport](https://ably.com/docs/ai-transport/why)
- [Sessions and turns](https://ably.com/docs/ai-transport/how-it-works/sessions-and-turns)
- [Transport architecture](https://ably.com/docs/ai-transport/how-it-works/transport)
- [Authentication](https://ably.com/docs/ai-transport/how-it-works/authentication)
- [Get started with Vercel AI SDK](https://ably.com/docs/ai-transport/getting-started/vercel-ai-sdk)
- [Vercel AI SDK framework guide](https://ably.com/docs/ai-transport/framework-guides/vercel-ai-sdk)
- [Internals](https://ably.com/docs/ai-transport/internals)
- [Ably documentation index](https://ably.com/llms.txt)
