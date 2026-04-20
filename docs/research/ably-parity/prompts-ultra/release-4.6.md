# Release 4.6 Ultra Prompt Pack — AI Transport

This is a consolidated release covering what was previously planned as releases 4.6 through 4.9:
AI Transport Core, AI Interaction Control, SDK/Framework Surface, and final parity hardening.

Prerequisites:
- Release 4.3 (Versioned Durable Messages) is complete.
- Release 4.4 (Annotation Engine) is complete. AI Transport requires the annotation substrate
  because the channel rule that enables versioned messages also enables the message persistence
  that AI session transcripts depend on.
- Release 4.5 (Push Notifications) is complete. Push is used for offline re-engagement
  when background AI tasks complete.

Research binding context for every prompt in this pack:
- `docs/research/ably-parity/ably-ai-transport-research.md`
- `docs/research/ably-parity/sockudo-ably-parity-release-plan.md`

---

## Prompt 4.6-01: Architecture and release contract
```text
Implement Sockudo release 4.6: AI Transport.

Before writing any code, produce the release contract and commit it to the repo.

This release covers all AI Transport capability in one consolidated feature release:
- Core session/turn model and wire protocol
- Advanced interaction semantics (branching, edit/regenerate, interruption, tools, HITL)
- Public SDK surfaces (client transport API, server transport API, codec system, React hooks)
- Final parity hardening and audit

Conceptual model — why this exists:
- HTTP/SSE streaming ties response lifetime to a single connection. Network changes, tab switches,
  or device suspends break the stream. Sockudo AI Transport decouples the HTTP request that
  starts work from the session that persists conversation state, the stream that carries model
  output, and the control plane that allows cancellation, interruption, and multi-device participation.

Core abstractions:
- Session: a Sockudo V2 channel functioning as a durable conversation container.
  Sessions survive disconnects because they are channel-based, not connection-based.
  All participants on the channel see the same prompts, responses, and control messages.
- Turn: one prompt/response unit. Turns create structure around flat token streams.
  Turns can be tracked, cancelled, replayed, and run concurrently.

Wire protocol requirement:
- AI Transport is a layered protocol over Sockudo's V2 pub/sub, not ad hoc message naming.
- Messages carry `x-sockudo-turn-id` headers to route events to the correct per-turn stream.
- The protocol must define: lifecycle events, content messages, control signals, and message identity.
- The protocol spec must be committed as a doc before implementation starts.

Codec system:
- Transport core and framework adaptation are separate layers.
- A codec has four responsibilities: encoder, decoder, accumulator, terminal detection.
- The default codec maps to Vercel AI SDK's UIMessage format.
- Custom codecs must be possible without modifying the transport core.
- Framework-specific event grammars must not be hard-wired into the wire protocol.

Conversation tree:
- The transcript is not a flat list; it is a tree with alternate futures.
- Branching, edit/regenerate, and concurrent turns all produce sibling groups in the tree.
- The tree is defined by: serial ordering, sibling groups, fork chains, and a flatten algorithm.
- This structure must be in the wire protocol spec before interaction semantics are implemented.

Release blockers:
- AI Transport is V2-native only. V1 connections must not receive AI Transport events.
- Sessions are channels; message persistence must be enabled on session channels (inherits from 4.3/4.4).
- Turn-scoped cancellation must not kill the whole session.
- Streams must survive disconnect/reconnect via connection recovery and history hydration.
- Multiple devices on the same session must see a coherent transcript.
- Tool calls and HITL approvals are durable conversational artifacts, not ephemeral in-memory callbacks.
- Optimistic user message inserts must reconcile without duplicates after server confirmation.
- Push re-engagement must work for background task completion notification.

Non-goals:
- No LLM or model integration in the Sockudo server. Sockudo is the transport only.
- No specific model provider SDK is bundled (OpenAI, Anthropic, etc.).
```

---

## Prompt 4.6-02: Domain model and invariants
```text
Design and implement the complete AI Transport domain model.

Session entity:
- `session_id`: maps to a V2 channel name (e.g. `ai-session:<uuid>`)
- `app_id`: owning Sockudo app
- `client_id`: primary initiating user identity
- `created_at`, `last_active_at`
- Sessions are created implicitly on first turn or explicitly via API.
- Session identity is independent of connection identity — the same session persists
  across connections, devices, and reconnects.

Turn entity:
- `turn_id`: unique within a session
- `session_id`: parent session
- `client_id`: identity of the user who initiated this turn
- `status`: pending | active | complete | cancelled | error
- `created_at`, `ended_at`
- `end_reason`: complete | cancelled | error (null if not ended)
- Turns are the atomic unit of cancellation. Cancelling a turn does not cancel the session.

Transcript message entity (extends versioned message from 4.3):
- `turn_id`: which turn this message belongs to
- `message_class`: user_prompt | assistant_response | tool_call | tool_result | reasoning | control
- `branch_id`: which branch of the conversation tree this belongs to (null = trunk)
- `parent_turn_id`: for branched turns, which turn was forked from
- Transcript messages use the same serial ordering as V2 messages.
- History exposes transcript messages in tree order, not arrival order.

Conversation tree invariants:
- Every turn belongs to exactly one branch.
- The trunk is the default linear path through the tree.
- Branching creates a new branch that diverges from a specific turn.
- Sibling groups: turns with the same parent_turn_id are siblings.
- Fork chains: a sequence of turns on a branch forms a fork chain.
- Flatten algorithm: for display, the tree is flattened by selecting the active branch at each fork point.
- Editing a turn creates a new branch starting from that turn's parent position.
- Regenerating a response creates a sibling branch.

Tool call entity:
- `tool_call_id`: unique identifier
- `turn_id`: the turn that triggered this tool call
- `tool_name`: string
- `arguments`: JSON
- `status`: pending | executing | complete | failed | awaiting_approval
- `result`: JSON nullable
- Tool calls are durable transcript artifacts, not ephemeral in-memory callbacks.

HITL approval entity:
- `approval_id`: unique identifier
- `tool_call_id`: the tool call awaiting approval
- `approver_client_id`: who is expected to approve
- `status`: pending | approved | rejected | expired
- Approval requests persist across reconnect and device switch.
```

---

## Prompt 4.6-03: Storage schema, migrations, and backfill
```text
Design and implement storage for AI sessions, turns, transcript messages, tool state, and approval state.

AI sessions table:
- `session_id` PK (= channel name)
- `app_id` FK
- `client_id`
- `created_at`, `last_active_at`

Turns table:
- `turn_id` PK
- `session_id` FK
- `client_id`
- `status` enum (pending | active | complete | cancelled | error)
- `end_reason` nullable enum
- `created_at`, `ended_at` nullable

Indexes:
- `(session_id, created_at)` — for ordered turn history
- `(session_id, status)` — for finding active turns

Transcript messages:
- Extend the versioned message store from 4.3 with:
  - `turn_id` nullable FK
  - `message_class` enum
  - `branch_id` nullable string
  - `parent_turn_id` nullable FK
- Index: `(session_id, branch_id, serial)` for ordered branch-aware replay.

Tool calls table:
- `tool_call_id` PK
- `turn_id` FK
- `session_id` FK
- `tool_name` string
- `arguments` JSON
- `status` enum
- `result` JSON nullable
- `created_at`, `updated_at`

HITL approvals table:
- `approval_id` PK
- `tool_call_id` FK
- `session_id` FK
- `approver_client_id` nullable
- `status` enum (pending | approved | rejected | expired)
- `decided_at` nullable

Optimistic message table:
- `optimistic_id` PK (client-generated)
- `session_id` FK
- `client_id`
- `status`: pending | confirmed | reconciled | discarded
- `server_message_serial` nullable — set when confirmed by server

Migration strategy:
- All AI Transport tables are new; no existing tables change.
- Add migrations to `ops/migrations/` for all persistent backends.
- On rollback: drop AI Transport tables. Message history from 4.3 is unaffected.

Note: AI Transport channels require the versioned message channel rule to be enabled
(because session transcripts depend on message persistence). Enforce this when a session channel is created.
```

---

## Prompt 4.6-04: Wire protocol and API contract
```text
Implement the AI Transport wire protocol and define all event shapes before writing any runtime code.

The wire protocol spec must be committed to `docs/content/5.reference/` before implementation starts.

Turn lifecycle events (on the session channel, action prefix `sockudo_ai:`):

Turn opened:
{
  "event": "sockudo_ai:turn.opened",
  "data": {
    "turnId": "t-abc123",
    "clientId": "user-123",
    "branchId": null,
    "parentTurnId": null,
    "timestamp": 1700000000000
  },
  "headers": { "x-sockudo-turn-id": "t-abc123" }
}

User prompt:
{
  "event": "sockudo_ai:turn.message",
  "data": {
    "turnId": "t-abc123",
    "messageClass": "user_prompt",
    "content": "...",
    "optimisticId": "opt-client-uuid"  // null if server-initiated
  },
  "headers": { "x-sockudo-turn-id": "t-abc123" }
}

Token/response stream (one event per token or per chunk):
{
  "event": "sockudo_ai:turn.message",
  "data": {
    "turnId": "t-abc123",
    "messageClass": "assistant_response",
    "delta": "token text",       // incremental (message-per-token mode)
    // OR "content": "full text" // accumulated (message-per-response mode)
    "append": true               // true = concatenate; false = replace (maps to message.append vs message.update)
  },
  "headers": { "x-sockudo-turn-id": "t-abc123" }
}

Turn ended:
{
  "event": "sockudo_ai:turn.ended",
  "data": {
    "turnId": "t-abc123",
    "reason": "complete",  // complete | cancelled | error
    "error": null
  }
}

Cancellation signal (published by client to session channel):
{
  "event": "sockudo_ai:cancel",
  "data": {
    "scope": "turn",   // turn | client | own | all
    "turnId": "t-abc123"  // required when scope=turn
  }
}

Tool call event:
{
  "event": "sockudo_ai:turn.tool_call",
  "data": {
    "toolCallId": "tc-xyz",
    "turnId": "t-abc123",
    "toolName": "search",
    "arguments": { "query": "..." },
    "requiresApproval": false
  }
}

Tool result event:
{
  "event": "sockudo_ai:turn.tool_result",
  "data": {
    "toolCallId": "tc-xyz",
    "result": { ... },
    "status": "complete"  // complete | failed
  }
}

HITL approval request:
{
  "event": "sockudo_ai:turn.approval_required",
  "data": {
    "approvalId": "ap-789",
    "toolCallId": "tc-xyz",
    "approverId": "admin-client-id"
  }
}

HITL approval response (published by approver):
{
  "event": "sockudo_ai:approval_decision",
  "data": {
    "approvalId": "ap-789",
    "decision": "approved"  // approved | rejected
  }
}

Agent presence status (published to presence channel):
- status values: "thinking" | "streaming" | "idle" | "offline"
- Uses existing V2 presence infrastructure.

HTTP API:

POST /apps/{app_id}/ai/sessions                          create or get a session
GET  /apps/{app_id}/ai/sessions/{session_id}             get session metadata
POST /apps/{app_id}/ai/sessions/{session_id}/turns       start a new turn (server-side)
POST /apps/{app_id}/ai/sessions/{session_id}/cancel      cancel turns by scope
GET  /apps/{app_id}/ai/sessions/{session_id}/turns       list turns
GET  /apps/{app_id}/ai/sessions/{session_id}/transcript  get full transcript (branch-aware, paginated)
POST /apps/{app_id}/ai/sessions/{session_id}/branch      create a branch from a turn
POST /apps/{app_id}/ai/approvals/{approval_id}/decide    approve or reject HITL request
```

---

## Prompt 4.6-05: Auth, capabilities, and security
```text
Implement authorization for AI Transport sessions, turns, cancellation, and HITL.

Capability model (add to Sockudo's capability system):

`ai-session`:
  - Required to access or create AI sessions.
  - The clientId must be embedded in the signed token (never accept client-claimed identity).

`ai-publish`:
  - Required to publish user prompts and control signals to a session channel.
  - Server-side agents use this to publish assistant responses and tool events.

`ai-cancel-own`:
  - Client may cancel their own turns (matched by clientId).

`ai-cancel-any`:
  - Client may cancel any turn in the session (admin/operator use).

`ai-history`:
  - Required to access full transcript history and paginated scroll-back.

`ai-approve`:
  - Required to approve or reject HITL approval requests.

Authorization rules:

Session access:
- A client may only access sessions where their clientId matches the session's authorized participants.
- Server-side agents use a separate server-to-server auth path (API key with ai-session + ai-publish).

Turn creation:
- Server-side: POST /ai/sessions/{id}/turns requires server auth with ai-session + ai-publish.
- Client-side: client sends `sockudo_ai:turn.message` via WebSocket; server creates the turn record.

Cancellation authorization:
- The server MUST honor an `onCancel` hook before executing cancellation.
- `onCancel` receives: turn_id, requesting_client_id, scope.
- The hook may approve or reject. If rejected, return an error event to the requesting client.
- If no `onCancel` hook is configured: ai-cancel-own allows cancelling own turns; ai-cancel-any allows all.

HITL approval authorization:
- Only the designated `approverId` clientId (or a client with ai-cancel-any) may decide an approval.
- Approval decisions are validated against the stored approval entity.

Reasoning stream access:
- Reasoning/chain-of-thought messages (`messageClass: reasoning`) must be access-controlled separately.
- Operators configure whether reasoning streams are visible to the session's subscribers.
- Default: reasoning content is not delivered to client subscribers unless explicitly enabled.
- This prevents accidental exposure of internal model reasoning to end users.
```

---

## Prompt 4.6-06: Server runtime — core transport
```text
Implement the core AI Transport server runtime.

Add `crates/sockudo-ai/` as a new crate (or extend sockudo-server) with these components:

ServerTransport (the primary server-side entry point):
- `new_turn(session_id, turn_id, client_id, branch_id?, parent_turn_id?)`:
  Creates turn record, publishes `sockudo_ai:turn.opened` to session channel.
  Returns a TurnHandle.

TurnHandle:
- `add_messages(messages)`:
  Accepts user prompts or system context. Publishes `sockudo_ai:turn.message` events
  with messageClass=user_prompt. Stores as versioned messages in transcript.
- `stream_response(stream)`:
  Accepts an async stream of tokens/chunks from the LLM.
  If message-per-token mode: publishes each token as `sockudo_ai:turn.message` with append=true.
  If message-per-response mode: publishes full response when stream completes.
  Uses append semantics from 4.3 (appendMessage) for efficient rollup — history returns
  the fully aggregated response; realtime receives incremental tokens.
- `end(reason)`:
  Publishes `sockudo_ai:turn.ended`. Updates turn record status.

StreamRouter (internal):
- Routes flat session channel events into per-turn streams using `x-sockudo-turn-id` header.
- Clients subscribe to a session channel; StreamRouter demuxes events to the right TurnHandle streams.

Cancellation routing:
- Listen for `sockudo_ai:cancel` events on the session channel.
- Route to the `onCancel` hook for authorization.
- If approved: set turn status to cancelled, abort the LLM stream, publish `sockudo_ai:turn.ended`
  with reason=cancelled.
- If rejected: publish an error event to the requesting client only.

History consistency:
- User prompts are stored as versioned messages (messageClass=user_prompt).
- Assistant response tokens are accumulated via append semantics; history returns the full response.
- History replay must reconstruct the correct turn-ordered, branch-aware transcript.

Reconnection handling:
- Short disconnect: V2 connection recovery (serial-based replay from replay buffer).
- Long disconnect (gap > replay buffer): history hydration from transcript store.
  On reattach, client transport requests transcript from last known position.
- The server must deliver a complete session state snapshot (active turn IDs + current transcript)
  to any newly attaching client, including late joiners on multi-device sessions.
```

---

## Prompt 4.6-07: Server runtime — interaction control
```text
Implement advanced AI interaction semantics on top of the core transport.

Conversation branching:
- POST /ai/sessions/{id}/branch creates a new branch from a specified turn.
- The branch has a new branch_id; turns on the branch have parent_turn_id pointing to the fork turn.
- Existing transcript on the trunk is preserved; the branch starts fresh from the fork point.
- The session channel delivers a branch creation event to all subscribers.

Edit and regenerate:
- Edit: client sends an edited version of a previous user prompt.
  This implicitly creates a branch from the turn before the original prompt.
  The edited prompt starts a new turn on the new branch.
- Regenerate: server generates a new response for an existing user prompt.
  This creates a sibling branch from the same parent turn as the original response.
  Both the original response and the regenerated response coexist as siblings in the tree.
- Both operations depend on the conversation tree invariants from Prompt 4.6-02.

Interruption and barge-in:
- Cancel-then-send: client cancels the active turn and immediately sends a new prompt.
  The cancellation and new turn creation are atomic from the client's perspective.
- Send-alongside: client sends a new prompt while an existing turn is still streaming.
  This creates a concurrent turn (see below).
- The server must handle both patterns without losing the active stream or the new prompt.

Double texting policy:
- When a client sends a second prompt while a turn is active, the server applies a configured policy:
  - `queue`: the new prompt is queued and starts after the current turn ends.
  - `cancel_and_replace`: cancel the active turn, start a new one with the new prompt.
  - `concurrent`: start a new concurrent turn alongside the active one.
- The policy is configurable per session or per app. Document the default.

Concurrent turns:
- Multiple turns may be active simultaneously (multi-agent or concurrent user sessions).
- Each turn has its own stream, its own turn_id, and its own cancellation scope.
- StreamRouter routes events to the correct turn stream by x-sockudo-turn-id.
- Concurrent turns on different branches are independent and do not interfere.

Tool calling:
- When the LLM emits a tool call, the server publishes `sockudo_ai:turn.tool_call`.
- Tool calls are stored as durable transcript entities (tool call entity from Prompt 4.6-02).
- If `requiresApproval: false`: server or client executes the tool and publishes tool_result.
- If `requiresApproval: true`: flow transitions to HITL (see below).

Human-in-the-loop (HITL):
- Server publishes `sockudo_ai:turn.approval_required` and stores an approval entity.
- Agent pauses — the turn stream is suspended but not cancelled.
- The designated approver receives the approval request. It persists across reconnect and device switch.
- On approval: server resumes the tool execution and continues the turn.
- On rejection: server publishes turn.ended with reason=error and appropriate error metadata.
- Approval entities have an optional TTL; expired approvals auto-reject with reason=expired.

Push re-engagement:
- When a background turn completes (no connected subscribers at session.last_active_at),
  the server uses the Push Notification Platform (4.5) to notify the session's client.
- Push payload includes the session_id so the client can deep-link back to the session.
```

---

## Prompt 4.6-08: Client transport and SDK behavior
```text
Define and implement the client transport layer and public SDK APIs.

ClientTransport (the primary client-side entry point):
- Subscribes to the session channel before attach (no messages missed).
- Decodes inbound session channel messages through the configured codec.
- Maintains the branch-aware conversation tree in local state.
- Tracks active turns across connected clients.
- Manages optimistic user message insertion.

Core client API:

`transport.send(messages)`:
- Inserts the user message optimistically into the local tree with a provisional node.
- Publishes the message to the session channel.
- On server confirmation (matching optimistic_id in server's turn.message event), the provisional
  node is replaced with the confirmed node. No duplicate or reordering.

`transport.cancel(scope, turnId?)`:
- Publishes a `sockudo_ai:cancel` event to the session channel.
- Scope options: turn (requires turnId), own (all turns by this clientId), all.
- Does not wait for server confirmation; optimistically marks local turn as cancelling.

`transport.getHistory(fromSerial?, branch?)`:
- Fetches paginated transcript history from the server.
- Returns branch-aware ordered transcript (flatten algorithm applied).

`transport.loadOlder()`:
- Loads older messages when the user scrolls back.
- Uses paginated history; automatically walks the conversation tree.

Active turn tracking:
- The client maintains a set of currently active turn IDs.
- On `sockudo_ai:turn.opened`: add to active set.
- On `sockudo_ai:turn.ended`: remove from active set.
- Expose `transport.activeTurns` for UI rendering (e.g. streaming indicators).

Reconnect behavior:
- Short disconnect: V2 connection recovery resumes from last serial.
- Long disconnect: client calls `transport.getHistory()` to hydrate conversation state.
- Optimistic messages not yet confirmed are re-sent after reconnect.

Codec system:
- `Codec` trait with four methods: `encode(message) → wire_event`, `decode(wire_event) → message`,
  `accumulate(existing, delta) → accumulated`, `isTerminal(message) → bool`.
- Default codec: `VercelUIMessageCodec` — maps to Vercel AI SDK's UIMessage format.
- Custom codecs implement the same trait; transport core does not care about specific message formats.

React hooks:
- `useAISession(sessionId)`: returns `{ messages, activeTurns, send, cancel, loadOlder }`.
- `useAgentPresence(sessionId)`: returns agent status (thinking | streaming | idle | offline).
- Hooks internally use ClientTransport; they handle reconnect, hydration, and optimistic updates.

Vercel AI SDK integration:
- `ChatTransport` wraps ClientTransport for use with Vercel's `useChat` hook.
- `UIMessageCodec` is the pre-wired codec for Vercel's UIMessage format.
- Example: `examples/vercel_ai_transport.js` — a chat UI backed by Sockudo AI Transport.

Examples to add:
- `examples/ai_session_basic.js` — server creates a turn, streams response, client receives and renders.
- `examples/ai_multi_device.js` — two clients on the same session, one sends, both receive.
- `examples/ai_cancellation.js` — client cancels active turn mid-stream.
- `examples/ai_branching.js` — user edits a message, new branch created, both branches navigable.
- `examples/ai_hitl.js` — tool call requires human approval, approval received, execution resumes.
- `examples/vercel_ai_transport.js` — Vercel AI SDK integration end-to-end.
```

---

## Prompt 4.6-09: Cluster and distributed correctness
```text
Harden AI Transport for clustered operation.

Session continuity across nodes:
- Session state (turns, tool calls, approvals, optimistic reconciliation) is stored in shared backing store.
- No per-node in-memory session state that cannot be lost on failover.
- A client that reconnects to a different node must see the same session state.

Transcript replay correctness:
- Transcript serials must be globally ordered (same rules as V2 serials).
- The flatten algorithm (branch-aware transcript display) must produce identical results
  on any node given the same transcript store contents.
- History hydration after long disconnect must return a consistent, gap-free transcript.

StreamRouter under reconnect:
- When a client reconnects to a different node, that node must reconstruct the StreamRouter
  state for the session by reading active turn IDs from the shared turns table.
- Active turn streams from LLMs may be running on the original node; the transport layer
  must handle the case where token events fan out to multiple nodes simultaneously.

Cancellation durability:
- Cancellation signals published to the session channel reach all nodes via the cluster adapter.
- The node running the LLM stream must receive the cancellation and abort the stream.
- Race: LLM stream completes at the same moment a cancel arrives — last writer wins on turn status.
  If turn ended (complete) before cancel was processed, the cancel is a no-op (do not re-open turn).

HITL approval durability:
- Approval entities are in the shared backing store.
- If the node handling the approval decision fails, the approval must still be processed on restart.
- On node startup, check for pending approved/rejected approvals that were not yet acted on.

Branch integrity:
- Concurrent branch creation requests for the same session/turn must be serialized.
- Use optimistic concurrency (CAS on the branch record) to prevent two branches with the same
  parent_turn_id being created with conflicting branch IDs.

Optimistic insert reconciliation:
- The optimistic_id must be stored in the turn.message event by the server.
- When multiple nodes receive the confirmation event, only the client that generated the optimistic_id
  reconciles its local tree — other clients receive the server message as a new turn event.
```

---

## Prompt 4.6-10: Test matrix and automation
```text
Build and run the full test matrix for release 4.6.

Core transport tests:
- Server creates a turn → client receives turn.opened and subsequent messages.
- Token stream survives client disconnect and reconnect (short gap: serial recovery).
- Token stream survives client long disconnect (long gap: history hydration).
- Two clients on the same session receive identical transcripts.
- Turn ends with reason=complete, cancelled, error — client sees correct turn.ended event.
- Server-side cancellation (onCancel hook approves) — stream aborts, turn.ended delivered.
- Server-side cancellation rejected by onCancel hook — error delivered to requesting client only.

Interaction control tests:
- Branching: create branch from turn N, new turns on branch do not affect trunk.
- Edit: send edited user prompt, new branch created from parent of original, both navigable.
- Regenerate: create sibling response, both original and regenerated coexist in tree.
- Interruption (cancel-then-send): active turn cancelled, new turn starts, no lost events.
- Send-alongside (concurrent): two concurrent turns both stream and both end correctly.
- Double texting (queue policy): second message queued, sent after first turn ends.
- Double texting (cancel-and-replace): second message cancels first, then starts new turn.
- Tool call (no approval): server publishes tool_call, result published, turn continues.
- HITL: tool_call with requiresApproval=true → approval_required published → approver approves →
  tool executes → turn continues.
- HITL: approver rejects → turn.ended with error.
- HITL: approval expires → auto-reject.
- Push re-engagement: background turn completes, push notification dispatched to session's clientId.

SDK tests:
- ClientTransport: send() inserts optimistic node, server confirmation reconciles node.
- ClientTransport: send() after reconnect — optimistic message re-sent without duplicate.
- React hook: useAISession messages array updates on each turn event.
- React hook: activeTurns set updates correctly on turn.opened and turn.ended.
- Codec: VercelUIMessageCodec encodes/decodes round-trip correctly.
- Custom codec: implement minimal test codec, confirm transport core is codec-agnostic.

Cluster tests:
- Two nodes, session active on both, concurrent token fan-out — both clients receive all tokens.
- Cancellation published to node A, LLM stream on node B — stream aborts correctly.
- Branch creation race — CAS prevents duplicate branches.
- HITL approval on node A, agent paused on node B — approval unblocks agent.

Auth tests:
- ai-cancel-own cannot cancel another client's turn.
- Reasoning stream not delivered to subscribers without explicit enable.
- HITL decision by wrong clientId rejected.
```

---

## Prompt 4.6-11: Observability, docs, and operator guidance
```text
Add metrics, logs, docs, wire protocol reference, and operator guidance for release 4.6.

Metrics (add to `crates/sockudo-metrics/src/prometheus.rs`):
- `sockudo_ai_sessions_total{app}` — counter for session creations
- `sockudo_ai_turns_total{app, status}` — counter per turn by end status
- `sockudo_ai_tokens_streamed_total{app}` — counter for streamed tokens
- `sockudo_ai_turn_duration_seconds{app}` — histogram for turn duration
- `sockudo_ai_cancellations_total{app, scope, outcome}` — counter (outcome: approved/rejected)
- `sockudo_ai_tool_calls_total{app, status}` — counter per tool call status
- `sockudo_ai_hitl_approvals_total{app, decision}` — counter per HITL decision
- `sockudo_ai_optimistic_reconciliations_total{app}` — counter for optimistic message reconciliations
- `sockudo_ai_recovery_short_total{app}` — counter for short-gap recoveries (serial replay)
- `sockudo_ai_recovery_long_total{app}` — counter for long-gap recoveries (history hydration)
- `sockudo_ai_stuck_turns_total{app}` — counter for turns that have been active beyond configured TTL

Logs:
- INFO on turn creation (session_id, turn_id, client_id)
- INFO on turn end (turn_id, reason, duration_ms)
- WARN on cancellation rejection by onCancel hook (turn_id, requesting_client_id)
- WARN on optimistic reconciliation conflict (optimistic_id, conflicting_serial)
- ERROR on stream abort failure (turn_id, error)
- ERROR on HITL approval expiry (approval_id, tool_call_id)
- WARN on stuck turn (turn_id, active_duration_seconds)
- ERROR on branch CAS conflict (session_id, parent_turn_id, conflicting_branch_id)

Docs to add:
- `docs/content/2.server/24.ai-transport.md` — comprehensive AI Transport server guide:
  - concept overview (session, turn, conversation tree)
  - server transport API and turn lifecycle
  - cancellation scopes and onCancel hook
  - tool calling and HITL flows
  - double texting policies
  - push re-engagement configuration
  - session channel feature flag requirements
  - environment variables and config

- `docs/content/3.client/5.ai-transport-client.md` — client transport guide:
  - ClientTransport API
  - codec system and custom codec implementation
  - React hooks
  - Vercel AI SDK integration
  - optimistic updates and reconciliation
  - reconnect and history hydration

- `docs/content/5.reference/6.ai-transport-wire-protocol.md` — wire protocol spec:
  - all event shapes
  - x-sockudo-turn-id header
  - lifecycle event sequence diagrams
  - error event catalog

- `docs/content/5.reference/7.ai-transport-codec.md` — codec architecture:
  - encoder, decoder, accumulator, terminal detection
  - VercelUIMessageCodec
  - custom codec implementation guide

- `docs/content/5.reference/8.conversation-tree.md` — conversation tree internals:
  - serial ordering
  - sibling groups and fork chains
  - flatten algorithm
  - branch navigation

Operator guidance:
- How to detect and clean up stuck turns (metric + admin API).
- How to configure double texting policy.
- How to enable reasoning stream access.
- How to configure HITL approval TTL.
- How to set up push re-engagement.
```

---

## Prompt 4.6-12: Final parity audit and release sign-off
```text
Run the final release audit for 4.6.

Parity matrix — verify each researched capability against implementation:

From ably-ai-transport-research.md:
✓ Session-as-channel semantics (session survives disconnect; channel-based, not connection-based)
✓ Durable history-backed transcripts (branch-aware, paginated, gap-free replay)
✓ Turn-scoped state and scoped cancellation (turn | client | own | all scopes)
✓ Client recovery: short gap (serial replay), long gap (history hydration)
✓ Branch-aware transcript model (conversation tree with fork chains and flatten algorithm)
✓ Explicit optimistic reconciliation (no duplicates, no reordering after confirmation)
✓ Presence-aware agent status (thinking | streaming | idle | offline via V2 presence)
✓ AI-specific wire protocol (sockudo_ai: events, x-sockudo-turn-id routing)
✓ Codec extension system (encoder/decoder/accumulator/terminal with VercelUIMessageCodec default)
✓ Framework adapters (Vercel AI SDK, React hooks)
✓ Auth/capability boundaries (ai-session, ai-publish, ai-cancel-own/any, ai-history, ai-approve)
✓ Push re-engagement for offline continuation (using 4.5 Push platform)
✓ Message-per-token and message-per-response streaming modes
✓ Cancellation authorization hook (onCancel)
✓ Multi-device sessions (session state shared; any device can join and see coherent transcript)
✓ Conversation branching (branch creation, sibling navigation)
✓ Edit and regenerate (fork from parent turn)
✓ Interruption and barge-in (cancel-then-send, send-alongside)
✓ Double texting policies (queue, cancel-and-replace, concurrent)
✓ Concurrent turns (independent streams, independent cancellation)
✓ Tool calling (durable tool call entities, server and client execution)
✓ Human-in-the-loop (durable approval entities, persist across reconnect)
✓ Optimistic updates (provisional local node, server confirmation reconciliation)
✓ Chain-of-thought / reasoning streams (separate messageClass, access-controlled)
✓ Background task push notification (session outlives user presence, push re-engagement)

Note intentional Sockudo-native differences from Ably (do not treat as gaps):
- Sockudo uses `sockudo_ai:` event prefix instead of Ably's prefix.
- Sockudo's codec API matches the four-method interface but has Sockudo-native naming.
- Sockudo does not bundle Vercel AI SDK or any model provider SDK.

Final checklist:
- All parity matrix items have passing tests.
- Wire protocol spec matches implementation (no undocumented events).
- Auth boundaries all tested (no privilege escalation paths).
- Cluster correctness tests pass (session continuity, cancellation across nodes, HITL durability).
- All metrics fire under expected conditions.
- Docs match implementation.

Deliver:
1. Completed parity matrix with evidence column (test name or manual verification).
2. Any gaps found — enumerated with severity and fix scope.
3. Intentional Sockudo-native differences documented.
4. Go/no-go decision with justification.
5. Release note draft for 4.6 covering all AI Transport features.
6. Executive summary of remaining behavioral differences from Ably (if any).
```
