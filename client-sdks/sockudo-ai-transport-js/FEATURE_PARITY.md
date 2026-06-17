# Sockudo AI Transport Feature Parity Receipt

Pinned Sockudo server SHA: `f66434eab44e688d3df42e56d8ebaf9aba6b1575`

This receipt is generated from the permanent conformance-suite manifest and is checked by
`test/conformance/feature-parity.test.ts`. Every GA row is either `✅` or requires an approved
waiver; there are no waivers for `0.1.0`.

| Feature                                                | Spec file                                   | Status |
| ------------------------------------------------------ | ------------------------------------------- | ------ |
| Golden transcript replay                               | test/conformance/golden-replay.test.ts      | ✅     |
| Golden transcript produce                              | test/conformance/golden-produce.test.ts     | ✅     |
| Token streaming and rollup content equality            | test/conformance/golden-replay.test.ts      | ✅     |
| Cancellation filters and cancelled turns               | test/conformance/golden-replay.test.ts      | ✅     |
| Reconnection and history fallback                      | test/conformance/golden-produce.test.ts     | ✅     |
| Multi-device observer rendering and active turns       | src/react/index.test.ts                     | ✅     |
| History/replay paging and branch materialization       | test/conformance/golden-replay.test.ts      | ✅     |
| Branching edit/regenerate/sibling navigation           | test/conformance/golden-replay.test.ts      | ✅     |
| Interruption cancel-then-send/send-alongside           | src/core/transport/client-transport.test.ts | ✅     |
| Concurrent turns and waitForTurn routing               | test/conformance/golden-replay.test.ts      | ✅     |
| Tool calling server/client continuation                | src/vercel/transport/chat-transport.test.ts | ✅     |
| Human-in-the-loop suspended continuation               | test/conformance/golden-replay.test.ts      | ✅     |
| Optimistic update POST failure and race reconciliation | src/core/transport/client-transport.test.ts | ✅     |
| Agent presence status flow                             | src/react/index.test.ts                     | ✅     |
| Chain-of-thought reasoning multiplexing                | src/vercel/codec/index.test.ts              | ✅     |
| Double texting patterns                                | src/vercel/transport/chat-transport.test.ts | ✅     |
| Chaos-lite websocket drop stages                       | test/conformance/chaos-lite.test.ts         | ✅     |
| Clock skew serial determinism                          | test/conformance/chaos-lite.test.ts         | ✅     |
| Slow-consumer stream backpressure                      | test/conformance/chaos-lite.test.ts         | ✅     |
| Concepts: sessions, turns, transport, codec, tree      | docs/concepts                               | ✅     |
| Core API surface                                       | etc/api-snapshot.d.ts                       | ✅     |
| React API surface                                      | etc/api-snapshot.d.ts                       | ✅     |
| Vue API surface                                        | etc/api-snapshot.d.ts                       | ✅     |
| Svelte API surface                                     | etc/api-snapshot.d.ts                       | ✅     |
| Vercel API surface                                     | etc/api-snapshot.d.ts                       | ✅     |
| Vercel React API surface                               | etc/api-snapshot.d.ts                       | ✅     |
| Vercel Vue API surface                                 | etc/api-snapshot.d.ts                       | ✅     |
| Vercel Svelte API surface                              | etc/api-snapshot.d.ts                       | ✅     |
| Direct provider adapters                               | src/providers/index.test.ts                 | ✅     |
