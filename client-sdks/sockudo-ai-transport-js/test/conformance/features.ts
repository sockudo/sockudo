export interface FeatureReceipt {
  feature: string;
  specFile: string;
  status: "✅";
}

export const FEATURE_RECEIPTS: readonly FeatureReceipt[] = [
  {
    feature: "Golden transcript replay",
    specFile: "test/conformance/golden-replay.test.ts",
    status: "✅",
  },
  {
    feature: "Golden transcript produce",
    specFile: "test/conformance/golden-produce.test.ts",
    status: "✅",
  },
  {
    feature: "Token streaming and rollup content equality",
    specFile: "test/conformance/golden-replay.test.ts",
    status: "✅",
  },
  {
    feature: "Cancellation filters and cancelled turns",
    specFile: "test/conformance/golden-replay.test.ts",
    status: "✅",
  },
  {
    feature: "Reconnection and history fallback",
    specFile: "test/conformance/golden-produce.test.ts",
    status: "✅",
  },
  {
    feature: "Multi-device observer rendering and active turns",
    specFile: "src/react/index.test.ts",
    status: "✅",
  },
  {
    feature: "History/replay paging and branch materialization",
    specFile: "test/conformance/golden-replay.test.ts",
    status: "✅",
  },
  {
    feature: "Branching edit/regenerate/sibling navigation",
    specFile: "test/conformance/golden-replay.test.ts",
    status: "✅",
  },
  {
    feature: "Interruption cancel-then-send/send-alongside",
    specFile: "src/core/transport/client-transport.test.ts",
    status: "✅",
  },
  {
    feature: "Concurrent turns and waitForTurn routing",
    specFile: "test/conformance/golden-replay.test.ts",
    status: "✅",
  },
  {
    feature: "Tool calling server/client continuation",
    specFile: "src/vercel/transport/chat-transport.test.ts",
    status: "✅",
  },
  {
    feature: "Human-in-the-loop suspended continuation",
    specFile: "test/conformance/golden-replay.test.ts",
    status: "✅",
  },
  {
    feature: "Optimistic update POST failure and race reconciliation",
    specFile: "src/core/transport/client-transport.test.ts",
    status: "✅",
  },
  {
    feature: "Agent presence status flow",
    specFile: "src/react/index.test.ts",
    status: "✅",
  },
  {
    feature: "Chain-of-thought reasoning multiplexing",
    specFile: "src/vercel/codec/index.test.ts",
    status: "✅",
  },
  {
    feature: "Double texting patterns",
    specFile: "src/vercel/transport/chat-transport.test.ts",
    status: "✅",
  },
  {
    feature: "Chaos-lite websocket drop stages",
    specFile: "test/conformance/chaos-lite.test.ts",
    status: "✅",
  },
  {
    feature: "Clock skew serial determinism",
    specFile: "test/conformance/chaos-lite.test.ts",
    status: "✅",
  },
  {
    feature: "Slow-consumer stream backpressure",
    specFile: "test/conformance/chaos-lite.test.ts",
    status: "✅",
  },
  {
    feature: "Concepts: sessions, turns, transport, codec, tree",
    specFile: "docs/concepts",
    status: "✅",
  },
  {
    feature: "Core API surface",
    specFile: "etc/api-snapshot.d.ts",
    status: "✅",
  },
  {
    feature: "React API surface",
    specFile: "etc/api-snapshot.d.ts",
    status: "✅",
  },
  {
    feature: "Vercel API surface",
    specFile: "etc/api-snapshot.d.ts",
    status: "✅",
  },
  {
    feature: "Vercel React API surface",
    specFile: "etc/api-snapshot.d.ts",
    status: "✅",
  },
];
