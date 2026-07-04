import { describe, expect, it } from "vitest";

import { normalizeInboundMessage } from "../../src/realtime/adapter.js";
import { createConversationTree } from "../../src/core/transport/tree.js";
import { createView } from "../../src/core/transport/view.js";
import { UIMessageCodec } from "../../src/vercel/codec/index.js";
import {
  EVENT_AI_RUN_RESUME,
  EVENT_AI_RUN_SUSPEND,
  EVENT_AI_TURN_END,
  EVENT_AI_TURN_START,
  HEADER_TURN_CONTINUE,
  HEADER_TURN_REASON,
} from "../../src/constants.js";
import { mergeHeaders } from "../../src/utils.js";
import {
  hydrateGoldenFrame,
  loadGoldenTranscripts,
  normalizeMaterialized,
} from "./golden-fixtures.js";
import { SOCKUDO_SERVER_SHA } from "./server-pin.js";

describe("golden transcript replay", () => {
  it("pins the Sockudo server SHA used for golden transcripts", () => {
    expect(SOCKUDO_SERVER_SHA).toMatch(/^[a-f0-9]{40}$/u);
  });

  it("replays server golden transcripts through decoder, tree, and view", async () => {
    const transcripts = await loadGoldenTranscripts();
    expect(transcripts.length).toBeGreaterThan(0);

    const materialized = transcripts.map((transcript) => {
      const tree = createConversationTree(UIMessageCodec);
      const view = createView({ tree, codec: UIMessageCodec });
      const decoder = UIMessageCodec.createDecoder();
      let decodedEvents = 0;
      let lifecycleEvents = 0;

      for (const [index, frame] of transcript.frames.entries()) {
        const raw = hydrateGoldenFrame(frame, index);
        if (!raw) {
          continue;
        }
        const message = normalizeInboundMessage(raw);
        const headers = message.getTransportHeaders();
        if (message.name === EVENT_AI_TURN_START) {
          tree.applyTurnLifecycle({
            type: "turn-start",
            headers,
            serial: message.historySerial,
          });
          lifecycleEvents += 1;
          continue;
        }
        if (message.name === EVENT_AI_RUN_RESUME) {
          tree.applyTurnLifecycle({
            type: "turn-start",
            headers: mergeHeaders(headers, { [HEADER_TURN_CONTINUE]: "true" }),
            serial: message.historySerial,
          });
          lifecycleEvents += 1;
          continue;
        }
        if (message.name === EVENT_AI_RUN_SUSPEND) {
          tree.applyTurnLifecycle({
            type: "turn-end",
            headers: mergeHeaders(headers, { [HEADER_TURN_REASON]: "suspended" }),
            serial: message.historySerial,
            reason: "suspended",
          });
          lifecycleEvents += 1;
          continue;
        }
        if (message.name === EVENT_AI_TURN_END) {
          const turnEnd = {
            type: "turn-end",
            headers,
            serial: message.historySerial,
          } as const;
          const turnReason = reason(headers[HEADER_TURN_REASON]);
          tree.applyTurnLifecycle(
            turnReason === undefined ? turnEnd : { ...turnEnd, reason: turnReason },
          );
          lifecycleEvents += 1;
          continue;
        }
        const decoded = decoder.decode(message);
        const events = [...decoded.inputs, ...decoded.outputs];
        decodedEvents += events.length;
        tree.applyMessage(events, headers, message.historySerial);
      }

      const nodes = tree.getTurnNodes();
      return {
        name: transcript.name,
        decodedEvents,
        lifecycleEvents,
        messages: normalizeMaterialized(view.getMessages()),
        nodes: nodes.map((node) => ({
          turnId: node.turnId,
          status: node.status,
          messageCount: UIMessageCodec.getMessages(node.projection).length,
        })),
        activeTurns: Array.from(tree.getActiveTurnIds()).map(([clientId, turns]) => [
          clientId,
          Array.from(turns).sort(),
        ]),
      };
    });

    expect(materialized).toMatchSnapshot();
  });
});

function reason(
  value: string | undefined,
): "complete" | "cancelled" | "error" | "suspended" | undefined {
  switch (value) {
    case "complete":
    case "cancelled":
    case "error":
    case "suspended":
      return value;
    default:
      return undefined;
  }
}
