import fc from "fast-check";
import { describe, expect, it } from "vitest";

import {
  HEADER_CODEC_MESSAGE_ID,
  HEADER_FORK_OF,
  HEADER_INPUT_CLIENT_ID,
  HEADER_INVOCATION_ID,
  HEADER_MSG_REGENERATE,
  HEADER_PARENT,
  HEADER_ROLE,
  HEADER_TURN_CLIENT_ID,
  HEADER_TURN_CONTINUE,
  HEADER_TURN_ID,
  HEADER_TURN_REASON,
} from "../../constants.js";
import type { HeaderMap } from "../../utils.js";
import { createConversationTree } from "./tree.js";
import type { ConversationTree, TreeSerial, TurnEndReason, TurnNode } from "./tree.js";
import type { DecodedEvent, Reducer } from "../codec/index.js";

const REPLAY_BUDGET_MS = process.env.CI === "true" ? 750 : 250;

describe("conversation tree", () => {
  it("guards against phantom turns from metadata-only messages", () => {
    const tree = createTestTree();

    expect(tree.applyMessage([], headers({ turnId: "turn-1" }), 1)).toBeUndefined();
    expect(tree.getTurnNode("turn-1")).toBeUndefined();
    expect(tree.structuralVersion).toBe(0);
  });

  it("routes fresh input and assistant output, promotes serials, and tracks active suspended turns", () => {
    const tree = createTestTree();

    const user = tree.applyMessage(
      [decoded("user", "msg-user", 2)],
      headers({
        turnId: "turn-1",
        codecMessageId: "msg-user",
        role: "user",
        inputClientId: "client-1",
      }),
      2,
    );
    tree.applyTurnLifecycle({
      type: "turn-start",
      headers: headers({
        turnId: "turn-1",
        turnClientId: "client-1",
        invocationId: "inv-1",
      }),
      serial: 1,
    });
    tree.applyMessage(
      [decoded("assistant", "msg-assistant", 3)],
      headers({
        turnId: "turn-1",
        codecMessageId: "msg-assistant",
        role: "assistant",
      }),
      3,
    );
    tree.applyTurnLifecycle({
      type: "turn-end",
      headers: headers({
        turnId: "turn-1",
        turnReason: "suspended",
      }),
      serial: 4,
    });

    expect(user?.turnId).toBe("turn-1");
    expect(tree.getTurnByCodecMessageId("msg-assistant")?.projection.events).toEqual([
      "2:msg-user:user",
      "3:msg-assistant:assistant",
    ]);
    expect(tree.getTurnNode("turn-1")).toMatchObject({
      startSerial: 1,
      endSerial: 4,
      status: "suspended",
      invocationId: "inv-1",
      clientId: "client-1",
    });
    expect(Array.from(tree.getActiveTurnIds().get("client-1") ?? [])).toEqual(["turn-1"]);
  });

  it("tolerates assistant output before turn-start and backfills parent metadata", () => {
    const tree = createTestTree();

    tree.applyMessage(
      [decoded("parent", "msg-parent", 1)],
      headers({ turnId: "parent", codecMessageId: "msg-parent" }),
      1,
    );
    tree.applyMessage(
      [decoded("child", "msg-child", 3)],
      headers({
        turnId: "child",
        codecMessageId: "msg-child",
        parent: "msg-parent",
      }),
      3,
    );
    tree.applyTurnLifecycle({
      type: "turn-start",
      headers: headers({
        turnId: "child",
        parent: "msg-parent",
        invocationId: "inv-child",
      }),
      serial: 2,
    });

    expect(tree.getTurnNode("child")).toMatchObject({
      parentTurnId: "parent",
      startSerial: 2,
      invocationId: "inv-child",
    });
  });

  it("routes continuation folds by codec message id and avoids self-cycle graph backfill", () => {
    const tree = createTestTree();

    tree.applyMessage(
      [decoded("start", "msg-1", 1)],
      headers({ turnId: "turn-1", codecMessageId: "msg-1" }),
      1,
    );
    tree.applyMessage(
      [decoded("continue", "msg-1", 2)],
      headers({
        turnId: "turn-other",
        codecMessageId: "msg-1",
        turnContinue: true,
        forkOf: "msg-1",
      }),
      2,
    );
    tree.applyTurnLifecycle({
      type: "turn-start",
      headers: headers({
        turnId: "turn-1",
        codecMessageId: "msg-1",
        turnContinue: true,
        invocationId: "inv-continue",
        forkOf: "msg-1",
      }),
      serial: 3,
    });

    expect(tree.getTurnNode("turn-other")).toBeUndefined();
    expect(tree.getTurnNode("turn-1")?.projection.events).toEqual([
      "1:msg-1:start",
      "2:msg-1:continue",
    ]);
    expect(tree.getTurnNode("turn-1")?.forkOf).toBeUndefined();
    expect(tree.getLatestContinuationInvocation("turn-1")).toBe("inv-continue");
  });

  it("computes fork siblings transitively and excludes descendants", () => {
    const tree = createTestTree();

    createTurn(tree, "parent", "msg-parent", 1);
    createTurn(tree, "a", "msg-a", 2, { parent: "msg-parent" });
    createTurn(tree, "b", "msg-b", 3, { forkOf: "msg-a" });
    createTurn(tree, "c", "msg-c", 4, { forkOf: "msg-b" });
    createTurn(tree, "descendant", "msg-descendant", 5, { parent: "msg-a" });

    expect(tree.getSiblingTurns("msg-a").map((node) => node.turnId)).toEqual(["a", "b", "c"]);
    expect(tree.hasSiblingTurns("msg-c")).toBe(true);
    expect(tree.getSiblingTurns("msg-descendant").map((node) => node.turnId)).toEqual([
      "descendant",
    ]);
  });

  it("orders regenerate groups owner first and backfills unresolved owners", () => {
    const tree = createTestTree();

    createTurn(tree, "regen", "msg-regen", 2, {
      regenerates: "msg-owner",
    });
    createTurn(tree, "owner", "msg-owner", 1);

    expect(tree.getRegenerateGroup("msg-owner").map((node) => node.turnId)).toEqual([
      "owner",
      "regen",
    ]);
    expect(tree.getTurnNode("regen")).toMatchObject({
      regeneratesCodecMessageId: "msg-owner",
    });
  });

  it("orders newly inserted turns by server serial even when delivery is out of order", () => {
    const tree = createTestTree();

    createTurn(tree, "late", "msg-late", 20);
    createTurn(tree, "early", "msg-early", 10);

    expect(tree.getTurnNodes().map((node) => node.turnId)).toEqual(["early", "late"]);
  });

  it("deletes unreachable turns and descendants by codec message id", () => {
    const tree = createTestTree();

    createTurn(tree, "parent", "msg-parent", 1);
    createTurn(tree, "child", "msg-child", 2, { parent: "msg-parent" });

    tree.delete("msg-parent");

    expect(tree.getTurnNode("parent")).toBeUndefined();
    expect(tree.getTurnNode("child")).toBeUndefined();
    expect(tree.getHeaders("msg-parent")).toBeUndefined();
  });

  it("emits structural and fold events without bumping on content-only folds", () => {
    const tree = createTestTree();
    const emitted: string[] = [];
    tree.on("message", (event) => emitted.push(`message:${event.turnId}`));
    tree.on("ably-message", (event) => emitted.push(`ably:${event.turnId}`));
    tree.on("turn-projection-updated", (event) => emitted.push(`projection:${event.turnId}`));
    tree.on("update", (event) => emitted.push(`update:${String(event.structuralVersion)}`));

    tree.applyMessage(
      [decoded("first", "msg-1", 1)],
      headers({ turnId: "turn-1", codecMessageId: "msg-1" }),
      1,
    );
    const afterCreate = tree.structuralVersion;
    tree.applyMessage(
      [decoded("append", "msg-1", 2)],
      headers({ turnId: "turn-1", codecMessageId: "msg-1" }),
      2,
    );

    expect(tree.structuralVersion).toBe(afterCreate);
    expect(emitted).toContain("message:turn-1");
    expect(emitted).toContain("ably:turn-1");
    expect(emitted).toContain("projection:turn-1");
  });

  it("folds shuffled valid op logs to an identical final tree", () => {
    const canonical = summarize(applyOps(validOps));

    fc.assert(
      fc.property(shuffledOps(), (ops) => {
        expect(summarize(applyOps(ops))).toEqual(canonical);
      }),
      { numRuns: 100 },
    );
  });

  it("replays 100k operations within the tree budget", () => {
    const tree = createTestTree();
    const started = performance.now();
    for (let index = 0; index < 100_000; index += 1) {
      tree.applyMessage(
        [decoded("x", "msg-1", index + 1)],
        headers({ turnId: "turn-1", codecMessageId: "msg-1" }),
        index + 1,
      );
    }
    const elapsed = performance.now() - started;

    expect(tree.getTurnNode("turn-1")?.projection.events).toHaveLength(100_000);
    expect(elapsed).toBeLessThan(REPLAY_BUDGET_MS);
  });
});

interface Projection {
  events: string[];
}

interface HeaderOptions {
  turnId?: string;
  codecMessageId?: string;
  parent?: string;
  forkOf?: string;
  regenerates?: string | boolean;
  role?: string;
  turnClientId?: string;
  inputClientId?: string;
  invocationId?: string;
  turnContinue?: boolean;
  turnReason?: TurnEndReason;
}

type TestTree = ConversationTree<string, Projection>;

const reducer: Reducer<string, Projection> = {
  init() {
    return { events: [] };
  },
  fold(state, event, meta) {
    state.events.push(`${String(meta.serial)}:${meta.messageId ?? "none"}:${event}`);
    return state;
  },
};

function createTestTree(): TestTree {
  return createConversationTree(reducer);
}

function headers(options: HeaderOptions): HeaderMap {
  const map = Object.create(null) as Record<string, string>;
  set(map, HEADER_TURN_ID, options.turnId);
  set(map, HEADER_CODEC_MESSAGE_ID, options.codecMessageId);
  set(map, HEADER_PARENT, options.parent);
  set(map, HEADER_FORK_OF, options.forkOf);
  set(map, HEADER_MSG_REGENERATE, headerValue(options.regenerates));
  set(map, HEADER_ROLE, options.role);
  set(map, HEADER_TURN_CLIENT_ID, options.turnClientId);
  set(map, HEADER_INPUT_CLIENT_ID, options.inputClientId);
  set(map, HEADER_INVOCATION_ID, options.invocationId);
  set(map, HEADER_TURN_CONTINUE, bool(options.turnContinue));
  set(map, HEADER_TURN_REASON, options.turnReason);
  return map;
}

function decoded(event: string, messageId: string, serial: TreeSerial): DecodedEvent<string> {
  return {
    event,
    messageId,
    meta: {
      serial,
      messageId,
    },
  };
}

function createTurn(
  tree: TestTree,
  turnId: string,
  codecMessageId: string,
  serial: number,
  metadata: HeaderOptions = {},
): TurnNode<Projection> | undefined {
  return tree.applyMessage(
    [decoded(turnId, codecMessageId, serial)],
    headers({
      ...metadata,
      turnId,
      codecMessageId,
    }),
    serial,
  );
}

interface Op {
  kind: "message" | "start" | "end";
  turnId: string;
  codecMessageId?: string;
  serial: number;
  parent?: string;
  forkOf?: string;
  regenerates?: string | boolean;
  reason?: TurnEndReason;
}

const validOps: Op[] = [
  { kind: "start", turnId: "root", serial: 1 },
  { kind: "message", turnId: "root", codecMessageId: "msg-root", serial: 2 },
  { kind: "end", turnId: "root", serial: 3, reason: "complete" },
  { kind: "start", turnId: "child", serial: 4, parent: "msg-root" },
  {
    kind: "message",
    turnId: "child",
    codecMessageId: "msg-child",
    serial: 5,
    parent: "msg-root",
  },
  { kind: "end", turnId: "child", serial: 6, reason: "complete" },
  {
    kind: "start",
    turnId: "regen",
    serial: 7,
    parent: "msg-root",
    regenerates: "msg-child",
  },
  {
    kind: "message",
    turnId: "regen",
    codecMessageId: "msg-regen",
    serial: 8,
    parent: "msg-root",
    regenerates: "msg-child",
  },
  { kind: "end", turnId: "regen", serial: 9, reason: "suspended" },
];

function shuffledOps(): fc.Arbitrary<Op[]> {
  return fc.shuffledSubarray(validOps, {
    minLength: validOps.length,
    maxLength: validOps.length,
  });
}

function applyOps(ops: readonly Op[]): TestTree {
  const tree = createTestTree();
  for (const op of ops) {
    if (op.kind === "message" && op.codecMessageId !== undefined) {
      const headerOptions: HeaderOptions = {
        turnId: op.turnId,
        codecMessageId: op.codecMessageId,
      };
      setOptional(headerOptions, "parent", op.parent);
      setOptional(headerOptions, "forkOf", op.forkOf);
      setOptional(headerOptions, "regenerates", op.regenerates);
      tree.applyMessage(
        [decoded(op.turnId, op.codecMessageId, op.serial)],
        headers(headerOptions),
        op.serial,
      );
    } else if (op.kind === "start") {
      const headerOptions: HeaderOptions = {
        turnId: op.turnId,
      };
      setOptional(headerOptions, "parent", op.parent);
      setOptional(headerOptions, "forkOf", op.forkOf);
      setOptional(headerOptions, "regenerates", op.regenerates);
      tree.applyTurnLifecycle({
        type: "turn-start",
        headers: headers(headerOptions),
        serial: op.serial,
      });
    } else {
      tree.applyTurnLifecycle({
        type: "turn-end",
        headers: headers({
          turnId: op.turnId,
          turnReason: op.reason ?? "complete",
        }),
        serial: op.serial,
      });
    }
  }
  return tree;
}

function summarize(tree: TestTree): unknown {
  return ["root", "child", "regen"].map((turnId) => {
    const node = tree.getTurnNode(turnId);
    return {
      turnId,
      parentTurnId: node?.parentTurnId,
      forkOf: node?.forkOf,
      regeneratesCodecMessageId: node?.regeneratesCodecMessageId,
      status: node?.status,
      startSerial: node?.startSerial,
      endSerial: node?.endSerial,
      events: node?.projection.events.slice().sort(),
    };
  });
}

function set(target: Record<string, string>, key: string, value: string | undefined): void {
  if (value !== undefined) {
    target[key] = value;
  }
}

function bool(value: boolean | undefined): string | undefined {
  return value === undefined ? undefined : value ? "true" : "false";
}

function headerValue(value: string | boolean | undefined): string | undefined {
  return typeof value === "boolean" ? bool(value) : value;
}

function setOptional<T extends object, K extends keyof T>(
  target: T,
  key: K,
  value: T[K] | undefined,
): void {
  if (value !== undefined) {
    target[key] = value;
  }
}
