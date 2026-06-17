import { describe, expect, it, vi } from "vitest";

import {
  EVENT_AI_TURN_END,
  EVENT_AI_TURN_START,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_FORK_OF,
  HEADER_MSG_REGENERATE,
  HEADER_PARENT,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_TURN_ID,
  HEADER_TURN_REASON,
} from "../../constants.js";
import { ErrorInfo } from "../../errors.js";
import type { HeaderMap } from "../../utils.js";
import type {
  Codec,
  DecodedBatch,
  DecodedEvent,
  Decoder,
  Reducer,
} from "../codec/index.js";
import type {
  InboundMessage,
  InboundMessageAction,
  PaginatedResult,
  Serial,
} from "../../realtime/types.js";
import { createConversationTree } from "./tree.js";
import { decodeHistoryPage } from "./decode-history.js";
import { createView } from "./view.js";
import type { ConversationTree } from "./tree.js";
import type { View } from "./view.js";

const TOKEN_PATCH_BUDGET_MS = process.env.CI === "true" ? 1 : 0.5;

describe("transport view", () => {
  it("flattens reachable selected branches with latest sibling by default", () => {
    const { tree, view } = setup();

    createTurn(tree, "parent", [{ id: "p", text: "parent" }], 1);
    createTurn(tree, "a", [{ id: "a", text: "older" }], 2, {
      parent: "p",
    });
    createTurn(tree, "b", [{ id: "b", text: "latest" }], 3, {
      forkOf: "a",
    });
    createTurn(tree, "child-a", [{ id: "child-a", text: "hidden" }], 4, {
      parent: "a",
    });
    createTurn(tree, "child-b", [{ id: "child-b", text: "visible" }], 5, {
      parent: "b",
    });

    expect(view.flattenNodes().map((node) => node.turnId)).toEqual([
      "parent",
      "b",
      "child-b",
    ]);
    expect(view.getMessages().map((message) => message.id)).toEqual([
      "p",
      "b",
      "child-b",
    ]);
    expect(view.getSelectedIndex("a")).toBe(1);

    view.select("a", 0);

    expect(view.flattenNodes().map((node) => node.turnId)).toEqual([
      "parent",
      "a",
      "child-a",
    ]);
    expect(view.getSelectedIndex("b")).toBe(0);
  });

  it("substitutes regenerate messages in place, including nested regens", () => {
    const { tree, view } = setup();

    createTurn(
      tree,
      "owner",
      [
        { id: "u1", text: "u1" },
        { id: "a1", text: "a1" },
        { id: "u2", text: "u2" },
        { id: "a2", text: "a2" },
      ],
      1,
    );
    createTurn(tree, "regen-1", [{ id: "a1r", text: "a1 regen" }], 2, {
      forkOf: "a1",
      regenerates: true,
    });
    createTurn(tree, "regen-2", [{ id: "a1rr", text: "a1 nested" }], 3, {
      forkOf: "a1r",
      regenerates: true,
    });

    expect(view.getMessages().map((message) => message.id)).toEqual([
      "u1",
      "a1rr",
    ]);
    expect(view.getMessageSiblings("a1").map((message) => message.id)).toEqual([
      "a1",
      "a1r",
    ]);
    expect(view.hasMessageSiblings("a1")).toBe(true);

    view.selectMessageSibling("a1", 0);

    expect(view.getMessages().map((message) => message.id)).toEqual([
      "u1",
      "a1",
      "u2",
      "a2",
    ]);
  });

  it("surfaces metadata and scoped events only for visible turns", () => {
    const { tree, view } = setup();
    const updates = vi.fn();
    const messages = vi.fn();
    const turns = vi.fn();
    view.on("update", updates);
    view.on("message", messages);
    view.on("turn", turns);

    createTurn(tree, "a", [{ id: "a", text: "a" }], 1);
    createTurn(tree, "b", [{ id: "b", text: "b" }], 2, { forkOf: "a" });
    updates.mockClear();
    messages.mockClear();
    turns.mockClear();
    tree.applyMessage(
      [decoded({ id: "a", text: "hidden update" }, "a", 3)],
      headers({ turnId: "a", codecMessageId: "a" }),
      3,
    );
    tree.applyMessage(
      [decoded({ id: "b", text: "visible update" }, "b", 4)],
      headers({ turnId: "b", codecMessageId: "b" }),
      4,
    );
    tree.applyTurnLifecycle({
      type: "turn-end",
      headers: headers({ turnId: "b", turnReason: "complete" }),
      serial: 5,
    });

    expect(messages).toHaveBeenCalledTimes(1);
    expect(turns).toHaveBeenCalledWith(
      expect.objectContaining({ turnId: "b" }),
    );
    expect(view.getMessageMetadata("b")).toEqual({
      codecMessageId: "b",
      turnId: "b",
      status: "complete",
    });
    expect(updates).toHaveBeenCalled();
  });

  it("drains withheld turns before loading history and guards concurrent loads", async () => {
    const { tree, codec, decoder } = setup();
    createTurn(tree, "old", [{ id: "old", text: "old" }], 1);
    createTurn(tree, "live", [{ id: "live", text: "live" }], 2);
    const history = createHistory();
    const view = createView({
      tree,
      codec,
      decoder,
      history,
      withheldTurnIds: ["old"],
    });

    expect(view.getMessages().map((message) => message.id)).toEqual(["live"]);
    expect(view.hasOlder()).toBe(true);

    await view.loadOlder(1);

    expect(view.getMessages().map((message) => message.id)).toEqual([
      "old",
      "live",
    ]);
    expect(history.calls).toBe(0);

    const first = view.loadOlder(1);
    const second = view.loadOlder(1);
    await Promise.all([first, second]);

    expect(history.calls).toBe(1);
    expect(view.getMessages().map((message) => message.id)).toContain("hist");
  });

  it("surfaces load errors and wraps missing send executors", async () => {
    const { tree, codec, decoder } = setup();
    const view = createView({
      tree,
      codec,
      decoder,
      history: {
        history: () => Promise.reject(new Error("history failed")),
      },
    });

    await view.loadOlder(1);

    expect(view.loadError).toBeInstanceOf(ErrorInfo);
    await expect(view.send({ id: "x", text: "x" })).rejects.toBeInstanceOf(
      ErrorInfo,
    );
  });

  it("delegates send helpers to the injected executor", async () => {
    const { tree, codec } = setup();
    const executor = {
      send: vi.fn(() => Promise.resolve("send")),
      sendInput: vi.fn(() => Promise.resolve("input")),
      regenerate: vi.fn(() => Promise.resolve("regen")),
      edit: vi.fn(() => Promise.resolve("edit")),
      update: vi.fn(() => Promise.resolve("update")),
    };
    const view = createView({ tree, codec, sendExecutor: executor });

    await expect(view.send({ id: "m", text: "m" })).resolves.toBe("send");
    await expect(view.sendInput({ id: "i", text: "i" })).resolves.toBe("input");
    await expect(view.regenerate("a", "p")).resolves.toBe("regen");
    await expect(view.edit("m", { id: "m2", text: "m2" })).resolves.toBe(
      "edit",
    );
    await expect(view.update("m", { text: "x" })).resolves.toBe("update");
  });

  it("patches token-streaming tail updates without changing untouched message references", () => {
    const codec = createCodec();
    const tree = createConversationTree(codec as Reducer<Message, Projection>);
    for (let index = 0; index < 10_000; index += 1) {
      createTurn(
        tree,
        `turn-${String(index)}`,
        [{ id: `msg-${String(index)}`, text: `message ${String(index)}` }],
        index + 1,
      );
    }
    const view = createView({ tree, codec, decoder: codec.createDecoder() });
    const before = view.getMessages();
    const first = before[0];
    const middle = before[5_000];
    const started = performance.now();
    for (let token = 0; token < 100; token += 1) {
      tree.applyMessage(
        [
          decoded(
            { id: "msg-9999", text: `token ${String(token)}` },
            "msg-9999",
            10_001 + token,
          ),
        ],
        headers({ turnId: "turn-9999", codecMessageId: "msg-9999" }),
        10_001 + token,
      );
    }
    const elapsedPerToken = (performance.now() - started) / 100;
    const after = view.getMessages();

    expect(after[0]).toBe(first);
    expect(after[5_000]).toBe(middle);
    expect(after.at(-1)).toEqual({ id: "msg-9999", text: "token 99" });
    expect(elapsedPerToken).toBeLessThan(TOKEN_PATCH_BUDGET_MS);
  }, 60_000);
});

describe("decode history", () => {
  it("folds lifecycle and decoded history messages through the tree", () => {
    const { tree, decoder } = setup();
    const page = pageFrom([
      inbound({
        name: EVENT_AI_TURN_START,
        action: "create",
        data: null,
        serial: 1,
        transport: { [HEADER_TURN_ID]: "turn-1" },
      }),
      inbound({
        name: "ai-output",
        action: "append",
        data: { id: "hist", text: "history" },
        serial: 2,
        messageSerial: "hist",
        transport: {
          [HEADER_TURN_ID]: "turn-1",
          [HEADER_CODEC_MESSAGE_ID]: "hist",
          [HEADER_STREAM]: "true",
          [HEADER_STATUS]: "complete",
        },
      }),
      inbound({
        name: EVENT_AI_TURN_END,
        action: "create",
        data: null,
        serial: 3,
        transport: {
          [HEADER_TURN_ID]: "turn-1",
          [HEADER_TURN_REASON]: "complete",
        },
      }),
    ]);

    expect(decodeHistoryPage(page, decoder, tree)).toEqual({
      processedMessages: 3,
      decodedEvents: 1,
      lifecycleEvents: 2,
    });
    expect(tree.getTurnNode("turn-1")).toMatchObject({
      status: "complete",
      endSerial: 3,
    });
    expect(tree.getTurnNode("turn-1")?.projection.messages).toEqual([
      { id: "hist", text: "history" },
    ]);
  });
});

interface Message {
  id: string;
  text: string;
}

interface Projection {
  messages: Message[];
}

interface HeaderOptions {
  turnId?: string;
  codecMessageId?: string;
  parent?: string;
  forkOf?: string;
  regenerates?: boolean;
  turnReason?: string;
}

function setup(): {
  tree: ConversationTree<Message, Projection>;
  codec: Codec<Message, Message, Projection, Message>;
  decoder: Decoder<Message, Message>;
  view: View<Message, Message>;
} {
  const codec = createCodec();
  const tree = createConversationTree(codec as Reducer<Message, Projection>);
  const decoder = codec.createDecoder();
  return {
    tree,
    codec,
    decoder,
    view: createView({ tree, codec, decoder }),
  };
}

function createCodec(): Codec<Message, Message, Projection, Message> {
  return {
    init() {
      return { messages: [] };
    },
    fold(state, event) {
      const index = state.messages.findIndex(
        (message) => message.id === event.id,
      );
      if (index === -1) {
        state.messages.push(event);
      } else {
        state.messages[index] = event;
      }
      return state;
    },
    createEncoder() {
      throw new Error("not used");
    },
    createDecoder() {
      return {
        decode(message): DecodedBatch<Message, Message> {
          return {
            inputs: [],
            outputs: [
              decoded(
                message.data as Message,
                message.messageSerial,
                message.historySerial,
              ),
            ],
          };
        },
      };
    },
    getMessages(projection) {
      return projection.messages;
    },
    createUserMessage(message) {
      return { message };
    },
    createRegenerate(target, parent) {
      return { target, parent };
    },
    resolveToolTarget() {
      return undefined;
    },
    isTerminal() {
      return false;
    },
  };
}

function createTurn(
  tree: ConversationTree<Message, Projection>,
  turnId: string,
  messages: readonly Message[],
  serial: number,
  metadata: HeaderOptions = {},
): void {
  const headerOptions: HeaderOptions = { ...metadata, turnId };
  const firstId = messages[0]?.id;
  if (firstId !== undefined) {
    headerOptions.codecMessageId = firstId;
  }
  tree.applyMessage(
    messages.map((message, offset) =>
      decoded(message, message.id, serial + offset / 100),
    ),
    headers(headerOptions),
    serial,
  );
}

function decoded(
  event: Message,
  messageId: string,
  serial: Serial,
): DecodedEvent<Message> {
  return {
    event,
    messageId,
    meta: {
      serial,
      messageId,
    },
  };
}

function headers(options: HeaderOptions): HeaderMap {
  const map = Object.create(null) as Record<string, string>;
  set(map, HEADER_TURN_ID, options.turnId);
  set(map, HEADER_CODEC_MESSAGE_ID, options.codecMessageId);
  set(map, HEADER_PARENT, options.parent);
  set(map, HEADER_FORK_OF, options.forkOf);
  set(map, HEADER_MSG_REGENERATE, bool(options.regenerates));
  set(map, HEADER_TURN_REASON, options.turnReason);
  return map;
}

function inbound(options: {
  name: string;
  action: InboundMessageAction;
  data: unknown;
  serial: Serial;
  messageSerial?: string;
  transport?: Record<string, string>;
}): InboundMessage {
  const transport = Object.create(null) as Record<string, string>;
  Object.assign(transport, options.transport);
  return {
    name: options.name,
    data: options.data,
    action: options.action,
    messageSerial: options.messageSerial ?? `msg-${String(options.serial)}`,
    historySerial: options.serial,
    timestamp: 0,
    raw: {},
    getTransportHeaders() {
      return transport;
    },
    getCodecHeaders() {
      return Object.create(null) as HeaderMap;
    },
  };
}

function pageFrom(
  items: readonly InboundMessage[],
  next?: PaginatedResult<InboundMessage>,
): PaginatedResult<InboundMessage> {
  return {
    items,
    hasNext() {
      return next !== undefined;
    },
    next() {
      return next
        ? Promise.resolve(next)
        : Promise.reject(new Error("no next page"));
    },
  };
}

function createHistory(): {
  calls: number;
  history(): Promise<PaginatedResult<InboundMessage>>;
} {
  return {
    calls: 0,
    history() {
      this.calls += 1;
      return Promise.resolve(
        pageFrom([
          inbound({
            name: "ai-output",
            action: "append",
            data: { id: "hist", text: "history" },
            serial: 0,
            messageSerial: "hist",
            transport: {
              [HEADER_TURN_ID]: "hist-turn",
              [HEADER_CODEC_MESSAGE_ID]: "hist",
            },
          }),
        ]),
      );
    },
  };
}

function set(
  target: Record<string, string>,
  key: string,
  value: string | undefined,
): void {
  if (value !== undefined) {
    target[key] = value;
  }
}

function bool(value: boolean | undefined): string | undefined {
  return value === undefined ? undefined : value ? "true" : "false";
}
