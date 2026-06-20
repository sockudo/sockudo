import {
  readUIMessageStream,
  type UIMessage as VercelAIMessage,
  type UIMessageChunk as VercelAIChunk,
} from "ai";
import fc from "fast-check";
import { describe, expect, it } from "vitest";

import { EVENT_AI_INPUT, EVENT_AI_OUTPUT, HEADER_CODEC_MESSAGE_ID } from "../../constants.js";
import { normalizeInboundMessage, type SockudoRawMessage } from "../../realtime/adapter.js";
import type { ChannelWriter } from "../../core/codec/index.js";
import type { MessageAck, MessageMutation, PublishMessage } from "../../realtime/index.js";
import { UIMessageCodec } from "./index.js";
import { createVercelProjection, foldVercelEvent } from "./reducer.js";
import { transitionToolPart } from "./tool-transitions.js";
import type { AI, VercelInput, VercelOutput } from "./events.js";

describe("UIMessageCodec", () => {
  it("round-trips discrete chunk registry entries through codec headers", async () => {
    const chunks: VercelOutput[] = [
      { type: "start", messageId: "m1", messageMetadata: { topic: "t" } },
      { type: "start-step" },
      { type: "finish-step" },
      { type: "finish", finishReason: "stop", metadata: { model: "m" } },
      { type: "error", errorText: "bad" },
      { type: "abort" },
      { type: "message-metadata", messageMetadata: { a: 1 } },
      { type: "tool-input-error", toolCallId: "tc1", errorText: "bad input" },
      {
        type: "tool-output-available",
        toolCallId: "tc1",
        output: { ok: true },
      },
      { type: "tool-output-error", toolCallId: "tc1", errorText: "bad output" },
      { type: "tool-approval-request", toolCallId: "tc1", approvalId: "ap1" },
      { type: "tool-output-denied", toolCallId: "tc1", reason: "no" },
      { type: "file", url: "https://file.test/a.png", mediaType: "image/png" },
      {
        type: "source-url",
        sourceId: "s1",
        url: "https://source.test",
        title: "S",
      },
      { type: "source-document", sourceId: "d1", title: "D" },
      { type: "data-weather", data: { c: 20 }, transient: false },
    ];
    for (const source of chunks) {
      const writer = createWriter();
      const encoder = UIMessageCodec.createEncoder(writer);
      const decoder = UIMessageCodec.createDecoder();

      await encoder.publishOutput(source);
      const decoded = decoder.decode(rawFromPublish(expectDefined(writer.publishes[0]), "msg-1"))
        .outputs[0]?.event;

      expect(decoded).toMatchObject(source);
    }
  });

  it("round-trips streamed text, reasoning, and tool-input chunks", async () => {
    const cases: {
      chunks: VercelOutput[];
      expected: VercelOutput[];
    }[] = [
      {
        chunks: [
          { type: "text-start", id: "text-1", messageId: "m1" },
          {
            type: "text-delta",
            id: "text-1",
            messageId: "m1",
            delta: "hi",
          },
          { type: "text-end", id: "text-1", messageId: "m1" },
        ],
        expected: [
          { type: "text-start", id: "text-1", messageId: "m1" },
          { type: "text-delta", id: "text-1", delta: "hi", messageId: "m1" },
          { type: "text-end", id: "text-1", messageId: "m1" },
        ],
      },
      {
        chunks: [
          { type: "reasoning-start", id: "reason-1", messageId: "m1" },
          {
            type: "reasoning-delta",
            id: "reason-1",
            messageId: "m1",
            delta: "why",
          },
          { type: "reasoning-end", id: "reason-1", messageId: "m1" },
        ],
        expected: [
          { type: "reasoning-start", id: "reason-1", messageId: "m1" },
          {
            type: "reasoning-delta",
            id: "reason-1",
            delta: "why",
            messageId: "m1",
          },
          { type: "reasoning-end", id: "reason-1", messageId: "m1" },
        ],
      },
      {
        chunks: [
          {
            type: "tool-input-start",
            toolCallId: "tc1",
            toolName: "search",
            messageId: "m1",
          },
          {
            type: "tool-input-delta",
            toolCallId: "tc1",
            messageId: "m1",
            delta: '{"q":"x"}',
          },
          {
            type: "tool-input-available",
            toolCallId: "tc1",
            toolName: "search",
            messageId: "m1",
          },
        ],
        expected: [
          {
            type: "tool-input-start",
            toolCallId: "tc1",
            toolName: "search",
            messageId: "m1",
          },
          {
            type: "tool-input-delta",
            toolCallId: "tc1",
            delta: '{"q":"x"}',
            messageId: "m1",
          },
          {
            type: "tool-input-available",
            toolCallId: "tc1",
            toolName: "search",
            input: { q: "x" },
            messageId: "m1",
          },
        ],
      },
    ];

    for (const { chunks, expected } of cases) {
      const writer = createWriter();
      const encoder = UIMessageCodec.createEncoder(writer);
      const decoder = UIMessageCodec.createDecoder();

      for (const chunk of chunks) {
        await encoder.publishOutput(chunk);
      }
      const initialData =
        typeof writer.publishes[0]?.data === "string" ? writer.publishes[0].data : "";

      const events = [
        ...decoder.decode(rawFromPublish(expectDefined(writer.publishes[0]), "msg-1")).outputs,
        ...decoder.decode(rawFromAppend(expectDefined(writer.appends[0]), "append", 2)).outputs,
        ...decoder.decode(
          rawFromUpdate(
            expectDefined(writer.updates[0]),
            3,
            `${initialData}${writer.appends[0]?.data ?? ""}`,
          ),
        ).outputs,
      ].map((event) => event.event);

      expect(events).toEqual(expected);
    }
  });

  it("falls tool-input-available back to a discrete chunk when no stream is active", async () => {
    const writer = createWriter();
    const encoder = UIMessageCodec.createEncoder(writer);
    const decoder = UIMessageCodec.createDecoder();
    const chunk: VercelOutput = {
      type: "tool-input-available",
      toolCallId: "tc1",
      toolName: "search",
      input: { q: "x" },
      providerExecuted: true,
      preliminary: false,
      messageId: "m1",
    };

    await encoder.publishOutput(chunk);

    expect(
      decoder.decode(rawFromPublish(expectDefined(writer.publishes[0]), "m1")).outputs[0]?.event,
    ).toEqual(chunk);
  });

  it("fans user messages into ai-input parts and decodes input commands", async () => {
    const writer = createWriter();
    const encoder = UIMessageCodec.createEncoder(writer);
    const decoder = UIMessageCodec.createDecoder();
    const inputs: { input: VercelInput; expected: VercelInput }[] = [
      {
        input: {
          message: {
            id: "u1",
            role: "user",
            parts: [{ type: "text", text: "hello" }],
          },
        },
        expected: {
          message: {
            id: "u1",
            role: "user",
            parts: [{ type: "text", text: "hello" }],
          },
        },
      },
      {
        input: { message: { id: "u2", role: "user", parts: [] } },
        expected: {
          message: {
            id: "u2",
            role: "user",
            parts: [{ type: "text", text: "" }],
          },
        },
      },
      {
        input: { type: "tool-result", toolCallId: "tc1", output: { ok: true } },
        expected: {
          type: "tool-result",
          toolCallId: "tc1",
          output: { ok: true },
        },
      },
      {
        input: {
          type: "tool-result-error",
          toolCallId: "tc1",
          message: "failed",
        },
        expected: {
          type: "tool-result-error",
          toolCallId: "tc1",
          message: "failed",
        },
      },
      {
        input: {
          type: "tool-approval-response",
          toolCallId: "tc1",
          approvalId: "ap1",
          approved: true,
          reason: "ok",
        },
        expected: {
          type: "tool-approval-response",
          toolCallId: "tc1",
          approvalId: "ap1",
          approved: true,
          reason: "ok",
        },
      },
      {
        input: { target: "assistant-2", parent: "assistant-1" },
        expected: { target: "assistant-2", parent: "assistant-1" },
      },
    ];

    for (const { input, expected } of inputs) {
      await encoder.publishInput(input);
      const publish = expectDefined(writer.publishes.shift());
      expect(publish.name).toBe(EVENT_AI_INPUT);
      expect(decoder.decode(rawFromPublish(publish, "u1")).inputs[0]?.event).toEqual(expected);
    }
  });

  it("decodes raw client-transport user message inputs for observers", () => {
    const decoder = UIMessageCodec.createDecoder();
    const message = {
      id: "u-raw",
      role: "user",
      parts: [{ type: "text", text: "visible on tab b" }],
    } satisfies AI.UIMessage;

    const decoded = decoder.decode(
      normalizeInboundMessage({
        event: "sockudo:message.create",
        name: EVENT_AI_INPUT,
        channel: "chat",
        data: { message },
        extras: {
          ai: {
            transport: {
              "turn-id": "turn-raw",
              "codec-message-id": message.id,
              role: "user",
            },
          },
        },
        message_serial: message.id,
        history_serial: 1,
        delivery_serial: 1,
      } satisfies SockudoRawMessage),
    );

    expect(decoded.inputs[0]?.event).toEqual({ message });
    expect(decoded.inputs[0]?.messageId).toBe(message.id);
  });

  it("reduces multiplexed text, reasoning, tool states, and transient data", () => {
    const projection = createVercelProjection();
    const fold = (event: VercelOutput, serial: number): void => {
      foldVercelEvent(projection, event, {
        serial,
        messageId: "m1",
      });
    };

    fold({ type: "start", messageId: "m1" }, 1);
    fold({ type: "text-start", id: "t", messageId: "m1" }, 2);
    fold({ type: "reasoning-start", id: "r", messageId: "m1" }, 3);
    fold({ type: "text-delta", id: "t", messageId: "m1", delta: "A" }, 4);
    fold({ type: "reasoning-delta", id: "r", messageId: "m1", delta: "B" }, 5);
    fold({ type: "data-temp", data: "skip", transient: true }, 6);
    fold(
      {
        type: "tool-input-start",
        toolCallId: "tc1",
        toolName: "search",
        messageId: "m1",
      },
      7,
    );
    fold({ type: "tool-input-delta", toolCallId: "tc1", delta: '{"q":"x"}' }, 8);
    fold({ type: "tool-input-available", toolCallId: "tc1", messageId: "m1" }, 9);
    fold({ type: "tool-output-available", toolCallId: "tc1", output: "done" }, 10);

    expect(UIMessageCodec.getMessages(projection)[0]).toMatchObject({
      id: "m1",
      parts: [
        { type: "text", text: "A" },
        { type: "reasoning", text: "B" },
        {
          type: "dynamic-tool",
          toolName: "search",
          toolCallId: "tc1",
          state: "output-available",
          input: { q: "x" },
          output: "done",
        },
      ],
    });
  });

  it("buffers out-of-order tool resolutions and applies highest serial conflicts", () => {
    const projection = createVercelProjection();

    foldVercelEvent(
      projection,
      { type: "tool-output-available", toolCallId: "tc1", output: "early" },
      {
        serial: 1,
        messageId: "m1",
      },
    );
    expect(projection.pendingToolResolutions.get("tc1")).toHaveLength(1);

    foldVercelEvent(
      projection,
      {
        type: "tool-input-start",
        toolCallId: "tc1",
        toolName: "tool",
        messageId: "m1",
      },
      {
        serial: 2,
        messageId: "m1",
      },
    );
    foldVercelEvent(
      projection,
      { type: "tool-input-available", toolCallId: "tc1", messageId: "m1" },
      {
        serial: 3,
        messageId: "m1",
      },
    );
    foldVercelEvent(
      projection,
      { type: "tool-output-error", toolCallId: "tc1", errorText: "late" },
      {
        serial: 0,
        messageId: "m1",
      },
    );

    expect(UIMessageCodec.getMessages(projection)[0]?.parts[0]).toMatchObject({
      state: "output-available",
      output: "early",
    });

    foldVercelEvent(
      projection,
      { type: "tool-output-error", toolCallId: "tc1", errorText: "newer" },
      {
        serial: 10,
        messageId: "m1",
      },
    );

    expect(UIMessageCodec.getMessages(projection)[0]?.parts[0]).toMatchObject({
      state: "output-error",
      errorText: "newer",
    });
  });

  it("resets text trackers at finish-step so stream ids can be reused", () => {
    const projection = createVercelProjection();
    const fold = (event: VercelOutput, serial: number): void => {
      foldVercelEvent(projection, event, { serial, messageId: "m1" });
    };

    fold({ type: "start", messageId: "m1" }, 1);
    fold({ type: "text-start", id: "t", messageId: "m1" }, 2);
    fold({ type: "text-delta", id: "t", messageId: "m1", delta: "first" }, 3);
    fold({ type: "finish-step" }, 4);
    fold({ type: "text-start", id: "t", messageId: "m1" }, 5);
    fold({ type: "text-delta", id: "t", messageId: "m1", delta: "second" }, 6);

    expect(UIMessageCodec.getMessages(projection)[0]?.parts).toMatchObject([
      { type: "text", text: "first" },
      { type: "text", text: "second" },
    ]);
  });

  it("moves requested tool approvals to responded before output arrives", () => {
    const projection = createVercelProjection();
    const fold = (event: VercelInput | VercelOutput, serial: number): void => {
      foldVercelEvent(projection, event, { serial, messageId: "m1" });
    };

    fold(
      {
        type: "tool-input-start",
        toolCallId: "tc1",
        toolName: "tool",
        messageId: "m1",
      },
      1,
    );
    fold({ type: "tool-input-available", toolCallId: "tc1", messageId: "m1" }, 2);
    fold(
      {
        type: "tool-approval-request",
        toolCallId: "tc1",
        approvalId: "ap1",
        messageId: "m1",
      },
      3,
    );
    fold(
      {
        type: "tool-approval-response",
        toolCallId: "tc1",
        approvalId: "ap1",
        approved: true,
      },
      4,
    );

    expect(UIMessageCodec.getMessages(projection)[0]?.parts[0]).toMatchObject({
      state: "approval-responded",
      approval: { approvalId: "ap1", approved: true },
    });

    fold({ type: "tool-output-available", toolCallId: "tc1", output: "done" }, 5);

    expect(UIMessageCodec.getMessages(projection)[0]?.parts[0]).toMatchObject({
      state: "output-available",
      output: "done",
    });
  });

  it("rejects illegal terminal tool transitions", () => {
    const part: AI.UIMessagePart = {
      type: "dynamic-tool",
      toolName: "tool",
      toolCallId: "tc1",
      state: "output-available",
      output: "done",
    };

    expect(transitionToolPart(part, "output-error", { errorText: "late" })).toEqual(part);
  });

  it("matches Vercel readUIMessageStream accumulation for generated valid streams", async () => {
    await fc.assert(
      fc.asyncProperty(validStreamSpec(), async (spec) => {
        const chunks = chunksFromSpec(spec);
        const projection = createVercelProjection();
        chunks.forEach((chunk, index) => {
          foldVercelEvent(projection, chunk, {
            serial: index + 1,
            messageId: spec.messageId,
          });
        });

        expect(normalizeMessages(UIMessageCodec.getMessages(projection))).toEqual(
          normalizeMessages(await readVercelMessages(chunks.map(toVercelAIChunk))),
        );
      }),
      { numRuns: 50 },
    );
  });

  it("matches the canonical Vercel turn wire transcript", async () => {
    const writer = createWriter();
    const encoder = UIMessageCodec.createEncoder(writer);

    await encoder.publishOutput({
      type: "start",
      messageId: "m1",
      messageMetadata: { topic: "demo" },
    });
    await encoder.publishOutput({
      type: "text-start",
      id: "t1",
      messageId: "m1",
    });
    await encoder.publishOutput({
      type: "text-delta",
      id: "t1",
      messageId: "m1",
      delta: "Hi",
    });
    await encoder.publishOutput({
      type: "text-end",
      id: "t1",
      messageId: "m1",
    });
    await encoder.publishOutput({
      type: "finish",
      finishReason: "stop",
      metadata: { model: "demo-model" },
    });

    expect(normalizeWireTranscript(writer)).toMatchInlineSnapshot(`
      [
        {
          "data": {
            "messageId": "m1",
            "messageMetadata": {
              "topic": "demo",
            },
            "type": "start",
          },
          "extras": {
            "ai": {
              "codec": {
                "message-id": "m1",
                "message-metadata": "{"topic":"demo"}",
                "type": "start",
              },
              "transport": {
                "codec-message-id": "m1",
                "discrete": "true",
                "role": "assistant",
                "stream": "false",
              },
            },
          },
          "kind": "publish",
          "messageId": "m1",
          "name": "ai-output",
        },
        {
          "data": " ",
          "extras": {
            "ai": {
              "codec": {
                "id": "t1",
                "message-id": "m1",
                "type": "text-start",
              },
              "transport": {
                "codec-message-id": "m1",
                "role": "assistant",
                "status": "streaming",
                "stream": "true",
                "stream-id": "text:m1:t1",
              },
            },
          },
          "kind": "publish",
          "messageId": "m1",
          "name": "ai-output",
        },
        {
          "data": "Hi",
          "extras": {
            "ai": {
              "codec": {
                "id": "t1",
                "message-id": "m1",
                "type": "text-delta",
              },
              "transport": {
                "codec-message-id": "m1",
                "role": "assistant",
                "status": "streaming",
                "stream": "true",
                "stream-id": "text:m1:t1",
              },
            },
          },
          "kind": "append",
          "messageSerial": "msg-2",
        },
        {
          "extras": {
            "ai": {
              "codec": {
                "id": "t1",
                "message-id": "m1",
                "type": "text-end",
              },
              "transport": {
                "codec-message-id": "m1",
                "role": "assistant",
                "status": "complete",
                "stream": "true",
                "stream-id": "text:m1:t1",
              },
            },
          },
          "kind": "update",
          "messageSerial": "msg-2",
        },
        {
          "data": {
            "finishReason": "stop",
            "metadata": {
              "model": "demo-model",
            },
            "type": "finish",
          },
          "extras": {
            "ai": {
              "codec": {
                "finish-reason": "stop",
                "message-id": "m1",
                "provider-metadata": "{"model":"demo-model"}",
                "type": "finish",
              },
              "transport": {
                "codec-message-id": "m1",
                "discrete": "true",
                "role": "assistant",
                "stream": "false",
              },
            },
          },
          "kind": "publish",
          "name": "ai-output",
        },
      ]
    `);
  });

  it("carries the active message id onto control chunks without their own ids", async () => {
    const writer = createWriter();
    const encoder = UIMessageCodec.createEncoder(writer);
    const decoder = UIMessageCodec.createDecoder();

    await encoder.publishOutput({ type: "start", messageId: "m1" });
    await encoder.publishOutput({ type: "start-step" });
    await encoder.publishOutput({
      type: "text-start",
      id: "text",
      messageId: "m1",
    });
    await encoder.publishOutput({
      type: "text-delta",
      id: "text",
      messageId: "m1",
      delta: "Hello",
    });
    await encoder.publishOutput({
      type: "text-end",
      id: "text",
      messageId: "m1",
    });
    await encoder.publishOutput({ type: "finish-step" });
    await encoder.publishOutput({ type: "finish", finishReason: "stop" });

    const projection = createVercelProjection();
    let publishCount = 0;
    writer.writes.forEach((write, index) => {
      if (write.kind === "publish") {
        publishCount += 1;
      }
      const message =
        write.kind === "publish"
          ? rawFromPublish(write.message, `msg-${String(publishCount)}`)
          : write.kind === "append"
            ? rawFromAppend(write, "append", index + 1)
            : rawFromUpdate(write, index + 1);
      const decoded = decoder.decode(message);
      [...decoded.inputs, ...decoded.outputs].forEach((event) => {
        foldVercelEvent(projection, event.event, event.meta);
      });
    });

    expect(UIMessageCodec.getMessages(projection).map((message) => message.id)).toEqual(["m1"]);
    expect(UIMessageCodec.getMessages(projection)[0]?.parts).toEqual([
      { type: "text", id: "text", text: "Hello" },
    ]);
  });

  it("uses the transport codec message id as the Vercel message fallback", async () => {
    const writer = createWriter();
    const encoder = UIMessageCodec.createEncoder(writer, {
      extras: {
        ai: {
          transport: {
            [HEADER_CODEC_MESSAGE_ID]: "assistant-generated",
          },
        },
      },
    });
    const decoder = UIMessageCodec.createDecoder();

    await encoder.publishOutput({ type: "start" });
    await encoder.publishOutput({ type: "text-start", id: "text" });
    await encoder.publishOutput({
      type: "text-delta",
      id: "text",
      delta: "Hello",
    });
    await encoder.publishOutput({ type: "text-end", id: "text" });
    await encoder.publishOutput({ type: "finish", finishReason: "stop" });

    expect(writer.publishes[0]?.extras).toMatchObject({
      ai: {
        codec: { "message-id": "assistant-generated" },
        transport: { [HEADER_CODEC_MESSAGE_ID]: "assistant-generated" },
      },
    });
    expect(writer.updates[0]?.options.extras).toMatchObject({
      ai: {
        codec: { "message-id": "assistant-generated" },
        transport: { [HEADER_CODEC_MESSAGE_ID]: "assistant-generated" },
      },
    });

    const projection = createVercelProjection();
    let publishCount = 0;
    writer.writes.forEach((write, index) => {
      if (write.kind === "publish") {
        publishCount += 1;
      }
      const message =
        write.kind === "publish"
          ? rawFromPublish(write.message, `msg-${String(publishCount)}`)
          : write.kind === "append"
            ? rawFromAppend(write, "append", index + 1)
            : rawFromUpdate(write, index + 1);
      const decoded = decoder.decode(message);
      [...decoded.inputs, ...decoded.outputs].forEach((event) => {
        foldVercelEvent(projection, event.event, event.meta);
      });
    });

    expect(UIMessageCodec.getMessages(projection).map((message) => message.id)).toEqual([
      "assistant-generated",
    ]);
    expect(UIMessageCodec.getMessages(projection)[0]?.parts).toEqual([
      { type: "text", id: "text", text: "Hello" },
    ]);
  });

  it("generates one assistant message id for Gateway chunks without message ids", async () => {
    const writer = createWriter();
    const encoder = UIMessageCodec.createEncoder(writer);
    const decoder = UIMessageCodec.createDecoder();

    await encoder.publishOutput({ type: "start" });
    await encoder.publishOutput({ type: "text-start", id: "text" });
    await encoder.publishOutput({
      type: "text-delta",
      id: "text",
      delta: "Eight",
    });
    await encoder.publishOutput({ type: "text-end", id: "text" });
    await encoder.publishOutput({ type: "finish", finishReason: "stop" });

    const projection = createVercelProjection();
    let publishCount = 0;
    writer.writes.forEach((write, index) => {
      if (write.kind === "publish") {
        publishCount += 1;
      }
      const message =
        write.kind === "publish"
          ? rawFromPublish(write.message, `msg-${String(publishCount)}`)
          : write.kind === "append"
            ? rawFromAppend(write, "append", index + 1)
            : rawFromUpdate(write, index + 1);
      const decoded = decoder.decode(message);
      [...decoded.inputs, ...decoded.outputs].forEach((event) => {
        foldVercelEvent(projection, event.event, event.meta);
      });
    });

    const messages = UIMessageCodec.getMessages(projection);
    expect(messages).toHaveLength(1);
    expect(messages[0]?.id).toMatch(/^msg_/);
    expect(messages[0]?.parts).toEqual([{ type: "text", id: "text", text: "Eight" }]);
  });
});

interface TestWriter extends ChannelWriter {
  publishes: PublishMessage[];
  appends: {
    messageSerial: string;
    data: string;
    options: Omit<MessageMutation, "data">;
  }[];
  updates: {
    messageSerial: string;
    options: MessageMutation;
  }[];
  writes: (
    | { kind: "publish"; message: PublishMessage }
    | {
        kind: "append";
        messageSerial: string;
        data: string;
        options: Omit<MessageMutation, "data">;
      }
    | {
        kind: "update";
        messageSerial: string;
        options: MessageMutation;
      }
  )[];
}

interface StreamSpec {
  messageId: string;
  text: string;
  reasoning: string;
  includeTool: boolean;
  toolInput: string;
  toolOutput: string;
  includeFile: boolean;
  includeSource: boolean;
  includeData: boolean;
}

function createWriter(): TestWriter {
  let count = 0;
  const writer: TestWriter = {
    publishes: [],
    appends: [],
    updates: [],
    writes: [],
    publish(message) {
      count += 1;
      writer.publishes.push(message);
      writer.writes.push({ kind: "publish", message });
      return Promise.resolve(ack(`msg-${String(count)}`));
    },
    appendMessage(messageSerial, data, options = {}) {
      writer.appends.push({ messageSerial, data, options });
      writer.writes.push({ kind: "append", messageSerial, data, options });
      return Promise.resolve(ack(messageSerial));
    },
    updateMessage(messageSerial, options = {}) {
      writer.updates.push({ messageSerial, options });
      writer.writes.push({ kind: "update", messageSerial, options });
      return Promise.resolve(ack(messageSerial));
    },
  };
  return writer;
}

function ack(messageSerial: string): MessageAck {
  return {
    messageSerial,
    historySerial: 1,
  };
}

function expectDefined<T>(value: T | undefined): T {
  if (value === undefined) {
    throw new Error("expected test fixture value to be defined");
  }
  return value;
}

function rawFromPublish(
  message: PublishMessage,
  messageSerial: string,
): ReturnType<typeof normalizeInboundMessage> {
  return normalizeInboundMessage({
    event: "sockudo:message.create",
    name: message.name ?? EVENT_AI_OUTPUT,
    channel: "chat",
    data: message.data,
    extras: message.extras,
    message_serial: messageSerial,
    history_serial: 1,
    delivery_serial: 1,
  } satisfies SockudoRawMessage);
}

function rawFromAppend(
  append: TestWriter["appends"][number],
  action: "append" | "update",
  serial: number,
): ReturnType<typeof normalizeInboundMessage> {
  return normalizeInboundMessage({
    event: `sockudo:message.${action}`,
    name: EVENT_AI_OUTPUT,
    channel: "chat",
    data: append.data,
    extras: append.options.extras,
    message_serial: append.messageSerial,
    history_serial: serial,
    delivery_serial: serial,
  } satisfies SockudoRawMessage);
}

function rawFromUpdate(
  update: TestWriter["updates"][number],
  serial: number,
  data: unknown = update.options.data,
): ReturnType<typeof normalizeInboundMessage> {
  return normalizeInboundMessage({
    event: "sockudo:message.update",
    name: EVENT_AI_OUTPUT,
    channel: "chat",
    data,
    extras: update.options.extras,
    message_serial: update.messageSerial,
    history_serial: serial,
    delivery_serial: serial,
  } satisfies SockudoRawMessage);
}

function validStreamSpec(): fc.Arbitrary<StreamSpec> {
  const shortText = fc.string({ minLength: 0, maxLength: 16 });
  return fc
    .record({
      messageId: fc.constantFrom("m1", "m2", "turn-3"),
      text: shortText,
      reasoning: shortText,
      includeTool: fc.boolean(),
      toolInput: shortText,
      toolOutput: shortText,
      includeFile: fc.boolean(),
      includeSource: fc.boolean(),
      includeData: fc.boolean(),
    })
    .filter((spec) => spec.text.trim().length > 0 || spec.includeTool);
}

function chunksFromSpec(spec: StreamSpec): VercelOutput[] {
  const chunks: VercelOutput[] = [{ type: "start", messageId: spec.messageId }];
  if (spec.text.length > 0) {
    chunks.push(
      { type: "text-start", id: "text", messageId: spec.messageId },
      {
        type: "text-delta",
        id: "text",
        delta: spec.text,
        messageId: spec.messageId,
      },
      { type: "text-end", id: "text", messageId: spec.messageId },
    );
  }
  if (spec.reasoning.length > 0) {
    chunks.push(
      { type: "reasoning-start", id: "reason", messageId: spec.messageId },
      {
        type: "reasoning-delta",
        id: "reason",
        delta: spec.reasoning,
        messageId: spec.messageId,
      },
      { type: "reasoning-end", id: "reason", messageId: spec.messageId },
    );
  }
  if (spec.includeTool) {
    const input = { value: spec.toolInput };
    chunks.push(
      {
        type: "tool-input-start",
        toolCallId: "tool-1",
        toolName: "lookup",
        messageId: spec.messageId,
      },
      {
        type: "tool-input-delta",
        toolCallId: "tool-1",
        delta: JSON.stringify(input),
        messageId: spec.messageId,
      },
      {
        type: "tool-input-available",
        toolCallId: "tool-1",
        toolName: "lookup",
        input,
        messageId: spec.messageId,
      },
      {
        type: "tool-output-available",
        toolCallId: "tool-1",
        output: { value: spec.toolOutput },
        messageId: spec.messageId,
      },
    );
  }
  if (spec.includeSource) {
    chunks.push({
      type: "source-url",
      sourceId: "source-1",
      url: "https://example.test/source",
      title: "Example",
    });
  }
  if (spec.includeFile) {
    chunks.push({
      type: "file",
      url: "https://example.test/file.txt",
      mediaType: "text/plain",
    });
  }
  if (spec.includeData) {
    chunks.push({
      type: "data-weather",
      data: { temperature: 22 },
      transient: false,
    });
    chunks.push({
      type: "data-transient",
      data: "ignored",
      transient: true,
    });
  }
  chunks.push({ type: "finish", finishReason: "stop" });
  return chunks;
}

function toVercelAIChunk(chunk: VercelOutput): VercelAIChunk {
  switch (chunk.type) {
    case "start":
    case "start-step":
    case "finish-step":
    case "error":
      return chunk;
    case "message-metadata":
      return {
        type: "message-metadata",
        messageMetadata: chunk.messageMetadata,
      };
    case "finish":
      return toVercelFinishChunk(chunk.finishReason);
    case "abort":
      return { type: "abort" };
    case "text-start":
      return { type: "text-start", id: chunk.id };
    case "text-delta":
      return { type: "text-delta", id: chunk.id, delta: chunk.delta };
    case "text-end":
      return { type: "text-end", id: chunk.id };
    case "reasoning-start":
      return { type: "reasoning-start", id: chunk.id };
    case "reasoning-delta":
      return { type: "reasoning-delta", id: chunk.id, delta: chunk.delta };
    case "reasoning-end":
      return { type: "reasoning-end", id: chunk.id };
    case "tool-input-start":
      return {
        type: "tool-input-start",
        toolCallId: chunk.toolCallId,
        toolName: chunk.toolName,
      };
    case "tool-input-delta":
      return {
        type: "tool-input-delta",
        toolCallId: chunk.toolCallId,
        inputTextDelta: chunk.delta,
      };
    case "tool-input-available":
      return {
        type: "tool-input-available",
        toolCallId: chunk.toolCallId,
        toolName: chunk.toolName ?? "tool",
        input: chunk.input,
      };
    case "tool-input-error":
      return {
        type: "tool-input-error",
        toolCallId: chunk.toolCallId,
        toolName: chunk.toolName ?? "tool",
        input: undefined,
        errorText: chunk.errorText,
      };
    case "tool-output-available":
      return {
        type: "tool-output-available",
        toolCallId: chunk.toolCallId,
        output: chunk.output,
      };
    case "tool-output-error":
      return {
        type: "tool-output-error",
        toolCallId: chunk.toolCallId,
        errorText: chunk.errorText,
      };
    case "tool-approval-request":
      return {
        type: "tool-approval-request",
        approvalId: chunk.approvalId ?? "",
        toolCallId: chunk.toolCallId,
      };
    case "tool-output-denied":
      return { type: "tool-output-denied", toolCallId: chunk.toolCallId };
    case "source-url":
      return {
        type: "source-url",
        sourceId: chunk.sourceId ?? "",
        url: chunk.url,
        ...(chunk.title !== undefined ? { title: chunk.title } : {}),
      };
    case "source-document":
      return {
        type: "source-document",
        sourceId: chunk.sourceId ?? "",
        title: chunk.title ?? "",
        mediaType: chunk.mediaType ?? "application/octet-stream",
        ...(chunk.filename !== undefined ? { filename: chunk.filename } : {}),
      };
    case "file":
      return {
        type: "file",
        url: chunk.url,
        mediaType: chunk.mediaType ?? "application/octet-stream",
      };
    default:
      return {
        type: chunk.type,
        data: chunk.data,
        ...(chunk.transient !== undefined ? { transient: chunk.transient } : {}),
      };
  }
}

function toVercelFinishChunk(
  finishReason: string | undefined,
): Extract<VercelAIChunk, { type: "finish" }> {
  if (isVercelFinishReason(finishReason)) {
    return { type: "finish", finishReason };
  }
  return { type: "finish" };
}

function isVercelFinishReason(
  value: string | undefined,
): value is NonNullable<Extract<VercelAIChunk, { type: "finish" }>["finishReason"]> {
  const allowed: readonly string[] = [
    "stop",
    "length",
    "content-filter",
    "tool-calls",
    "error",
    "other",
  ];
  return value !== undefined && allowed.includes(value);
}

async function readVercelMessages(chunks: readonly VercelAIChunk[]): Promise<VercelAIMessage[]> {
  let last: VercelAIMessage | undefined;
  const stream = new ReadableStream<VercelAIChunk>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
  for await (const message of readUIMessageStream({ stream })) {
    last = message;
  }
  return last ? [last] : [];
}

function normalizeMessages(messages: readonly (AI.UIMessage | VercelAIMessage)[]): unknown {
  return messages.map((message) => ({
    id: message.id,
    role: message.role,
    parts: message.parts.map((part) => normalizePart(part)),
    ...(message.metadata !== undefined ? { metadata: message.metadata } : {}),
  }));
}

function normalizePart(part: AI.UIMessagePart | VercelAIMessage["parts"][number]): unknown {
  const value = part as Record<string, unknown>;
  if (typeof value.type === "string" && value.type.startsWith("tool-")) {
    return stripUndefinedRecord({
      type: "dynamic-tool",
      toolName: value.type.slice("tool-".length),
      toolCallId: value.toolCallId,
      state: value.state,
      input: value.input,
      output: value.output,
      errorText: value.errorText,
      approval: value.approval,
    });
  }
  if (value.type === "dynamic-tool") {
    return stripUndefinedRecord({
      type: "dynamic-tool",
      toolName: value.toolName,
      toolCallId: value.toolCallId,
      state: value.state,
      input: value.input,
      output: value.output,
      errorText: value.errorText,
      approval: value.approval,
    });
  }
  const clone = { ...value };
  delete clone.id;
  delete clone.state;
  if (clone.transient === false) {
    delete clone.transient;
  }
  return clone;
}

function normalizeWireTranscript(writer: TestWriter): unknown {
  return writer.writes.map((write) => {
    if (write.kind === "publish") {
      return stripUndefinedRecord({
        kind: "publish",
        name: write.message.name,
        messageId: write.message.messageId,
        data: write.message.data,
        extras: write.message.extras,
      });
    }
    return {
      kind: write.kind,
      messageSerial: write.messageSerial,
      extras: write.options.extras,
      ...(write.kind === "append" ? { data: write.data } : {}),
    };
  });
}

function stripUndefinedRecord(value: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    if (item !== undefined) {
      result[key] = item;
    }
  }
  return result;
}
