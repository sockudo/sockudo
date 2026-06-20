import { describe, expect, it, vi } from "vitest";

import {
  HEADER_DISCRETE,
  HEADER_ERROR_MESSAGE,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_STREAM_ID,
} from "../../constants.js";
import { ErrorCode, ErrorInfo } from "../../errors.js";
import { getCodecHeaders, getTransportHeaders } from "../../utils.js";
import { createEncoderCore } from "./encoder.js";
import type { ChannelWriter, EncoderOutboundMessage } from "./types.js";
import type { MessageAck, MessageMutation, PublishMessage } from "../../realtime/types.js";

describe("encoder core", () => {
  it("publishes discrete messages with discrete stream headers", async () => {
    const writer = createWriter();
    const encoder = createEncoderCore(writer);

    await expect(
      encoder.publishDiscrete("hello", {
        name: "ai-output",
        transport: { "turn-id": "turn-1" },
        codec: { provider: "unit" },
      }),
    ).resolves.toMatchObject({ messageSerial: "msg-1" });

    const publish = writer.publishes[0];
    expect(publish?.name).toBe("ai-output");
    expect(getTransportHeaders(publish?.extras)[HEADER_STREAM]).toBe("false");
    expect(getTransportHeaders(publish?.extras)[HEADER_DISCRETE]).toBe("true");
    expect(getTransportHeaders(publish?.extras)["turn-id"]).toBe("turn-1");
    expect(getCodecHeaders(publish?.extras).provider).toBe("unit");
  });

  it("tracks start serial and repeats persistent headers on appends", async () => {
    const writer = createWriter();
    const encoder = createEncoderCore(writer);

    await expect(
      encoder.startStream("stream-1", "a", {
        name: "ai-output",
        transport: { "turn-id": "turn-1" },
        codec: { provider: "unit" },
      }),
    ).resolves.toBe("msg-1");
    encoder.appendStream("stream-1", "b");
    await encoder.closeStream("stream-1");

    const createHeaders = getTransportHeaders(writer.publishes[0]?.extras);
    expect(createHeaders[HEADER_STREAM]).toBe("true");
    expect(createHeaders[HEADER_STATUS]).toBe("streaming");
    expect(createHeaders[HEADER_STREAM_ID]).toBe("stream-1");
    expect(writer.appends).toHaveLength(1);
    expect(writer.updates).toHaveLength(1);
    expect(writer.updates[0]).toMatchObject({
      messageSerial: "msg-1",
      options: {
        data: "ab",
      },
    });
    expect(getTransportHeaders(writer.appends.at(0)?.options.extras)["turn-id"]).toBe("turn-1");
    expect(getCodecHeaders(writer.appends.at(0)?.options.extras).provider).toBe("unit");
    expect(getTransportHeaders(writer.updates.at(0)?.options.extras)[HEADER_STATUS]).toBe(
      "complete",
    );
  });

  it("serializes stream appends before the terminal mutation", async () => {
    const writer = createSequencingWriter();
    const encoder = createEncoderCore(writer);

    await encoder.startStream("stream-1", "a");
    encoder.appendStream("stream-1", "b");
    encoder.appendStream("stream-1", "c");
    const closed = encoder.closeStream("stream-1");
    await flushMicrotasks();

    expect(writer.appends.map((append) => append.data)).toEqual(["b"]);
    expect(writer.updates).toHaveLength(0);

    writer.resolveNextAppend();
    await flushMicrotasks();

    expect(writer.appends.map((append) => append.data)).toEqual(["b", "c"]);
    expect(writer.updates).toHaveLength(0);

    writer.resolveNextAppend();
    await closed;

    expect(writer.updates).toHaveLength(1);
    expect(getTransportHeaders(writer.updates[0]?.options.extras)[HEADER_STATUS]).toBe("complete");
  });

  it("recovers failed appends with one aggregate update", async () => {
    const writer = createWriter({
      appendFailures: new Set([2]),
    });
    const encoder = createEncoderCore(writer);

    await encoder.startStream("stream-1", "a");
    encoder.appendStream("stream-1", "b");
    encoder.appendStream("stream-1", "c");
    await encoder.closeStream("stream-1");

    expect(writer.updates).toHaveLength(1);
    expect(writer.updates[0]).toMatchObject({
      messageSerial: "msg-1",
      options: {
        data: "abc",
      },
    });
    expect(getTransportHeaders(writer.updates[0]?.options.extras)[HEADER_STATUS]).toBe("complete");
  });

  it("throws 104000 when recovery update fails", async () => {
    const writer = createWriter({
      appendFailures: new Set([1]),
      updateFails: true,
    });
    const encoder = createEncoderCore(writer);

    await encoder.startStream("stream-1", "a");

    await expect(encoder.closeStream("stream-1")).rejects.toMatchObject({
      code: ErrorCode.EncoderRecoveryFailed,
    });
  });

  it("serializes concurrent terminal flushes", async () => {
    const writer = createWriter();
    const encoder = createEncoderCore(writer);

    await encoder.startStream("stream-1", "a");
    const first = encoder.closeStream("stream-1");
    const second = encoder.cancelStream("stream-1", "too late");
    await Promise.all([first, second]);

    expect(writer.updates).toHaveLength(1);
    expect(getTransportHeaders(writer.updates[0]?.options.extras)[HEADER_STATUS]).toBe("complete");
  });

  it("isolates onMessage hook exceptions and allows mutation", async () => {
    const seen: EncoderOutboundMessage[] = [];
    const hook = vi.fn((message: EncoderOutboundMessage) => {
      seen.push(message);
      if (message.kind === "publish" && message.publish) {
        message.publish.name = "mutated";
      }
      throw new Error("hook failure");
    });
    const writer = createWriter();
    const encoder = createEncoderCore(writer, { onMessage: hook });

    await encoder.publishDiscrete("x", { name: "original" });

    expect(writer.publishes[0]?.name).toBe("mutated");
    expect(seen).toHaveLength(1);
  });

  it("writes cancellation terminal status and reason", async () => {
    const writer = createWriter();
    const encoder = createEncoderCore(writer);

    await encoder.startStream("stream-1", "a");
    await encoder.cancelStream("stream-1", "user cancelled");

    const headers = getTransportHeaders(writer.updates[0]?.options.extras);
    expect(headers[HEADER_STATUS]).toBe("cancelled");
    expect(headers[HEADER_ERROR_MESSAGE]).toBe("user cancelled");
  });

  it("rejects unknown streams with ErrorInfo", () => {
    const encoder = createEncoderCore(createWriter());

    expect(() => {
      encoder.appendStream("missing", "x");
    }).toThrow(ErrorInfo);
  });
});

interface WriterOptions {
  appendFailures?: Set<number>;
  updateFails?: boolean;
}

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
}

function createWriter(options: WriterOptions = {}): TestWriter {
  let publishCount = 0;
  let appendCount = 0;
  const writer: TestWriter = {
    publishes: [],
    appends: [],
    updates: [],
    publish(message) {
      publishCount += 1;
      writer.publishes.push(message);
      return Promise.resolve(ack(`msg-${String(publishCount)}`));
    },
    appendMessage(messageSerial, data, mutation = {}) {
      appendCount += 1;
      writer.appends.push({ messageSerial, data, options: mutation });
      if (options.appendFailures?.has(appendCount)) {
        return Promise.reject(new Error("append failed"));
      }
      return Promise.resolve(ack(messageSerial));
    },
    updateMessage(messageSerial, mutation = {}) {
      writer.updates.push({ messageSerial, options: mutation });
      if (options.updateFails) {
        return Promise.reject(new Error("update failed"));
      }
      return Promise.resolve(ack(messageSerial));
    },
  };
  return writer;
}

interface SequencingWriter extends TestWriter {
  resolveNextAppend(): void;
}

function createSequencingWriter(): SequencingWriter {
  let publishCount = 0;
  const appendResolvers: (() => void)[] = [];
  const writer: SequencingWriter = {
    publishes: [],
    appends: [],
    updates: [],
    publish(message) {
      publishCount += 1;
      writer.publishes.push(message);
      return Promise.resolve(ack(`msg-${String(publishCount)}`));
    },
    appendMessage(messageSerial, data, mutation = {}) {
      writer.appends.push({ messageSerial, data, options: mutation });
      return new Promise<MessageAck>((resolve) => {
        appendResolvers.push(() => {
          resolve(ack(messageSerial));
        });
      });
    },
    updateMessage(messageSerial, mutation = {}) {
      writer.updates.push({ messageSerial, options: mutation });
      return Promise.resolve(ack(messageSerial));
    },
    resolveNextAppend() {
      appendResolvers.shift()?.();
    },
  };
  return writer;
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function ack(messageSerial: string): MessageAck {
  return {
    messageSerial,
    historySerial: 1,
  };
}
