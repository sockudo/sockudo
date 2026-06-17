import { readdir, readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  EVENT_AI_OUTPUT,
  EVENT_AI_TURN_END,
  EVENT_AI_TURN_START,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_INVOCATION_ID,
  HEADER_ROLE,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_STREAM_ID,
  HEADER_TURN_ID,
  HEADER_TURN_REASON,
} from "../../src/constants.js";
import type { SockudoRawMessage } from "../../src/realtime/adapter.js";
import { GOLDEN_TRANSCRIPT_RELATIVE_DIR } from "./server-pin.js";

export interface GoldenFrame {
  event: string;
  channel?: string;
  data?: unknown;
  action?: string;
  message_serial?: string;
  history_serial?: string | number;
  delivery_serial?: string | number;
  serial?: string | number;
  version?: { serial?: string; timestamp_ms?: string | number };
}

export interface GoldenTranscript {
  name: string;
  path: string;
  frames: readonly GoldenFrame[];
}

const thisDir = dirname(fileURLToPath(import.meta.url));
const sdkRoot = resolve(thisDir, "../..");
const defaultServerRoot = resolve(sdkRoot, "../sockudo");

export function serverRoot(): string {
  return process.env.SOCKUDO_SERVER_ROOT ?? defaultServerRoot;
}

export async function loadGoldenTranscripts(): Promise<GoldenTranscript[]> {
  const dir = join(serverRoot(), GOLDEN_TRANSCRIPT_RELATIVE_DIR);
  const files = (await readdir(dir))
    .filter((file) => file.endsWith(".json"))
    .sort();
  return Promise.all(
    files.map(async (file) => ({
      name: file.replace(/\.json$/u, ""),
      path: join(dir, file),
      frames: JSON.parse(
        await readFile(join(dir, file), "utf8"),
      ) as GoldenFrame[],
    })),
  );
}

export function hydrateGoldenFrame(
  frame: GoldenFrame,
  index: number,
): SockudoRawMessage | undefined {
  if (frame.event === "sockudo:subscription_succeeded") {
    return undefined;
  }
  const channel = frame.channel ?? "private-ai-unknown-golden";
  const scenario = scenarioName(channel);
  const historyFrame = historyPayload(frame);
  const messageSerial = placeholder(
    historyFrame?.message_serial ?? frame.message_serial,
    `msg-${scenario}`,
  );
  const serial = placeholder(
    historyFrame?.history_serial ?? frame.history_serial ?? frame.serial,
    index + 1,
  );
  const delivery = placeholder(
    historyFrame?.delivery_serial ?? frame.delivery_serial ?? frame.serial,
    serial,
  );
  const transport = transportHeaders(frame, scenario, messageSerial);
  const codec = codecHeaders(frame, messageSerial);
  return {
    event: frame.event,
    name: logicalName(frame),
    channel,
    data: historyFrame?.data ?? frame.data,
    action: hydratedAction(frame),
    message_serial: messageSerial,
    history_serial: serial,
    delivery_serial: delivery,
    serial: delivery,
    version: {
      serial: placeholder(frame.version?.serial, `ver-${String(index + 1)}`),
      timestamp_ms: placeholder(frame.version?.timestamp_ms, 1),
    },
    extras: {
      ai: {
        transport,
        codec,
      },
    },
  };
}

export function normalizeMaterialized(value: unknown): unknown {
  return JSON.parse(JSON.stringify(value)) as unknown;
}

function logicalName(frame: GoldenFrame): string {
  if (frame.event === "history:get_latest") {
    return EVENT_AI_OUTPUT;
  }
  return frame.event.startsWith("sockudo:message.")
    ? EVENT_AI_OUTPUT
    : frame.event;
}

function rawMutableAction(action: string | undefined): string | undefined {
  switch (action) {
    case "append":
      return "message.append";
    case "update":
      return "message.update";
    case "delete":
      return "message.delete";
    case "summary":
      return "message.summary";
    default:
      return action;
  }
}

function hydratedAction(frame: GoldenFrame): string | undefined {
  if (frame.event === "history:get_latest" || isStateOnlyOutput(frame)) {
    return "message.append";
  }
  return rawMutableAction(frame.action);
}

function scenarioName(channel: string): string {
  return channel
    .replace(/^private-ai-/u, "")
    .replace(/-golden$/u, "")
    .replace(/[^a-z0-9-]/gu, "-");
}

function transportHeaders(
  frame: GoldenFrame,
  scenario: string,
  messageSerial: string,
): Record<string, string> {
  const turnId = `turn-${scenario}`;
  const transport: Record<string, string> = {
    [HEADER_TURN_ID]: turnId,
  };
  if (frame.event === EVENT_AI_TURN_START) {
    transport[HEADER_INVOCATION_ID] = `invoke-${scenario}`;
  }
  if (frame.event === EVENT_AI_TURN_END) {
    transport[HEADER_TURN_REASON] = turnReason(scenario);
  }
  if (
    frame.event === EVENT_AI_OUTPUT ||
    frame.event === "history:get_latest" ||
    frame.event.startsWith("sockudo:message.")
  ) {
    transport[HEADER_ROLE] = "assistant";
    transport[HEADER_CODEC_MESSAGE_ID] = messageSerial;
    transport[HEADER_STREAM] = "true";
    transport[HEADER_STREAM_ID] = `text:${messageSerial}:text`;
    transport[HEADER_STATUS] = streamStatus(frame, scenario);
  }
  return transport;
}

function codecHeaders(
  frame: GoldenFrame,
  messageSerial: string,
): Record<string, string> {
  if (
    !(
      frame.event === EVENT_AI_OUTPUT ||
      frame.event === "history:get_latest" ||
      frame.event.startsWith("sockudo:message.")
    )
  ) {
    return {};
  }
  return {
    type: codecType(frame),
    id: "text",
    "message-id": messageSerial,
  };
}

function codecType(frame: GoldenFrame): string {
  if (frame.event === EVENT_AI_OUTPUT && !isStateOnlyOutput(frame)) {
    return "text-start";
  }
  if (frame.event === "history:get_latest" || isStateOnlyOutput(frame)) {
    return "text-delta";
  }
  return streamStatus(frame, "") === "streaming" ? "text-delta" : "text-end";
}

function streamStatus(frame: GoldenFrame, scenario: string): string {
  if (frame.event === EVENT_AI_OUTPUT && !isStateOnlyOutput(frame)) {
    return "streaming";
  }
  if (isStateOnlyOutput(frame)) {
    return "complete";
  }
  if (frame.event === "history:get_latest") {
    return "complete";
  }
  if (frame.action === "update" && scenario === "abort") {
    return "cancelled";
  }
  return isTerminalMutation(frame) ? "complete" : "streaming";
}

function isStateOnlyOutput(frame: GoldenFrame): boolean {
  return (
    frame.event === EVENT_AI_OUTPUT &&
    typeof frame.data === "string" &&
    frame.data !== ""
  );
}

function isTerminalMutation(frame: GoldenFrame): boolean {
  return (
    frame.action === "update" ||
    frame.data === "hello world" ||
    frame.data === "new answer" ||
    frame.data === "recoverable"
  );
}

function historyPayload(frame: GoldenFrame):
  | {
      data?: unknown;
      message_serial?: string;
      history_serial?: string | number;
      delivery_serial?: string | number;
    }
  | undefined {
  if (frame.event !== "history:get_latest") {
    return undefined;
  }
  const data = frame.data;
  return data !== null && typeof data === "object" ? data : undefined;
}

function turnReason(scenario: string): string {
  switch (scenario) {
    case "cancel":
      return "cancelled";
    case "error":
      return "error";
    case "suspended":
      return "suspended";
    default:
      return "complete";
  }
}

function placeholder<T extends string | number>(
  value: T | string | undefined,
  fallback: T,
): T {
  return value === undefined ||
    (typeof value === "string" && value.startsWith("<"))
    ? fallback
    : (value as T);
}
