import type { SockudoEvent } from "./connection/protocol/message-types";

export type MutableMessageAction =
  | "message.create"
  | "message.update"
  | "message.delete"
  | "message.append";

export interface MutableMessageVersionInfo {
  action: MutableMessageAction;
  event: string;
  messageSerial: string;
  versionSerial?: string;
  historySerial?: number;
  versionTimestampMs?: number;
}

export interface MutableMessageState<T = unknown> {
  messageSerial: string;
  action: MutableMessageAction;
  data: T | null;
  event: string;
  serial?: number;
  streamId?: string;
  messageId?: string;
  versionSerial?: string;
  historySerial?: number;
  versionTimestampMs?: number;
}

function parseNumericHeader(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

export function isMutableMessageEvent(
  event: Pick<SockudoEvent, "event" | "extras">,
): boolean {
  return getMutableMessageInfo(event) !== null;
}

export function getMutableMessageInfo(
  event: Pick<SockudoEvent, "event" | "extras">,
): MutableMessageVersionInfo | null {
  const actionHeader = event.extras?.headers?.sockudo_action;
  const messageSerialHeader = event.extras?.headers?.sockudo_message_serial;
  const versionSerialHeader = event.extras?.headers?.sockudo_version_serial;
  const historySerialHeader = event.extras?.headers?.sockudo_history_serial;
  const versionTimestampHeader =
    event.extras?.headers?.sockudo_version_timestamp_ms;

  if (
    typeof actionHeader !== "string" ||
    typeof messageSerialHeader !== "string"
  ) {
    return null;
  }

  if (
    actionHeader !== "message.create" &&
    actionHeader !== "message.update" &&
    actionHeader !== "message.delete" &&
    actionHeader !== "message.append"
  ) {
    return null;
  }

  return {
    action: actionHeader,
    event: event.event,
    messageSerial: messageSerialHeader,
    versionSerial:
      typeof versionSerialHeader === "string" ? versionSerialHeader : undefined,
    historySerial: parseNumericHeader(historySerialHeader),
    versionTimestampMs: parseNumericHeader(versionTimestampHeader),
  };
}

export function reduceMutableMessageEvent(
  current: MutableMessageState | null,
  event: SockudoEvent,
): MutableMessageState {
  const info = getMutableMessageInfo(event);
  if (!info) {
    throw new Error("Event is not a mutable-message event");
  }

  if (current && current.messageSerial !== info.messageSerial) {
    throw new Error(
      `Mutable-message reducer expected message_serial '${current.messageSerial}' but received '${info.messageSerial}'`,
    );
  }

  let nextData: unknown;
  switch (info.action) {
    case "message.append": {
      if (typeof current?.data !== "string") {
        throw new Error(
          "message.append requires an existing string base; seed state from a create/update payload or latest-view history first",
        );
      }
      if (typeof event.data !== "string") {
        throw new Error(
          "message.append payload must be a string fragment when applying client-side concatenation",
        );
      }
      nextData = `${current.data}${event.data}`;
      break;
    }
    case "message.delete":
      nextData = event.data ?? null;
      break;
    case "message.create":
    case "message.update":
      nextData = event.data ?? null;
      break;
  }

  return {
    messageSerial: info.messageSerial,
    action: info.action,
    data: nextData,
    event: info.event,
    serial: event.serial,
    streamId: event.stream_id,
    messageId: event.message_id,
    versionSerial: info.versionSerial,
    historySerial: info.historySerial,
    versionTimestampMs: info.versionTimestampMs,
  };
}

export function reduceMutableMessageEvents(
  events: SockudoEvent[],
): MutableMessageState | null {
  let state: MutableMessageState | null = null;
  for (const event of events) {
    state = reduceMutableMessageEvent(state, event);
  }
  return state;
}
