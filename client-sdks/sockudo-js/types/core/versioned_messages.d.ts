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

export function isMutableMessageEvent(
  event: Pick<SockudoEvent, "event" | "extras">,
): boolean;

export function getMutableMessageInfo(
  event: Pick<SockudoEvent, "event" | "extras">,
): MutableMessageVersionInfo | null;

export function reduceMutableMessageEvent(
  current: MutableMessageState | null,
  event: SockudoEvent,
): MutableMessageState;

export function reduceMutableMessageEvents(
  events: SockudoEvent[],
): MutableMessageState | null;
