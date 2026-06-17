import { headerReader, headerWriter, type HeaderMap } from "../../utils.js";
import type { AI } from "./events.js";

/** Codec header keys used by UIMessageCodec. */
export const VERCEL_HEADER_KEYS = [
  "type",
  "id",
  "message-id",
  "tool-call-id",
  "tool-name",
  "dynamic",
  "title",
  "provider-executed",
  "preliminary",
  "approval-id",
  "approved",
  "reason",
  "finish-reason",
  "source-id",
  "media-type",
  "filename",
  "transient",
  "provider-metadata",
  "message-metadata",
] as const;

const WIRE_KEYS = {
  messageId: "message-id",
  toolCallId: "tool-call-id",
  toolName: "tool-name",
  providerExecuted: "provider-executed",
  approvalId: "approval-id",
  finishReason: "finish-reason",
  sourceId: "source-id",
  mediaType: "media-type",
  providerMetadata: "provider-metadata",
  messageMetadata: "message-metadata",
} as const;

/** Builds codec headers for a Vercel chunk. */
export function headersForChunk(chunk: AI.UIMessageChunk): HeaderMap {
  const writer = headerWriter();
  writer.set("type", chunk.type);
  if ("id" in chunk) {
    writer.set("id", chunk.id);
  }
  if ("messageId" in chunk) {
    writer.set(WIRE_KEYS.messageId, chunk.messageId);
  }
  if ("toolCallId" in chunk) {
    writer.set(WIRE_KEYS.toolCallId, chunk.toolCallId);
  }
  if ("toolName" in chunk) {
    writer.set(WIRE_KEYS.toolName, chunk.toolName);
  }
  if ("dynamic" in chunk) {
    writer.set("dynamic", chunk.dynamic);
  }
  if ("title" in chunk) {
    writer.set("title", chunk.title);
  }
  if ("providerExecuted" in chunk) {
    writer.set(WIRE_KEYS.providerExecuted, chunk.providerExecuted);
  }
  if ("preliminary" in chunk) {
    writer.set("preliminary", chunk.preliminary);
  }
  if ("approvalId" in chunk) {
    writer.set(WIRE_KEYS.approvalId, chunk.approvalId);
  }
  if ("reason" in chunk) {
    writer.set("reason", chunk.reason);
  }
  if ("finishReason" in chunk) {
    writer.set(WIRE_KEYS.finishReason, chunk.finishReason);
  }
  if ("sourceId" in chunk) {
    writer.set(WIRE_KEYS.sourceId, chunk.sourceId);
  }
  if ("mediaType" in chunk) {
    writer.set(WIRE_KEYS.mediaType, chunk.mediaType);
  }
  if ("filename" in chunk) {
    writer.set("filename", chunk.filename);
  }
  if ("transient" in chunk) {
    writer.set("transient", chunk.transient);
  }
  if ("metadata" in chunk) {
    writer.json(WIRE_KEYS.providerMetadata, chunk.metadata);
  }
  if ("messageMetadata" in chunk) {
    writer.json(WIRE_KEYS.messageMetadata, chunk.messageMetadata);
  }
  return writer.headers;
}

/** Normalizes Vercel codec headers from wire-safe keys with legacy fallback. */
export function normalizeVercelHeaders(headers: HeaderMap): HeaderMap {
  const normalized = Object.create(null) as Record<string, string>;
  for (const [key, value] of Object.entries(headers)) {
    normalized[key] = value;
  }
  for (const [legacy, wire] of Object.entries(WIRE_KEYS)) {
    const value = headers[wire] ?? headers[legacy];
    if (value !== undefined) {
      normalized[legacy] = value;
      normalized[wire] = value;
    }
  }
  return normalized;
}

/** Reads a Vercel chunk type from codec headers. */
export function chunkType(headers: HeaderMap): string | undefined {
  return headerReader(normalizeVercelHeaders(headers)).str("type");
}

/** Reads JSON metadata from codec headers. */
export function readJsonHeader(headers: HeaderMap, key: string): unknown {
  return headerReader(normalizeVercelHeaders(headers)).json(key);
}
