import crypto from "crypto";
import nacl from "tweetnacl";
import naclUtil from "tweetnacl-util";
import { isEncryptedChannel } from "./util";
import type {
  BatchEvent,
  IdempotencyKey,
  ResponseWithIdempotency,
  TriggerParams,
} from "./types";
import type Sockudo = require("./sockudo");

function generateUUIDv4(): string {
  const bytes = crypto.randomBytes(16);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20, 32),
  ].join("-");
}

function resolveIdempotencyKey(
  params?: TriggerParams,
): TriggerParams | undefined {
  if (!params || params.idempotency_key === undefined) {
    return params;
  }

  const resolved = { ...params };
  if (resolved.idempotency_key === true) {
    resolved.idempotency_key = generateUUIDv4();
  }
  return resolved;
}

function ensureJSON(data: unknown): string {
  return typeof data === "string" ? data : JSON.stringify(data);
}

function encrypt(sockudo: Sockudo, channel: string, data: unknown): string {
  if (sockudo.config.encryptionMasterKey === undefined) {
    throw new Error(
      "Set encryptionMasterKey before triggering events on encrypted channels",
    );
  }

  const nonceBytes = nacl.randomBytes(24);
  const ciphertextBytes = nacl.secretbox(
    naclUtil.decodeUTF8(JSON.stringify(data)),
    nonceBytes,
    sockudo.channelSharedSecret(channel),
  );

  return JSON.stringify({
    nonce: naclUtil.encodeBase64(nonceBytes),
    ciphertext: naclUtil.encodeBase64(ciphertextBytes),
  });
}

export function trigger(
  sockudo: Sockudo,
  channels: string[],
  eventName: string,
  data: unknown,
  params?: TriggerParams,
): Promise<ResponseWithIdempotency> {
  const resolvedParams = resolveIdempotencyKey(params);
  const headers: Record<string, string> = {};

  if (resolvedParams?.idempotency_key) {
    headers["X-Idempotency-Key"] = String(resolvedParams.idempotency_key);
  }

  if (channels.length === 1 && isEncryptedChannel(channels[0])) {
    const channel = channels[0];
    const event = {
      name: eventName,
      data: encrypt(sockudo, channel, data),
      channels: [channel],
      ...resolvedParams,
    };
    return sockudo.post({ path: "/events", body: event, headers });
  }

  for (const channel of channels) {
    if (isEncryptedChannel(channel)) {
      throw new Error(
        "You cannot trigger to multiple channels when using encrypted channels",
      );
    }
  }

  const event = {
    name: eventName,
    data: ensureJSON(data),
    channels,
    ...resolvedParams,
  };
  return sockudo.post({ path: "/events", body: event, headers });
}

export function triggerBatch(
  sockudo: Sockudo,
  batch: BatchEvent[],
): Promise<ResponseWithIdempotency> {
  for (const item of batch) {
    item.data = isEncryptedChannel(item.channel)
      ? encrypt(sockudo, item.channel, item.data)
      : ensureJSON(item.data);

    if (item.idempotency_key === true) {
      item.idempotency_key = generateUUIDv4() as IdempotencyKey;
    }
  }

  return sockudo.post({ path: "/batch_events", body: { batch } });
}
