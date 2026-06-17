import crypto from "crypto";
import type { RequestParams } from "./types";

export function toOrderedArray(map: RequestParams): string[] {
  return Object.keys(map)
    .map((key) => [key, map[key]] as const)
    .sort((a, b) => {
      if (a[0] < b[0]) {
        return -1;
      }
      if (a[0] > b[0]) {
        return 1;
      }
      return 0;
    })
    .map(([key, value]) => `${key}=${String(value)}`);
}

export function toOrderedArrayLowercaseKeys(map: RequestParams): string[] {
  return Object.keys(map)
    .map((key) => [key.toLowerCase(), map[key]] as const)
    .sort((a, b) => {
      if (a[0] < b[0]) {
        return -1;
      }
      if (a[0] > b[0]) {
        return 1;
      }
      return 0;
    })
    .map(([key, value]) => `${key}=${String(value)}`);
}

export function getMD5(body: string): string {
  return crypto.createHash("md5").update(body, "utf8").digest("hex");
}

export function secureCompare(a: string, b: string): boolean {
  if (a.length !== b.length) {
    return false;
  }

  let result = 0;
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return result === 0;
}

export function isEncryptedChannel(channel: string): boolean {
  return channel.startsWith("private-encrypted-");
}
