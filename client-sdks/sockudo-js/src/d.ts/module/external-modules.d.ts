declare module "fossil-delta" {
  export function applyDelta(base: Uint8Array, delta: Uint8Array): Uint8Array;
  export function createDelta(base: Uint8Array, target: Uint8Array): Uint8Array;
}

declare module "@ably/vcdiff-decoder" {
  export function decode(base: Uint8Array, delta: Uint8Array): Uint8Array;
}

declare module "tweetnacl";

declare module "@stablelib/utf8";

declare module "@stablelib/base64";
