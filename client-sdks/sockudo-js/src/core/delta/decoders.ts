/**
 * Delta Decoders for Fossil and Xdelta3 algorithms
 */

import { applyDelta as fossilApplyDelta } from "fossil-delta";
import { decode as vcdiffDecode } from "@ably/vcdiff-decoder";

// Also check for globals (for browser builds without bundler)
const fossilDeltaGlobal =
  typeof window !== "undefined" ? (window as any).fossilDelta : undefined;
const vcdiffGlobal =
  typeof window !== "undefined" ? (window as any).vcdiff : undefined;

/**
 * Base64 decode a string to Uint8Array
 */
function base64ToBytes(base64: string): Uint8Array {
  const binaryString = atob(base64);
  const bytes = new Uint8Array(binaryString.length);
  for (let i = 0; i < binaryString.length; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }
  return bytes;
}

/**
 * Convert Uint8Array to string
 */
function bytesToString(bytes: Uint8Array | number[]): string {
  // Handle array-like objects that might not be proper Uint8Array
  if (Array.isArray(bytes) || !(bytes instanceof Uint8Array)) {
    bytes = new Uint8Array(bytes);
  }
  return new TextDecoder().decode(bytes);
}

/**
 * Convert string to Uint8Array
 */
function stringToBytes(str: string): Uint8Array {
  return new TextEncoder().encode(str);
}

/**
 * Fossil Delta decoder
 */
export class FossilDeltaDecoder {
  static isAvailable(): boolean {
    return (
      typeof fossilApplyDelta !== "undefined" ||
      (typeof fossilDeltaGlobal !== "undefined" && fossilDeltaGlobal.apply)
    );
  }

  static apply(base: string | Uint8Array, delta: string | Uint8Array): string {
    if (!this.isAvailable()) {
      throw new Error("Fossil Delta library not loaded");
    }

    const baseBytes = typeof base === "string" ? stringToBytes(base) : base;
    const deltaBytes = typeof delta === "string" ? stringToBytes(delta) : delta;

    try {
      let result: Uint8Array;

      // Try ES6 import first
      if (typeof fossilApplyDelta !== "undefined") {
        result = fossilApplyDelta(baseBytes, deltaBytes);
      }
      // Fall back to global
      else if (fossilDeltaGlobal && fossilDeltaGlobal.apply) {
        result = fossilDeltaGlobal.apply(baseBytes, deltaBytes);
      } else {
        throw new Error("No fossil-delta implementation found");
      }

      return bytesToString(result);
    } catch (error) {
      throw new Error(
        `Fossil delta decode failed: ${(error as Error).message} (base=${baseBytes.length}B delta=${deltaBytes.length}B)`,
      );
    }
  }
}

/**
 * Xdelta3 (VCDIFF) decoder
 */
export class Xdelta3Decoder {
  static isAvailable(): boolean {
    return (
      typeof vcdiffDecode !== "undefined" ||
      (typeof vcdiffGlobal !== "undefined" && vcdiffGlobal.decode)
    );
  }

  static apply(base: string | Uint8Array, delta: string | Uint8Array): string {
    if (!this.isAvailable()) {
      throw new Error("Xdelta3/VCDIFF library not loaded");
    }

    const baseBytes = typeof base === "string" ? stringToBytes(base) : base;
    const deltaBytes = typeof delta === "string" ? stringToBytes(delta) : delta;

    try {
      let result: Uint8Array;

      // Try ES6 import first
      if (typeof vcdiffDecode !== "undefined") {
        // @ably/vcdiff-decoder API: decode(delta, source)
        result = vcdiffDecode(deltaBytes, baseBytes);
      }
      // Fall back to global
      else if (vcdiffGlobal && vcdiffGlobal.decode) {
        result = vcdiffGlobal.decode(deltaBytes, baseBytes);
      } else {
        throw new Error("No VCDIFF decoder implementation found");
      }

      return bytesToString(result);
    } catch (error) {
      throw new Error(
        `Xdelta3 decode failed: ${(error as Error).message} (base=${baseBytes.length}B delta=${deltaBytes.length}B)`,
      );
    }
  }
}

export { base64ToBytes, bytesToString, stringToBytes };
