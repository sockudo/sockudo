/**
 * Delta Compression Manager
 *
 * Handles delta compression for Sockudo messages with support for:
 * - Multiple delta algorithms (Fossil, Xdelta3)
 * - Conflation keys for efficient per-entity caching
 * - Bandwidth statistics tracking
 */

import Logger from "../logger";
import { SockudoEvent } from "../connection/protocol/message-types";
import { prefixedEvent } from "../protocol_prefix";
import ChannelState from "./channel_state";
import { FossilDeltaDecoder, Xdelta3Decoder, base64ToBytes } from "./decoders";
import {
  DeltaOptions,
  DeltaStats,
  DeltaMessage,
  DeltaAlgorithm,
  CacheSyncData,
} from "./types";

export default class DeltaCompressionManager {
  private options: Required<DeltaOptions>;
  private enabled: boolean;
  private channelStates: Map<string, ChannelState>;
  private stats: {
    totalMessages: number;
    deltaMessages: number;
    fullMessages: number;
    totalBytesWithoutCompression: number;
    totalBytesWithCompression: number;
    errors: number;
  };
  private availableAlgorithms: DeltaAlgorithm[];
  private sendEventCallback: (event: string, data: any) => boolean;
  private defaultAlgorithm: DeltaAlgorithm = "fossil";

  constructor(
    options: DeltaOptions = {},
    sendEventCallback: (event: string, data: any) => boolean,
  ) {
    this.options = {
      enabled: options.enabled !== false,
      algorithms: options.algorithms || ["fossil", "xdelta3"],
      debug: options.debug || false,
      onStats: options.onStats || null,
      onError: options.onError || null,
    };

    this.enabled = false;
    this.channelStates = new Map();
    this.stats = {
      totalMessages: 0,
      deltaMessages: 0,
      fullMessages: 0,
      totalBytesWithoutCompression: 0,
      totalBytesWithCompression: 0,
      errors: 0,
    };

    this.sendEventCallback = sendEventCallback;
    this.availableAlgorithms = this.detectAvailableAlgorithms();

    if (this.availableAlgorithms.length === 0) {
      Logger.warn(
        "[DeltaCompression] No delta algorithms available. " +
          "Please include fossil-delta or vcdiff-decoder libraries.",
      );
    }
  }

  /**
   * Detect which algorithm libraries are loaded
   */
  private detectAvailableAlgorithms(): DeltaAlgorithm[] {
    const available: DeltaAlgorithm[] = [];

    if (FossilDeltaDecoder.isAvailable()) {
      available.push("fossil");
      this.log("Fossil Delta decoder available");
    }

    if (Xdelta3Decoder.isAvailable()) {
      available.push("xdelta3");
      this.log("Xdelta3 decoder available");
    }

    return available;
  }

  /**
   * Enable delta compression
   */
  enable(): void {
    if (this.enabled || !this.options.enabled) {
      return;
    }

    if (this.availableAlgorithms.length === 0) {
      this.log("No delta algorithms available, cannot enable");
      return;
    }

    // Filter to only algorithms we support AND want to use
    const supportedAlgorithms = this.availableAlgorithms.filter((algo) =>
      this.options.algorithms.includes(algo),
    );

    if (supportedAlgorithms.length === 0) {
      this.log("No mutually supported algorithms");
      return;
    }

    // Send enable request
    this.log("Sending enable request", supportedAlgorithms);
    this.sendEventCallback(prefixedEvent("enable_delta_compression"), {
      algorithms: supportedAlgorithms,
    });
  }

  /**
   * Disable delta compression
   * Note: We intentionally do NOT clear channelStates here.
   * This allows state to be preserved across enable/disable cycles,
   * which is important for reconnection scenarios and user toggling.
   * Use clearChannelState() if you need to explicitly clear state.
   */
  disable(): void {
    this.enabled = false;
  }

  /**
   * Handle delta compression enabled confirmation
   */
  handleEnabled(data: any): void {
    this.enabled = data.enabled || true;
    if (data.algorithm) {
      this.defaultAlgorithm = data.algorithm;
    }
    this.log("Delta compression enabled", data);
  }

  /**
   * Handle cache sync message (conflation keys)
   */
  handleCacheSync(channel: string, data: CacheSyncData): void {
    this.log("Received cache sync", {
      channel,
      conflationKey: data.conflation_key,
      groupCount: Object.keys(data.states || {}).length,
    });

    let channelState = this.channelStates.get(channel);
    if (!channelState) {
      channelState = new ChannelState(channel);
      this.channelStates.set(channel, channelState);
    }

    channelState.initializeFromCacheSync(data);
    this.log("Cache initialized", channelState.getStats());
  }

  /**
   * Handle delta-compressed message
   */
  handleDeltaMessage(
    channel: string,
    deltaData: DeltaMessage,
  ): SockudoEvent | null {
    let deltaBytes: Uint8Array | null = null;

    try {
      const event = deltaData.event;
      const delta = deltaData.delta;
      const sequence = deltaData.seq;
      const algorithm =
        deltaData.algorithm || this.defaultAlgorithm || "fossil";
      const conflationKey = deltaData.conflation_key;
      const baseIndex = deltaData.base_index;

      this.log("Received delta message", {
        channel,
        event,
        sequence,
        algorithm,
        conflationKey,
        baseIndex,
        deltaSize: delta.length,
      });

      // Get channel state
      let channelState = this.channelStates.get(channel);
      if (!channelState) {
        // Silently ignore - this is likely an in-flight delta message for an unsubscribed channel
        // No need to log errors or request resync since the channel is gone
        this.log(`Ignoring delta for unsubscribed channel: ${channel}`);
        return null;
      }

      // Get base message
      let baseMessage: string | null;
      if (channelState.conflationKey) {
        baseMessage = channelState.getBaseMessage(conflationKey, baseIndex);
        if (!baseMessage) {
          this.error(
            `No base message for channel ${channel}, key ${conflationKey}, index ${baseIndex}`,
          );
          if (this.options.debug) {
            this.log("Current conflation cache snapshot", {
              channel,
              conflationKey: channelState.conflationKey,
              cacheSizes: Array.from(
                channelState.conflationCaches.entries(),
              ).map(([key, cache]) => ({ key, size: cache.length })),
            });
          }
          this.requestResync(channel);
          return null;
        }
      } else {
        baseMessage = channelState.baseMessage;
        if (!baseMessage) {
          this.error(`No base message for channel ${channel}`);
          if (this.options.debug) {
            this.log("Channel state missing base", {
              channel,
              lastSequence: channelState.lastSequence,
            });
          }
          this.requestResync(channel);
          return null;
        }
      }

      // Decode base64 delta
      deltaBytes = base64ToBytes(delta);

      // Apply delta based on algorithm
      let reconstructedMessage: string;
      if (algorithm === "fossil") {
        reconstructedMessage = FossilDeltaDecoder.apply(
          baseMessage,
          deltaBytes,
        );
      } else if (algorithm === "xdelta3") {
        reconstructedMessage = Xdelta3Decoder.apply(baseMessage, deltaBytes);
      } else {
        throw Error(`Unknown algorithm: ${algorithm}`);
      }

      // Update conflation cache with reconstructed message
      if (channelState.conflationKey) {
        channelState.updateConflationCache(
          conflationKey,
          reconstructedMessage,
          sequence,
        );
      } else {
        // Update base for non-conflation channels
        channelState.setBase(reconstructedMessage, sequence);
      }

      // Update state
      channelState.updateSequence(sequence);
      channelState.recordDelta();

      // Update stats
      this.stats.totalMessages++;
      this.stats.deltaMessages++;
      this.stats.totalBytesWithCompression += deltaBytes.length;
      this.stats.totalBytesWithoutCompression += reconstructedMessage.length;
      this.updateStats();

      this.log("Delta applied successfully", {
        channel,
        event,
        conflationKey,
        originalSize: reconstructedMessage.length,
        deltaSize: deltaBytes.length,
        compressionRatio:
          ((deltaBytes.length / reconstructedMessage.length) * 100).toFixed(1) +
          "%",
      });

      // Parse and return the reconstructed event
      try {
        const parsedMessage = JSON.parse(reconstructedMessage);
        return {
          event: event,
          channel: channel,
          data: parsedMessage.data || parsedMessage,
        };
      } catch {
        // If not JSON, return as-is
        return {
          event: event,
          channel: channel,
          data: reconstructedMessage,
        };
      }
    } catch (error) {
      this.error("Delta decode failed", {
        channel,
        event: deltaData.event,
        sequence: deltaData.seq,
        algorithm: deltaData.algorithm,
        conflationKey: deltaData.conflation_key,
        baseIndex: deltaData.base_index,
        deltaSize: deltaData.delta?.length,
        decodedDeltaBytes: deltaBytes ? deltaBytes.length : "n/a",
        message: (error as Error).message,
      });
      this.stats.errors++;
      return null;
    }
  }

  /**
   * Handle regular (full) message with delta sequence markers
   */
  handleFullMessage(
    channel: string,
    rawMessage: string,
    sequence?: number,
    conflationKey?: string,
  ): void {
    if (!sequence && sequence !== 0) {
      // Attempt to extract __delta_seq from payload when not provided separately
      try {
        const parsed = JSON.parse(rawMessage);
        const candidate =
          typeof parsed.data === "string"
            ? (JSON.parse(parsed.data).__delta_seq ?? parsed.__delta_seq)
            : (parsed.data?.__delta_seq ?? parsed.__delta_seq);
        if (candidate === 0 || candidate) {
          sequence = candidate;
        } else {
          this.log("handleFullMessage missing sequence, skipping", {
            channel,
            hasSequence: false,
          });
          return;
        }
      } catch {
        this.log("handleFullMessage missing sequence and parse failed", {
          channel,
          hasSequence: false,
        });
        return;
      }
    }

    const messageSize = rawMessage.length;

    let channelState = this.channelStates.get(channel);
    if (!channelState) {
      channelState = new ChannelState(channel);
      this.channelStates.set(channel, channelState);
    }

    // The rawMessage is already stripped of __delta_seq and __conflation_key by sockudo.ts
    // We use it directly as the base - NO re-parsing/re-stringifying to preserve exact bytes
    // (JSON.parse/stringify can change float representations which breaks delta checksums)

    // Use the provided conflationKey parameter (already extracted by sockudo.ts from original message)
    const finalConflationKey = conflationKey;

    // Initialize conflation if we have a conflation key
    if (finalConflationKey !== undefined && !channelState.conflationKey) {
      channelState.conflationKey = "enabled";
    }

    if (channelState.conflationKey && finalConflationKey !== undefined) {
      channelState.updateConflationCache(
        finalConflationKey,
        rawMessage, // Store raw message directly
        sequence,
      );
      this.log("Stored full message (conflation)", {
        channel,
        conflationKey: finalConflationKey,
        sequence,
        size: messageSize,
      });
    } else {
      channelState.setBase(rawMessage, sequence); // Store raw message directly
      this.log("Stored full message", {
        channel,
        sequence,
        size: messageSize,
      });
    }

    channelState.recordFullMessage();

    // Update stats
    this.stats.totalMessages++;
    this.stats.fullMessages++;
    this.stats.totalBytesWithoutCompression += messageSize;
    this.stats.totalBytesWithCompression += messageSize;
    this.updateStats();
  }

  /**
   * Request resync for a channel
   */
  private requestResync(channel: string): void {
    this.log("Requesting resync for channel", channel);
    this.sendEventCallback(prefixedEvent("delta_sync_error"), { channel });
    this.channelStates.delete(channel);
  }

  /**
   * Update and emit stats
   */
  private updateStats(): void {
    if (this.options.onStats) {
      this.options.onStats(this.getStats());
    }
  }

  /**
   * Get current statistics
   */
  getStats(): DeltaStats {
    const bandwidthSaved =
      this.stats.totalBytesWithoutCompression -
      this.stats.totalBytesWithCompression;
    const bandwidthSavedPercent =
      this.stats.totalBytesWithoutCompression > 0
        ? (bandwidthSaved / this.stats.totalBytesWithoutCompression) * 100
        : 0;

    const channelStats = Array.from(this.channelStates.values()).map((state) =>
      state.getStats(),
    );

    return {
      ...this.stats,
      bandwidthSaved,
      bandwidthSavedPercent,
      channelCount: this.channelStates.size,
      channels: channelStats,
    };
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = {
      totalMessages: 0,
      deltaMessages: 0,
      fullMessages: 0,
      totalBytesWithoutCompression: 0,
      totalBytesWithCompression: 0,
      errors: 0,
    };
    this.updateStats();
  }

  /**
   * Clear channel state
   */
  clearChannelState(channel?: string): void {
    if (channel) {
      this.channelStates.delete(channel);
    } else {
      this.channelStates.clear();
    }
  }

  /**
   * Check if delta compression is enabled
   */
  isEnabled(): boolean {
    return this.enabled;
  }

  /**
   * Get available algorithms
   */
  getAvailableAlgorithms(): DeltaAlgorithm[] {
    return this.availableAlgorithms;
  }

  /**
   * Log message (if debug enabled)
   */
  private log(...args: any[]): void {
    if (this.options.debug) {
      Logger.debug("[DeltaCompression]", ...args);
    }
  }

  /**
   * Log error
   */
  private error(...args: any[]): void {
    Logger.error("[DeltaCompression]", ...args);
    if (this.options.onError) {
      this.options.onError(args);
    }
  }
}
