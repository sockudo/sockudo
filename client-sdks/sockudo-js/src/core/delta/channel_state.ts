/**
 * Per-channel delta state tracking with conflation key support
 */

import { CachedMessage, CacheSyncData, ChannelDeltaStats } from "./types";

/**
 * Most (event, conflation key) groups kept per channel. The least recently
 * used group is evicted beyond this, protecting memory on long-lived channels
 * whose event names or conflation keys keep changing.
 */
export const MAX_EVENT_BASE_GROUPS = 4096;

export default class ChannelState {
  channelName: string;
  conflationKey: string | null;
  maxMessagesPerKey: number;
  conflationCaches: Map<string, CachedMessage[]>;

  /**
   * Bases keyed by event name and conflation key. The server caches bases per
   * (event, conflation key) with independent sequences, so two events on one
   * channel can both be at sequence 0; a single per-channel base cannot tell
   * them apart.
   */
  eventBases: Map<string, CachedMessage[]>;

  /** A group was evicted, so an unknown key may be one we dropped. */
  eventBasesEvicted: boolean;

  // Legacy single-base tracking (for non-conflation channels)
  baseMessage: string | null;
  baseSequence: number | null;
  lastSequence: number | null;

  // Statistics
  deltaCount: number;
  fullMessageCount: number;

  constructor(channelName: string) {
    this.channelName = channelName;
    this.conflationKey = null;
    this.maxMessagesPerKey = 30; // Increased to 30 to prevent base eviction
    this.conflationCaches = new Map();
    this.eventBases = new Map();
    this.eventBasesEvicted = false;

    this.baseMessage = null;
    this.baseSequence = null;
    this.lastSequence = null;

    this.deltaCount = 0;
    this.fullMessageCount = 0;
  }

  /**
   * Initialize cache from server sync
   */
  initializeFromCacheSync(data: CacheSyncData): void {
    this.conflationKey = data.conflation_key || null;
    // Force at least 30 to safely handle server's full_message_interval=10 + deltas
    this.maxMessagesPerKey = Math.max(data.max_messages_per_key || 10, 30);
    this.conflationCaches.clear();

    if (data.states) {
      for (const [key, messages] of Object.entries(data.states)) {
        const cache = messages.map((msg) => ({
          content: msg.content,
          sequence: msg.seq,
        }));
        this.conflationCaches.set(key, cache);
      }
    }
  }

  /**
   * Set new base message (legacy - for non-conflation channels)
   */
  setBase(message: string, sequence: number): void {
    this.baseMessage = message;
    this.baseSequence = sequence;
    this.lastSequence = sequence;
  }

  /**
   * Get base message for a conflation key at specific index
   * Note: baseIndex is the sequence number, not array index
   */
  getBaseMessage(conflationKeyValue?: string, baseIndex?: number): string | null {
    if (!this.conflationKey) {
      // Legacy mode: return single base
      return this.baseMessage;
    }

    const key = conflationKeyValue || "";
    const cache = this.conflationCaches.get(key);

    if (!cache || baseIndex === undefined) {
      console.error("[ChannelState] No cache or baseIndex undefined", {
        hasCache: !!cache,
        baseIndex,
        key,
        allCacheKeys: Array.from(this.conflationCaches.keys()),
      });
      return null;
    }

    // baseIndex is actually a sequence number, not an array index
    // Find the message with matching sequence number
    const message = cache.find((msg) => msg.sequence === baseIndex);

    if (!message) {
      console.error("[ChannelState] Could not find message with sequence", {
        searchingFor: baseIndex,
        cacheSize: cache.length,
        cacheSequences: cache.map((m) => m.sequence),
        key: conflationKeyValue,
      });
      return null;
    }

    return message.content;
  }

  /**
   * Add or update message in conflation cache
   */
  updateConflationCache(
    conflationKeyValue: string | undefined,
    message: string,
    sequence: number,
  ): void {
    const key = conflationKeyValue || "";
    let cache = this.conflationCaches.get(key);

    if (!cache) {
      cache = [];
      this.conflationCaches.set(key, cache);
    }

    // Add message to cache
    cache.push({ content: message, sequence });

    // Enforce max size (FIFO eviction)
    while (cache.length > this.maxMessagesPerKey) {
      cache.shift();
    }
  }

  /**
   * Check if we have a valid base
   */
  hasBase(): boolean {
    if (this.conflationKey) {
      return this.conflationCaches.size > 0;
    }
    return this.baseMessage !== null && this.baseSequence !== null;
  }

  /**
   * Validate sequence number
   */
  isValidSequence(sequence: number): boolean {
    if (this.lastSequence === null) {
      return true; // First message
    }
    return sequence > this.lastSequence;
  }

  /**
   * Update sequence after processing a message
   */
  updateSequence(sequence: number): void {
    this.lastSequence = sequence;
  }

  /**
   * Record delta received
   */
  recordDelta(): void {
    this.deltaCount++;
  }

  /**
   * Record full message received
   */
  recordFullMessage(): void {
    this.fullMessageCount++;
  }

  /**
   * Get statistics
   */
  getStats(): ChannelDeltaStats {
    return {
      channelName: this.channelName,
      conflationKey: this.conflationKey,
      conflationGroupCount: this.conflationCaches.size,
      deltaCount: this.deltaCount,
      fullMessageCount: this.fullMessageCount,
      totalMessages: this.deltaCount + this.fullMessageCount,
    };
  }

  /**
   * Record a base for (event, conflation key), matching the server cache.
   */
  storeEventBase(
    event: string,
    conflationKeyValue: string | undefined,
    message: string,
    sequence: number,
  ): void {
    const key = eventBaseKey(event, conflationKeyValue);
    let cache = this.eventBases.get(key);
    if (cache) {
      // Re-insert to mark the group most recently used (Map keeps order).
      this.eventBases.delete(key);
    } else {
      cache = [];
      while (this.eventBases.size >= MAX_EVENT_BASE_GROUPS) {
        const oldest = this.eventBases.keys().next().value;
        if (oldest === undefined) break;
        this.eventBases.delete(oldest);
        this.eventBasesEvicted = true;
      }
    }
    this.eventBases.set(key, cache);
    cache.push({ content: message, sequence });
    while (cache.length > this.maxMessagesPerKey) {
      cache.shift();
    }
  }

  /**
   * Find the base for a delta.
   *
   * Returns `undefined` when nothing is known for this (event, conflation key)
   * so callers can fall back to cache-sync or legacy single-base state, and
   * `null` when the key is known (or may have been evicted) but the requested
   * base is not cached, which must trigger a resync rather than a guess.
   */
  findEventBase(
    event: string,
    conflationKeyValue: string | undefined,
    baseIndex: number | undefined,
  ): string | null | undefined {
    const cache = this.eventBases.get(eventBaseKey(event, conflationKeyValue));
    if (!cache || cache.length === 0) {
      return this.eventBasesEvicted ? null : undefined;
    }
    if (baseIndex === undefined) {
      return cache[cache.length - 1].content;
    }
    for (let index = cache.length - 1; index >= 0; index--) {
      if (cache[index].sequence === baseIndex) {
        return cache[index].content;
      }
    }
    return null;
  }
}

function eventBaseKey(event: string, conflationKeyValue: string | undefined): string {
  return `${event}\u0000${conflationKeyValue ?? ""}`;
}
