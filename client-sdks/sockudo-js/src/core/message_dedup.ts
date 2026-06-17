/**
 * LRU-style message deduplicator.
 *
 * Keeps the last N message IDs in a Set (for O(1) lookup) backed by a queue
 * (Array used as a ring buffer) so the oldest entry can be evicted once
 * capacity is reached.
 */
export default class MessageDeduplicator {
  private seen: Set<string>;
  private queue: string[];
  private head: number;
  private capacity: number;

  constructor(capacity: number = 1000) {
    this.capacity = capacity;
    this.seen = new Set();
    this.queue = new Array(capacity);
    this.head = 0;
  }

  /**
   * Returns true if this message ID has already been seen.
   */
  isDuplicate(messageId: string): boolean {
    return this.seen.has(messageId);
  }

  /**
   * Track a message ID as seen. If the buffer is full, the oldest entry
   * is evicted from both the queue and the Set.
   */
  track(messageId: string): void {
    if (this.seen.has(messageId)) {
      return;
    }

    // Evict the oldest entry if at capacity
    if (this.seen.size >= this.capacity) {
      const evicted = this.queue[this.head];
      if (evicted !== undefined) {
        this.seen.delete(evicted);
      }
    }

    this.queue[this.head] = messageId;
    this.head = (this.head + 1) % this.capacity;
    this.seen.add(messageId);
  }

  /**
   * Reset all tracked state. Useful on reconnection if desired.
   */
  reset(): void {
    this.seen.clear();
    this.queue = new Array(this.capacity);
    this.head = 0;
  }
}
