import Foundation

/// Tracks recently seen message IDs to skip duplicate deliveries.
/// Uses an insertion-ordered set with LRU eviction when capacity is exceeded.
final class MessageDeduplicator: @unchecked Sendable {
  private let capacity: Int
  private var order: [String] = []
  private var seen: Set<String> = []

  init(capacity: Int = 1000) {
    precondition(capacity > 0, "Deduplication capacity must be positive")
    self.capacity = capacity
    order.reserveCapacity(capacity)
  }

  /// Returns `true` if this message ID has already been tracked.
  func isDuplicate(_ messageId: String) -> Bool {
    seen.contains(messageId)
  }

  /// Records a message ID. If the cache is at capacity the oldest entry is evicted.
  func track(_ messageId: String) {
    guard seen.contains(messageId) == false else { return }
    if order.count >= capacity {
      let evicted = order.removeFirst()
      seen.remove(evicted)
    }
    order.append(messageId)
    seen.insert(messageId)
  }

  /// Removes all tracked message IDs.
  func reset() {
    order.removeAll(keepingCapacity: true)
    seen.removeAll(keepingCapacity: true)
  }
}
