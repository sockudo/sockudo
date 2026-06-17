import 'dart:collection';

/// Tracks recently seen message IDs to filter out duplicates.
///
/// Uses a [LinkedHashSet] for O(1) lookup and insertion-order eviction
/// when the capacity limit is reached.
class MessageDeduplicator {
  MessageDeduplicator({this.capacity = 1000});

  /// Maximum number of message IDs to retain before evicting the oldest.
  final int capacity;

  final LinkedHashSet<String> _seen = LinkedHashSet<String>();

  /// Returns `true` if [messageId] has already been seen.
  bool isDuplicate(String messageId) => _seen.contains(messageId);

  /// Records [messageId] as seen. If the set is at capacity, the oldest
  /// entry is evicted first.
  void track(String messageId) {
    if (_seen.contains(messageId)) return;
    if (_seen.length >= capacity) {
      _seen.remove(_seen.first);
    }
    _seen.add(messageId);
  }

  /// Removes all tracked message IDs.
  void clear() => _seen.clear();

  /// The number of message IDs currently tracked.
  int get length => _seen.length;
}
