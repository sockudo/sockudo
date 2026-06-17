import Foundation

/// Represents a batch of events to be triggered in a single API request.
struct EventBatch: Encodable {
  /// The array of events to trigger.
  let batch: [Event]
}
