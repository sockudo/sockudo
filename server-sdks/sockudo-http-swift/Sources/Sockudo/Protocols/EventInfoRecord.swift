import Foundation

/// An information record for a specific event.
protocol EventInfoRecord {

  /// The channels to which the event will be sent (if publishing to multiple channels).
  var channels: [Channel]? { get }

  /// The channel to which the event will be sent (if publishing to a single channel).
  var channel: Channel? { get }

  /// The event name.
  var name: String { get }

  /// The event payload data.
  var data: Data { get }

  /// A connection to which the event will not be sent.
  var socketId: String? { get }

  /// An optional idempotency key to prevent duplicate event processing.
  var idempotencyKey: String? { get }
}
