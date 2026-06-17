import Foundation

/// A type that maintains a count of all subscriptions.
protocol SubscriptionCountable {

  /// The number of all connections currently subscribed.
  var subscriptionCount: UInt? { get }
}
