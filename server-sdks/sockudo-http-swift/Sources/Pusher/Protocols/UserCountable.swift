import Foundation

/// A type that maintains a count of distinct subscribed users.
protocol UserCountable {

  /// The number of distinct users that are currently subscribed.
  var userCount: UInt? { get }
}
