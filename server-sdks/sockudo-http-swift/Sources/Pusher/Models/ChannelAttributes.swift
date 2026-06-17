import Foundation

/// Channel attributes that can be fetched depending on the `ChannelAttributeFetchOptions`
/// provided to a top-level API method (see: `Pusher`).
public struct ChannelAttributes: SubscriptionCountable, UserCountable, Decodable {

  /// The number of all connections currently subscribed.
  public let subscriptionCount: UInt?

  /// The number of distinct users that are currently subscribed.
  public let userCount: UInt?

  // MARK: - Decodable conformance

  enum CodingKeys: String, CodingKey {
    case subscriptionCount = "subscription_count"
    case userCount = "user_count"
  }
}
