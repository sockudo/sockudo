import Foundation

/// An information record for an occupied `Channel`.
///
/// The channel `attributes` are populated depending on the `ChannelAttributeFetchOptions`
/// provided to a top-level API method (see: `Pusher`).
public struct ChannelInfo: ChannelInfoRecord, Decodable {

  /// The channel is classed as occupied if there is at least one user subscribed to it.
  public let isOccupied: Bool

  /// The fetched channel attributes.
  public let attributes: ChannelAttributes

  // MARK: - Decodable conformance

  enum CodingKeys: String, CodingKey {
    case isOccupied = "occupied"
    case subscriptionCount = "subscription_count"
    case userCount = "user_count"
  }

  // MARK: - Lifecycle

  init(isOccupied: Bool, attributes: ChannelAttributes) {
    self.isOccupied = isOccupied
    self.attributes = attributes
  }

  // MARK: - Custom Decodable initializer

  public init(from decoder: Decoder) throws {

    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.isOccupied = try container.decode(Bool.self, forKey: .isOccupied)
    let subscriptionCount = try container.decodeIfPresent(UInt.self, forKey: .subscriptionCount)
    let userCount = try container.decodeIfPresent(UInt.self, forKey: .userCount)
    self.attributes = ChannelAttributes(subscriptionCount: subscriptionCount, userCount: userCount)
  }
}
