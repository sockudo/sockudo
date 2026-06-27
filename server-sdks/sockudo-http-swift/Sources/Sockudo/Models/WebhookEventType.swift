import Foundation

/// The Sockudo Channels Webhook event types.
public enum WebhookEventType: Codable, Equatable, RawRepresentable {

  public typealias RawValue = String

  /// A `Channel` has become occupied. (i.e. there is at least one subscriber).
  case channelOccupied

  /// A `Channel` has become vacated. (i.e. there are no subscribers).
  case channelVacated

  /// A `User` has subscribed to a presence channel.
  case memberAdded

  /// A `User` has unsubscribed from a presence channel.
  case memberRemoved

  /// A client event has been triggered on a private or presence channel.
  case clientEvent

  /// A future Sockudo webhook event type.
  case unknown(String)

  public init(rawValue: String) {
    switch rawValue {
    case "channel_occupied":
      self = .channelOccupied
    case "channel_vacated":
      self = .channelVacated
    case "member_added":
      self = .memberAdded
    case "member_removed":
      self = .memberRemoved
    case "client_event":
      self = .clientEvent
    default:
      self = .unknown(rawValue)
    }
  }

  public var rawValue: String {
    switch self {
    case .channelOccupied:
      return "channel_occupied"
    case .channelVacated:
      return "channel_vacated"
    case .memberAdded:
      return "member_added"
    case .memberRemoved:
      return "member_removed"
    case .clientEvent:
      return "client_event"
    case .unknown(let value):
      return value
    }
  }

  public init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()
    self.init(rawValue: try container.decode(String.self))
  }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.singleValueContainer()
    try container.encode(rawValue)
  }
}
