import Foundation

/// The Sockudo Channels Webhook event types.
public enum WebhookEventType: String, Codable {

  /// A `Channel` has become occupied. (i.e. there is at least one subscriber).
  case channelOccupied = "channel_occupied"

  /// A `Channel` has become vacated. (i.e. there are no subscribers).
  case channelVacated = "channel_vacated"

  /// A `User` has subscribed to a presence channel.
  case memberAdded = "member_added"

  /// A `User` has unsubscribed from a presence channel.
  case memberRemoved = "member_removed"

  /// A client event has been triggered on a private or presence channel.
  case clientEvent = "client_event"
}
