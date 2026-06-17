import Foundation

/// An information record for an event on a received Webhook.
protocol WebhookEventRecord {

  /// The event type.
  var eventType: WebhookEventType { get }

  /// The channel relating to the Webhook event.
  var channel: Channel { get }

  /// The event (only set if `eventType` is `clientEvent`).
  var event: Event? { get }

  /// The identifier of the socket that sent the event (only set if `eventType` is `.clientEvent`).
  var socketId: String? { get }

  /// The user identifier associated with the socket that sent the event (only set for presence channels).
  var userId: String? { get }
}
