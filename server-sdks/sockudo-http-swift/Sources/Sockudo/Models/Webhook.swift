import Foundation

/// A representation of a Webhook that contains an array of `events` related to changes
/// in the internal state of a Sockudo Channels application, which were received as a POST request
/// to a user-specified Webhook URL.
///
/// The contained events have been verified as geniune Webhooks that were received directly from Sockudo.
public struct Webhook: Codable {

  /// The `Date` that Sockudo servers originally created the Webhook request.
  public let createdAt: Date

  /// The events contained within the Webhook request.
  public let events: [WebhookEvent]

  enum CodingKeys: String, CodingKey {
    case createdAt = "time_ms"
    case events
  }

  // MARK: - Lifecycle (used in Tests)

  init(createdAt: Date, events: [WebhookEvent]) {
    self.createdAt = createdAt
    self.events = events
  }

  // MARK: - Custom Encodable conformance (used in Tests)

  public func encode(to encoder: Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)

    try container.encode(createdAt.timeIntervalSince1970 * 1000, forKey: .createdAt)
    try container.encode(events, forKey: .events)
  }

  // MARK: - Custom Decodable initializer

  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)

    // Convert `time_ms` in JSON to a `Date`
    let timestamp = try container.decode(Int.self, forKey: .createdAt) / 1000
    self.createdAt = Date(timeIntervalSince1970: TimeInterval(timestamp))

    self.events = try container.decode([WebhookEvent].self, forKey: .events)
  }
}
