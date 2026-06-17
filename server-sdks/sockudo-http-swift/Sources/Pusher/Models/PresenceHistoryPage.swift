import AnyCodable
import Foundation

public struct PresenceHistoryPage: Decodable, Equatable {
  public let items: [PresenceHistoryItem]
  public let direction: String
  public let limit: UInt
  public let hasMore: Bool
  public let nextCursor: String?
  public let bounds: PresenceHistoryBounds
  public let continuity: PresenceHistoryContinuity

  enum CodingKeys: String, CodingKey {
    case items
    case direction
    case limit
    case hasMore = "has_more"
    case nextCursor = "next_cursor"
    case bounds
    case continuity
  }
}

public struct PresenceHistoryItem: Decodable, Equatable {
  public let streamID: String
  public let serial: UInt64
  public let publishedAtMS: Int64
  public let event: String
  public let cause: String
  public let userID: String
  public let connectionID: String?
  public let deadNodeID: String?
  public let payloadSizeBytes: UInt
  public let presenceEvent: AnyCodable

  enum CodingKeys: String, CodingKey {
    case streamID = "stream_id"
    case serial
    case publishedAtMS = "published_at_ms"
    case event
    case cause
    case userID = "user_id"
    case connectionID = "connection_id"
    case deadNodeID = "dead_node_id"
    case payloadSizeBytes = "payload_size_bytes"
    case presenceEvent = "presence_event"
  }
}

public struct PresenceHistoryBounds: Decodable, Equatable {
  public let startSerial: UInt64?
  public let endSerial: UInt64?
  public let startTimeMS: Int64?
  public let endTimeMS: Int64?

  enum CodingKeys: String, CodingKey {
    case startSerial = "start_serial"
    case endSerial = "end_serial"
    case startTimeMS = "start_time_ms"
    case endTimeMS = "end_time_ms"
  }
}

public struct PresenceHistoryContinuity: Decodable, Equatable {
  public let streamID: String?
  public let oldestAvailableSerial: UInt64?
  public let newestAvailableSerial: UInt64?
  public let oldestAvailablePublishedAtMS: Int64?
  public let newestAvailablePublishedAtMS: Int64?
  public let retainedEvents: UInt64
  public let retainedBytes: UInt64
  public let degraded: Bool
  public let complete: Bool
  public let truncatedByRetention: Bool

  enum CodingKeys: String, CodingKey {
    case streamID = "stream_id"
    case oldestAvailableSerial = "oldest_available_serial"
    case newestAvailableSerial = "newest_available_serial"
    case oldestAvailablePublishedAtMS = "oldest_available_published_at_ms"
    case newestAvailablePublishedAtMS = "newest_available_published_at_ms"
    case retainedEvents = "retained_events"
    case retainedBytes = "retained_bytes"
    case degraded
    case complete
    case truncatedByRetention = "truncated_by_retention"
  }
}
