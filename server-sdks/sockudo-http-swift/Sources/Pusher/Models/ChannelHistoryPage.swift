import AnyCodable
import Foundation

public struct ChannelHistoryPage: Decodable, Equatable {
  public let items: [ChannelHistoryItem]
  public let direction: String
  public let limit: UInt
  public let hasMore: Bool
  public let nextCursor: String?
  public let bounds: ChannelHistoryBounds
  public let continuity: ChannelHistoryContinuity

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

public struct ChannelHistoryItem: Decodable, Equatable {
  public let streamID: String
  public let serial: UInt64
  public let publishedAtMS: Int64
  public let messageID: String?
  public let eventName: String?
  public let operationKind: String
  public let payloadSizeBytes: UInt
  public let message: AnyCodable

  enum CodingKeys: String, CodingKey {
    case streamID = "stream_id"
    case serial
    case publishedAtMS = "published_at_ms"
    case messageID = "message_id"
    case eventName = "event_name"
    case operationKind = "operation_kind"
    case payloadSizeBytes = "payload_size_bytes"
    case message
  }
}

public struct ChannelHistoryBounds: Decodable, Equatable {
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

public struct ChannelHistoryContinuity: Decodable, Equatable {
  public let streamID: String?
  public let oldestAvailableSerial: UInt64?
  public let newestAvailableSerial: UInt64?
  public let oldestAvailablePublishedAtMS: Int64?
  public let newestAvailablePublishedAtMS: Int64?
  public let retainedMessages: UInt64
  public let retainedBytes: UInt64
  public let complete: Bool
  public let truncatedByRetention: Bool

  enum CodingKeys: String, CodingKey {
    case streamID = "stream_id"
    case oldestAvailableSerial = "oldest_available_serial"
    case newestAvailableSerial = "newest_available_serial"
    case oldestAvailablePublishedAtMS = "oldest_available_published_at_ms"
    case newestAvailablePublishedAtMS = "newest_available_published_at_ms"
    case retainedMessages = "retained_messages"
    case retainedBytes = "retained_bytes"
    case complete
    case truncatedByRetention = "truncated_by_retention"
  }
}
