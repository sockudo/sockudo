import AnyCodable
import Foundation

public struct MessageVersionMetadata: Decodable, Equatable {
  public let serial: String
  public let timestampMS: Int64
  public let clientID: String?
  public let description: String?
  public let metadata: AnyCodable?

  enum CodingKeys: String, CodingKey {
    case serial
    case timestampMS = "timestamp_ms"
    case clientID = "client_id"
    case description
    case metadata
  }
}

public struct VersionedMessage: Decodable, Equatable {
  public let event: String?
  public let channel: String?
  public let data: AnyCodable?
  public let name: String?
  public let serial: UInt64?
  public let action: String
  public let messageSerial: String
  public let historySerial: UInt64?
  public let deliverySerial: UInt64?
  public let version: MessageVersionMetadata?
  public let extras: AnyCodable?

  enum CodingKeys: String, CodingKey {
    case event
    case channel
    case data
    case name
    case serial
    case action
    case messageSerial = "message_serial"
    case historySerial = "history_serial"
    case deliverySerial = "delivery_serial"
    case version
    case extras
  }
}

public struct GetMessageResponse: Decodable, Equatable {
  public let channel: String
  public let item: VersionedMessage
}

public struct MessageVersionsPage: Decodable, Equatable {
  public let channel: String
  public let direction: String
  public let limit: UInt
  public let hasMore: Bool
  public let nextCursor: String?
  public let items: [VersionedMessage]

  enum CodingKeys: String, CodingKey {
    case channel
    case direction
    case limit
    case hasMore = "has_more"
    case nextCursor = "next_cursor"
    case items
  }
}

public struct MutationResponse: Decodable, Equatable {
  public let channel: String
  public let messageSerial: String
  public let action: String
  public let accepted: Bool
  public let versionSerial: String?
  public let status: String

  enum CodingKeys: String, CodingKey {
    case channel
    case messageSerial = "message_serial"
    case action
    case accepted
    case versionSerial = "version_serial"
    case status
  }
}

public struct PublishAnnotationResponse: Decodable, Equatable {
  public let annotationSerial: String
}

public struct DeleteAnnotationResponse: Decodable, Equatable {
  public let annotationSerial: String
  public let deletedAnnotationSerial: String
}

public struct AnnotationEvent: Decodable, Equatable {
  public let action: String
  public let id: String?
  public let serial: String
  public let messageSerial: String
  public let type: String
  public let name: String?
  public let clientID: String?
  public let count: UInt64?
  public let data: AnyCodable?
  public let encoding: String?
  public let timestamp: Int64?

  enum CodingKeys: String, CodingKey {
    case action
    case id
    case serial
    case messageSerial
    case type
    case name
    case clientID = "clientId"
    case count
    case data
    case encoding
    case timestamp
  }
}

public struct AnnotationEventsPage: Decodable, Equatable {
  public let channel: String
  public let messageSerial: String
  public let limit: UInt
  public let hasMore: Bool
  public let nextCursor: String?
  public let items: [AnnotationEvent]
}
