import AnyCodable
import Foundation

public enum ApnsChannelStoragePolicy: String, Codable {
  case noStorage
  case mostRecent
}

public enum ApnsLiveActivityEvent: String {
  case start
  case update
  case end
}

public enum ApnsLiveActivityPriority: String {
  case lowPower
  case conservePower
  case immediate
}

public struct ApnsBroadcastChannel: Decodable {
  public let channelID: String
  public let storagePolicy: ApnsChannelStoragePolicy

  enum CodingKeys: String, CodingKey {
    case channelID = "channelId"
    case storagePolicy
  }
}

public struct ApnsBroadcastChannelList: Decodable {
  public let channels: [String]
}

public enum ApnsLiveActivity {
  public static func directRecipient(activityToken: String) -> [String: Any] {
    ["transportType": "apnsLiveActivity", "activityToken": activityToken]
  }

  public static func broadcastRecipient(
    channelID: String,
    storagePolicy: ApnsChannelStoragePolicy = .noStorage
  ) -> [String: Any] {
    [
      "transportType": "apnsLiveActivityBroadcast",
      "channelId": channelID,
      "storagePolicy": storagePolicy.rawValue,
    ]
  }

  public static func payload(
    event: ApnsLiveActivityEvent,
    timestamp: UInt64,
    contentState: [String: Any],
    priority: ApnsLiveActivityPriority = .conservePower
  ) -> [String: Any] {
    [
      "event": event.rawValue,
      "timestamp": timestamp,
      "contentState": contentState,
      "priority": priority.rawValue,
    ]
  }
}

public struct PushCursorFetchOptions {
  public let limit: Int?
  public let cursor: String?

  public init(limit: Int? = nil, cursor: String? = nil) {
    self.limit = limit
    self.cursor = cursor
  }

  var queryItems: [URLQueryItem] {
    var items = [URLQueryItem]()
    if let limit { items.append(URLQueryItem(name: "limit", value: "\(limit)")) }
    if let cursor { items.append(URLQueryItem(name: "cursor", value: cursor)) }
    return items
  }
}

public struct PushSubscriptionFetchOptions {
  public let limit: Int?
  public let cursor: String?
  public let channel: String?
  public let deviceID: String?

  public init(
    limit: Int? = nil,
    cursor: String? = nil,
    channel: String? = nil,
    deviceID: String? = nil
  ) {
    self.limit = limit
    self.cursor = cursor
    self.channel = channel
    self.deviceID = deviceID
  }

  var queryItems: [URLQueryItem] {
    var items = PushCursorFetchOptions(limit: limit, cursor: cursor).queryItems
    if let channel { items.append(URLQueryItem(name: "channel", value: channel)) }
    if let deviceID { items.append(URLQueryItem(name: "deviceId", value: deviceID)) }
    return items
  }
}

public struct PushObjectResponse: Decodable {
  public let values: [String: AnyCodable]

  public init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()
    self.values = try container.decode([String: AnyCodable].self)
  }

  public subscript(key: String) -> AnyCodable? {
    values[key]
  }
}

public struct PushListResponse<Item: Decodable>: Decodable {
  public let items: [Item]
  public let nextCursor: String?
  public let hasMore: Bool

  enum CodingKeys: String, CodingKey {
    case items
    case nextCursor = "next_cursor"
    case hasMore = "has_more"
  }
}

public struct PushItemsResponse<Item: Decodable>: Decodable {
  public let items: [Item]
}

public struct PushPublishAcceptedResponse: Decodable {
  public let publishID: String?
  public let status: String
  public let expectedRecipients: Int?
  public let fanoutRegime: String?
  public let renderedPayloads: [PushObjectResponse]?

  enum CodingKeys: String, CodingKey {
    case publishID = "publish_id"
    case status
    case expectedRecipients = "expected_recipients"
    case fanoutRegime = "fanout_regime"
    case renderedPayloads = "rendered_payloads"
  }
}

public struct PushNoContentResponse {
  public init() {}
}
