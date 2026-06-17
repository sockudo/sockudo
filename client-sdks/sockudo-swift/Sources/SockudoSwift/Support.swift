import Foundation

public struct EventMetadata: Sendable, Equatable {
  public let userID: String?

  public init(userID: String? = nil) {
    self.userID = userID
  }
}

public struct PresenceHistoryParams: Sendable, Equatable {
  public let direction: String?
  public let limit: Int?
  public let cursor: String?
  public let startSerial: Int64?
  public let endSerial: Int64?
  public let startTimeMS: Int64?
  public let endTimeMS: Int64?
  public let start: Int64?
  public let end: Int64?

  public init(
    direction: String? = nil,
    limit: Int? = nil,
    cursor: String? = nil,
    startSerial: Int64? = nil,
    endSerial: Int64? = nil,
    startTimeMS: Int64? = nil,
    endTimeMS: Int64? = nil,
    start: Int64? = nil,
    end: Int64? = nil
  ) {
    self.direction = direction
    self.limit = limit
    self.cursor = cursor
    self.startSerial = startSerial
    self.endSerial = endSerial
    self.startTimeMS = startTimeMS
    self.endTimeMS = endTimeMS
    self.start = start
    self.end = end
  }

  var payload: [String: Any] {
    var data: [String: Any] = [:]
    if let direction { data["direction"] = direction }
    if let limit { data["limit"] = limit }
    if let cursor { data["cursor"] = cursor }
    if let startSerial { data["start_serial"] = startSerial }
    if let endSerial { data["end_serial"] = endSerial }
    if let startTimeMS {
      data["start_time_ms"] = startTimeMS
    } else if let start {
      data["start_time_ms"] = start
    }
    if let endTimeMS {
      data["end_time_ms"] = endTimeMS
    } else if let end {
      data["end_time_ms"] = end
    }
    return data
  }
}

public struct PresenceSnapshotParams: Sendable, Equatable {
  public let atTimeMS: Int64?
  public let at: Int64?
  public let atSerial: Int64?

  public init(
    atTimeMS: Int64? = nil,
    at: Int64? = nil,
    atSerial: Int64? = nil
  ) {
    self.atTimeMS = atTimeMS
    self.at = at
    self.atSerial = atSerial
  }

  var payload: [String: Any] {
    var data: [String: Any] = [:]
    if let atTimeMS {
      data["at_time_ms"] = atTimeMS
    } else if let at {
      data["at_time_ms"] = at
    }
    if let atSerial { data["at_serial"] = atSerial }
    return data
  }
}

public struct PresenceHistoryItem: @unchecked Sendable, Equatable {
  public let streamID: String
  public let serial: Int64
  public let publishedAtMS: Int64
  public let event: String
  public let cause: String
  public let userID: String
  public let connectionID: String?
  public let deadNodeID: String?
  public let payloadSizeBytes: Int
  public let presenceEvent: [String: AnyHashable]
}

public struct PresenceHistoryBounds: Sendable, Equatable {
  public let startSerial: Int64?
  public let endSerial: Int64?
  public let startTimeMS: Int64?
  public let endTimeMS: Int64?
}

public struct PresenceHistoryContinuity: Sendable, Equatable {
  public let streamID: String?
  public let oldestAvailableSerial: Int64?
  public let newestAvailableSerial: Int64?
  public let oldestAvailablePublishedAtMS: Int64?
  public let newestAvailablePublishedAtMS: Int64?
  public let retainedEvents: Int64
  public let retainedBytes: Int64
  public let degraded: Bool
  public let complete: Bool
  public let truncatedByRetention: Bool
}

public struct PresenceSnapshotMember: Sendable, Equatable {
  public let userID: String
  public let lastEvent: String
  public let lastEventSerial: Int64
  public let lastEventAtMS: Int64
}

public struct PresenceSnapshot: Sendable, Equatable {
  public let channel: String
  public let members: [PresenceSnapshotMember]
  public let memberCount: Int
  public let eventsReplayed: Int64
  public let snapshotSerial: Int64?
  public let snapshotTimeMS: Int64?
  public let continuity: PresenceHistoryContinuity
}

public struct ChannelHistoryParams: Sendable, Equatable {
  public let direction: String?
  public let limit: Int?
  public let cursor: String?
  public let startSerial: Int64?
  public let endSerial: Int64?
  public let startTimeMS: Int64?
  public let endTimeMS: Int64?

  public init(
    direction: String? = nil,
    limit: Int? = nil,
    cursor: String? = nil,
    startSerial: Int64? = nil,
    endSerial: Int64? = nil,
    startTimeMS: Int64? = nil,
    endTimeMS: Int64? = nil
  ) {
    self.direction = direction
    self.limit = limit
    self.cursor = cursor
    self.startSerial = startSerial
    self.endSerial = endSerial
    self.startTimeMS = startTimeMS
    self.endTimeMS = endTimeMS
  }

  var payload: [String: Any] {
    var data: [String: Any] = [:]
    if let direction { data["direction"] = direction }
    if let limit { data["limit"] = limit }
    if let cursor { data["cursor"] = cursor }
    if let startSerial { data["start_serial"] = startSerial }
    if let endSerial { data["end_serial"] = endSerial }
    if let startTimeMS { data["start_time_ms"] = startTimeMS }
    if let endTimeMS { data["end_time_ms"] = endTimeMS }
    return data
  }
}

public final class ChannelHistoryPage: @unchecked Sendable {
  public let items: [[String: Any]]
  public let direction: String
  public let limit: Int
  public let hasMore: Bool
  public let nextCursor: String?
  public let bounds: [String: Any]
  public let continuity: [String: Any]
  private let fetchNext: (@Sendable (String, @escaping @Sendable (Result<ChannelHistoryPage, Error>) -> Void) -> Void)?

  init(
    items: [[String: Any]],
    direction: String,
    limit: Int,
    hasMore: Bool,
    nextCursor: String?,
    bounds: [String: Any],
    continuity: [String: Any],
    fetchNext: (@Sendable (String, @escaping @Sendable (Result<ChannelHistoryPage, Error>) -> Void) -> Void)?
  ) {
    self.items = items
    self.direction = direction
    self.limit = limit
    self.hasMore = hasMore
    self.nextCursor = nextCursor
    self.bounds = bounds
    self.continuity = continuity
    self.fetchNext = fetchNext
  }

  public func hasNext() -> Bool {
    hasMore && nextCursor != nil
  }

  public func next(
    completion: @escaping @Sendable (Result<ChannelHistoryPage, Error>) -> Void
  ) {
    guard hasNext(), let nextCursor, let fetchNext else {
      completion(.failure(SockudoError.invalidOptions("No more pages available")))
      return
    }
    fetchNext(nextCursor, completion)
  }
}

public struct MessageVersionsParams: Sendable, Equatable {
  public let direction: String?
  public let limit: Int?
  public let cursor: String?

  public init(
    direction: String? = nil,
    limit: Int? = nil,
    cursor: String? = nil
  ) {
    self.direction = direction
    self.limit = limit
    self.cursor = cursor
  }

  var payload: [String: Any] {
    var data: [String: Any] = [:]
    if let direction { data["direction"] = direction }
    if let limit { data["limit"] = limit }
    if let cursor { data["cursor"] = cursor }
    return data
  }
}

public final class MessageVersionsPage: @unchecked Sendable {
  public let channel: String
  public let items: [[String: Any]]
  public let direction: String
  public let limit: Int
  public let hasMore: Bool
  public let nextCursor: String?
  private let fetchNext: (@Sendable (String, @escaping @Sendable (Result<MessageVersionsPage, Error>) -> Void) -> Void)?

  init(
    channel: String,
    items: [[String: Any]],
    direction: String,
    limit: Int,
    hasMore: Bool,
    nextCursor: String?,
    fetchNext: (@Sendable (String, @escaping @Sendable (Result<MessageVersionsPage, Error>) -> Void) -> Void)?
  ) {
    self.channel = channel
    self.items = items
    self.direction = direction
    self.limit = limit
    self.hasMore = hasMore
    self.nextCursor = nextCursor
    self.fetchNext = fetchNext
  }

  public func hasNext() -> Bool {
    hasMore && nextCursor != nil
  }

  public func next(
    completion: @escaping @Sendable (Result<MessageVersionsPage, Error>) -> Void
  ) {
    guard hasNext(), let nextCursor, let fetchNext else {
      completion(.failure(SockudoError.invalidOptions("No more pages available")))
      return
    }
    fetchNext(nextCursor, completion)
  }
}

public final class PresenceHistoryPage: @unchecked Sendable {
  public let items: [PresenceHistoryItem]
  public let direction: String
  public let limit: Int
  public let hasMore: Bool
  public let nextCursor: String?
  public let bounds: PresenceHistoryBounds
  public let continuity: PresenceHistoryContinuity
  private let fetchNext: (@Sendable (String, @escaping @Sendable (Result<PresenceHistoryPage, Error>) -> Void) -> Void)?

  init(
    items: [PresenceHistoryItem],
    direction: String,
    limit: Int,
    hasMore: Bool,
    nextCursor: String?,
    bounds: PresenceHistoryBounds,
    continuity: PresenceHistoryContinuity,
    fetchNext: (@Sendable (String, @escaping @Sendable (Result<PresenceHistoryPage, Error>) -> Void) -> Void)?
  ) {
    self.items = items
    self.direction = direction
    self.limit = limit
    self.hasMore = hasMore
    self.nextCursor = nextCursor
    self.bounds = bounds
    self.continuity = continuity
    self.fetchNext = fetchNext
  }

  public func hasNext() -> Bool {
    hasMore && nextCursor != nil
  }

  public func next(
    completion: @escaping @Sendable (Result<PresenceHistoryPage, Error>) -> Void
  ) {
    guard hasNext(), let nextCursor, let fetchNext else {
      completion(.failure(SockudoError.invalidOptions("No more pages available")))
      return
    }
    fetchNext(nextCursor, completion)
  }
}

public struct EventBindingToken: Hashable, Sendable {
  fileprivate let id: UUID

  fileprivate init(id: UUID = UUID()) {
    self.id = id
  }
}

public enum SockudoError: Error, LocalizedError, Equatable {
  case invalidAppKey
  case invalidOptions(String)
  case unsupportedFeature(String)
  case badEventName(String)
  case badChannelName(String)
  case messageParseError(String)
  case authFailure(statusCode: Int?, message: String)
  case invalidHandshake
  case decryptionFailure(String)
  case deltaFailure(String)
  case invalidURL(String)
  case connectionUnavailable

  public var errorDescription: String? {
    switch self {
    case .invalidAppKey:
      return "You must pass your app key when you instantiate SockudoClient."
    case .invalidOptions(let message):
      return message
    case .unsupportedFeature(let message):
      return message
    case .badEventName(let message):
      return message
    case .badChannelName(let message):
      return message
    case .messageParseError(let message):
      return message
    case .authFailure(_, let message):
      return message
    case .invalidHandshake:
      return "Invalid handshake"
    case .decryptionFailure(let message):
      return message
    case .deltaFailure(let message):
      return message
    case .invalidURL(let message):
      return message
    case .connectionUnavailable:
      return "Connection unavailable"
    }
  }
}

enum JSON {
  static func decode(_ data: Data) throws -> Any {
    try JSONSerialization.jsonObject(with: data, options: [])
  }

  static func decodeString(_ string: String) throws -> Any {
    guard let data = string.data(using: .utf8) else {
      throw SockudoError.messageParseError("Unable to decode UTF-8 payload")
    }
    return try decode(data)
  }

  static func encodeString(_ value: Any) throws -> String {
    let data = try encodeData(value)
    guard let string = String(data: data, encoding: .utf8) else {
      throw SockudoError.messageParseError("Unable to encode UTF-8 payload")
    }
    return string
  }

  static func encodeData(_ value: Any) throws -> Data {
    guard JSONSerialization.isValidJSONObject(value) else {
      throw SockudoError.messageParseError("Payload is not JSON-serializable")
    }
    return try JSONSerialization.data(withJSONObject: value, options: [])
  }
}

enum QueryString {
  static func encode(_ params: [String: AuthValue]) -> Data {
    let body =
      params
      .sorted { $0.key < $1.key }
      .map { "\(percentEncode($0.key))=\(percentEncode($0.value.stringValue))" }
      .joined(separator: "&")
    return Data(body.utf8)
  }

  private static func percentEncode(_ value: String) -> String {
    var allowed = CharacterSet.urlQueryAllowed
    allowed.remove(charactersIn: "&=+")
    return value.addingPercentEncoding(withAllowedCharacters: allowed) ?? value
  }
}

final class EventDispatcher {
  typealias EventCallback = (Any?, EventMetadata?) -> Void
  typealias GlobalCallback = (String, Any?) -> Void

  private var callbacks: [String: [EventBindingToken: EventCallback]] = [:]
  private var globalCallbacks: [EventBindingToken: GlobalCallback] = [:]
  private let failThrough: ((String, Any?) -> Void)?

  init(failThrough: ((String, Any?) -> Void)? = nil) {
    self.failThrough = failThrough
  }

  @discardableResult
  func bind(_ eventName: String, callback: @escaping EventCallback) -> EventBindingToken {
    let token = EventBindingToken()
    callbacks[eventName, default: [:]][token] = callback
    return token
  }

  @discardableResult
  func bindGlobal(_ callback: @escaping GlobalCallback) -> EventBindingToken {
    let token = EventBindingToken()
    globalCallbacks[token] = callback
    return token
  }

  func unbind(eventName: String? = nil, token: EventBindingToken? = nil) {
    if let eventName {
      guard let token else {
        callbacks[eventName] = nil
        return
      }
      callbacks[eventName]?[token] = nil
      if callbacks[eventName]?.isEmpty == true {
        callbacks[eventName] = nil
      }
      return
    }

    if let token {
      for key in callbacks.keys {
        callbacks[key]?[token] = nil
        if callbacks[key]?.isEmpty == true {
          callbacks[key] = nil
        }
      }
      globalCallbacks[token] = nil
      return
    }

    callbacks.removeAll()
    globalCallbacks.removeAll()
  }

  func emit(_ eventName: String, data: Any?, metadata: EventMetadata? = nil) {
    for callback in globalCallbacks.values {
      callback(eventName, data)
    }

    if let eventCallbacks = callbacks[eventName], eventCallbacks.isEmpty == false {
      for callback in eventCallbacks.values {
        callback(data, metadata)
      }
    } else {
      failThrough?(eventName, data)
    }
  }
}

public enum AuthValue: Sendable, Equatable {
  case string(String)
  case int(Int)
  case double(Double)
  case bool(Bool)

  var stringValue: String {
    switch self {
    case .string(let value):
      return value
    case .int(let value):
      return String(value)
    case .double(let value):
      return String(value)
    case .bool(let value):
      return value ? "true" : "false"
    }
  }
}

public enum Transport: String, Sendable, CaseIterable, Codable {
  case ws
  case wss
}

public enum SockudoWireFormat: String, Sendable, Codable, Equatable {
  case json
  case messagepack
  case protobuf

  var isBinary: Bool {
    self != .json
  }

  var queryValue: String {
    rawValue
  }
}

public enum ConnectionState: String, Sendable, Equatable {
  case initialized
  case connecting
  case connected
  case disconnected
  case unavailable
  case failed
}

public enum DeltaAlgorithm: String, Sendable, Codable, CaseIterable {
  case fossil
  case xdelta3
}

public struct ChannelDeltaSettings: Sendable, Codable, Equatable {
  public var enabled: Bool?
  public var algorithm: DeltaAlgorithm?

  public init(enabled: Bool? = nil, algorithm: DeltaAlgorithm? = nil) {
    self.enabled = enabled
    self.algorithm = algorithm
  }

  func subscriptionValue() -> Any {
    if enabled == nil, let algorithm {
      return algorithm.rawValue
    }
    if enabled == false, algorithm == nil {
      return false
    }
    if enabled == true, algorithm == nil {
      return true
    }
    var result: [String: Any] = [:]
    if let enabled {
      result["enabled"] = enabled
    }
    if let algorithm {
      result["algorithm"] = algorithm.rawValue
    }
    return result
  }
}

public enum ExtraValue: Sendable, Codable, Equatable {
  case string(String)
  case int(Int)
  case double(Double)
  case bool(Bool)

  public init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()
    if let value = try? container.decode(Bool.self) {
      self = .bool(value)
    } else if let value = try? container.decode(Int.self) {
      self = .int(value)
    } else if let value = try? container.decode(Double.self) {
      self = .double(value)
    } else {
      self = .string(try container.decode(String.self))
    }
  }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.singleValueContainer()
    switch self {
    case .string(let value):
      try container.encode(value)
    case .int(let value):
      try container.encode(value)
    case .double(let value):
      try container.encode(value)
    case .bool(let value):
      try container.encode(value)
    }
  }

  var rawValue: Any {
    switch self {
    case .string(let value): value
    case .int(let value): value
    case .double(let value): value
    case .bool(let value): value
    }
  }
}

public struct MessageExtras: Sendable, Codable, Equatable {
  public var headers: [String: ExtraValue]?
  public var ephemeral: Bool?
  public var idempotencyKey: String?
  public var echo: Bool?

  public init(
    headers: [String: ExtraValue]? = nil, ephemeral: Bool? = nil, idempotencyKey: String? = nil,
    echo: Bool? = nil
  ) {
    self.headers = headers
    self.ephemeral = ephemeral
    self.idempotencyKey = idempotencyKey
    self.echo = echo
  }
}

public struct SubscriptionOptions: Sendable, Codable, Equatable {
  public var filter: FilterNode?
  public var delta: ChannelDeltaSettings?
  public var events: [String]?
  public var rewind: SubscriptionRewind?
  public var annotationSubscribe: Bool

  public init(
    filter: FilterNode? = nil,
    delta: ChannelDeltaSettings? = nil,
    events: [String]? = nil,
    rewind: SubscriptionRewind? = nil,
    annotationSubscribe: Bool = false
  ) {
    self.filter = filter
    self.delta = delta
    self.events = events
    self.rewind = rewind
    self.annotationSubscribe = annotationSubscribe
  }
}

public struct PublishAnnotationRequest: Sendable, Equatable {
  public let type: String
  public let name: String?
  public let count: Int?
  public let data: [String: ExtraValue]?
  public let clientID: String?
  public let extras: [String: ExtraValue]?
  public let idempotencyKey: String?

  public init(
    type: String,
    name: String? = nil,
    count: Int? = nil,
    data: [String: ExtraValue]? = nil,
    clientID: String? = nil,
    extras: [String: ExtraValue]? = nil,
    idempotencyKey: String? = nil
  ) {
    self.type = type
    self.name = name
    self.count = count
    self.data = data
    self.clientID = clientID
    self.extras = extras
    self.idempotencyKey = idempotencyKey
  }

  var payload: [String: Any] {
    var data: [String: Any] = ["type": type]
    if let name { data["name"] = name }
    if let count { data["count"] = count }
    if let annotationData = self.data { data["data"] = annotationData.mapValues(\.rawValue) }
    if let clientID { data["clientId"] = clientID }
    if let extras { data["extras"] = extras.mapValues(\.rawValue) }
    if let idempotencyKey { data["idempotencyKey"] = idempotencyKey }
    return data
  }
}

public struct PublishAnnotationResponse: @unchecked Sendable {
  public let annotation: [String: Any]
  public let summary: [String: Any]?

  public init(annotation: [String: Any], summary: [String: Any]? = nil) {
    self.annotation = annotation
    self.summary = summary
  }
}

public struct DeleteAnnotationResponse: @unchecked Sendable {
  public let deleted: Bool
  public let annotationSerial: String
  public let summary: [String: Any]?

  public init(deleted: Bool, annotationSerial: String, summary: [String: Any]? = nil) {
    self.deleted = deleted
    self.annotationSerial = annotationSerial
    self.summary = summary
  }
}

public struct AnnotationEventsParams: Sendable, Equatable {
  public let direction: String?
  public let limit: Int?
  public let cursor: String?
  public let type: String?
  public let fromSerial: String?

  public init(
    direction: String? = nil,
    limit: Int? = nil,
    cursor: String? = nil,
    type: String? = nil,
    fromSerial: String? = nil
  ) {
    self.direction = direction
    self.limit = limit
    self.cursor = cursor
    self.type = type
    self.fromSerial = fromSerial
  }

  var payload: [String: Any] {
    var data: [String: Any] = [:]
    if let direction { data["direction"] = direction }
    if let limit { data["limit"] = limit }
    if let cursor { data["cursor"] = cursor }
    if let type { data["type"] = type }
    if let fromSerial { data["from_serial"] = fromSerial }
    return data
  }
}

public final class AnnotationEventsPage: @unchecked Sendable {
  public let items: [[String: Any]]
  public let direction: String
  public let limit: Int
  public let hasMore: Bool
  public let nextCursor: String?
  private let fetchNext: (@Sendable (String, @escaping @Sendable (Result<AnnotationEventsPage, Error>) -> Void) -> Void)?

  init(
    items: [[String: Any]],
    direction: String,
    limit: Int,
    hasMore: Bool,
    nextCursor: String?,
    fetchNext: (@Sendable (String, @escaping @Sendable (Result<AnnotationEventsPage, Error>) -> Void) -> Void)?
  ) {
    self.items = items
    self.direction = direction
    self.limit = limit
    self.hasMore = hasMore
    self.nextCursor = nextCursor
    self.fetchNext = fetchNext
  }

  public func hasNext() -> Bool {
    hasMore && nextCursor != nil
  }

  public func next(
    completion: @escaping @Sendable (Result<AnnotationEventsPage, Error>) -> Void
  ) {
    guard hasNext(), let nextCursor, let fetchNext else {
      completion(.failure(SockudoError.invalidOptions("No more pages available")))
      return
    }
    fetchNext(nextCursor, completion)
  }
}

public enum SubscriptionRewind: Sendable, Codable, Equatable {
  case count(Int)
  case seconds(Int)

  func subscriptionValue() -> Any {
    switch self {
    case .count(let count):
      return count
    case .seconds(let seconds):
      return ["seconds": seconds]
    }
  }
}

public struct RecoveryPosition: Sendable, Codable, Equatable {
  public let streamID: String?
  public let serial: Int
  public let lastMessageID: String?

  public init(streamID: String? = nil, serial: Int, lastMessageID: String? = nil) {
    self.streamID = streamID
    self.serial = serial
    self.lastMessageID = lastMessageID
  }
}

public struct ResumeRecoveredChannel: Sendable, Codable, Equatable {
  public let channel: String
  public let source: String
  public let replayed: Int
}

public struct ResumeFailedChannel: Sendable, Codable, Equatable {
  public let channel: String
  public let code: String
  public let reason: String
  public let expectedStreamID: String?
  public let currentStreamID: String?
  public let oldestAvailableSerial: Int?
  public let newestAvailableSerial: Int?
}

public struct ResumeSuccessData: Sendable, Codable, Equatable {
  public let recovered: [ResumeRecoveredChannel]
  public let failed: [ResumeFailedChannel]
}

public struct RewindCompleteData: Sendable, Codable, Equatable {
  public let historicalCount: Int
  public let liveCount: Int
  public let complete: Bool
  public let truncatedByRetention: Bool
  public let truncatedByLimit: Bool
}

public struct DeltaOptions {
  public var enabled: Bool?
  public var algorithms: [DeltaAlgorithm]
  public var debug: Bool
  public var onStats: (@Sendable (DeltaStats) -> Void)?
  public var onError: (@Sendable (Error) -> Void)?

  public init(
    enabled: Bool? = nil,
    algorithms: [DeltaAlgorithm] = [.fossil, .xdelta3],
    debug: Bool = false,
    onStats: (@Sendable (DeltaStats) -> Void)? = nil,
    onError: (@Sendable (Error) -> Void)? = nil
  ) {
    self.enabled = enabled
    self.algorithms = algorithms
    self.debug = debug
    self.onStats = onStats
    self.onError = onError
  }
}

public struct ChannelDeltaStats: Sendable, Equatable {
  public let channelName: String
  public let conflationKey: String?
  public let conflationGroupCount: Int
  public let deltaCount: Int
  public let fullMessageCount: Int
  public let totalMessages: Int
}

public struct DeltaStats: Sendable, Equatable {
  public let totalMessages: Int
  public let deltaMessages: Int
  public let fullMessages: Int
  public let totalBytesWithoutCompression: Int
  public let totalBytesWithCompression: Int
  public let bandwidthSaved: Int
  public let bandwidthSavedPercent: Double
  public let errors: Int
  public let channelCount: Int
  public let channels: [ChannelDeltaStats]
}

public struct PresenceMember: Equatable {
  public let id: String
  public let info: AnyHashable?

  public init(id: String, info: AnyHashable?) {
    self.id = id
    self.info = info
  }
}

public struct SockudoEvent: @unchecked Sendable {
  let event: String
  let channel: String?
  let data: Any?
  let userID: String?
  let streamID: String?
  let messageId: String?
  let rawMessage: String
  let sequence: Int?
  let conflationKey: String?
  let serial: Int?
  let extras: MessageExtras?
}

enum Logger {
  nonisolated(unsafe) static var logToConsole = false
  nonisolated(unsafe) static var customLog: ((String) -> Void)?

  static func debug(_ items: Any...) {
    log(items)
  }

  static func warn(_ items: Any...) {
    log(items)
  }

  static func error(_ items: Any...) {
    log(items)
  }

  private static func log(_ items: [Any]) {
    let message = (["Sockudo"] + items.map { String(describing: $0) }).joined(separator: " : ")
    if let customLog {
      customLog(message)
    } else if logToConsole {
      print(message)
    }
  }
}
