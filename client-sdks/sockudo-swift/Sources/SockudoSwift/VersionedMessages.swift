import Foundation

public enum MutableMessageAction: String, Sendable, Equatable, CaseIterable {
  case create = "message.create"
  case update = "message.update"
  case delete = "message.delete"
  case append = "message.append"
}

extension MutableMessageAction {
  init?(ackValue: Any?) {
    guard let value = ackValue as? String else { return nil }
    if let action = MutableMessageAction(rawValue: value) {
      self = action
      return
    }
    switch value {
    case "create":
      self = .create
    case "update":
      self = .update
    case "delete":
      self = .delete
    case "append":
      self = .append
    default:
      return nil
    }
  }
}

public enum VersionedMessageClearField: String, Sendable, Codable, Equatable, CaseIterable {
  case name
  case data
  case extras
}

public struct VersionedMessageCreateRequest: @unchecked Sendable {
  public let name: String
  public let data: Any
  public let extras: MessageExtras?
  public let messageID: String?
  public let idempotencyKey: String?
  public let socketID: String?
  public let info: String?

  public init(
    name: String,
    data: Any,
    extras: MessageExtras? = nil,
    messageID: String? = nil,
    idempotencyKey: String? = nil,
    socketID: String? = nil,
    info: String? = "message_serial"
  ) {
    self.name = name
    self.data = data
    self.extras = extras
    self.messageID = messageID
    self.idempotencyKey = idempotencyKey
    self.socketID = socketID
    self.info = info
  }

  var payload: [String: Any] {
    var payload: [String: Any] = [
      "name": name,
      "data": data,
    ]
    if let extras { payload["extras"] = extras.payloadValue }
    if let messageID { payload["message_id"] = messageID }
    if let idempotencyKey { payload["idempotency_key"] = idempotencyKey }
    if let socketID { payload["socket_id"] = socketID }
    if let info { payload["info"] = info }
    return payload
  }
}

public struct VersionedMessageUpdateRequest: @unchecked Sendable {
  public let name: String?
  public let data: Any?
  public let extras: MessageExtras?
  public let clearFields: [VersionedMessageClearField]
  public let clientID: String?
  public let socketID: String?
  public let description: String?
  public let metadata: Any?
  public let opID: String?

  public init(
    name: String? = nil,
    data: Any? = nil,
    extras: MessageExtras? = nil,
    clearFields: [VersionedMessageClearField] = [],
    clientID: String? = nil,
    socketID: String? = nil,
    description: String? = nil,
    metadata: Any? = nil,
    opID: String? = nil
  ) {
    self.name = name
    self.data = data
    self.extras = extras
    self.clearFields = clearFields
    self.clientID = clientID
    self.socketID = socketID
    self.description = description
    self.metadata = metadata
    self.opID = opID
  }

  var payload: [String: Any] {
    var payload: [String: Any] = [:]
    if let name { payload["name"] = name }
    if let data { payload["data"] = data }
    if let extras { payload["extras"] = extras.payloadValue }
    if clearFields.isEmpty == false { payload["clear_fields"] = clearFields.map(\.rawValue) }
    if let clientID { payload["client_id"] = clientID }
    if let socketID { payload["socket_id"] = socketID }
    if let description { payload["description"] = description }
    if let metadata { payload["metadata"] = metadata }
    if let opID { payload["op_id"] = opID }
    return payload
  }
}

public struct VersionedMessageAppendRequest: @unchecked Sendable {
  public let data: String
  public let extras: MessageExtras?
  public let clientID: String?
  public let socketID: String?
  public let description: String?
  public let metadata: Any?
  public let opID: String?

  public init(
    data: String,
    extras: MessageExtras? = nil,
    clientID: String? = nil,
    socketID: String? = nil,
    description: String? = nil,
    metadata: Any? = nil,
    opID: String? = nil
  ) {
    self.data = data
    self.extras = extras
    self.clientID = clientID
    self.socketID = socketID
    self.description = description
    self.metadata = metadata
    self.opID = opID
  }

  var payload: [String: Any] {
    var payload: [String: Any] = ["data": data]
    if let extras { payload["extras"] = extras.payloadValue }
    if let clientID { payload["client_id"] = clientID }
    if let socketID { payload["socket_id"] = socketID }
    if let description { payload["description"] = description }
    if let metadata { payload["metadata"] = metadata }
    if let opID { payload["op_id"] = opID }
    return payload
  }
}

public struct VersionedMessageDeleteRequest: @unchecked Sendable {
  public let data: Any?
  public let extras: MessageExtras?
  public let clearFields: [VersionedMessageClearField]
  public let clientID: String?
  public let socketID: String?
  public let description: String?
  public let metadata: Any?
  public let opID: String?

  public init(
    data: Any? = nil,
    extras: MessageExtras? = nil,
    clearFields: [VersionedMessageClearField] = [],
    clientID: String? = nil,
    socketID: String? = nil,
    description: String? = nil,
    metadata: Any? = nil,
    opID: String? = nil
  ) {
    self.data = data
    self.extras = extras
    self.clearFields = clearFields
    self.clientID = clientID
    self.socketID = socketID
    self.description = description
    self.metadata = metadata
    self.opID = opID
  }

  var payload: [String: Any] {
    var payload: [String: Any] = [:]
    if let data { payload["data"] = data }
    if let extras { payload["extras"] = extras.payloadValue }
    if clearFields.isEmpty == false { payload["clear_fields"] = clearFields.map(\.rawValue) }
    if let clientID { payload["client_id"] = clientID }
    if let socketID { payload["socket_id"] = socketID }
    if let description { payload["description"] = description }
    if let metadata { payload["metadata"] = metadata }
    if let opID { payload["op_id"] = opID }
    return payload
  }
}

public struct VersionedMessageAck: @unchecked Sendable {
  public let channel: String
  public let messageSerial: String
  public let action: MutableMessageAction
  public let accepted: Bool
  public let versionSerial: String?
  public let historySerial: Int64?
  public let deliverySerial: Int64?
  public let status: String
  public let raw: [String: Any]

  public init(
    channel: String,
    messageSerial: String,
    action: MutableMessageAction,
    accepted: Bool,
    versionSerial: String? = nil,
    historySerial: Int64? = nil,
    deliverySerial: Int64? = nil,
    status: String,
    raw: [String: Any] = [:]
  ) {
    self.channel = channel
    self.messageSerial = messageSerial
    self.action = action
    self.accepted = accepted
    self.versionSerial = versionSerial
    self.historySerial = historySerial
    self.deliverySerial = deliverySerial
    self.status = status
    self.raw = raw
  }
}

public struct MutableMessageVersionInfo: Sendable, Equatable {
  public let action: MutableMessageAction
  public let event: String
  public let messageSerial: String
  public let versionSerial: String?
  public let historySerial: Int?
  public let versionTimestampMs: Int?

  public init(
    action: MutableMessageAction,
    event: String,
    messageSerial: String,
    versionSerial: String? = nil,
    historySerial: Int? = nil,
    versionTimestampMs: Int? = nil
  ) {
    self.action = action
    self.event = event
    self.messageSerial = messageSerial
    self.versionSerial = versionSerial
    self.historySerial = historySerial
    self.versionTimestampMs = versionTimestampMs
  }
}

public struct MutableMessageState: @unchecked Sendable, Equatable {
  public let messageSerial: String
  public let action: MutableMessageAction
  public let data: Any?
  public let event: String
  public let serial: Int?
  public let streamId: String?
  public let messageId: String?
  public let versionSerial: String?
  public let historySerial: Int?
  public let versionTimestampMs: Int?

  public init(
    messageSerial: String,
    action: MutableMessageAction,
    data: Any?,
    event: String,
    serial: Int? = nil,
    streamId: String? = nil,
    messageId: String? = nil,
    versionSerial: String? = nil,
    historySerial: Int? = nil,
    versionTimestampMs: Int? = nil
  ) {
    self.messageSerial = messageSerial
    self.action = action
    self.data = data
    self.event = event
    self.serial = serial
    self.streamId = streamId
    self.messageId = messageId
    self.versionSerial = versionSerial
    self.historySerial = historySerial
    self.versionTimestampMs = versionTimestampMs
  }

  public static func == (lhs: MutableMessageState, rhs: MutableMessageState) -> Bool {
    lhs.messageSerial == rhs.messageSerial
      && lhs.action == rhs.action
      && lhs.event == rhs.event
      && lhs.serial == rhs.serial
      && lhs.streamId == rhs.streamId
      && lhs.messageId == rhs.messageId
      && lhs.versionSerial == rhs.versionSerial
      && lhs.historySerial == rhs.historySerial
      && lhs.versionTimestampMs == rhs.versionTimestampMs
  }
}

private func parseNumericHeader(_ value: ExtraValue?) -> Int? {
  guard let value else { return nil }
  switch value {
  case .int(let integerValue): return integerValue
  case .double(let doubleValue):
    let integerValue = Int(exactly: doubleValue)
    return integerValue
  case .string(let stringValue):
    if let integerValue = Int(stringValue) {
      return integerValue
    }
    if let doubleValue = Double(stringValue), let integerValue = Int(exactly: doubleValue) {
      return integerValue
    }
    return nil
  case .bool:
    return nil
  }
}

public func isMutableMessageEvent(_ event: SockudoEvent) -> Bool {
  getMutableMessageInfo(event) != nil
}

public func getMutableMessageInfo(_ event: SockudoEvent) -> MutableMessageVersionInfo? {
  guard
    let actionRaw = event.extras?.headers?["sockudo_action"],
    let messageSerialRaw = event.extras?.headers?["sockudo_message_serial"],
    case .string(let actionStr) = actionRaw,
    case .string(let messageSerial) = messageSerialRaw,
    let action = MutableMessageAction(rawValue: actionStr)
  else {
    return nil
  }

  let versionSerial: String?
  if case .string(let versionSerialValue) = event.extras?.headers?["sockudo_version_serial"] {
    versionSerial = versionSerialValue
  } else {
    versionSerial = nil
  }

  let historySerial = parseNumericHeader(event.extras?.headers?["sockudo_history_serial"])
  let versionTimestampMs = parseNumericHeader(
    event.extras?.headers?["sockudo_version_timestamp_ms"])

  return MutableMessageVersionInfo(
    action: action,
    event: event.event,
    messageSerial: messageSerial,
    versionSerial: versionSerial,
    historySerial: historySerial,
    versionTimestampMs: versionTimestampMs
  )
}

public func reduceMutableMessageEvent(
  current: MutableMessageState?,
  event: SockudoEvent
) throws -> MutableMessageState {
  guard let info = getMutableMessageInfo(event) else {
    throw SockudoError.messageParseError("Event is not a mutable-message event")
  }

  if let current, current.messageSerial != info.messageSerial {
    throw SockudoError.messageParseError(
      "Mutable-message reducer expected message_serial '\(current.messageSerial)'"
        + " but received '\(info.messageSerial)'"
    )
  }

  let nextData: Any?
  switch info.action {
  case .append:
    guard let base = current?.data as? String else {
      throw SockudoError.messageParseError(
        "message.append requires an existing string base;"
          + " seed state from a create/update payload or latest-view history first"
      )
    }
    guard let fragment = event.data as? String else {
      throw SockudoError.messageParseError(
        "message.append payload must be a string fragment when applying client-side concatenation"
      )
    }
    nextData = base + fragment
  case .delete, .create, .update:
    nextData = event.data
  }

  return MutableMessageState(
    messageSerial: info.messageSerial,
    action: info.action,
    data: nextData,
    event: info.event,
    serial: event.serial,
    streamId: event.streamID,
    messageId: event.messageId,
    versionSerial: info.versionSerial,
    historySerial: info.historySerial,
    versionTimestampMs: info.versionTimestampMs
  )
}

public func reduceMutableMessageEvents(
  _ events: [SockudoEvent]
) throws -> MutableMessageState? {
  var state: MutableMessageState? = nil
  for event in events {
    state = try reduceMutableMessageEvent(current: state, event: event)
  }
  return state
}
