import Foundation

public enum MutableMessageAction: String, Sendable, Equatable, CaseIterable {
  case create = "message.create"
  case update = "message.update"
  case delete = "message.delete"
  case append = "message.append"
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
  case .int(let v): return v
  case .double(let v):
    let i = Int(exactly: v)
    return i
  case .string(let s):
    if let d = Double(s), let i = Int(exactly: d) {
      return i
    }
    return Int(s)
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
  if case .string(let vs) = event.extras?.headers?["sockudo_version_serial"] {
    versionSerial = vs
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
