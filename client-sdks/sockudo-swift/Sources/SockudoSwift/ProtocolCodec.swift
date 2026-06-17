import Foundation

enum EncodedPayload {
  case string(String)
  case data(Data)
}

struct DecodedEnvelope {
  let envelope: [String: Any]
  let rawMessage: String
}

enum ProtocolCodec {
  static func encodeEnvelope(_ envelope: [String: Any], format: SockudoWireFormat) throws
    -> EncodedPayload
  {
    switch format {
    case .json:
      return .string(try JSON.encodeString(envelope))
    case .messagepack:
      return .data(try MessagePackCodec.encode(envelope))
    case .protobuf:
      return .data(try ProtobufCodec.encode(envelope))
    }
  }

  static func decodeEnvelope(_ message: URLSessionWebSocketTask.Message, format: SockudoWireFormat)
    throws
    -> DecodedEnvelope
  {
    switch format {
    case .json:
      let text =
        switch message {
        case .string(let text): text
        case .data(let data): String(decoding: data, as: UTF8.self)
        @unknown default: throw SockudoError.messageParseError("Unsupported WebSocket message")
        }
      guard let envelope = try JSON.decodeString(text) as? [String: Any] else {
        throw SockudoError.messageParseError("Unable to decode event envelope")
      }
      return .init(envelope: envelope, rawMessage: text)
    case .messagepack:
      let data = try data(from: message)
      let envelope = try MessagePackCodec.decode(data)
      return .init(envelope: envelope, rawMessage: try JSON.encodeString(envelope))
    case .protobuf:
      let data = try data(from: message)
      let envelope = try ProtobufCodec.decode(data)
      return .init(envelope: envelope, rawMessage: try JSON.encodeString(envelope))
    }
  }

  static func decodeEvent(_ message: URLSessionWebSocketTask.Message, format: SockudoWireFormat)
    throws -> SockudoEvent
  {
    let decoded = try decodeEnvelope(message, format: format)
    let envelope = decoded.envelope
    guard let event = envelope["event"] as? String else {
      throw SockudoError.messageParseError("Unable to decode event envelope")
    }
    var eventData = envelope["data"]
    if let stringData = eventData as? String, let parsed = try? JSON.decodeString(stringData) {
      eventData = parsed
    }
    var parsedExtras: MessageExtras?
    if let extras = envelope["extras"] as? [String: Any] {
      let headers =
        (extras["headers"] as? [String: Any])?.compactMapValues { value -> ExtraValue? in
          switch value {
          case let value as Bool:
            return .bool(value)
          case let value as Int:
            return .int(value)
          case let value as NSNumber where CFGetTypeID(value) != CFBooleanGetTypeID():
            if floor(value.doubleValue) == value.doubleValue {
              return .int(value.intValue)
            }
            return .double(value.doubleValue)
          case let value as Double:
            return .double(value)
          case let value as String:
            return .string(value)
          default:
            return nil
          }
        }
      parsedExtras = MessageExtras(
        headers: headers,
        ephemeral: extras["ephemeral"] as? Bool,
        idempotencyKey: (extras["idempotency_key"] ?? extras["idempotencyKey"]) as? String,
        echo: extras["echo"] as? Bool
      )
    }

    return SockudoEvent(
      event: event,
      channel: envelope["channel"] as? String,
      data: eventData,
      userID: envelope["user_id"] as? String,
      streamID: envelope["stream_id"] as? String,
      messageId: envelope["message_id"] as? String,
      rawMessage: decoded.rawMessage,
      sequence: (envelope["__delta_seq"] as? NSNumber)?.intValue
        ?? (envelope["sequence"] as? NSNumber)?.intValue,
      conflationKey: envelope["__conflation_key"] as? String ?? envelope["conflation_key"]
        as? String,
      serial: (envelope["serial"] as? NSNumber)?.intValue,
      extras: parsedExtras
    )
  }

  private static func data(from message: URLSessionWebSocketTask.Message) throws -> Data {
    switch message {
    case .data(let data):
      return data
    case .string(let text):
      return Data(text.utf8)
    @unknown default:
      throw SockudoError.messageParseError("Unsupported WebSocket message")
    }
  }
}

private enum MessagePackCodec {
  private static let envelopeFields = [
    "event",
    "channel",
    "data",
    "name",
    "user_id",
    "tags",
    "sequence",
    "conflation_key",
    "message_id",
    "stream_id",
    "serial",
    "idempotency_key",
    "extras",
    "__delta_seq",
    "__conflation_key",
  ]

  static func encode(_ value: Any) throws -> Data {
    var encoder = MessagePackEncoder()
    try encoder.encode(encodeEnvelopeValue(value))
    return encoder.data
  }

  static func decode(_ data: Data) throws -> [String: Any] {
    var decoder = try MessagePackDecoder(data: data)
    let decoded = try decoder.decode()
    if let array = decoded as? [Any] {
      var envelope: [String: Any] = [:]
      for (index, field) in envelopeFields.enumerated() where index < array.count {
        if let value = decodeEnvelopeValue(array[index]) {
          envelope[field] = value
        }
      }
      return envelope
    }
    guard let map = decodeEnvelopeValue(decoded) as? [String: Any] else {
      throw SockudoError.messageParseError("Unable to decode event envelope")
    }
    return map
  }

  private static func encodeEnvelopeValue(_ value: Any) -> Any {
    guard let envelope = value as? [String: Any] else {
      return value
    }
    let dataValue: Any
    if let rawData = envelope["data"] {
      if let stringData = rawData as? String {
        dataValue = ["string", stringData]
      } else {
        dataValue = ["json", (try? JSON.encodeString(rawData)) ?? "null"]
      }
    } else {
      dataValue = NSNull()
    }

    let extrasValue: Any = {
      guard let extras = envelope["extras"] as? [String: Any] else { return NSNull() }
      var encoded: [String: Any] = [:]
      if let headers = extras["headers"] as? [String: Any] {
        encoded["headers"] = headers.mapValues { headerValue in
          switch headerValue {
          case let value as Bool:
            return ["bool", value]
          case let value as Int:
            return ["number", value]
          case let value as Double:
            return ["number", value]
          case let value as NSNumber where CFGetTypeID(value) != CFBooleanGetTypeID():
            return ["number", value.doubleValue]
          default:
            return ["string", String(describing: headerValue)]
          }
        }
      }
      if let ephemeral = extras["ephemeral"] { encoded["ephemeral"] = ephemeral }
      if let idempotencyKey = extras["idempotency_key"] ?? extras["idempotencyKey"] {
        encoded["idempotency_key"] = idempotencyKey
      }
      if let echo = extras["echo"] { encoded["echo"] = echo }
      return encoded
    }()

    return [
      envelope["event"] ?? NSNull(),
      envelope["channel"] ?? NSNull(),
      dataValue,
      envelope["name"] ?? NSNull(),
      envelope["user_id"] ?? NSNull(),
      envelope["tags"] ?? NSNull(),
      envelope["sequence"] ?? NSNull(),
      envelope["conflation_key"] ?? NSNull(),
      envelope["message_id"] ?? NSNull(),
      envelope["stream_id"] ?? NSNull(),
      envelope["serial"] ?? NSNull(),
      envelope["idempotency_key"] ?? NSNull(),
      extrasValue,
      envelope["__delta_seq"] ?? NSNull(),
      envelope["__conflation_key"] ?? NSNull(),
    ]
  }

  private static func decodeEnvelopeValue(_ value: Any) -> Any? {
    if value is NSNull {
      return nil
    }
    if let array = value as? [Any] {
      if array.count == 2, let kind = array[0] as? String {
        switch kind {
        case "string", "json", "number", "bool":
          return decodeEnvelopeValue(array[1]) ?? array[1]
        case "structured":
          return decodeEnvelopeValue(array[1])
        default:
          return array.compactMap { decodeEnvelopeValue($0) }
        }
      }
      return array.compactMap { decodeEnvelopeValue($0) }
    }
    if let map = value as? [String: Any] {
      return Dictionary(
        uniqueKeysWithValues: map.map { key, entryValue in
          (key, decodeEnvelopeValue(entryValue) as Any)
        })
    }
    return value
  }
}

private struct MessagePackEncoder {
  private(set) var data = Data()

  mutating func encode(_ value: Any) throws {
    switch value {
    case is NSNull:
      data.append(0xc0)
    case let value as Bool:
      data.append(value ? 0xc3 : 0xc2)
    case let value as Int:
      try encode(integer: Int64(value))
    case let value as Int64:
      try encode(integer: value)
    case let value as UInt64:
      try encode(uinteger: value)
    case let value as Double:
      data.append(0xcb)
      append(bigEndian: value.bitPattern)
    case let value as Float:
      data.append(0xca)
      append(bigEndian: value.bitPattern)
    case let value as String:
      try encode(string: value)
    case let value as [String: Any]:
      try encode(map: value)
    case let value as [Any]:
      try encode(array: value)
    case let value as [String: String]:
      try encode(map: value.mapValues { $0 })
    case let value as MessageExtras:
      var map: [String: Any] = [:]
      if let headers = value.headers { map["headers"] = headers.mapValues(\.rawValue) }
      if let ephemeral = value.ephemeral { map["ephemeral"] = ephemeral }
      if let idempotencyKey = value.idempotencyKey { map["idempotency_key"] = idempotencyKey }
      if let echo = value.echo { map["echo"] = echo }
      try encode(map: map)
    default:
      if let number = value as? NSNumber {
        if CFGetTypeID(number) == CFBooleanGetTypeID() {
          data.append(number.boolValue ? 0xc3 : 0xc2)
        } else if String(cString: number.objCType) == "d" || String(cString: number.objCType) == "f"
        {
          data.append(0xcb)
          append(bigEndian: number.doubleValue.bitPattern)
        } else {
          try encode(integer: number.int64Value)
        }
        return
      }
      throw SockudoError.messageParseError("Unsupported MessagePack value")
    }
  }

  private mutating func encode(string: String) throws {
    let utf8 = Data(string.utf8)
    switch utf8.count {
    case 0...31:
      data.append(UInt8(0xa0 | utf8.count))
    case 32...0xff:
      data.append(0xd9)
      data.append(UInt8(utf8.count))
    case 0x100...0xffff:
      data.append(0xda)
      append(bigEndian: UInt16(utf8.count))
    default:
      data.append(0xdb)
      append(bigEndian: UInt32(utf8.count))
    }
    data.append(utf8)
  }

  private mutating func encode(array: [Any]) throws {
    switch array.count {
    case 0...15:
      data.append(UInt8(0x90 | array.count))
    case 16...0xffff:
      data.append(0xdc)
      append(bigEndian: UInt16(array.count))
    default:
      data.append(0xdd)
      append(bigEndian: UInt32(array.count))
    }
    for item in array {
      try encode(item)
    }
  }

  private mutating func encode(map: [String: Any]) throws {
    switch map.count {
    case 0...15:
      data.append(UInt8(0x80 | map.count))
    case 16...0xffff:
      data.append(0xde)
      append(bigEndian: UInt16(map.count))
    default:
      data.append(0xdf)
      append(bigEndian: UInt32(map.count))
    }
    for key in map.keys.sorted() {
      try encode(string: key)
      try encode(map[key] ?? NSNull())
    }
  }

  private mutating func encode(integer: Int64) throws {
    if (0...127).contains(integer) {
      data.append(UInt8(integer))
    } else if (-32 ... -1).contains(integer) {
      data.append(UInt8(bitPattern: Int8(integer)))
    } else if Int64(Int8.min)...Int64(Int8.max) ~= integer {
      data.append(0xd0)
      data.append(UInt8(bitPattern: Int8(integer)))
    } else if Int64(Int16.min)...Int64(Int16.max) ~= integer {
      data.append(0xd1)
      append(bigEndian: UInt16(bitPattern: Int16(integer)))
    } else if Int64(Int32.min)...Int64(Int32.max) ~= integer {
      data.append(0xd2)
      append(bigEndian: UInt32(bitPattern: Int32(integer)))
    } else {
      data.append(0xd3)
      append(bigEndian: UInt64(bitPattern: integer))
    }
  }

  private mutating func encode(uinteger: UInt64) throws {
    if uinteger <= 127 {
      data.append(UInt8(uinteger))
    } else if uinteger <= UInt8.max {
      data.append(0xcc)
      data.append(UInt8(uinteger))
    } else if uinteger <= UInt16.max {
      data.append(0xcd)
      append(bigEndian: UInt16(uinteger))
    } else if uinteger <= UInt32.max {
      data.append(0xce)
      append(bigEndian: UInt32(uinteger))
    } else {
      data.append(0xcf)
      append(bigEndian: uinteger)
    }
  }

  private mutating func append<T: FixedWidthInteger>(bigEndian value: T) {
    var big = value.bigEndian
    withUnsafeBytes(of: &big) { data.append(contentsOf: $0) }
  }
}

private struct MessagePackDecoder {
  let data: Data
  var offset: Data.Index

  init(data: Data) throws {
    self.data = data
    self.offset = data.startIndex
  }

  mutating func decode() throws -> Any {
    guard offset < data.endIndex else {
      throw SockudoError.messageParseError("Unexpected end of MessagePack payload")
    }
    let byte = readByte()
    switch byte {
    case 0xc0: return NSNull()
    case 0xc2: return false
    case 0xc3: return true
    case 0xca: return Double(Float(bitPattern: try readUInt32()))
    case 0xcb: return Double(bitPattern: try readUInt64())
    case 0xcc: return Int(readByte())
    case 0xcd: return Int(try readUInt16())
    case 0xce: return Int(try readUInt32())
    case 0xcf: return Int(try readUInt64())
    case 0xd0: return Int(Int8(bitPattern: readByte()))
    case 0xd1: return Int(Int16(bitPattern: try readUInt16()))
    case 0xd2: return Int(Int32(bitPattern: try readUInt32()))
    case 0xd3: return Int(Int64(bitPattern: try readUInt64()))
    case 0xd9: return try readString(length: Int(readByte()))
    case 0xda: return try readString(length: Int(try readUInt16()))
    case 0xdb: return try readString(length: Int(try readUInt32()))
    case 0xdc: return try readArray(length: Int(try readUInt16()))
    case 0xdd: return try readArray(length: Int(try readUInt32()))
    case 0xde: return try readMap(length: Int(try readUInt16()))
    case 0xdf: return try readMap(length: Int(try readUInt32()))
    case 0x00...0x7f: return Int(byte)
    case 0x80...0x8f: return try readMap(length: Int(byte & 0x0f))
    case 0x90...0x9f: return try readArray(length: Int(byte & 0x0f))
    case 0xa0...0xbf: return try readString(length: Int(byte & 0x1f))
    case 0xe0...0xff: return Int(Int8(bitPattern: byte))
    default:
      throw SockudoError.messageParseError("Unsupported MessagePack type")
    }
  }

  private mutating func readArray(length: Int) throws -> [Any] {
    var result: [Any] = []
    result.reserveCapacity(length)
    for _ in 0..<length {
      result.append(try decode())
    }
    return result
  }

  private mutating func readMap(length: Int) throws -> [String: Any] {
    var result: [String: Any] = [:]
    for _ in 0..<length {
      guard let key = try decode() as? String else {
        throw SockudoError.messageParseError("Expected string map key")
      }
      result[key] = try decode()
    }
    return result
  }

  private mutating func readString(length: Int) throws -> String {
    let bytes = try readBytes(length: length)
    guard let string = String(data: bytes, encoding: .utf8) else {
      throw SockudoError.messageParseError("Invalid UTF-8 in MessagePack payload")
    }
    return string
  }

  private mutating func readByte() -> UInt8 {
    defer { offset = data.index(after: offset) }
    return data[offset]
  }

  private mutating func readBytes(length: Int) throws -> Data {
    guard data.distance(from: offset, to: data.endIndex) >= length else {
      throw SockudoError.messageParseError("Unexpected end of MessagePack payload")
    }
    let end = data.index(offset, offsetBy: length)
    let slice = data[offset..<end]
    offset = end
    return Data(slice)
  }

  private mutating func readUInt16() throws -> UInt16 {
    let bytes = try readBytes(length: 2)
    return bytes.reduce(UInt16(0)) { ($0 << 8) | UInt16($1) }
  }

  private mutating func readUInt32() throws -> UInt32 {
    let bytes = try readBytes(length: 4)
    return bytes.reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
  }

  private mutating func readUInt64() throws -> UInt64 {
    let bytes = try readBytes(length: 8)
    return bytes.reduce(UInt64(0)) { ($0 << 8) | UInt64($1) }
  }
}

private enum ProtobufCodec {
  static func encode(_ envelope: [String: Any]) throws -> Data {
    var writer = ProtoWriter()
    if let event = envelope["event"] as? String {
      writer.writeString(field: 1, value: event)
    }
    if let channel = envelope["channel"] as? String {
      writer.writeString(field: 2, value: channel)
    }
    if let data = envelope["data"] {
      var nested = ProtoWriter()
      if let string = data as? String {
        nested.writeString(field: 1, value: string)
      } else {
        nested.writeString(field: 3, value: try JSON.encodeString(data))
      }
      writer.writeData(field: 3, value: nested.data)
    }
    if let userID = envelope["user_id"] as? String {
      writer.writeString(field: 5, value: userID)
    }
    if let sequence = (envelope["sequence"] as? NSNumber)?.uint64Value {
      writer.writeVarint(field: 7, value: sequence)
    }
    if let conflationKey = envelope["conflation_key"] as? String {
      writer.writeString(field: 8, value: conflationKey)
    }
    if let messageID = envelope["message_id"] as? String {
      writer.writeString(field: 9, value: messageID)
    }
    if let streamID = envelope["stream_id"] as? String {
      writer.writeString(field: 15, value: streamID)
    }
    if let serial = (envelope["serial"] as? NSNumber)?.uint64Value {
      writer.writeVarint(field: 10, value: serial)
    }
    if let extras = envelope["extras"] as? [String: Any] {
      var nested = ProtoWriter()
      if let headers = extras["headers"] as? [String: Any] {
        for key in headers.keys.sorted() {
          var entry = ProtoWriter()
          entry.writeString(field: 1, value: key)
          var valueWriter = ProtoWriter()
          let value = headers[key]
          let encodedValue: Any =
            if let extraValue = value as? ExtraValue {
              extraValue.rawValue
            } else if let value {
              value
            } else {
              ""
            }
          if let bool = encodedValue as? Bool {
            valueWriter.writeBool(field: 3, value: bool)
          } else if let number = encodedValue as? NSNumber,
            CFGetTypeID(number) != CFBooleanGetTypeID()
          {
            valueWriter.writeDouble(field: 2, value: number.doubleValue)
          } else {
            valueWriter.writeString(field: 1, value: String(describing: encodedValue))
          }
          entry.writeData(field: 2, value: valueWriter.data)
          nested.writeData(field: 1, value: entry.data)
        }
      }
      if let ephemeral = extras["ephemeral"] as? Bool {
        nested.writeBool(field: 2, value: ephemeral)
      }
      if let idempotencyKey = (extras["idempotency_key"] ?? extras["idempotencyKey"]) as? String {
        nested.writeString(field: 3, value: idempotencyKey)
      }
      if let echo = extras["echo"] as? Bool {
        nested.writeBool(field: 4, value: echo)
      }
      writer.writeData(field: 12, value: nested.data)
    }
    if let deltaSequence = (envelope["__delta_seq"] as? NSNumber)?.uint64Value {
      writer.writeVarint(field: 13, value: deltaSequence)
    }
    if let deltaConflationKey = envelope["__conflation_key"] as? String {
      writer.writeString(field: 14, value: deltaConflationKey)
    }
    return writer.data
  }

  static func decode(_ data: Data) throws -> [String: Any] {
    var reader = ProtoReader(data: data)
    var envelope: [String: Any] = [:]
    while let (field, wireType) = try reader.nextField() {
      switch (field, wireType) {
      case (1, .lengthDelimited): envelope["event"] = try reader.readString()
      case (2, .lengthDelimited): envelope["channel"] = try reader.readString()
      case (3, .lengthDelimited): envelope["data"] = try decodeMessageData(reader.readData())
      case (5, .lengthDelimited): envelope["user_id"] = try reader.readString()
      case (7, .varint): envelope["sequence"] = NSNumber(value: try reader.readVarint())
      case (8, .lengthDelimited): envelope["conflation_key"] = try reader.readString()
      case (9, .lengthDelimited): envelope["message_id"] = try reader.readString()
      case (10, .varint): envelope["serial"] = NSNumber(value: try reader.readVarint())
      case (12, .lengthDelimited): envelope["extras"] = try decodeExtras(reader.readData())
      case (13, .varint): envelope["__delta_seq"] = NSNumber(value: try reader.readVarint())
      case (14, .lengthDelimited): envelope["__conflation_key"] = try reader.readString()
      case (15, .lengthDelimited): envelope["stream_id"] = try reader.readString()
      default: try reader.skip(wireType: wireType)
      }
    }
    return envelope
  }

  private static func decodeMessageData(_ data: Data) throws -> Any {
    var reader = ProtoReader(data: data)
    while let (field, wireType) = try reader.nextField() {
      switch (field, wireType) {
      case (1, .lengthDelimited): return try reader.readString()
      case (3, .lengthDelimited):
        let raw = try reader.readString()
        return (try? JSON.decodeString(raw)) ?? raw
      default: try reader.skip(wireType: wireType)
      }
    }
    return NSNull()
  }

  private static func decodeExtras(_ data: Data) throws -> [String: Any] {
    var reader = ProtoReader(data: data)
    var extras: [String: Any] = [:]
    var headers: [String: Any] = [:]
    while let (field, wireType) = try reader.nextField() {
      switch (field, wireType) {
      case (1, .lengthDelimited):
        let (key, value) = try decodeHeaderEntry(reader.readData())
        headers[key] = value
      case (2, .varint):
        extras["ephemeral"] = try reader.readVarint() != 0
      case (3, .lengthDelimited):
        extras["idempotency_key"] = try reader.readString()
      case (4, .varint):
        extras["echo"] = try reader.readVarint() != 0
      default:
        try reader.skip(wireType: wireType)
      }
    }
    if headers.isEmpty == false {
      extras["headers"] = headers
    }
    return extras
  }

  private static func decodeHeaderEntry(_ data: Data) throws -> (String, Any) {
    var reader = ProtoReader(data: data)
    var key = ""
    var value: Any = ""
    while let (field, wireType) = try reader.nextField() {
      switch (field, wireType) {
      case (1, .lengthDelimited): key = try reader.readString()
      case (2, .lengthDelimited): value = try decodeHeaderValue(reader.readData())
      default: try reader.skip(wireType: wireType)
      }
    }
    return (key, value)
  }

  private static func decodeHeaderValue(_ data: Data) throws -> Any {
    var reader = ProtoReader(data: data)
    while let (field, wireType) = try reader.nextField() {
      switch (field, wireType) {
      case (1, .lengthDelimited): return try reader.readString()
      case (2, .fixed64):
        return NSNumber(value: Double(bitPattern: try reader.readFixed64()))
      case (3, .varint):
        return try reader.readVarint() != 0
      default:
        try reader.skip(wireType: wireType)
      }
    }
    return ""
  }
}

private struct ProtoWriter {
  private(set) var data = Data()

  mutating func writeString(field: UInt64, value: String) {
    writeTag(field: field, wireType: .lengthDelimited)
    let bytes = Data(value.utf8)
    writeRawVarint(UInt64(bytes.count))
    data.append(bytes)
  }

  mutating func writeData(field: UInt64, value: Data) {
    writeTag(field: field, wireType: .lengthDelimited)
    writeRawVarint(UInt64(value.count))
    data.append(value)
  }

  mutating func writeVarint(field: UInt64, value: UInt64) {
    writeTag(field: field, wireType: .varint)
    writeRawVarint(value)
  }

  mutating func writeBool(field: UInt64, value: Bool) {
    writeVarint(field: field, value: value ? 1 : 0)
  }

  mutating func writeDouble(field: UInt64, value: Double) {
    writeTag(field: field, wireType: .fixed64)
    var bitPattern = value.bitPattern.littleEndian
    withUnsafeBytes(of: &bitPattern) { data.append(contentsOf: $0) }
  }

  mutating func writeTag(field: UInt64, wireType: ProtoWireType) {
    writeRawVarint((field << 3) | wireType.rawValue)
  }

  mutating func writeRawVarint(_ value: UInt64) {
    var remaining = value
    while true {
      if remaining < 0x80 {
        data.append(UInt8(remaining))
        return
      }
      data.append(UInt8((remaining & 0x7f) | 0x80))
      remaining >>= 7
    }
  }
}

private enum ProtoWireType: UInt64 {
  case varint = 0
  case fixed64 = 1
  case lengthDelimited = 2
  case fixed32 = 5
}

private struct ProtoReader {
  let data: Data
  var offset: Data.Index
  private var currentTag: UInt64 = 0

  init(data: Data) {
    self.data = data
    self.offset = data.startIndex
  }

  mutating func nextField() throws -> (UInt64, ProtoWireType)? {
    guard offset < data.endIndex else { return nil }
    currentTag = try readVarint()
    guard let wireType = ProtoWireType(rawValue: currentTag & 0x07) else {
      throw SockudoError.messageParseError("Unsupported protobuf wire type")
    }
    return (currentTag >> 3, wireType)
  }

  mutating func readVarint() throws -> UInt64 {
    var result: UInt64 = 0
    var shift: UInt64 = 0
    while offset < data.endIndex {
      let byte = data[offset]
      offset = data.index(after: offset)
      result |= UInt64(byte & 0x7f) << shift
      if byte & 0x80 == 0 {
        return result
      }
      shift += 7
    }
    throw SockudoError.messageParseError("Unexpected end of protobuf payload")
  }

  mutating func readData() -> Data {
    let length = Int((try? readVarint()) ?? 0)
    let end = data.index(offset, offsetBy: length)
    let slice = Data(data[offset..<end])
    offset = end
    return slice
  }

  mutating func readString() throws -> String {
    let data = readData()
    guard let string = String(data: data, encoding: .utf8) else {
      throw SockudoError.messageParseError("Invalid UTF-8 protobuf payload")
    }
    return string
  }

  mutating func readFixed64() throws -> UInt64 {
    guard data.distance(from: offset, to: data.endIndex) >= 8 else {
      throw SockudoError.messageParseError("Unexpected end of protobuf payload")
    }
    let end = data.index(offset, offsetBy: 8)
    let slice = data[offset..<end]
    offset = end
    return slice.enumerated().reduce(UInt64(0)) { partial, pair in
      partial | (UInt64(pair.element) << (UInt64(pair.offset) * 8))
    }
  }

  mutating func skip(wireType: ProtoWireType) throws {
    switch wireType {
    case .varint:
      _ = try readVarint()
    case .fixed64:
      _ = try readFixed64()
    case .lengthDelimited:
      _ = readData()
    case .fixed32:
      guard data.distance(from: offset, to: data.endIndex) >= 4 else {
        throw SockudoError.messageParseError("Unexpected end of protobuf payload")
      }
      offset = data.index(offset, offsetBy: 4)
    }
  }
}
