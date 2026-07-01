import Foundation

final class MessagePipeline: Sendable {
  let wireFormat: SockudoWireFormat

  init(wireFormat: SockudoWireFormat) {
    self.wireFormat = wireFormat
  }

  func decode(_ message: URLSessionWebSocketTask.Message) throws -> SockudoEvent {
    try ProtocolCodec.decodeEvent(message, format: wireFormat)
  }

  func reconstructDelta(base: String, deltaPayload: Data, algorithm: DeltaAlgorithm) throws
    -> String
  {
    switch algorithm {
    case .fossil:
      return try String(
        decoding: FossilDelta.apply(base: Data(base.utf8), delta: deltaPayload), as: UTF8.self)
    case .xdelta3:
      return try String(
        decoding: XDelta3.decode(delta: deltaPayload, base: Data(base.utf8)), as: UTF8.self)
    }
  }

  func parseJSON(_ string: String) -> Any? {
    guard let data = string.data(using: .utf8) else { return nil }
    return try? JSON.decode(data)
  }

  func stripDeltaMetadata(from rawMessage: URLSessionWebSocketTask.Message) -> String {
    let decoded =
      (try? ProtocolCodec.decodeEnvelope(rawMessage, format: wireFormat).rawMessage) ?? ""
    var result = decoded
    result = result.replacingOccurrences(
      of: #","__delta_seq":\d+"#, with: "", options: .regularExpression)
    result = result.replacingOccurrences(
      of: #"__delta_seq":\d+,"#, with: "", options: .regularExpression)
    result = result.replacingOccurrences(
      of: #","__conflation_key":"[^"]*""#, with: "", options: .regularExpression)
    result = result.replacingOccurrences(
      of: #"__conflation_key":"[^"]*","#, with: "", options: .regularExpression)
    return result
  }
}
