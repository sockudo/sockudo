import CryptoKit
import Foundation
import Testing

@testable import SockudoSwift

private final class Box<T>: @unchecked Sendable {
  var value: T?
}

private final class PushURLProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var requestHandler: ((URLRequest) throws -> (HTTPURLResponse, Data?))?

  override class func canInit(with request: URLRequest) -> Bool {
    true
  }

  override class func canonicalRequest(for request: URLRequest) -> URLRequest {
    request
  }

  override func startLoading() {
    guard let handler = Self.requestHandler else {
      client?.urlProtocol(self, didFailWithError: TimeoutError())
      return
    }

    do {
      let (response, data) = try handler(request)
      client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
      if let data {
        client?.urlProtocol(self, didLoad: data)
      }
      client?.urlProtocolDidFinishLoading(self)
    } catch {
      client?.urlProtocol(self, didFailWithError: error)
    }
  }

  override func stopLoading() {}
}

private struct TimeoutError: Error {}

private func waitForValue<T>(
  timeout: TimeInterval = 5,
  pollInterval: UInt64 = 50_000_000,
  _ body: @escaping () -> T?
) async throws -> T {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    if let value = body() {
      return value
    }
    try await Task.sleep(nanoseconds: pollInterval)
  }
  throw TimeoutError()
}

private func sha256HMAC(_ string: String, secret: String) -> String {
  let key = SymmetricKey(data: Data(secret.utf8))
  let signature = HMAC<SHA256>.authenticationCode(for: Data(string.utf8), using: key)
  return signature.map { String(format: "%02x", $0) }.joined()
}

private func md5Hex(_ data: Data) -> String {
  Insecure.MD5.hash(data: data).map { String(format: "%02x", $0) }.joined()
}

private func liveWireFormat() -> SockudoWireFormat {
  switch ProcessInfo.processInfo.environment["SOCKUDO_WIRE_FORMAT"]?.lowercased() {
  case "messagepack", "msgpack":
    return .messagepack
  case "protobuf", "proto":
    return .protobuf
  default:
    return .json
  }
}

private func publishToLocalSockudo(
  channel: String,
  eventName: String,
  payload: [String: Any]
) async throws {
  let path = "/apps/app-id/events"
  let eventData = try JSONSerialization.data(withJSONObject: payload, options: [])
  let bodyObject: [String: Any] = [
    "name": eventName,
    "channels": [channel],
    "data": String(decoding: eventData, as: UTF8.self),
  ]
  let body = try JSONSerialization.data(withJSONObject: bodyObject, options: [])
  let bodyMD5 = md5Hex(body)
  let timestamp = String(Int(Date().timeIntervalSince1970))
  let queryItems = [
    ("auth_key", "app-key"),
    ("auth_timestamp", timestamp),
    ("auth_version", "1.0"),
    ("body_md5", bodyMD5),
  ]
  let canonicalQuery =
    queryItems
    .sorted { $0.0 < $1.0 }
    .map { "\($0)=\($1)" }
    .joined(separator: "&")
  let stringToSign = "POST\n\(path)\n\(canonicalQuery)"
  let signature = sha256HMAC(stringToSign, secret: "app-secret")

  guard
    let url = URL(
      string: "http://127.0.0.1:6001\(path)?\(canonicalQuery)&auth_signature=\(signature)")
  else {
    throw TimeoutError()
  }

  var request = URLRequest(url: url)
  request.httpMethod = "POST"
  request.httpBody = body
  request.setValue("application/json", forHTTPHeaderField: "Content-Type")

  let (_, response) = try await URLSession.shared.data(for: request)
  let statusCode = (response as? HTTPURLResponse)?.statusCode ?? 0
  #expect(statusCode == 200 || statusCode == 202)
}

private enum ReceiveTimeoutError: Error {
  case timedOut
}

private final class ContinuationGate: @unchecked Sendable {
  private let lock = NSLock()
  private var resolved = false

  func run(_ body: () -> Void) {
    lock.lock()
    defer { lock.unlock() }
    guard !resolved else { return }
    resolved = true
    body()
  }
}

private func liveSockudoURL(protocolVersion: Int) -> URL {
  var components = URLComponents()
  components.scheme = "ws"
  components.host = "127.0.0.1"
  components.port = 6001
  components.path = "/app/app-key"
  var queryItems = [
    URLQueryItem(name: "protocol", value: "\(protocolVersion)"),
    URLQueryItem(name: "client", value: "swift-e2e"),
    URLQueryItem(name: "version", value: "1.0.0"),
  ]
  if protocolVersion == 2 {
    queryItems.append(URLQueryItem(name: "format", value: liveWireFormat().queryValue))
  }
  components.queryItems = queryItems
  return components.url!
}

private func receiveMessage(
  from task: URLSessionWebSocketTask,
  timeout: TimeInterval
) async throws -> URLSessionWebSocketTask.Message {
  try await withCheckedThrowingContinuation { continuation in
    let gate = ContinuationGate()
    let timeoutTask = Task {
      try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
      gate.run {
        continuation.resume(throwing: ReceiveTimeoutError.timedOut)
      }
    }

    task.receive { result in
      gate.run {
        timeoutTask.cancel()
        continuation.resume(with: result)
      }
    }
  }
}

private func sendJSON(
  _ payload: [String: Any],
  to task: URLSessionWebSocketTask
) async throws {
  let data = try JSONSerialization.data(withJSONObject: payload, options: [])
  let text = String(decoding: data, as: UTF8.self)
  try await task.send(.string(text))
}

private func decodedEvent(
  _ message: URLSessionWebSocketTask.Message
) throws -> SockudoEvent {
  try ProtocolCodec.decodeEvent(message, format: .json)
}

private func requestBodyData(_ request: URLRequest) -> Data? {
  if let body = request.httpBody {
    return body
  }
  guard let stream = request.httpBodyStream else {
    return nil
  }

  stream.open()
  defer { stream.close() }

  let bufferSize = 1024
  var data = Data()
  let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
  defer { buffer.deallocate() }

  while stream.hasBytesAvailable {
    let read = stream.read(buffer, maxLength: bufferSize)
    if read <= 0 {
      break
    }
    data.append(buffer, count: read)
  }

  return data.isEmpty ? nil : data
}

@Test
func filterValidationAcceptsNestedFilters() {
  let filter = Filter.or(
    .init(key: "sport", cmp: "eq", val: "football"),
    Filter.and(
      Filter.eq("type", "goal"),
      Filter.gte("xg", "0.8")
    )
  )

  #expect(validateFilter(filter) == nil)
}

@Test
func filterValidationRejectsInvalidNotNode() {
  let invalid = FilterNode(op: "not", nodes: [])
  #expect(validateFilter(invalid) == "NOT operation requires exactly one child node, got 0")
}

@Test
func deltaSettingsSerializeAsExpected() {
  #expect(ChannelDeltaSettings(enabled: true).subscriptionValue() as? Bool == true)
  #expect(ChannelDeltaSettings(enabled: false).subscriptionValue() as? Bool == false)
  #expect(ChannelDeltaSettings(algorithm: .fossil).subscriptionValue() as? String == "fossil")
  #expect(SubscriptionRewind.count(10).subscriptionValue() as? Int == 10)
  #expect((SubscriptionRewind.seconds(30).subscriptionValue() as? [String: Int])?["seconds"] == 30)
}

@Test
func presenceHistoryParamsNormalizeAblyAliases() {
  let payload = PresenceHistoryParams(
    direction: "newest_first",
    limit: 50,
    start: 1000,
    end: 2000
  ).payload

  #expect(payload["direction"] as? String == "newest_first")
  #expect(payload["limit"] as? Int == 50)
  #expect(payload["start_time_ms"] as? Int64 == 1000)
  #expect(payload["end_time_ms"] as? Int64 == 2000)
}

@Test
func presenceHistoryPageNextUsesNextCursor() async throws {
  let cursor = Box<String>()
  let page = PresenceHistoryPage(
    items: [],
    direction: "newest_first",
    limit: 50,
    hasMore: true,
    nextCursor: "cursor-2",
    bounds: .init(startSerial: nil, endSerial: nil, startTimeMS: nil, endTimeMS: nil),
    continuity: .init(
      streamID: nil,
      oldestAvailableSerial: nil,
      newestAvailableSerial: nil,
      oldestAvailablePublishedAtMS: nil,
      newestAvailablePublishedAtMS: nil,
      retainedEvents: 0,
      retainedBytes: 0,
      degraded: false,
      complete: true,
      truncatedByRetention: false
    ),
    fetchNext: { next, completion in
      cursor.value = next
      completion(
        .success(
          PresenceHistoryPage(
            items: [],
            direction: "newest_first",
            limit: 50,
            hasMore: false,
            nextCursor: nil,
            bounds: .init(startSerial: nil, endSerial: nil, startTimeMS: nil, endTimeMS: nil),
            continuity: .init(
              streamID: nil,
              oldestAvailableSerial: nil,
              newestAvailableSerial: nil,
              oldestAvailablePublishedAtMS: nil,
              newestAvailablePublishedAtMS: nil,
              retainedEvents: 0,
              retainedBytes: 0,
              degraded: false,
              complete: true,
              truncatedByRetention: false
            ),
            fetchNext: nil
          )
        ))
    }
  )

  try await withCheckedThrowingContinuation { continuation in
    page.next { result in
      switch result {
      case .success:
        continuation.resume()
      case .failure(let error):
        continuation.resume(throwing: error)
      }
    }
  }

  #expect(cursor.value == "cursor-2")
}

@Test
func annotationRequestPayloadUsesProxyShape() {
  let payload = PublishAnnotationRequest(
    type: "reactions:distinct.v1",
    name: "like",
    count: 2,
    data: ["emoji": .string("thumbs-up")],
    clientID: "client-1",
    extras: ["source": .string("ios")],
    idempotencyKey: "anno-1"
  ).payload

  #expect(payload["type"] as? String == "reactions:distinct.v1")
  #expect(payload["name"] as? String == "like")
  #expect(payload["count"] as? Int == 2)
  #expect((payload["data"] as? [String: Any])?["emoji"] as? String == "thumbs-up")
  #expect(payload["clientId"] as? String == "client-1")
  #expect((payload["extras"] as? [String: Any])?["source"] as? String == "ios")
  #expect(payload["idempotencyKey"] as? String == "anno-1")
}

@Test
func annotationEventsPageNextUsesNextCursor() {
  let cursor = Box<String>()
  let page = AnnotationEventsPage(
    items: [],
    direction: "oldest_first",
    limit: 10,
    hasMore: true,
    nextCursor: "anno-cursor-2",
    fetchNext: { next, completion in
      cursor.value = next
      completion(
        .success(
          AnnotationEventsPage(
            items: [],
            direction: "oldest_first",
            limit: 10,
            hasMore: false,
            nextCursor: nil,
            fetchNext: nil
          )
        ))
    }
  )

  page.next { _ in }
  #expect(cursor.value == "anno-cursor-2")
}

@Test
func pushProxyHelpersUseBackendEndpointAndAsyncPublishDefaults() async throws {
  let requests = Box<[URLRequest]>()
  requests.value = []

  let configuration = URLSessionConfiguration.ephemeral
  configuration.protocolClasses = [PushURLProtocol.self]
  let session = URLSession(configuration: configuration)
  defer {
    PushURLProtocol.requestHandler = nil
    session.invalidateAndCancel()
  }

  PushURLProtocol.requestHandler = { request in
    requests.value?.append(request)
    let path = request.url?.path ?? ""
    let payload: [String: Any]
    let status: Int
    if path.hasSuffix("/publish") {
      payload = ["publish_id": "pub_123"]
      status = 202
    } else {
      payload = ["items": [], "has_more": false]
      status = 200
    }
    let response = HTTPURLResponse(
      url: request.url!,
      statusCode: status,
      httpVersion: nil,
      headerFields: ["Content-Type": "application/json"])!
    let data = try JSONSerialization.data(withJSONObject: payload, options: [])
    return (response, data)
  }

  let client = SockudoPushRegistration(
    options: .init(
      endpoint: "https://api.example.test/push/",
      headers: ["Authorization": "Bearer session"]
    ),
    urlSession: session
  )

  let publishBox = Box<Result<[String: Any], Error>>()
  client.publish(
    [
      "recipients": [["type": "channel", "channel": "orders"]],
      "payload": ["title": "Order", "body": "Updated"],
    ]
  ) { result in
    publishBox.value = result
  }
  let publish = try await waitForValue { publishBox.value }.get()

  let updateBox = Box<Result<[String: Any], Error>>()
  client.updateDeviceRegistration(
    [
      "id": "device-1",
      "formFactor": "phone",
      "platform": "ios",
      "timezone": "UTC",
      "locale": "en",
      "push": ["recipient": ["transportType": "apns", "deviceToken": "rotated"]],
    ],
    deviceIdentityToken: "identity"
  ) { result in
    updateBox.value = result
  }
  _ = try await waitForValue { updateBox.value }.get()

  let pageBox = Box<Result<[String: Any], Error>>()
  client.listChannelSubscriptions(
    params: .init(deviceID: "device-1", limit: 10, cursor: "c1")
  ) { result in
    pageBox.value = result
  }
  let page = try await waitForValue { pageBox.value }.get()

  #expect(publish["publish_id"] as? String == "pub_123")
  #expect((page["items"] as? [Any])?.isEmpty == true)

  let publishRequest = try #require(requests.value?[0])
  let updateRequest = try #require(requests.value?[1])
  let listRequest = try #require(requests.value?[2])

  #expect(publishRequest.url?.absoluteString == "https://api.example.test/push/publish")
  #expect(publishRequest.httpMethod == "POST")
  let publishBody = try #require(requestBodyData(publishRequest))
  let publishJSON = try #require(try JSON.decode(publishBody) as? [String: Any])
  #expect(publishJSON["sync"] as? Bool == false)
  #expect(publishRequest.value(forHTTPHeaderField: "Authorization") == "Bearer session")

  #expect(updateRequest.url?.absoluteString == "https://api.example.test/push/deviceRegistrations")
  #expect(updateRequest.value(forHTTPHeaderField: "X-Sockudo-Device-Identity-Token") == "identity")

  #expect(
    listRequest.url?.absoluteString
      == "https://api.example.test/push/channelSubscriptions?deviceId=device-1&limit=10&cursor=c1"
  )
}

@Test
func channelExposesAnnotationInternalEvents() throws {
  let client = try SockudoClient(
    "app-key",
    options: .init(cluster: "local", protocolVersion: 2)
  )
  let channel = client.subscribe("chat")
  let summary = Box<[String: Any]>()
  let raw = Box<[String: Any]>()

  _ = channel.bind("message.summary") { data, _ in
    summary.value = data as? [String: Any]
  }
  _ = channel.bind("annotation.create") { data, _ in
    raw.value = data as? [String: Any]
  }

  channel.handle(
    event: SockudoEvent(
      event: "sockudo_internal:message",
      channel: "chat",
      data: ["action": "message.summary", "messageSerial": "msg-1"],
      userID: nil,
      streamID: nil,
      messageId: nil,
      rawMessage: "",
      sequence: nil,
      conflationKey: nil,
      serial: nil,
      extras: nil
    ))
  channel.handle(
    event: SockudoEvent(
      event: "sockudo_internal:annotation",
      channel: "chat",
      data: ["action": "annotation.create", "messageSerial": "msg-1"],
      userID: nil,
      streamID: nil,
      messageId: nil,
      rawMessage: "",
      sequence: nil,
      conflationKey: nil,
      serial: nil,
      extras: nil
    ))

  #expect(summary.value?["messageSerial"] as? String == "msg-1")
  #expect(raw.value?["action"] as? String == "annotation.create")
}

@Test
func websocketURLIncludesV2FormatQuery() throws {
  let client = try SockudoClient(
    "app-key",
    options: .init(
      cluster: "local",
      protocolVersion: 2,
      forceTLS: false,
      enabledTransports: [.ws],
      wsHost: "ws.example.com",
      wsPort: 6001,
      wssPort: 6002,
      wireFormat: .messagepack
    )
  )

  let url = try client.socketURL(for: .ws)
  let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
  let queryItems: [URLQueryItem] = components?.queryItems ?? []
  let query = Dictionary(
    uniqueKeysWithValues: queryItems.map { ($0.name, $0.value ?? "") })

  #expect(query["protocol"] == "2")
  #expect(query["format"] == "messagepack")
}

@Test
func websocketURLUsesV1ByDefaultAndOmitsFormatQuery() throws {
  let client = try SockudoClient(
    "app-key",
    options: .init(
      cluster: "local",
      forceTLS: false,
      enabledTransports: [.ws],
      wsHost: "ws.example.com",
      wsPort: 6001,
      wssPort: 6002,
      wireFormat: .messagepack
    )
  )

  let url = try client.socketURL(for: .ws)
  let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
  let query = Dictionary(
    uniqueKeysWithValues: (components?.queryItems ?? []).map { ($0.name, $0.value ?? "") })

  #expect(query["protocol"] == "7")
  #expect(query["format"] == nil)
}

@Test
func messagePackRoundTrip() throws {
  let payload = try ProtocolCodec.encodeEnvelope(
    [
      "event": "sockudo:test",
      "channel": "chat:room-1",
      "data": [
        "hello": "world",
        "count": 3,
      ],
      "stream_id": "stream-1",
      "message_id": "msg-1",
      "serial": 7,
      "__delta_seq": 7,
      "__conflation_key": "room",
    ],
    format: .messagepack
  )
  let message: URLSessionWebSocketTask.Message =
    switch payload {
    case .string(let text): .string(text)
    case .data(let data): .data(data)
    }

  let decoded = try ProtocolCodec.decodeEvent(message, format: .messagepack)

  #expect(decoded.event == "sockudo:test")
  #expect(decoded.channel == "chat:room-1")
  #expect((decoded.data as? [String: Any])?["hello"] as? String == "world")
  #expect(((decoded.data as? [String: Any])?["count"] as? NSNumber)?.intValue == 3)
  #expect(decoded.streamID == "stream-1")
  #expect(decoded.messageId == "msg-1")
  #expect(decoded.serial == 7)
  #expect(decoded.sequence == 7)
  #expect(decoded.conflationKey == "room")
}

@Test
func protobufRoundTrip() throws {
  let payload = try ProtocolCodec.encodeEnvelope(
    [
      "event": "sockudo:test",
      "channel": "chat:room-1",
      "data": [
        "hello": "world"
      ],
      "stream_id": "stream-2",
      "message_id": "msg-2",
      "serial": 9,
      "__delta_seq": 11,
      "__conflation_key": "btc",
      "extras": [
        "headers": [
          "region": "eu",
          "ttl": 5,
          "replay": true,
        ],
        "echo": false,
      ],
    ],
    format: .protobuf
  )
  let message: URLSessionWebSocketTask.Message =
    switch payload {
    case .string(let text): .string(text)
    case .data(let data): .data(data)
    }

  let decoded = try ProtocolCodec.decodeEvent(message, format: .protobuf)

  #expect(decoded.event == "sockudo:test")
  #expect(decoded.channel == "chat:room-1")
  #expect((decoded.data as? [String: Any])?["hello"] as? String == "world")
  #expect(decoded.streamID == "stream-2")
  #expect(decoded.messageId == "msg-2")
  #expect(decoded.serial == 9)
  #expect(decoded.sequence == 11)
  #expect(decoded.conflationKey == "btc")
  #expect(decoded.extras?.headers?["region"] == .string("eu"))
  #expect(decoded.extras?.headers?["ttl"] == .int(5))
  #expect(decoded.extras?.headers?["replay"] == .bool(true))
  #expect(decoded.extras?.echo == false)
}

@Test
func localSockudoIntegrationConnectsAndReceivesPublishedEvent() async throws {
  guard ProcessInfo.processInfo.environment["SOCKUDO_LIVE_TESTS"] == "1" else {
    return
  }

  let connected = Box<Bool>()
  let subscribed = Box<Bool>()
  let received = Box<[String: Any]>()

  let client = try SockudoClient(
    "app-key",
    options: .init(
      cluster: "local",
      protocolVersion: 2,
      forceTLS: false,
      enabledTransports: [.ws],
      wsHost: "127.0.0.1",
      wsPort: 6001,
      wssPort: 6001,
      wireFormat: liveWireFormat()
    )
  )

  let channel = client.subscribe("public-updates")
  client.bind("connected") { _, _ in
    connected.value = true
  }
  channel.bind("sockudo:subscription_succeeded") { _, _ in
    subscribed.value = true
  }
  channel.bind("integration-event") { data, _ in
    received.value = data as? [String: Any]
  }

  client.connect()

  _ = try await waitForValue { connected.value }
  _ = try await waitForValue { subscribed.value }

  try await publishToLocalSockudo(
    channel: "public-updates",
    eventName: "integration-event",
    payload: [
      "message": "hello from test",
      "item_id": "swift-client",
      "padding": String(repeating: "x", count: 140),
    ]
  )

  let payload = try await waitForValue(timeout: 8) { received.value }
  #expect(payload["message"] as? String == "hello from test")
  client.disconnect()
}

@Test
func liveV2HeartbeatUsesControlFramesOnIdle() async throws {
  guard ProcessInfo.processInfo.environment["SOCKUDO_LIVE_TESTS"] == "1" else {
    return
  }

  let session = URLSession(configuration: .default)
  let task = session.webSocketTask(with: liveSockudoURL(protocolVersion: 2))
  task.resume()
  defer {
    task.cancel(with: .normalClosure, reason: nil)
    session.invalidateAndCancel()
  }

  let handshake = try decodedEvent(try await receiveMessage(from: task, timeout: 3))
  #expect(handshake.event == "sockudo:connection_established")

  do {
    let unexpected = try await receiveMessage(from: task, timeout: 8)
    let event = try decodedEvent(unexpected)
    Issue.record("Expected no protocol heartbeat messages on idle V2 connection, got \(event.event)")
  } catch ReceiveTimeoutError.timedOut {
    // Expected: control-frame heartbeats are not surfaced as normal protocol messages.
  }
}

@Test
func liveV2FallbackPongHasNoMetadata() async throws {
  guard ProcessInfo.processInfo.environment["SOCKUDO_LIVE_TESTS"] == "1" else {
    return
  }

  let session = URLSession(configuration: .default)
  let task = session.webSocketTask(with: liveSockudoURL(protocolVersion: 2))
  task.resume()
  defer {
    task.cancel(with: .normalClosure, reason: nil)
    session.invalidateAndCancel()
  }

  let handshake = try decodedEvent(try await receiveMessage(from: task, timeout: 3))
  #expect(handshake.event == "sockudo:connection_established")

  try await sendJSON(["event": "sockudo:ping", "data": [:]], to: task)
  let pong = try decodedEvent(try await receiveMessage(from: task, timeout: 3))
  #expect(pong.event == "sockudo:pong")
  #expect(pong.messageId == nil)
  #expect(pong.serial == nil)
  #expect(pong.streamID == nil)
}

@Test
func liveV1HeartbeatStillUsesProtocolPing() async throws {
  guard ProcessInfo.processInfo.environment["SOCKUDO_LIVE_TESTS"] == "1" else {
    return
  }

  let session = URLSession(configuration: .default)
  let task = session.webSocketTask(with: liveSockudoURL(protocolVersion: 7))
  task.resume()
  defer {
    task.cancel(with: .normalClosure, reason: nil)
    session.invalidateAndCancel()
  }

  let handshake = try decodedEvent(try await receiveMessage(from: task, timeout: 3))
  #expect(handshake.event == "pusher:connection_established")

  let ping = try decodedEvent(try await receiveMessage(from: task, timeout: 6))
  #expect(ping.event == "pusher:ping")

  try await sendJSON(["event": "pusher:pong", "data": [:]], to: task)

  do {
    _ = try await receiveMessage(from: task, timeout: 1.5)
  } catch ReceiveTimeoutError.timedOut {
    // Connection remained open without immediate timeout close, which is what we want.
  }
}
