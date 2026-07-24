import Foundation
import Sodium

@SockudoActor public class Channel: Sendable {
  public let name: String
  unowned let client: SockudoClient
  let dispatcher: EventDispatcher
  var isSubscribed = false
  var subscriptionPending = false
  var subscriptionCancelled = false
  var subscriptionCount: Int?
  public internal(set) var attachSerial: Int64?
  var tagsFilter: FilterNode?
  var deltaSettings: ChannelDeltaSettings?
  var eventsFilter: [String]?
  var expressionFilter: SubscriptionExpression?
  var rewind: SubscriptionRewind?
  var annotationSubscribe = false

  private struct QueuedClientEvent {
    let name: String
    let data: Any
  }

  private var unsentEvents: [QueuedClientEvent] = []
  private let maxBufferedEvents = 50

  var bufferedEventCount: Int { unsentEvents.count }

  init(name: String, client: SockudoClient) {
    self.name = name
    self.client = client
    self.dispatcher = EventDispatcher { event, _ in
      Logger.debug("No callbacks on \(name) for \(event)")
    }
  }

  @discardableResult
  public func on(
    _ eventName: String, callback: @escaping @SockudoActor (Any?, EventMetadata?) -> Void
  )
    -> EventBindingToken
  {
    dispatcher.bind(eventName, callback: callback)
  }

  @discardableResult
  public func bind(
    _ eventName: String, callback: @escaping @SockudoActor (Any?, EventMetadata?) -> Void
  )
    -> EventBindingToken
  {
    on(eventName, callback: callback)
  }

  @discardableResult
  public func onGlobal(_ callback: @escaping @SockudoActor (String, Any?) -> Void)
    -> EventBindingToken
  {
    dispatcher.bindGlobal(callback)
  }

  @discardableResult
  public func bindGlobal(_ callback: @escaping @SockudoActor (String, Any?) -> Void)
    -> EventBindingToken
  {
    onGlobal(callback)
  }

  public func off(eventName: String? = nil, token: EventBindingToken? = nil) {
    dispatcher.unbind(eventName: eventName, token: token)
  }

  public func unbind(eventName: String? = nil, token: EventBindingToken? = nil) {
    off(eventName: eventName, token: token)
  }

  public func unbindAll() {
    dispatcher.unbind()
  }

  public func setDeltaSettings(_ settings: ChannelDeltaSettings?) {
    deltaSettings = settings
  }

  public func trigger(event: String, data: Any) throws -> Bool {
    guard event.hasPrefix("client-") else {
      throw SockudoError.badEventName("Event '\(event)' does not start with 'client-'")
    }
    if isSubscribed {
      return (try? client.sendEvent(name: event, data: data, channel: name)) ?? false
    } else {
      if unsentEvents.count >= maxBufferedEvents {
        unsentEvents.removeFirst()
        Logger.warn("Client event buffer full for channel '\(name)', dropping oldest event")
      }
      unsentEvents.append(QueuedClientEvent(name: event, data: data))
      return true
    }
  }

  fileprivate func drainUnsentEvents() {
    while !unsentEvents.isEmpty {
      let queued = unsentEvents.removeFirst()
      _ = try? client.sendEvent(name: queued.name, data: queued.data, channel: name)
    }
  }

  func clearUnsentEvents() {
    unsentEvents.removeAll()
  }

  func authorize(
    socketID: String,
    completion: @escaping @Sendable (Result<ChannelAuthorizationData, Error>) -> Void
  ) {
    completion(
      .success(ChannelAuthorizationData(auth: "", channelData: nil, sharedSecret: nil)))
  }

  func subscribeIfPossible() {
    if subscriptionPending, subscriptionCancelled {
      subscriptionCancelled = false
    } else if subscriptionPending == false, client.connectionState == .connected {
      subscribe()
    }
  }

  func subscribe() {
    guard isSubscribed == false else { return }
    subscriptionPending = true
    subscriptionCancelled = false
    authorize(socketID: client.socketID ?? "") { [weak self] result in
      Task { @SockudoActor in
        guard let self else { return }
        switch result {
        case .failure(let error):
          self.subscriptionPending = false
          self.dispatcher.emit(
            self.client.p.event("subscription_error"),
            data: [
              "type": "AuthError",
              "error": error.localizedDescription,
            ])
        case .success(let data):
          var payload: [String: Any] = [
            "auth": data.auth,
            "channel": self.name,
          ]
          if let channelData = data.channelData {
            payload["channel_data"] = channelData
          }
          if self.eventsFilter != nil || self.expressionFilter != nil {
            var filterPayload: [String: Any] = [:]
            if let eventsFilter = self.eventsFilter {
              filterPayload["events"] = eventsFilter
            }
            if let filter = self.tagsFilter, let filterJSON = try? JSON.encodeData(filter),
              let tags = try? JSON.decode(filterJSON)
            {
              filterPayload["tags"] = tags
            }
            if let expression = self.expressionFilter {
              filterPayload["expression"] = expression.subscriptionValue
            }
            payload["filter"] = filterPayload
          } else if let filter = self.tagsFilter, let filterJSON = try? JSON.encodeData(filter),
            let json = try? JSON.decode(filterJSON)
          {
            payload["tags_filter"] = json
          }
          if let deltaSettings = self.deltaSettings {
            payload["delta"] = deltaSettings.subscriptionValue()
          }
          if let rewind = self.rewind {
            payload["rewind"] = rewind.subscriptionValue()
          }
          if self.annotationSubscribe {
            payload["modes"] = ["SUBSCRIBE", "ANNOTATION_SUBSCRIBE"]
          }
          do {
            _ = try self.client.sendEvent(
              name: self.client.p.event("subscribe"), data: payload, channel: nil)
          } catch {
            self.subscriptionPending = false
            self.dispatcher.emit(
              self.client.p.event("subscription_error"),
              data: [
                "type": "ConnectionError",
                "error": error.localizedDescription,
              ])
          }
        }
      }
    }
  }

  func unsubscribe() {
    isSubscribed = false
    attachSerial = nil
    _ = try? client.sendEvent(
      name: client.p.event("unsubscribe"), data: ["channel": name], channel: nil)
  }

  func disconnect() {
    isSubscribed = false
    subscriptionPending = false
    attachSerial = nil
  }

  func handle(event: SockudoEvent) {
    let p = client.p
    if event.event == p.internal("subscription_succeeded") {
      subscriptionPending = false
      isSubscribed = true
      attachSerial = Self.extractAttachSerial(from: event.data)
      if subscriptionCancelled {
        client.unsubscribe(name)
      } else {
        drainUnsentEvents()
        dispatcher.emit(p.event("subscription_succeeded"), data: event.data)
      }
    } else if event.event == p.internal("subscription_count") {
      if let data = event.data as? [String: Any],
        let count = data["subscription_count"] as? Int
      {
        subscriptionCount = count
      }
      dispatcher.emit(p.event("subscription_count"), data: event.data)
    } else if event.event == p.internal("message"),
      let data = event.data as? [String: Any],
      data["action"] as? String == "message.summary"
    {
      dispatcher.emit("message.summary", data: data, metadata: EventMetadata(userID: event.userID))
    } else if event.event == p.internal("annotation"),
      let data = event.data as? [String: Any],
      let action = data["action"] as? String
    {
      dispatcher.emit(action, data: data, metadata: EventMetadata(userID: event.userID))
    } else if p.isInternalEvent(event.event) {
      dispatcher.emit(
        event.event, data: event.data, metadata: EventMetadata(userID: event.userID))
    } else if p.isInternalEvent(event.event) == false {
      dispatcher.emit(
        event.event, data: event.data, metadata: EventMetadata(userID: event.userID))
    }
  }

  static func extractAttachSerial(from data: Any?) -> Int64? {
    guard let payload = data as? [String: Any] else { return nil }
    return wireInt64(payload["attach_serial"] ?? payload["attachSerial"])
  }

  public func publishAnnotation(
    messageSerial: String,
    annotation: PublishAnnotationRequest,
    completion: sending @escaping @Sendable (Result<PublishAnnotationResponse, Error>) -> Void
  ) {
    client.publishAnnotation(
      channelName: name,
      messageSerial: messageSerial,
      annotation: annotation,
      completion: completion
    )
  }

  public func deleteAnnotation(
    messageSerial: String,
    annotationSerial: String,
    socketID: String? = nil,
    completion: sending @escaping @Sendable (Result<DeleteAnnotationResponse, Error>) -> Void
  ) {
    client.deleteAnnotation(
      channelName: name,
      messageSerial: messageSerial,
      annotationSerial: annotationSerial,
      socketID: socketID,
      completion: completion
    )
  }

  public func listAnnotations(
    messageSerial: String,
    params: AnnotationEventsParams = .init(),
    completion: sending @escaping @Sendable (Result<AnnotationEventsPage, Error>) -> Void
  ) {
    client.listAnnotations(
      channelName: name,
      messageSerial: messageSerial,
      params: params,
      completion: completion
    )
  }

  public func channelHistory(
    _ params: ChannelHistoryParams = .init(),
    completion: sending @escaping @Sendable (Result<ChannelHistoryPage, Error>) -> Void
  ) {
    client.fetchChannelHistory(channelName: name, params: params, completion: completion)
  }

  public func getMessage(
    _ messageSerial: String,
    completion: sending @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    client.fetchLatestMessage(
      channelName: name, messageSerial: messageSerial, completion: completion)
  }

  public func getMessageVersions(
    _ messageSerial: String,
    params: MessageVersionsParams = .init(),
    completion: sending @escaping @Sendable (Result<MessageVersionsPage, Error>) -> Void
  ) {
    client.fetchMessageVersions(
      channelName: name,
      messageSerial: messageSerial,
      params: params,
      completion: completion
    )
  }

  public func createMessage(
    _ request: VersionedMessageCreateRequest,
    completion: sending @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    client.createVersionedMessage(
      channelName: name,
      request: request,
      completion: completion
    )
  }

  public func updateMessage(
    _ messageSerial: String,
    request: VersionedMessageUpdateRequest,
    completion: sending @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    client.updateVersionedMessage(
      channelName: name,
      messageSerial: messageSerial,
      request: request,
      completion: completion
    )
  }

  public func appendMessage(
    _ messageSerial: String,
    request: VersionedMessageAppendRequest,
    completion: sending @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    client.appendVersionedMessage(
      channelName: name,
      messageSerial: messageSerial,
      request: request,
      completion: completion
    )
  }

  public func deleteMessage(
    _ messageSerial: String,
    request: VersionedMessageDeleteRequest = .init(),
    completion: sending @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    client.deleteVersionedMessage(
      channelName: name,
      messageSerial: messageSerial,
      request: request,
      completion: completion
    )
  }
}

@SockudoActor public class PrivateChannel: Channel {
  override func authorize(
    socketID: String,
    completion: @escaping @Sendable (Result<ChannelAuthorizationData, Error>) -> Void
  ) {
    client.config.channelAuthorizer(
      ChannelAuthorizationRequest(socketID: socketID, channelName: name),
      completion
    )
  }
}

@SockudoActor public final class PresenceMembers: Sendable {
  private(set) var members: [String: AnyHashable] = [:]
  public private(set) var count = 0
  public private(set) var myID: String?
  public private(set) var me: PresenceMember?

  public func member(id: String) -> PresenceMember? {
    guard let info = members[id] else { return nil }
    return PresenceMember(id: id, info: info)
  }

  public func forEach(_ body: (PresenceMember) -> Void) {
    for (id, info) in members {
      body(PresenceMember(id: id, info: info))
    }
  }

  func setMyID(_ id: String) {
    myID = id
  }

  func applySubscriptionData(_ data: [String: Any]) {
    let presence = data["presence"] as? [String: Any]
    let hash = presence?["hash"] as? [String: Any] ?? [:]
    members = hash.compactMapValues(Self.normalizedInfo)
    count = presence?["count"] as? Int ?? hash.count
    if let myID {
      me = member(id: myID)
    }
  }

  func add(_ data: [String: Any]) -> PresenceMember? {
    guard let userID = data["user_id"] as? String,
      let info = Self.normalizedInfo(data["user_info"])
    else { return nil }
    if members[userID] == nil {
      count += 1
    }
    members[userID] = info
    return PresenceMember(id: userID, info: info)
  }

  func update(_ data: [String: Any]) -> PresenceMember? {
    guard let userID = data["user_id"] as? String,
      members[userID] != nil,
      let info = Self.normalizedInfo(data["user_info"])
    else { return nil }
    members[userID] = info
    if userID == myID {
      me = PresenceMember(id: userID, info: info)
    }
    return PresenceMember(id: userID, info: info)
  }

  func remove(_ data: [String: Any]) -> PresenceMember? {
    guard let userID = data["user_id"] as? String,
      let info = members.removeValue(forKey: userID)
    else {
      return nil
    }
    count = max(0, count - 1)
    return PresenceMember(id: userID, info: info)
  }

  func reset() {
    members.removeAll()
    count = 0
    myID = nil
    me = nil
  }

  private static func normalizedInfo(_ value: Any?) -> AnyHashable? {
    if let jsonValue = SockudoJSONValue.from(value) {
      switch jsonValue {
      case .object, .array, .null:
        return AnyHashable(jsonValue)
      case .string, .int, .double, .bool:
        break
      }
    }
    if let value = value as? AnyHashable {
      return value
    }
    guard let jsonValue = SockudoJSONValue.from(value) else {
      return nil
    }
    return AnyHashable(jsonValue)
  }
}

@SockudoActor public final class PresenceChannel: PrivateChannel {
  public let members = PresenceMembers()

  override func authorize(
    socketID: String,
    completion: @escaping @Sendable (Result<ChannelAuthorizationData, Error>) -> Void
  ) {
    super.authorize(socketID: socketID) { [weak self] result in
      Task { @SockudoActor in
        guard let self else { return }
        switch result {
        case .failure:
          completion(result)
        case .success(let data):
          if let channelData = data.channelData,
            let channelDataAny = try? JSON.decodeString(channelData),
            let dictionary = channelDataAny as? [String: Any],
            let userID = dictionary["user_id"] as? String
          {
            self.members.setMyID(userID)
            completion(.success(data))
            return
          }

          if let userID = self.client.user.userID {
            self.members.setMyID(userID)
            completion(.success(data))
          } else {
            completion(
              .failure(
                SockudoError.authFailure(
                  statusCode: nil,
                  message: "Invalid auth response for presence channel '\(self.name)'"
                )))
          }
        }
      }
    }
  }

  override func handle(event: SockudoEvent) {
    let p = client.p
    if event.event == p.internal("subscription_succeeded") {
      subscriptionPending = false
      isSubscribed = true
      attachSerial = Channel.extractAttachSerial(from: event.data)
      if subscriptionCancelled {
        client.unsubscribe(name)
      } else if let data = event.data as? [String: Any] {
        drainUnsentEvents()
        members.applySubscriptionData(data)
        dispatcher.emit(p.event("subscription_succeeded"), data: members)
      }
    } else if event.event == p.internal("subscription_count") {
      super.handle(event: event)
    } else if event.event == p.internal("member_added") {
      if let data = event.data as? [String: Any], let member = members.add(data) {
        dispatcher.emit(p.event("member_added"), data: member)
      }
    } else if event.event == p.internal("member_removed") {
      if let data = event.data as? [String: Any], let member = members.remove(data) {
        dispatcher.emit(p.event("member_removed"), data: member)
      }
    } else if event.event == p.internal("presence_update") {
      if let data = event.data as? [String: Any], let member = members.update(data) {
        dispatcher.emit(p.event("presence_update"), data: member)
      }
    } else if event.event == p.internal("message"),
      let data = event.data as? [String: Any],
      data["action"] as? String == "message.summary"
    {
      dispatcher.emit("message.summary", data: data, metadata: EventMetadata(userID: event.userID))
    } else if event.event == p.internal("annotation"),
      let data = event.data as? [String: Any],
      let action = data["action"] as? String
    {
      dispatcher.emit(action, data: data, metadata: EventMetadata(userID: event.userID))
    } else if p.isInternalEvent(event.event) {
      dispatcher.emit(
        event.event, data: event.data, metadata: EventMetadata(userID: event.userID))
    } else if p.isInternalEvent(event.event) == false {
      dispatcher.emit(
        event.event, data: event.data, metadata: EventMetadata(userID: event.userID))
    }
  }

  override func disconnect() {
    members.reset()
    super.disconnect()
  }

  public func history(
    _ params: PresenceHistoryParams = .init(),
    completion: sending @escaping @Sendable (Result<PresenceHistoryPage, Error>) -> Void
  ) {
    client.fetchPresenceHistory(channelName: name, params: params, completion: completion)
  }

  override public func channelHistory(
    _ params: ChannelHistoryParams = .init(),
    completion: sending @escaping @Sendable (Result<ChannelHistoryPage, Error>) -> Void
  ) {
    client.fetchChannelHistory(channelName: name, params: params, completion: completion)
  }

  override public func getMessage(
    _ messageSerial: String,
    completion: sending @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    client.fetchLatestMessage(
      channelName: name, messageSerial: messageSerial, completion: completion)
  }

  override public func getMessageVersions(
    _ messageSerial: String,
    params: MessageVersionsParams = .init(),
    completion: sending @escaping @Sendable (Result<MessageVersionsPage, Error>) -> Void
  ) {
    client.fetchMessageVersions(
      channelName: name,
      messageSerial: messageSerial,
      params: params,
      completion: completion
    )
  }

  public func snapshot(
    _ params: PresenceSnapshotParams = .init(),
    completion: sending @escaping @Sendable (Result<PresenceSnapshot, Error>) -> Void
  ) {
    client.fetchPresenceSnapshot(channelName: name, params: params, completion: completion)
  }

  public func update(data: Any) throws -> Bool {
    guard client.config.protocolVersion == 2 else {
      throw SockudoError.unsupportedFeature("presence.update(data:) requires Protocol V2")
    }
    return try client.sendEvent(
      name: client.p.event("presence_update"),
      data: [
        "channel": name,
        "data": data,
      ],
      channel: nil
    )
  }
}

@SockudoActor public final class EncryptedChannel: PrivateChannel {
  private var sharedSecret: Bytes?
  private let sodium = Sodium()

  override func authorize(
    socketID: String,
    completion: @escaping @Sendable (Result<ChannelAuthorizationData, Error>) -> Void
  ) {
    super.authorize(socketID: socketID) { [weak self] result in
      Task { @SockudoActor in
        guard let self else { return }
        switch result {
        case .failure:
          completion(result)
        case .success(let data):
          guard let secret = data.sharedSecret, let decoded = Data(base64Encoded: secret)
          else {
            completion(
              .failure(
                SockudoError.authFailure(
                  statusCode: nil,
                  message:
                    "No shared_secret key in auth payload for encrypted channel: \(self.name)"
                )))
            return
          }
          self.sharedSecret = Array(decoded)
          completion(
            .success(
              ChannelAuthorizationData(
                auth: data.auth, channelData: data.channelData, sharedSecret: nil)))
        }
      }
    }
  }

  override public func trigger(event: String, data: Any) throws -> Bool {
    throw SockudoError.unsupportedFeature(
      "Client events are not currently supported for encrypted channels")
  }

  override func handle(event: SockudoEvent) {
    let p = client.p
    if p.isInternalEvent(event.event) || p.isPlatformEvent(event.event) {
      super.handle(event: event)
      return
    }
    guard
      let secret = sharedSecret,
      let payload = event.data as? [String: Any],
      let ciphertext = payload["ciphertext"] as? String,
      let nonce = payload["nonce"] as? String,
      let ciphertextData = Data(base64Encoded: ciphertext),
      let nonceData = Data(base64Encoded: nonce)
    else {
      Logger.error("Unexpected format for encrypted event on \(name)")
      return
    }

    guard
      let decrypted = sodium.secretBox.open(
        authenticatedCipherText: Array(ciphertextData), secretKey: secret,
        nonce: Array(nonceData))
    else {
      super.authorize(socketID: client.socketID ?? "") { [weak self] result in
        Task { @SockudoActor in
          guard let self else { return }
          if case .success(let authData) = result,
            let sharedSecret = authData.sharedSecret,
            let refreshedData = Data(base64Encoded: sharedSecret)
          {
            self.sharedSecret = Array(refreshedData)
            self.handle(event: event)
          }
        }
      }
      return
    }

    let bytes = Data(decrypted)
    let value = (try? JSON.decode(bytes)) ?? String(decoding: bytes, as: UTF8.self)
    dispatcher.emit(event.event, data: value)
  }
}
