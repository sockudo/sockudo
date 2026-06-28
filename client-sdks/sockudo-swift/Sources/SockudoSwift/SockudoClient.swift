import Foundation
import Network

@MainActor public final class SockudoClient: Sendable {
  public struct Options {
    public var cluster: String
    public var protocolVersion: Int
    public var activityTimeout: TimeInterval
    public var forceTLS: Bool?
    public var enabledTransports: [Transport]?
    public var disabledTransports: [Transport]?
    public var wsHost: String?
    public var wsPort: Int
    public var wssPort: Int
    public var wsPath: String
    public var httpHost: String?
    public var httpPort: Int
    public var httpsPort: Int
    public var httpPath: String
    public var pongTimeout: TimeInterval
    public var unavailableTimeout: TimeInterval
    public var enableStats: Bool
    public var statsHost: String
    public var timelineParams: [String: AuthValue]
    public var channelAuthorization: ChannelAuthorizationOptions
    public var userAuthentication: UserAuthenticationOptions
    public var presenceHistory: PresenceHistoryOptions?
    public var versionedMessages: VersionedMessagesOptions?
    public var capabilityToken: CapabilityTokenOptions?
    public var deltaCompression: DeltaOptions?
    public var messageDeduplication: Bool
    public var messageDeduplicationCapacity: Int
    public var connectionRecovery: Bool
    public var echoMessages: Bool
    public var wireFormat: SockudoWireFormat
    public var appendMode: SockudoAppendMode
    public var appendRollupWindow: Int?
    public var maxReconnectAttempts: Int? = 6
    public var maxReconnectGapInSeconds: Double = 120

    public init(
      cluster: String,
      protocolVersion: Int = 7,
      activityTimeout: TimeInterval = 120,
      forceTLS: Bool? = nil,
      enabledTransports: [Transport]? = nil,
      disabledTransports: [Transport]? = nil,
      wsHost: String? = nil,
      wsPort: Int = 80,
      wssPort: Int = 443,
      wsPath: String = "",
      httpHost: String? = nil,
      httpPort: Int = 80,
      httpsPort: Int = 443,
      httpPath: String = "/sockudo",
      pongTimeout: TimeInterval = 30,
      unavailableTimeout: TimeInterval = 10,
      enableStats: Bool = false,
      statsHost: String = "stats.sockudo.com",
      timelineParams: [String: AuthValue] = [:],
      channelAuthorization: ChannelAuthorizationOptions = .init(),
      userAuthentication: UserAuthenticationOptions = .init(),
      presenceHistory: PresenceHistoryOptions? = nil,
      versionedMessages: VersionedMessagesOptions? = nil,
      capabilityToken: CapabilityTokenOptions? = nil,
      deltaCompression: DeltaOptions? = nil,
      messageDeduplication: Bool = true,
      messageDeduplicationCapacity: Int = 1000,
      connectionRecovery: Bool = false,
      echoMessages: Bool = true,
      wireFormat: SockudoWireFormat = .json,
      appendMode: SockudoAppendMode = .delta,
      appendRollupWindow: Int? = nil,
      maxReconnectAttempts: Int? = 6,
      maxReconnectGapInSeconds: Double = 120
    ) {
      self.cluster = cluster
      self.protocolVersion = protocolVersion
      self.activityTimeout = activityTimeout
      self.forceTLS = forceTLS
      self.enabledTransports = enabledTransports
      self.disabledTransports = disabledTransports
      self.wsHost = wsHost
      self.wsPort = wsPort
      self.wssPort = wssPort
      self.wsPath = wsPath
      self.httpHost = httpHost
      self.httpPort = httpPort
      self.httpsPort = httpsPort
      self.httpPath = httpPath
      self.pongTimeout = pongTimeout
      self.unavailableTimeout = unavailableTimeout
      self.enableStats = enableStats
      self.statsHost = statsHost
      self.timelineParams = timelineParams
      self.channelAuthorization = channelAuthorization
      self.userAuthentication = userAuthentication
      self.presenceHistory = presenceHistory
      self.versionedMessages = versionedMessages
      self.capabilityToken = capabilityToken
      self.deltaCompression = deltaCompression
      self.messageDeduplication = messageDeduplication
      self.messageDeduplicationCapacity = messageDeduplicationCapacity
      self.connectionRecovery = connectionRecovery
      self.echoMessages = echoMessages
      self.wireFormat = wireFormat
      self.appendMode = appendMode
      self.appendRollupWindow = appendRollupWindow
      self.maxReconnectAttempts = maxReconnectAttempts
      self.maxReconnectGapInSeconds = maxReconnectGapInSeconds
    }
  }

  public static var logToConsole: Bool {
    get { Logger.logToConsole }
    set { Logger.logToConsole = newValue }
  }

  public static var logHandler: ((String) -> Void)? {
    get { Logger.customLog }
    set { Logger.customLog = newValue }
  }

  public let key: String
  let p: ProtocolPrefix
  nonisolated let pipeline: MessagePipeline
  private nonisolated let messageProcessor: MessageProcessor
  var config: ResolvedConfiguration
  public private(set) var connectionState: ConnectionState = .initialized
  public private(set) var socketID: String?
  public private(set) var capabilityTokenAuth: CapabilityTokenAuthData?
  public private(set) var lastCapabilityTokenExpired: CapabilityTokenExpiredData?
  public let user: UserFacade
  public let watchlist: WatchlistFacade

  let dispatcher = EventDispatcher()
  private var channels: [String: Channel] = [:]
  private let urlSession: URLSession
  private let webSocketDelegate = WebSocketDelegate()
  private var webSocketTask: URLSessionWebSocketTask?
  private var activityTimer: Timer?
  private var unavailableTimer: Timer?
  private var retryTimer: Timer?
  private var timelineSenderTimer: Timer?
  private var capabilityTokenRefreshTimer: Timer?
  private let reachability = ReachabilityMonitor()
  private var channelPositions: [String: RecoveryPosition] = [:]
  private var timeline = Timeline()
  private var currentTransport: Transport?
  private var attemptedFallback = false
  private var manuallyDisconnected = false
  private var terminalEventHandled = false
  private var tokenRequestInFlight = false
  var reconnectAttempts = 0
  private var cachedDeltaStats: DeltaStats?

  public init(_ key: String, options: Options, urlSession: URLSession? = nil) throws {
    guard key.isEmpty == false else { throw SockudoError.invalidAppKey }
    guard options.cluster.isEmpty == false else {
      throw SockudoError.invalidOptions("Options must provide a cluster")
    }
    guard Self.isAllowedAppendRollupWindow(options.appendRollupWindow) else {
      throw SockudoError.invalidOptions(
        "appendRollupWindow must be one of 0, 20, 40, 100, or 500 milliseconds"
      )
    }

    let p = ProtocolPrefix(version: options.protocolVersion)
    let pipeline = MessagePipeline(wireFormat: options.wireFormat)
    self.key = key
    self.p = p
    self.pipeline = pipeline
    self.messageProcessor = MessageProcessor(
      pipeline: pipeline,
      prefix: p,
      deduplicationCapacity: options.messageDeduplication ? options.messageDeduplicationCapacity : nil,
      deltaOptions: options.deltaCompression
    )
    self.config = ResolvedConfiguration(options: options)
    self.user = UserFacade()
    self.watchlist = WatchlistFacade()
    let configuration = URLSessionConfiguration.default
    configuration.waitsForConnectivity = true
    self.urlSession =
      urlSession
      ?? URLSession(
        configuration: configuration, delegate: webSocketDelegate, delegateQueue: nil)

    reachability.stateDidChange = { [weak self] isOnline in
      Task { @MainActor in
        guard let self else { return }
        if isOnline {
          if self.connectionState == .connecting || self.connectionState == .unavailable {
            self.scheduleRetry(after: 0)
          }
        } else if self.webSocketTask != nil {
          self.sendPing()
        }
      }
    }

    user.attach(client: self)
    watchlist.attach(client: self)
  }

  private static func isAllowedAppendRollupWindow(_ value: Int?) -> Bool {
    guard let value else { return true }
    return [0, 20, 40, 100, 500].contains(value)
  }

  static func capabilityTokenRefreshDelay(for token: String, now: Date = Date()) -> TimeInterval? {
    guard let claims = capabilityTokenClaims(token),
      let exp = wireInt64(claims["exp"])
    else {
      return nil
    }

    let nowSeconds = now.timeIntervalSince1970
    let expSeconds = TimeInterval(exp)
    let refreshAt: TimeInterval
    if let iat = wireInt64(claims["iat"]) {
      let iatSeconds = TimeInterval(iat)
      let lifetime = expSeconds - iatSeconds
      guard lifetime > 0 else { return nil }
      refreshAt = iatSeconds + (lifetime * 0.8)
    } else {
      let remaining = expSeconds - nowSeconds
      guard remaining > 0 else { return 0 }
      refreshAt = nowSeconds + (remaining * 0.8)
    }

    return max(0, refreshAt - nowSeconds)
  }

  private static func capabilityTokenClaims(_ token: String) -> [String: Any]? {
    let parts = token.split(separator: ".", omittingEmptySubsequences: false)
    guard parts.count >= 2,
      let data = base64URLDecodedData(String(parts[1])),
      let object = try? JSON.decode(data) as? [String: Any]
    else {
      return nil
    }
    return object
  }

  private static func base64URLDecodedData(_ value: String) -> Data? {
    var encoded = value
      .replacingOccurrences(of: "-", with: "+")
      .replacingOccurrences(of: "_", with: "/")
    let padding = encoded.count % 4
    if padding > 0 {
      encoded += String(repeating: "=", count: 4 - padding)
    }
    return Data(base64Encoded: encoded)
  }

  deinit {
    // Timer cleanup is handled by disconnect(). The timers hold [weak self]
    // so any stragglers become no-ops once this instance is released.
    // NWPathMonitor.cancel() and URLSession invalidation are safe from any
    // context and must run here to release system resources.
    reachability.stop()
    urlSession.invalidateAndCancel()
  }

  @discardableResult
  public func on(_ eventName: String, callback: @escaping (Any?, EventMetadata?) -> Void)
    -> EventBindingToken
  {
    dispatcher.bind(eventName, callback: callback)
  }

  @discardableResult
  public func bind(_ eventName: String, callback: @escaping (Any?, EventMetadata?) -> Void)
    -> EventBindingToken
  {
    on(eventName, callback: callback)
  }

  @discardableResult
  public func onGlobal(_ callback: @escaping (String, Any?) -> Void) -> EventBindingToken {
    dispatcher.bindGlobal(callback)
  }

  @discardableResult
  public func bindGlobal(_ callback: @escaping (String, Any?) -> Void) -> EventBindingToken {
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

  public func channel(named name: String) -> Channel? {
    channels[name]
  }

  public func allChannels() -> [Channel] {
    channels.values.sorted { $0.name < $1.name }
  }

  public func subscribe(_ name: String, options: SubscriptionOptions? = nil) -> Channel {
    let channel = channels[name] ?? makeChannel(named: name)
    channels[name] = channel
    if let options {
      channel.tagsFilter = options.filter
      channel.deltaSettings = options.delta
      channel.eventsFilter = options.events
      channel.rewind = options.rewind
      channel.annotationSubscribe = options.annotationSubscribe
    }
    channel.subscribeIfPossible()
    return channel
  }

  public func subscribe(_ name: String, filter: FilterNode) -> Channel {
    subscribe(name, options: SubscriptionOptions(filter: filter))
  }

  public func unsubscribe(_ name: String) {
    if let channel = channels[name], channel.subscriptionPending {
      channel.subscriptionCancelled = true
    } else if let channel = channels.removeValue(forKey: name) {
      channel.clearUnsentEvents()
      if channel.isSubscribed {
        channel.unsubscribe()
      }
    }
    channelPositions.removeValue(forKey: name)
    Task { [messageProcessor] in await messageProcessor.clearDeltaState(name) }
  }

  public func connect() {
    guard webSocketTask == nil, tokenRequestInFlight == false else { return }
    let transports = transportSequence()
    guard transports.isEmpty == false else {
      updateState(.failed)
      return
    }
    manuallyDisconnected = false
    terminalEventHandled = false
    attemptedFallback = false
    reconnectAttempts = 0
    updateState(.connecting)
    openWebSocketAfterInitialAuth(using: transports[0])
    setUnavailableTimer()
  }

  public func disconnect() {
    manuallyDisconnected = true
    terminalEventHandled = true
    reconnectAttempts = 0
    invalidateTimers()
    webSocketTask?.cancel(with: .normalClosure, reason: nil)
    webSocketTask = nil
    currentTransport = nil
    for channel in channels.values {
      channel.disconnect()
    }
    updateState(.disconnected)
  }

  private func openWebSocketAfterInitialAuth(using transport: Transport) {
    guard config.protocolVersion == 2,
      config.capabilityToken == nil,
      config.capabilityTokenProvider != nil
    else {
      if config.protocolVersion == 2, let token = config.capabilityToken {
        scheduleCapabilityTokenRefreshIfNeeded(for: token)
      }
      openWebSocket(using: transport)
      return
    }

    requestCapabilityToken { [weak self] result in
      Task { @MainActor in
        guard let self else { return }
        switch result {
        case .success:
          self.openWebSocket(using: transport)
        case .failure(let error):
          self.clearUnavailableTimer()
          self.updateState(.failed)
          self.dispatcher.emit("error", data: error)
        }
      }
    }
  }

  public var shouldUseTLS: Bool {
    config.useTLS
  }

  public func signIn() {
    user.signIn()
  }

  public func getDeltaStats() -> DeltaStats? {
    cachedDeltaStats
  }

  public func resetDeltaStats() {
    cachedDeltaStats = nil
    Task { [messageProcessor] in await messageProcessor.resetDeltaStats() }
  }

  public func recoveryPosition(for channelName: String) -> RecoveryPosition? {
    channelPositions[channelName]
  }

  public func recoveryPositions() -> [String: RecoveryPosition] {
    channelPositions
  }

  public func setRecoveryPosition(_ position: RecoveryPosition?, for channelName: String) {
    channelPositions[channelName] = position
  }

  public func setRecoveryPositions(_ positions: [String: RecoveryPosition]) {
    channelPositions = positions
  }

  func sendEvent(name: String, data: Any, channel: String?) throws -> Bool {
    guard let webSocketTask else { return false }
    var envelope: [String: Any] = [
      "event": name,
      "data": data,
    ]
    if let channel {
      envelope["channel"] = channel
    }
    let payload = try ProtocolCodec.encodeEnvelope(envelope, format: config.wireFormat)
    let message: URLSessionWebSocketTask.Message =
      switch payload {
      case .string(let text): .string(text)
      case .data(let data): .data(data)
      }
    webSocketTask.send(message) { error in
      if let error {
        Task { @MainActor in
          Logger.error("Send failed", error.localizedDescription)
        }
      }
    }
    return true
  }

  private func subscribeAll() {
    for channel in channels.values {
      channel.subscribeIfPossible()
    }
  }

  private func makeChannel(named name: String) -> Channel {
    if name.hasPrefix("private-encrypted-") {
      return EncryptedChannel(name: name, client: self)
    }
    if name.hasPrefix("private-") {
      return PrivateChannel(name: name, client: self)
    }
    if name.hasPrefix("presence-") {
      return PresenceChannel(name: name, client: self)
    }
    if name.hasPrefix("#") {
      Logger.error("Cannot create a channel with name '\(name)'")
    }
    return Channel(name: name, client: self)
  }

  private func openWebSocket(using transport: Transport) {
    do {
      currentTransport = transport
      let url = try socketURL(for: transport)
      let task = urlSession.webSocketTask(with: url)
      webSocketDelegate.didOpen = { [weak self] in
        Task { @MainActor in self?.readNextMessage() }
      }
      webSocketDelegate.didClose = { [weak self] code, reason in
        Task { @MainActor in
          self?.handleSocketClosed(code: code, reason: reason)
        }
      }
      webSocketTask = task
      task.resume()
    } catch {
      updateState(.failed)
      dispatcher.emit("error", data: error)
    }
  }

  private func requestCapabilityToken(
    completion: @escaping @Sendable (Result<String, Error>) -> Void
  ) {
    guard let provider = config.capabilityTokenProvider else {
      if let token = config.capabilityToken, token.isEmpty == false {
        completion(.success(token))
      } else {
        completion(
          .failure(
            SockudoError.invalidOptions(
              "capabilityToken.token or capabilityToken.provider must provide a non-empty token"
            )))
      }
      return
    }

    guard tokenRequestInFlight == false else { return }
    tokenRequestInFlight = true
    provider { [weak self] result in
      Task { @MainActor in
        guard let self else { return }
        self.tokenRequestInFlight = false
        switch result {
        case .success(let token) where token.isEmpty == false:
          self.config.capabilityToken = token
          self.scheduleCapabilityTokenRefreshIfNeeded(for: token)
          completion(.success(token))
        case .success:
          completion(
            .failure(
              SockudoError.invalidOptions("capabilityToken.provider returned an empty token")))
        case .failure(let error):
          completion(.failure(error))
        }
      }
    }
  }

  private func readNextMessage() {
    webSocketTask?.receive { [weak self, messageProcessor] result in
      guard let self else { return }
      switch result {
      case .failure(let error):
        Task { @MainActor in
          self.dispatcher.emit("error", data: error)
          self.handleSocketClosed(code: .abnormalClosure, reason: error.localizedDescription)
        }
      case .success(let message):
        Task {
          do {
            guard let processed = try await messageProcessor.process(message) else {
              await MainActor.run { self.readNextMessage() }
              return
            }
            await MainActor.run {
              self.deliverMessage(processed)
              self.readNextMessage()
            }
          } catch {
            await MainActor.run {
              self.dispatcher.emit("error", data: error)
              self.readNextMessage()
            }
          }
        }
      }
    }
  }

  func deliverMessage(_ message: ProcessedMessage) {
    if config.connectionRecovery, let update = message.recoveryUpdate {
      channelPositions[update.channel] = update.position
    }
    if let resyncChannel = message.resyncChannel {
      _ = try? sendEvent(name: p.event("delta_sync_error"), data: ["channel": resyncChannel], channel: nil)
    }
    if let stats = message.updatedDeltaStats {
      cachedDeltaStats = stats
    }
    resetActivityTimer()

    do {
      let event = message.event
      let eventName = event.event
      if eventName == p.event("connection_established") {
        guard let payload = event.data as? [String: Any],
          let socketID = payload["socket_id"] as? String
        else {
          throw SockudoError.invalidHandshake
        }
        self.socketID = socketID
        let negotiatedTimeout =
          (payload["activity_timeout"] as? Double ?? config.activityTimeout) * 1000
        config.activityTimeout =
          min(config.activityTimeout * 1000, negotiatedTimeout) / 1000
        reconnectAttempts = 0
        clearUnavailableTimer()
        updateState(.connected, metadata: ["socket_id": socketID])
        reachability.start()
        subscribeAll()
        if config.connectionRecovery, channelPositions.isEmpty == false {
          let positionsPayload: [String: Any] = [
            "channel_positions": Dictionary(
              uniqueKeysWithValues: channelPositions.map { channel, position in
                (
                  channel,
                  [
                    "serial": position.serial,
                    "stream_id": position.streamID as Any,
                    "last_message_id": position.lastMessageID as Any,
                  ].compactMapValues { $0 }
                )
              })
          ]
          if let jsonData = try? JSON.encodeString(positionsPayload) {
            _ = try? sendEvent(name: p.event("resume"), data: jsonData, channel: nil)
          }
        }
        if config.enableStats {
          startTimelineSender()
        }
        if let deltaOptions = config.deltaOptions {
          let payload: [String: Any] = ["algorithms": deltaOptions.algorithms.map(\.rawValue)]
          _ = try? sendEvent(name: p.event("enable_delta_compression"), data: payload, channel: nil)
        }
        user.handleConnected()
      } else {
        if eventName == p.event("error") {
          dispatcher.emit("error", data: event.data)
        } else if eventName == p.event("ping") {
          _ = try? sendEvent(name: p.event("pong"), data: [:], channel: nil)
        } else if eventName == p.event("pong") {
        } else if eventName == p.event("signin_success") {
          user.handleSignInSuccess(event.data)
        } else if eventName == p.event("auth_success") {
          capabilityTokenAuth = Self.decodeCapabilityTokenAuthData(event.data)
          dispatcher.emit(eventName, data: event.data)
        } else if eventName == p.event("token_expired") {
          let expired = Self.decodeCapabilityTokenExpiredData(event.data)
          lastCapabilityTokenExpired = expired
          dispatcher.emit(eventName, data: event.data)
          if expired.code == 40142 || expired.code == 40160 {
            refreshCapabilityToken()
          }
        } else if eventName == p.internal("watchlist_events") {
          watchlist.handle(event.data)
        } else if eventName == p.event("delta_compression_enabled") {
          dispatcher.emit(eventName, data: event.data)
        } else if eventName == p.event("delta_cache_sync") {
        } else if eventName == p.event("resume_success") {
          let data = Self.decodeResumeSuccessData(event.data)
          Logger.debug("Connection recovery succeeded", data)
          dispatcher.emit(eventName, data: data)
        } else if eventName == p.event("resume_failed") {
          let failData = Self.decodeResumeFailedData(event.data)
          if failData.channel.isEmpty == false {
            channelPositions.removeValue(forKey: failData.channel)
            Logger.warn("Connection recovery failed for channel", failData.channel)
            channels[failData.channel]?.subscribeIfPossible()
          }
          dispatcher.emit(eventName, data: failData)
        } else {
          let normalizedEvent =
            eventName == p.event("rewind_complete")
            ? SockudoEvent(
              event: event.event,
              channel: event.channel,
              data: Self.decodeRewindCompleteData(event.data),
              userID: event.userID,
              streamID: event.streamID,
              messageId: event.messageId,
              rawMessage: event.rawMessage,
              sequence: event.sequence,
              conflationKey: event.conflationKey,
              serial: event.serial,
              extras: event.extras
            ) : event
          if let channelName = normalizedEvent.channel {
            channels[channelName]?.handle(event: normalizedEvent)
          }
          if shouldEmitClientEvent(eventName) {
            dispatcher.emit(
              eventName, data: normalizedEvent.data,
              metadata: EventMetadata(userID: normalizedEvent.userID)
            )
          }
        }
      }
    } catch {
      dispatcher.emit("error", data: error)
    }
  }

  func handle(rawMessage: URLSessionWebSocketTask.Message) {
    guard let event = try? pipeline.decode(rawMessage) else { return }
    deliverMessage(ProcessedMessage(event: event, recoveryUpdate: nil, resyncChannel: nil, updatedDeltaStats: nil))
  }

  private func refreshCapabilityToken() {
    guard config.protocolVersion == 2, config.capabilityTokenProvider != nil else { return }
    invalidateCapabilityTokenRefreshTimer()
    requestCapabilityToken { [weak self] result in
      Task { @MainActor in
        guard let self else { return }
        switch result {
        case .success(let token):
          do {
            let sent = try self.sendEvent(
              name: self.p.event("auth"),
              data: ["token": token],
              channel: nil
            )
            if sent == false {
              self.dispatcher.emit("error", data: SockudoError.connectionUnavailable)
            }
          } catch {
            self.dispatcher.emit("error", data: error)
          }
        case .failure(let error):
          self.dispatcher.emit("error", data: error)
        }
      }
    }
  }

  private func shouldEmitClientEvent(_ eventName: String) -> Bool {
    if p.isInternalEvent(eventName) == false {
      return true
    }
    return isKnownInternalEvent(eventName) == false
  }

  private func isKnownInternalEvent(_ eventName: String) -> Bool {
    eventName == p.internal("subscription_succeeded")
      || eventName == p.internal("subscription_count")
      || eventName == p.internal("member_added")
      || eventName == p.internal("member_removed")
      || eventName == p.internal("message")
      || eventName == p.internal("annotation")
      || eventName == p.internal("watchlist_events")
      || eventName == p.internal("presence_update")
  }

  private func handleSocketClosed(code: URLSessionWebSocketTask.CloseCode, reason: String?) {
    guard !terminalEventHandled else { return }
    terminalEventHandled = true
    invalidateActivityTimer()
    clearUnavailableTimer()
    webSocketTask = nil
    for channel in channels.values {
      channel.disconnect()
    }

    let action = closeAction(for: code)
    switch action {
    case .tlsOnly:
      config.useTLS = true
      scheduleRetry(after: reconnectDelay(for: action))
    case .backoff:
      scheduleRetry(after: reconnectDelay(for: action))
    case .retry:
      scheduleRetry(after: reconnectDelay(for: action))
    case .refused:
      updateState(.disconnected)
    case .none:
      if manuallyDisconnected == false {
        scheduleRetry(after: reconnectDelay(for: nil))
      }
    }

    if let reason, reason.isEmpty == false {
      dispatcher.emit("error", data: SockudoError.connectionUnavailable)
      Logger.warn("Socket closed", code.rawValue, reason)
    }
  }

  func reconnectDelay(for action: CloseAction?) -> TimeInterval {
    if action == .retry { return 0 }  // close codes 4200-4299: reconnect immediately
    if action == .tlsOnly { return 0 }  // TLS upgrade: reconnect immediately
    let interval = Double(reconnectAttempts * reconnectAttempts)
    return min(interval, config.maxReconnectGapInSeconds)
  }

  private func closeAction(for code: URLSessionWebSocketTask.CloseCode) -> CloseAction? {
    let value = Int(code.rawValue)
    if value < 4000 {
      if (1002...1004).contains(value) { return .backoff }
      return nil
    }
    if value == 4000 { return .tlsOnly }
    if value < 4100 { return .refused }
    if value < 4200 { return .backoff }
    if value < 4300 { return .retry }
    return .refused
  }

  func socketURL(for transport: Transport) throws -> URL {
    let scheme = transport == .wss ? "wss" : "ws"
    let host = config.wsHost
    let port = transport == .wss ? config.wssPort : config.wsPort
    let path = "\(config.wsPath)/app/\(key)"
    var components = URLComponents()
    components.scheme = scheme
    components.host = host
    components.port = port
    components.path = path
    var queryItems: [URLQueryItem] = [
      .init(name: "protocol", value: "\(p.version)"),
      .init(name: "client", value: "swift"),
      .init(name: "version", value: "2.1.0"),
      .init(name: "flash", value: "false"),
    ]
    if config.protocolVersion == 2 {
      queryItems.append(.init(name: "format", value: config.wireFormat.queryValue))
      queryItems.append(.init(name: "append_mode", value: config.appendMode.queryValue))
      if let appendRollupWindow = config.appendRollupWindow {
        queryItems.append(.init(name: "append_rollup_window", value: "\(appendRollupWindow)"))
      }
      if let token = config.capabilityToken, token.isEmpty == false {
        queryItems.append(.init(name: "token", value: token))
      }
    }
    components.queryItems = queryItems
    guard let url = components.url else {
      throw SockudoError.invalidURL("Unable to build WebSocket URL")
    }
    return url
  }

  private func transportSequence() -> [Transport] {
    var transports = config.useTLS ? [Transport.wss] : [Transport.ws, .wss]
    if let enabled = config.enabledTransports {
      transports = transports.filter { enabled.contains($0) }
    }
    if let disabled = config.disabledTransports {
      transports.removeAll { disabled.contains($0) }
    }
    return transports
  }

  private func sendPing() {
    if config.protocolVersion == 2, let task = webSocketTask {
      invalidateActivityTimer()
      activityTimer = Timer.scheduledTimer(withTimeInterval: config.pongTimeout, repeats: false) {
        [weak self] _ in
        Task { @MainActor in
          self?.scheduleRetry(after: 0)
        }
      }
      task.sendPing { [weak self] error in
        Task { @MainActor in
          guard let self else { return }
          if error != nil {
            self.scheduleRetry(after: 0)
            return
          }
          self.resetActivityTimer()
        }
      }
      return
    }

    _ = try? sendEvent(name: p.event("ping"), data: [:], channel: nil)
    invalidateActivityTimer()
    activityTimer = Timer.scheduledTimer(withTimeInterval: config.pongTimeout, repeats: false) {
      [weak self] _ in
      Task { @MainActor in
        self?.scheduleRetry(after: 0)
      }
    }
  }

  private func resetActivityTimer() {
    invalidateActivityTimer()
    activityTimer = Timer.scheduledTimer(
      withTimeInterval: config.activityTimeout, repeats: false
    ) { [weak self] _ in
      Task { @MainActor in
        self?.sendPing()
      }
    }
  }

  private func invalidateActivityTimer() {
    activityTimer?.invalidate()
    activityTimer = nil
  }

  private func setUnavailableTimer() {
    clearUnavailableTimer()
    unavailableTimer = Timer.scheduledTimer(
      withTimeInterval: config.unavailableTimeout, repeats: false
    ) { [weak self] _ in
      Task { @MainActor in
        self?.updateState(.unavailable)
      }
    }
  }

  private func clearUnavailableTimer() {
    unavailableTimer?.invalidate()
    unavailableTimer = nil
  }

  func scheduleRetry(after seconds: TimeInterval) {
    guard manuallyDisconnected == false else { return }
    if let max = config.maxReconnectAttempts, reconnectAttempts >= max {
      updateState(.disconnected)
      return
    }
    reconnectAttempts += 1
    retryTimer?.invalidate()
    retryTimer = Timer.scheduledTimer(withTimeInterval: seconds, repeats: false) {
      [weak self] _ in
      Task { @MainActor in
        guard let self else { return }
        self.terminalEventHandled = false
        self.webSocketTask?.cancel(with: .goingAway, reason: nil)
        self.webSocketTask = nil
        self.updateState(.reconnecting)
        let transports = self.transportSequence()
        if self.currentTransport == .ws, self.attemptedFallback == false,
          transports.contains(.wss)
        {
          self.attemptedFallback = true
          self.openWebSocket(using: .wss)
        } else {
          self.attemptedFallback = false
          self.openWebSocket(using: transports.first ?? .wss)
        }
        self.setUnavailableTimer()
      }
    }
  }

  private func invalidateTimers() {
    invalidateActivityTimer()
    clearUnavailableTimer()
    retryTimer?.invalidate()
    retryTimer = nil
    timelineSenderTimer?.invalidate()
    timelineSenderTimer = nil
    invalidateCapabilityTokenRefreshTimer()
  }

  func scheduleCapabilityTokenRefreshIfNeeded(for token: String, now: Date = Date()) {
    invalidateCapabilityTokenRefreshTimer()
    guard config.protocolVersion == 2,
      config.capabilityTokenProvider != nil,
      let delay = Self.capabilityTokenRefreshDelay(for: token, now: now)
    else { return }

    let timer = Timer(timeInterval: max(0.001, delay), repeats: false) { [weak self] _ in
      Task { @MainActor in
        self?.refreshCapabilityToken()
      }
    }
    capabilityTokenRefreshTimer = timer
    RunLoop.main.add(timer, forMode: .common)
  }

  var hasCapabilityTokenRefreshTimer: Bool {
    capabilityTokenRefreshTimer != nil
  }

  private func invalidateCapabilityTokenRefreshTimer() {
    capabilityTokenRefreshTimer?.invalidate()
    capabilityTokenRefreshTimer = nil
  }

  private func updateState(_ state: ConnectionState, metadata: [String: Any]? = nil) {
    let previous = connectionState
    connectionState = state
    dispatcher.emit(
      "state_change", data: ["previous": previous.rawValue, "current": state.rawValue])
    dispatcher.emit(state.rawValue, data: metadata)
  }

  private func startTimelineSender() {
    timelineSenderTimer?.invalidate()
    timelineSenderTimer = Timer.scheduledTimer(withTimeInterval: 60, repeats: true) {
      [weak self] _ in
      Task { @MainActor in
        self?.sendTimeline()
      }
    }
    sendTimeline()
  }

  private func sendTimeline() {
    guard timeline.isEmpty == false else { return }
    var payload = timeline.payload(key: key, cluster: config.cluster)
    for (key, value) in config.timelineParams {
      payload[key] = value.stringValue
    }
    var components = URLComponents()
    components.scheme = config.useTLS ? "https" : "http"
    components.host = config.statsHost
    components.path = "/timeline/v2/fetch/2"
    components.percentEncodedQuery =
      payload
      .map { key, value in
        "\(key)=\(value.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? value)"
      }
      .sorted()
      .joined(separator: "&")
    guard let url = components.url else { return }
    urlSession.dataTask(with: url).resume()
    timeline.markSent()
  }
}

@MainActor public final class UserFacade: Sendable {
  private(set) weak var client: SockudoClient?
  private let dispatcher = EventDispatcher { event, _ in
    Logger.debug("No callbacks on user for \(event)")
  }
  public private(set) var isSignInRequested = false
  public private(set) var userData: [String: Any]?
  public var userID: String? { userData?["id"] as? String }
  fileprivate var serverChannel: Channel?

  init() {}

  func attach(client: SockudoClient) {
    self.client = client
  }

  @discardableResult
  public func on(_ eventName: String, callback: @escaping (Any?, EventMetadata?) -> Void)
    -> EventBindingToken
  {
    dispatcher.bind(eventName, callback: callback)
  }

  public func signIn() {
    isSignInRequested = true
    attemptSignIn()
  }

  func handleConnected() {
    attemptSignIn()
  }

  func handleSignInSuccess(_ data: Any?) {
    guard let payload = data as? [String: Any],
      let userDataString = payload["user_data"] as? String
    else {
      cleanup()
      return
    }
    guard let parsed = try? JSON.decodeString(userDataString) as? [String: Any],
      let id = parsed["id"] as? String, id.isEmpty == false
    else {
      cleanup()
      return
    }
    userData = parsed
    subscribeServerChannel(userID: id)
  }

  private func attemptSignIn() {
    guard isSignInRequested, let client, client.connectionState == .connected,
      let socketID = client.socketID
    else { return }
    client.config.userAuthenticator(UserAuthenticationRequest(socketID: socketID)) {
      [weak self] result in
      Task { @MainActor in
        guard let self, let client = self.client else { return }
        switch result {
        case .failure:
          self.cleanup()
        case .success(let authData):
          _ = try? client.sendEvent(
            name: client.p.event("signin"),
            data: [
              "auth": authData.auth,
              "user_data": authData.userData,
            ], channel: nil)
        }
      }
    }
  }

  private func subscribeServerChannel(userID: String) {
    guard let client else { return }
    let channel = Channel(name: "#server-to-user-\(userID)", client: client)
    channel.onGlobal { [weak self] eventName, data in
      guard let self, let client = self.client else { return }
      guard client.p.isInternalEvent(eventName) == false,
        client.p.isPlatformEvent(eventName) == false
      else { return }
      self.dispatcher.emit(eventName, data: data)
    }
    serverChannel = channel
    channel.subscribeIfPossible()
  }

  private func cleanup() {
    userData = nil
    serverChannel?.unbindAll()
    serverChannel?.disconnect()
    serverChannel = nil
  }
}

@MainActor public final class WatchlistFacade: Sendable {
  private weak var client: SockudoClient?
  private let dispatcher = EventDispatcher { event, _ in
    Logger.debug("No callbacks on watchlist for \(event)")
  }

  init() {}

  func attach(client: SockudoClient) {
    self.client = client
  }

  @discardableResult
  public func on(_ eventName: String, callback: @escaping (Any?, EventMetadata?) -> Void)
    -> EventBindingToken
  {
    dispatcher.bind(eventName, callback: callback)
  }

  func handle(_ data: Any?) {
    guard let payload = data as? [String: Any],
      let events = payload["events"] as? [[String: Any]]
    else { return }
    for event in events {
      if let name = event["name"] as? String {
        dispatcher.emit(name, data: event)
      }
    }
  }
}

private extension SockudoClient {
  static func decodeCapabilityTokenAuthData(_ raw: Any?) -> CapabilityTokenAuthData {
    let payload = raw as? [String: Any] ?? [:]
    return CapabilityTokenAuthData(
      clientID: payload["client_id"] as? String ?? payload["clientId"] as? String,
      jti: payload["jti"] as? String,
      exp: wireInt64(payload["exp"])
    )
  }

  static func decodeCapabilityTokenExpiredData(_ raw: Any?) -> CapabilityTokenExpiredData {
    let payload = raw as? [String: Any] ?? [:]
    return CapabilityTokenExpiredData(
      code: wireInt64(payload["code"]).flatMap { Int(exactly: $0) },
      reason: payload["reason"] as? String
    )
  }

  static func decodeResumeSuccessData(_ raw: Any?) -> ResumeSuccessData {
    let payload = raw as? [String: Any] ?? [:]
    let recovered = (payload["recovered"] as? [[String: Any]] ?? []).map {
      ResumeRecoveredChannel(
        channel: $0["channel"] as? String ?? "",
        source: $0["source"] as? String ?? "",
        replayed: ($0["replayed"] as? NSNumber)?.intValue ?? 0
      )
    }
    let failed = (payload["failed"] as? [[String: Any]] ?? []).map(decodeResumeFailedData)
    return ResumeSuccessData(recovered: recovered, failed: failed)
  }

  static func decodeResumeFailedData(_ raw: [String: Any]) -> ResumeFailedChannel {
    ResumeFailedChannel(
      channel: raw["channel"] as? String ?? "",
      code: raw["code"] as? String ?? "",
      reason: raw["reason"] as? String ?? "",
      expectedStreamID: raw["expected_stream_id"] as? String,
      currentStreamID: raw["current_stream_id"] as? String,
      oldestAvailableSerial: (raw["oldest_available_serial"] as? NSNumber)?.intValue,
      newestAvailableSerial: (raw["newest_available_serial"] as? NSNumber)?.intValue
    )
  }

  static func decodeResumeFailedData(_ raw: Any?) -> ResumeFailedChannel {
    decodeResumeFailedData(raw as? [String: Any] ?? [:])
  }

  static func decodeRewindCompleteData(_ raw: Any?) -> RewindCompleteData {
    let payload = raw as? [String: Any] ?? [:]
    return RewindCompleteData(
      historicalCount: (payload["historical_count"] as? NSNumber)?.intValue ?? 0,
      liveCount: (payload["live_count"] as? NSNumber)?.intValue ?? 0,
      complete: payload["complete"] as? Bool ?? false,
      truncatedByRetention: payload["truncated_by_retention"] as? Bool ?? false,
      truncatedByLimit: payload["truncated_by_limit"] as? Bool ?? false
    )
  }
}

extension SockudoClient {
  struct ResolvedConfiguration {
    let cluster: String
    let protocolVersion: Int
    var activityTimeout: TimeInterval
    var useTLS: Bool
    let wireFormat: SockudoWireFormat
    let appendMode: SockudoAppendMode
    let appendRollupWindow: Int?
    var capabilityToken: String?
    let capabilityTokenProvider: CapabilityTokenProvider?
    let wsHost: String
    let wsPort: Int
    let wssPort: Int
    let wsPath: String
    let httpHost: String
    let httpPort: Int
    let httpsPort: Int
    let httpPath: String
    let pongTimeout: TimeInterval
    let unavailableTimeout: TimeInterval
    let enableStats: Bool
    let statsHost: String
    let timelineParams: [String: AuthValue]
    let enabledTransports: [Transport]?
    let disabledTransports: [Transport]?
    let channelAuthorizer: ChannelAuthorizationHandler
    let userAuthenticator: UserAuthenticationHandler
    let presenceHistory: PresenceHistoryOptions?
    let versionedMessages: VersionedMessagesOptions?
    let deltaCompressionEnabled: Bool
    let deltaOptions: DeltaOptions?
    let messageDeduplication: Bool
    let messageDeduplicationCapacity: Int
    let connectionRecovery: Bool
    let maxReconnectAttempts: Int?
    let maxReconnectGapInSeconds: Double

    init(options: Options) {
      cluster = options.cluster
      protocolVersion = options.protocolVersion
      activityTimeout = options.activityTimeout
      useTLS = options.forceTLS == false ? false : true
      wireFormat = options.wireFormat
      appendMode = options.appendMode
      appendRollupWindow = options.appendRollupWindow
      capabilityToken = options.capabilityToken?.token
      capabilityTokenProvider = options.capabilityToken?.provider
      wsHost = options.wsHost ?? "ws-\(options.cluster).sockudo.com"
      wsPort = options.wsPort
      wssPort = options.wssPort
      wsPath = options.wsPath
      httpHost = options.httpHost ?? "sockjs-\(options.cluster).sockudo.com"
      httpPort = options.httpPort
      httpsPort = options.httpsPort
      httpPath = options.httpPath
      pongTimeout = options.pongTimeout
      unavailableTimeout = options.unavailableTimeout
      enableStats = options.enableStats
      statsHost = options.statsHost
      timelineParams = options.timelineParams
      enabledTransports = options.enabledTransports
      disabledTransports = options.disabledTransports
      deltaCompressionEnabled = options.deltaCompression?.enabled == true
      deltaOptions = options.deltaCompression
      messageDeduplication = options.messageDeduplication
      messageDeduplicationCapacity = options.messageDeduplicationCapacity
      connectionRecovery = options.connectionRecovery
      maxReconnectAttempts = options.maxReconnectAttempts
      maxReconnectGapInSeconds = options.maxReconnectGapInSeconds
      channelAuthorizer =
        options.channelAuthorization.customHandler
        ?? Self.makeChannelAuthorizer(options.channelAuthorization)
      userAuthenticator =
        options.userAuthentication.customHandler
        ?? Self.makeUserAuthenticator(options.userAuthentication)
      presenceHistory = options.presenceHistory
      versionedMessages = options.versionedMessages
    }

    private static func makeChannelAuthorizer(_ options: ChannelAuthorizationOptions)
      -> ChannelAuthorizationHandler
    {
      { request, completion in
        Self.performAuthRequest(
          endpoint: options.endpoint,
          headers: options.headers.merging(
            options.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
          params: options.params.merging(
            options.paramsProvider?() ?? [:], uniquingKeysWith: { _, new in new }
          ).merging(
            [
              "socket_id": .string(request.socketID),
              "channel_name": .string(request.channelName),
            ], uniquingKeysWith: { _, new in new }),
          completion: completion
        )
      }
    }

    private static func makeUserAuthenticator(_ options: UserAuthenticationOptions)
      -> UserAuthenticationHandler
    {
      { request, completion in
        Self.performAuthRequest(
          endpoint: options.endpoint,
          headers: options.headers.merging(
            options.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
          params: options.params.merging(
            options.paramsProvider?() ?? [:], uniquingKeysWith: { _, new in new }
          ).merging(
            [
              "socket_id": .string(request.socketID)
            ], uniquingKeysWith: { _, new in new })
        ) { (result: Result<UserAuthenticationData, Error>) in
          completion(result)
        }
      }
    }

    private static func performAuthRequest<T>(
      endpoint: String,
      headers: [String: String],
      params: [String: AuthValue],
      completion: @escaping @Sendable (Result<T, Error>) -> Void
    ) where T: Sendable {
      guard let url = URL(string: endpoint, relativeTo: nil) ?? URL(string: endpoint) else {
        completion(.failure(SockudoError.invalidURL("Invalid auth endpoint \(endpoint)")))
        return
      }
      var request = URLRequest(url: url)
      request.httpMethod = "POST"
      request.httpBody = QueryString.encode(params)
      request.setValue(
        "application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
      for (name, value) in headers {
        request.setValue(value, forHTTPHeaderField: name)
      }
      URLSession.shared.dataTask(with: request) { data, response, error in
        Task { @MainActor in
          if let error {
            completion(.failure(error))
            return
          }
          let status = (response as? HTTPURLResponse)?.statusCode
          guard status == 200, let data else {
            completion(
              .failure(
                SockudoError.authFailure(
                  statusCode: status,
                  message:
                    "Could not get auth info from endpoint, status: \(status ?? -1)"
                )))
            return
          }
          do {
            let object = try JSON.decode(data)
            if T.self == ChannelAuthorizationData.self,
              let dict = object as? [String: Any], let auth = dict["auth"] as? String
            {
              completion(
                .success(
                  ChannelAuthorizationData(
                    auth: auth, channelData: dict["channel_data"] as? String,
                    sharedSecret: dict["shared_secret"] as? String) as! T))
            } else if T.self == UserAuthenticationData.self,
              let dict = object as? [String: Any], let auth = dict["auth"] as? String,
              let userData = dict["user_data"] as? String
            {
              completion(
                .success(
                  UserAuthenticationData(auth: auth, userData: userData) as! T))
            } else {
              completion(
                .failure(
                  SockudoError.authFailure(
                    statusCode: 200,
                    message: "JSON returned from auth endpoint was invalid")))
            }
          } catch {
            completion(.failure(error))
          }
        }
      }.resume()
    }
  }
}

extension SockudoClient {
  func fetchPresenceHistory(
    channelName: String,
    params: PresenceHistoryParams,
    completion: @escaping @Sendable (Result<PresenceHistoryPage, Error>) -> Void
  ) {
    guard let history = config.presenceHistory else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "presenceHistory.endpoint must be configured to use presence.history(). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: history.endpoint,
      headers: history.headers.merging(
        history.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: params.payload,
      action: "history"
    ) { [weak self] result in
      guard let self else { return }
      completion(
        result.flatMap { payload in
          .success(
            self.decodePresenceHistoryPage(
              payload: payload,
              channelName: channelName,
              originalParams: params))
        })
    }
  }

  func fetchPresenceSnapshot(
    channelName: String,
    params: PresenceSnapshotParams,
    completion: @escaping @Sendable (Result<PresenceSnapshot, Error>) -> Void
  ) {
    guard let history = config.presenceHistory else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "presenceHistory.endpoint must be configured to use presence.snapshot(). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: history.endpoint,
      headers: history.headers.merging(
        history.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: params.payload,
      action: "snapshot"
    ) { [weak self] result in
      guard let self else { return }
      completion(result.flatMap { payload in .success(self.decodePresenceSnapshot(payload: payload)) })
    }
  }

  func fetchChannelHistory(
    channelName: String,
    params: ChannelHistoryParams,
    completion: @escaping @Sendable (Result<ChannelHistoryPage, Error>) -> Void
  ) {
    guard let config = self.config.versionedMessages else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "versionedMessages.endpoint must be configured to use channelHistory(). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: config.headers.merging(
        config.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: params.payload,
      action: "channel_history"
    ) { [weak self] result in
      guard let self else { return }
      completion(
        result.flatMap { payload in
          .success(
            self.decodeChannelHistoryPage(
              payload: payload,
              channelName: channelName,
              originalParams: params))
        })
    }
  }

  func fetchLatestMessage(
    channelName: String,
    messageSerial: String,
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    guard let config = self.config.versionedMessages else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "versionedMessages.endpoint must be configured to use getMessage(). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: config.headers.merging(
        config.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: [:],
      action: "get_message",
      messageSerial: messageSerial
    ) { result in
      completion(
        result.flatMap { payload in
          .success(payload["item"] as? [String: Any] ?? [:])
        })
    }
  }

  func fetchMessageVersions(
    channelName: String,
    messageSerial: String,
    params: MessageVersionsParams,
    completion: @escaping @Sendable (Result<MessageVersionsPage, Error>) -> Void
  ) {
    guard let config = self.config.versionedMessages else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "versionedMessages.endpoint must be configured to use getMessageVersions(). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: config.headers.merging(
        config.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: params.payload,
      action: "get_message_versions",
      messageSerial: messageSerial
    ) { [weak self] result in
      guard let self else { return }
      completion(
        result.flatMap { payload in
          .success(
            self.decodeMessageVersionsPage(
              payload: payload,
              channelName: channelName,
              messageSerial: messageSerial,
              originalParams: params))
        })
    }
  }

  func createVersionedMessage(
    channelName: String,
    request: VersionedMessageCreateRequest,
    completion: @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    performVersionedMessageMutation(
      channelName: channelName,
      messageSerial: nil,
      action: "create_message",
      fallbackAction: .create,
      body: request.payload,
      completion: completion
    )
  }

  func updateVersionedMessage(
    channelName: String,
    messageSerial: String,
    request: VersionedMessageUpdateRequest,
    completion: @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    performVersionedMessageMutation(
      channelName: channelName,
      messageSerial: messageSerial,
      action: "update_message",
      fallbackAction: .update,
      body: request.payload,
      completion: completion
    )
  }

  func appendVersionedMessage(
    channelName: String,
    messageSerial: String,
    request: VersionedMessageAppendRequest,
    completion: @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    performVersionedMessageMutation(
      channelName: channelName,
      messageSerial: messageSerial,
      action: "append_message",
      fallbackAction: .append,
      body: request.payload,
      completion: completion
    )
  }

  func deleteVersionedMessage(
    channelName: String,
    messageSerial: String,
    request: VersionedMessageDeleteRequest,
    completion: @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    performVersionedMessageMutation(
      channelName: channelName,
      messageSerial: messageSerial,
      action: "delete_message",
      fallbackAction: .delete,
      body: request.payload,
      completion: completion
    )
  }

  private func performVersionedMessageMutation(
    channelName: String,
    messageSerial: String?,
    action: String,
    fallbackAction: MutableMessageAction,
    body: [String: Any],
    completion: @escaping @Sendable (Result<VersionedMessageAck, Error>) -> Void
  ) {
    guard let config = self.config.versionedMessages else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "versionedMessages.endpoint must be configured to use \(action). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: config.headers.merging(
        config.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: [:],
      action: action,
      messageSerial: messageSerial,
      bodyPayload: body,
      timeout: 10
    ) { result in
      completion(
        result.flatMap { payload in
          Self.decodeVersionedMessageAck(
            payload: payload,
            channelName: channelName,
            messageSerial: messageSerial,
            fallbackAction: fallbackAction
          )
        })
    }
  }

  func publishAnnotation(
    channelName: String,
    messageSerial: String,
    annotation: PublishAnnotationRequest,
    completion: @escaping @Sendable (Result<PublishAnnotationResponse, Error>) -> Void
  ) {
    guard let config = self.config.versionedMessages else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "versionedMessages.endpoint must be configured to use publishAnnotation(). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: config.headers.merging(
        config.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: [:],
      action: "publish_annotation",
      messageSerial: messageSerial,
      annotation: annotation.payload
    ) { result in
      completion(
        result.flatMap { payload in
          .success(
            PublishAnnotationResponse(
              annotation: payload["annotation"] as? [String: Any] ?? [:],
              summary: payload["summary"] as? [String: Any]
            ))
        })
    }
  }

  func deleteAnnotation(
    channelName: String,
    messageSerial: String,
    annotationSerial: String,
    socketID: String?,
    completion: @escaping @Sendable (Result<DeleteAnnotationResponse, Error>) -> Void
  ) {
    guard let config = self.config.versionedMessages else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "versionedMessages.endpoint must be configured to use deleteAnnotation(). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: config.headers.merging(
        config.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: [:],
      action: "delete_annotation",
      messageSerial: messageSerial,
      annotationSerial: annotationSerial,
      socketID: socketID
    ) { result in
      completion(
        result.flatMap { payload in
          .success(
            DeleteAnnotationResponse(
              deleted: payload["deleted"] as? Bool ?? false,
              annotationSerial: payload["annotationSerial"] as? String ?? annotationSerial,
              summary: payload["summary"] as? [String: Any]
            ))
        })
    }
  }

  func listAnnotations(
    channelName: String,
    messageSerial: String,
    params: AnnotationEventsParams,
    completion: @escaping @Sendable (Result<AnnotationEventsPage, Error>) -> Void
  ) {
    guard let config = self.config.versionedMessages else {
      completion(
        .failure(
          SockudoError.unsupportedFeature(
            "versionedMessages.endpoint must be configured to use listAnnotations(). This endpoint should proxy requests to the Sockudo server REST API."
          )))
      return
    }

    performPresenceHistoryRequest(
      endpoint: config.endpoint,
      headers: config.headers.merging(
        config.headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }),
      channelName: channelName,
      params: params.payload,
      action: "list_annotations",
      messageSerial: messageSerial
    ) { [weak self] result in
      guard let self else { return }
      completion(
        result.flatMap { payload in
          .success(
            self.decodeAnnotationEventsPage(
              payload: payload,
              channelName: channelName,
              messageSerial: messageSerial,
              originalParams: params))
        })
    }
  }

  private func performPresenceHistoryRequest(
    endpoint: String,
    headers: [String: String],
    channelName: String,
    params: [String: Any],
    action: String,
    messageSerial: String? = nil,
    annotationSerial: String? = nil,
    socketID: String? = nil,
    annotation: [String: Any]? = nil,
    bodyPayload: [String: Any]? = nil,
    timeout: TimeInterval? = nil,
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    guard let url = URL(string: endpoint, relativeTo: nil) ?? URL(string: endpoint) else {
      completion(.failure(SockudoError.invalidURL("Invalid presence history endpoint \(endpoint)")))
      return
    }

    do {
      var request = URLRequest(url: url)
      request.httpMethod = "POST"
      if let timeout {
        request.timeoutInterval = timeout
      }
      var payload: [String: Any] = [
        "channel": channelName,
        "params": params,
        "action": action,
      ]
      if let messageSerial {
        payload["messageSerial"] = messageSerial
      }
      if let annotationSerial {
        payload["annotationSerial"] = annotationSerial
      }
      if let socketID {
        payload["socketId"] = socketID
      }
      if let annotation {
        payload["annotation"] = annotation
      }
      if let requestPayload = bodyPayload {
        payload["payload"] = requestPayload
      }
      request.httpBody = try JSON.encodeData(payload)
      request.setValue("application/json", forHTTPHeaderField: "Content-Type")
      for (name, value) in headers {
        request.setValue(value, forHTTPHeaderField: name)
      }

      urlSession.dataTask(with: request) { data, response, error in
        Task { @MainActor in
          if let error {
            completion(.failure(error))
            return
          }
          let status = (response as? HTTPURLResponse)?.statusCode ?? -1
          guard (200..<300).contains(status), let data else {
            let body = data.flatMap { String(data: $0, encoding: .utf8) } ?? ""
            completion(
              .failure(
                SockudoError.invalidOptions(
                  "Presence \(action) request failed (\(status)): \(body)"
                )))
            return
          }
          do {
            let object = try JSON.decode(data)
            guard let dictionary = object as? [String: Any] else {
              completion(
                .failure(
                  SockudoError.invalidOptions(
                    "Presence \(action) endpoint returned invalid JSON"
                  )))
              return
            }
            completion(.success(dictionary))
          } catch {
            completion(.failure(error))
          }
        }
      }.resume()
    } catch {
      completion(.failure(error))
    }
  }

  private nonisolated func decodePresenceHistoryPage(
    payload: [String: Any],
    channelName: String,
    originalParams: PresenceHistoryParams
  ) -> PresenceHistoryPage {
    let items = ((payload["items"] as? [Any]) ?? []).compactMap { raw -> PresenceHistoryItem? in
      guard let item = raw as? [String: Any] else { return nil }
      return PresenceHistoryItem(
        streamID: item["stream_id"] as? String ?? "",
        serial: (item["serial"] as? NSNumber)?.int64Value ?? 0,
        publishedAtMS: (item["published_at_ms"] as? NSNumber)?.int64Value ?? 0,
        event: item["event"] as? String ?? "",
        cause: item["cause"] as? String ?? "",
        userID: item["user_id"] as? String ?? "",
        connectionID: item["connection_id"] as? String,
        deadNodeID: item["dead_node_id"] as? String,
        payloadSizeBytes: (item["payload_size_bytes"] as? NSNumber)?.intValue ?? 0,
        presenceEvent: item["presence_event"] as? [String: AnyHashable] ?? [:]
      )
    }

    return PresenceHistoryPage(
      items: items,
      direction: payload["direction"] as? String ?? "oldest_first",
      limit: (payload["limit"] as? NSNumber)?.intValue ?? 0,
      hasMore: payload["has_more"] as? Bool ?? false,
      nextCursor: payload["next_cursor"] as? String,
      bounds: decodePresenceHistoryBounds(payload["bounds"] as? [String: Any]),
      continuity: decodePresenceHistoryContinuity(payload["continuity"] as? [String: Any]),
      fetchNext: { [weak self] cursor, completion in
        Task { @MainActor in
          self?.fetchPresenceHistory(
            channelName: channelName,
            params: PresenceHistoryParams(
              direction: originalParams.direction,
              limit: originalParams.limit,
              cursor: cursor,
              startSerial: originalParams.startSerial,
              endSerial: originalParams.endSerial,
              startTimeMS: originalParams.startTimeMS,
              endTimeMS: originalParams.endTimeMS,
              start: originalParams.start,
              end: originalParams.end
            ),
            completion: completion
          )
        }
      }
    )
  }

  private nonisolated func decodePresenceSnapshot(payload: [String: Any]) -> PresenceSnapshot {
    let members = ((payload["members"] as? [Any]) ?? []).compactMap { raw -> PresenceSnapshotMember? in
      guard let member = raw as? [String: Any] else { return nil }
      return PresenceSnapshotMember(
        userID: member["user_id"] as? String ?? "",
        lastEvent: member["last_event"] as? String ?? "",
        lastEventSerial: (member["last_event_serial"] as? NSNumber)?.int64Value ?? 0,
        lastEventAtMS: (member["last_event_at_ms"] as? NSNumber)?.int64Value ?? 0
      )
    }

    return PresenceSnapshot(
      channel: payload["channel"] as? String ?? "",
      members: members,
      memberCount: (payload["member_count"] as? NSNumber)?.intValue ?? 0,
      eventsReplayed: (payload["events_replayed"] as? NSNumber)?.int64Value ?? 0,
      snapshotSerial: (payload["snapshot_serial"] as? NSNumber)?.int64Value,
      snapshotTimeMS: (payload["snapshot_time_ms"] as? NSNumber)?.int64Value,
      continuity: decodePresenceHistoryContinuity(payload["continuity"] as? [String: Any])
    )
  }

  private nonisolated func decodeChannelHistoryPage(
    payload: [String: Any],
    channelName: String,
    originalParams: ChannelHistoryParams
  ) -> ChannelHistoryPage {
    let items = ((payload["items"] as? [Any]) ?? []).compactMap { $0 as? [String: Any] }

    return ChannelHistoryPage(
      items: items,
      direction: payload["direction"] as? String ?? "oldest_first",
      limit: (payload["limit"] as? NSNumber)?.intValue ?? 0,
      hasMore: payload["has_more"] as? Bool ?? false,
      nextCursor: payload["next_cursor"] as? String,
      bounds: payload["bounds"] as? [String: Any] ?? [:],
      continuity: payload["continuity"] as? [String: Any] ?? [:],
      fetchNext: { [weak self] cursor, completion in
        Task { @MainActor in
          self?.fetchChannelHistory(
            channelName: channelName,
            params: ChannelHistoryParams(
              direction: originalParams.direction,
              limit: originalParams.limit,
              cursor: cursor,
              startSerial: originalParams.startSerial,
              endSerial: originalParams.endSerial,
              startTimeMS: originalParams.startTimeMS,
              endTimeMS: originalParams.endTimeMS,
              untilAttach: originalParams.untilAttach
            ),
            completion: completion
          )
        }
      }
    )
  }

  private nonisolated func decodeMessageVersionsPage(
    payload: [String: Any],
    channelName: String,
    messageSerial: String,
    originalParams: MessageVersionsParams
  ) -> MessageVersionsPage {
    let items = ((payload["items"] as? [Any]) ?? []).compactMap { $0 as? [String: Any] }

    return MessageVersionsPage(
      channel: payload["channel"] as? String ?? channelName,
      items: items,
      direction: payload["direction"] as? String ?? "oldest_first",
      limit: (payload["limit"] as? NSNumber)?.intValue ?? 0,
      hasMore: payload["has_more"] as? Bool ?? false,
      nextCursor: payload["next_cursor"] as? String,
      fetchNext: { [weak self] cursor, completion in
        Task { @MainActor in
          self?.fetchMessageVersions(
            channelName: channelName,
            messageSerial: messageSerial,
            params: MessageVersionsParams(
              direction: originalParams.direction,
              limit: originalParams.limit,
              cursor: cursor
            ),
            completion: completion
          )
        }
      }
    )
  }

  private nonisolated static func decodeVersionedMessageAck(
    payload: [String: Any],
    channelName: String,
    messageSerial: String?,
    fallbackAction: MutableMessageAction
  ) -> Result<VersionedMessageAck, Error> {
    let ackPayload = extractVersionedMessageAckPayload(payload, channelName: channelName)
    let serial =
      ackPayload["message_serial"] as? String
      ?? ackPayload["messageSerial"] as? String
      ?? messageSerial
    guard let serial, serial.isEmpty == false else {
      return .failure(
        SockudoError.messageParseError(
          "Versioned message proxy response did not include message_serial"
        ))
    }

    let action = MutableMessageAction(ackValue: ackPayload["action"]) ?? fallbackAction
    let accepted = ackPayload["accepted"] as? Bool ?? true
    let status = ackPayload["status"] as? String ?? (accepted ? "accepted" : "rejected")

    return .success(
      VersionedMessageAck(
        channel: ackPayload["channel"] as? String ?? channelName,
        messageSerial: serial,
        action: action,
        accepted: accepted,
        versionSerial: ackPayload["version_serial"] as? String
          ?? ackPayload["versionSerial"] as? String,
        historySerial: wireInt64(ackPayload["history_serial"] ?? ackPayload["historySerial"]),
        deliverySerial: wireInt64(ackPayload["delivery_serial"] ?? ackPayload["deliverySerial"]),
        status: status,
        raw: payload
      ))
  }

  private nonisolated static func extractVersionedMessageAckPayload(
    _ payload: [String: Any],
    channelName: String
  ) -> [String: Any] {
    if let ack = payload["ack"] as? [String: Any] {
      return ack
    }
    if let channels = payload["channels"] as? [String: Any],
      let channelAck = channels[channelName] as? [String: Any]
    {
      return channelAck
    }
    return payload
  }

  private nonisolated func decodeAnnotationEventsPage(
    payload: [String: Any],
    channelName: String,
    messageSerial: String,
    originalParams: AnnotationEventsParams
  ) -> AnnotationEventsPage {
    let items = ((payload["items"] as? [Any]) ?? []).compactMap { $0 as? [String: Any] }

    return AnnotationEventsPage(
      items: items,
      direction: payload["direction"] as? String ?? "oldest_first",
      limit: (payload["limit"] as? NSNumber)?.intValue ?? 0,
      hasMore: payload["has_more"] as? Bool ?? false,
      nextCursor: payload["next_cursor"] as? String,
      fetchNext: { [weak self] cursor, completion in
        Task { @MainActor in
          self?.listAnnotations(
            channelName: channelName,
            messageSerial: messageSerial,
            params: AnnotationEventsParams(
              direction: originalParams.direction,
              limit: originalParams.limit,
              cursor: cursor,
              type: originalParams.type,
              fromSerial: originalParams.fromSerial
            ),
            completion: completion
          )
        }
      }
    )
  }

  private nonisolated func decodePresenceHistoryBounds(_ payload: [String: Any]?) -> PresenceHistoryBounds {
    PresenceHistoryBounds(
      startSerial: (payload?["start_serial"] as? NSNumber)?.int64Value,
      endSerial: (payload?["end_serial"] as? NSNumber)?.int64Value,
      startTimeMS: (payload?["start_time_ms"] as? NSNumber)?.int64Value,
      endTimeMS: (payload?["end_time_ms"] as? NSNumber)?.int64Value
    )
  }

  private nonisolated func decodePresenceHistoryContinuity(_ payload: [String: Any]?) -> PresenceHistoryContinuity {
    PresenceHistoryContinuity(
      streamID: payload?["stream_id"] as? String,
      oldestAvailableSerial: (payload?["oldest_available_serial"] as? NSNumber)?.int64Value,
      newestAvailableSerial: (payload?["newest_available_serial"] as? NSNumber)?.int64Value,
      oldestAvailablePublishedAtMS: (payload?["oldest_available_published_at_ms"] as? NSNumber)?.int64Value,
      newestAvailablePublishedAtMS: (payload?["newest_available_published_at_ms"] as? NSNumber)?.int64Value,
      retainedEvents: (payload?["retained_events"] as? NSNumber)?.int64Value ?? 0,
      retainedBytes: (payload?["retained_bytes"] as? NSNumber)?.int64Value ?? 0,
      degraded: payload?["degraded"] as? Bool ?? false,
      complete: payload?["complete"] as? Bool ?? false,
      truncatedByRetention: payload?["truncated_by_retention"] as? Bool ?? false
    )
  }
}

private final class WebSocketDelegate: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {
  var didOpen: (@Sendable () -> Void)?
  var didClose: (@Sendable (URLSessionWebSocketTask.CloseCode, String?) -> Void)?

  func urlSession(
    _ session: URLSession, webSocketTask: URLSessionWebSocketTask,
    didOpenWithProtocol protocol: String?
  ) {
    didOpen?()
  }

  func urlSession(
    _ session: URLSession, webSocketTask: URLSessionWebSocketTask,
    didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?
  ) {
    let reasonText = reason.flatMap { String(data: $0, encoding: .utf8) }
    didClose?(closeCode, reasonText)
  }
}

enum CloseAction {
  case tlsOnly
  case refused
  case backoff
  case retry
}

@MainActor private final class ReachabilityMonitor: Sendable {
  private let monitor = NWPathMonitor()
  private let queue = DispatchQueue(label: "sockudo.reachability")
  var stateDidChange: (@Sendable (Bool) -> Void)?

  func start() {
    let callback = stateDidChange
    monitor.pathUpdateHandler = { path in
      Task { @MainActor in
        callback?(path.status == .satisfied)
      }
    }
    monitor.start(queue: queue)
  }

  nonisolated func stop() {
    monitor.cancel()
  }
}

private struct Timeline {
  private var events: [[String: Any]] = []
  private var sent = 0

  var isEmpty: Bool { events.isEmpty }

  mutating func info(_ event: [String: Any]) {
    events.append(
      event.merging(
        ["timestamp": Int(Date().timeIntervalSince1970 * 1000)],
        uniquingKeysWith: { _, new in new }))
    if events.count > 50 {
      events.removeFirst()
    }
  }

  mutating func markSent() {
    sent += 1
    events.removeAll()
  }

  func payload(key: String, cluster: String) -> [String: String] {
    [
      "session": UUID().uuidString,
      "bundle": String(sent + 1),
      "key": key,
      "lib": "swift",
      "version": "1.1.0",
      "cluster": cluster,
      "timeline": (try? JSON.encodeString(events)) ?? "[]",
    ]
  }
}
