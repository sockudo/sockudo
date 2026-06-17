import Foundation

/// An event that is contained within a received `Webhook`.
public struct WebhookEvent: WebhookEventRecord, Codable {

  /// The event type.
  public let eventType: WebhookEventType

  /// The channel relating to the Webhook event.
  public let channel: Channel

  /// The event (only set if `eventType` is `clientEvent`).
  public let event: Event?

  /// The identifier of the socket that sent the event (only set if `eventType` is `clientEvent`).
  public let socketId: String?

  /// The user identifier associated with the socket that sent the event
  /// (only set if `channel` is of type `presence`).
  public let userId: String?

  enum CodingKeys: String, CodingKey {
    case eventType = "name"
    case channelName = "channel"
    case eventName = "event"
    case eventData = "data"
    case socketId = "socket_id"
    case userId = "user_id"
  }

  // MARK: - Lifecycle (used in Tests)

  init(
    eventType: WebhookEventType,
    channel: Channel,
    event: Event? = nil,
    socketId: String? = nil,
    userId: String? = nil
  ) {
    self.eventType = eventType
    self.channel = channel
    self.event = event
    self.socketId = socketId
    self.userId = userId
  }

  // MARK: - Custom Encodable conformance (used in Tests)

  public func encode(to encoder: Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)

    try container.encode(eventType, forKey: .eventType)
    try container.encode(channel.fullName, forKey: .channelName)
    try container.encodeIfPresent(event?.name, forKey: .eventName)
    try container.encodeIfPresent(event?.data, forKey: .eventData)
    try container.encodeIfPresent(socketId, forKey: .socketId)
    try container.encodeIfPresent(userId, forKey: .userId)
  }

  // MARK: - Custom Decodable initializer

  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)

    eventType = try container.decode(WebhookEventType.self, forKey: .eventType)

    let channelName = try container.decode(String.self, forKey: .channelName)
    channel = Channel(fullName: channelName)

    if let eventName = try container.decodeIfPresent(String.self, forKey: .eventName),
      let eventData = try container.decodeIfPresent(Data.self, forKey: .eventData)
    {
      event = try Event(name: eventName, data: eventData, channel: channel)
    } else {
      event = nil
    }

    socketId = try container.decodeIfPresent(String.self, forKey: .socketId)
    userId = try container.decodeIfPresent(String.self, forKey: .userId)
  }

  // MARK: - Event data decryption

  /// Returns an `WebhookEvent` with decrypted `eventData` if `channelName` indicates
  /// the event occured on an encrypted channel.
  ///
  /// The event data ciphertext is decrypted using the received nonce and a shared secret which is
  /// a concatenation of the channel name and the `encryptionMasterKey` from `options`.
  /// - Parameter options: Configuration options used to managing the connection.
  /// - Throws: An `SockudoError` if decrypting the `eventData` fails for some reason.
  /// - Returns: A copy of the receiver, but with decrypted `eventData`. If the `channel` is not
  ///            encrypted, the receiver will be returned unaltered.
  func decrypted(using options: SockudoClientOptions) throws -> Self {
    guard let event = event, channel.type == .encrypted else {
      return self
    }

    let encryptedData = try JSONDecoder().decode(EncryptedData.self, from: event.data)
    let sharedSecretString = "\(channel.fullName)\(options.encryptionMasterKey)"
    let sharedSecret = CryptoService.sha256Digest(data: sharedSecretString.toData())
    let decryptedEventData = try CryptoService.decrypt(
      data: Data(base64Encoded: encryptedData.ciphertext)!,
      nonce: Data(base64Encoded: encryptedData.nonce)!,
      key: sharedSecret)
    let decryptedEvent = try Event(
      name: event.name,
      data: decryptedEventData,
      channel: channel)
    return WebhookEvent(
      eventType: eventType,
      channel: channel,
      event: decryptedEvent,
      socketId: socketId,
      userId: userId)
  }
}
