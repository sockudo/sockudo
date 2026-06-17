import Foundation

/// An event to trigger on a specific `Channel` (or multiple channels).
public struct Event: EventInfoRecord, Encodable {

  /// An error generated during initialization.
  enum Error: LocalizedError {

    /// Encrypted channels cannot be triggered to when initializing a multichannel `Event`.
    case invalidMultichannelEventConfiguration

    /// A localized human-readable description of the error.
    public var errorDescription: String? {

      switch self {
      case .invalidMultichannelEventConfiguration:
        return NSLocalizedString(
          "Multichannel events cannot be configured with any encrypted channels.",
          comment: "'.encryptedChannelsInvalidWithMultichannelEvents' error text")
      }
    }
  }

  /// The channels to which the event will be sent (if publishing to multiple channels).
  public let channels: [Channel]?

  /// The channel to which the event will be sent (if publishing to a single channel).
  public let channel: Channel?

  /// The event name.
  public let name: String

  /// This is the `Data` representation of the original `data` parameter of either of the `init(...)` methods.
  /// The data will be encrypted if a `channel` is set and its `ChannelType` is `encrypted`.
  public let data: Data

  /// A connection to which the event will not be sent.
  public let socketId: String?

  /// The channel attributes to fetch that will be present in the API response.
  let attributeOptions: ChannelAttributeFetchOptions

  // MARK: - Encodable conformance

  enum CodingKeys: String, CodingKey {
    case channels
    case channel
    case name
    case data
    case socketId = "socket_id"
    case attributeOptions = "info"
  }

  // MARK: - Lifecycle

  /// Creates an event to be triggered on a specific `Channel`.
  /// - Parameters:
  ///   - name: The name of the event.
  ///   - data: An event data object, whose type must conform to `Encodable`.
  ///   - channel: The channel on which to trigger the event.
  ///   - socketId: A connection to which the event will not be sent.
  ///   - attributeOptions: A set of attributes that should be returned for the `channel`.
  /// - Throws: An `PusherError` if encoding the event `data` fails for some reason.
  public init<EventData: Encodable>(
    name: String,
    data: EventData,
    channel: Channel,
    socketId: String? = nil,
    attributeOptions: ChannelAttributeFetchOptions = []
  ) throws {

    self.channel = channel
    self.channels = nil
    self.name = name
    self.data = try Self.encodeEventData(data)
    self.socketId = socketId
    self.attributeOptions = attributeOptions
  }

  /// Creates an `Event` which will be triggered on multiple `Channel` instances.
  /// - Parameters:
  ///   - name: The name of the event.
  ///   - data: An event data object, whose type must conform to `Encodable`.
  ///   - channels: An array of channels on which to trigger the event.
  ///   - socketId: A connection to which the event will not be sent.
  ///   - attributeOptions: A set of attributes that should be returned for each channel in `channels`.
  /// - Throws: An `PusherError` if encoding the event `data` fails for some reason,
  ///           or if `channels` contains any encrypted channels.
  public init<EventData: Encodable>(
    name: String,
    data: EventData,
    channels: [Channel],
    socketId: String? = nil,
    attributeOptions: ChannelAttributeFetchOptions = []
  ) throws {

    // Throw an error if `channels` contains any encrypted channels
    // (Triggering an event on multiple channels is not allowed if any are encrypted).
    let containsEncryptedChannels = channels.contains { $0.type == .encrypted }
    guard !containsEncryptedChannels else {
      throw Error.invalidMultichannelEventConfiguration
    }

    self.channel = nil
    self.channels = channels
    self.name = name
    self.data = try Self.encodeEventData(data)
    self.socketId = socketId
    self.attributeOptions = attributeOptions
  }

  // MARK: - Custom Encodable conformance

  public func encode(to encoder: Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)

    try container.encode(name, forKey: .name)

    // This custom `encode(to:)` implementation is neccessary since
    // event data is expected as a `String` rather than as a JSON object
    let eventDataString = data.toString()
    try container.encode(eventDataString, forKey: .data)

    try container.encode(channels?.map { $0.fullName }, forKey: .channels)
    try container.encode(channel?.fullName, forKey: .channel)
    try container.encode(socketId, forKey: .socketId)
    if !attributeOptions.description.isEmpty {
      try container.encode(attributeOptions.description, forKey: .attributeOptions)
    }
  }

  // MARK: - Event data encryption

  /// Returns an `Event` with encrypted event `data` if a `channel` is set
  /// and its `ChannelType` is `.encrypted`.
  ///
  /// The event data is encrypted using a random nonce and a shared secret which is
  /// a concatenation of the channel name and the `encryptionMasterKey` from `options`.
  ///
  /// If the provided `channel` is not an encrypted (or multiple `channels` are provided instead),
  /// then the receiver is returned unaltered.
  /// - Parameter options: Configuration options used to managing the connection.
  /// - Throws: An `PusherError` if encrypting the event `data` fails for some reason.
  /// - Returns: A copy of the receiver, but with encrypted event `data`. If the `channel` is not
  ///            encrypted, the receiver will be returned unaltered.
  func encrypted(using options: PusherClientOptions) throws -> Self {

    guard let channel = channel, channel.type == .encrypted else {
      return self
    }

    do {
      let eventNonce = try CryptoService.secureRandomData(count: 24)
      let sharedSecretString = "\(channel.fullName)\(options.encryptionMasterKey)"
      let sharedSecret = CryptoService.sha256Digest(data: sharedSecretString.toData())
      let eventCiphertext = try CryptoService.encrypt(
        data: data,
        nonce: eventNonce,
        key: sharedSecret)
      let encryptedEvent = EncryptedData(
        nonceData: eventNonce,
        ciphertextData: eventCiphertext)
      let encryptedEventData = try Self.encodeEventData(encryptedEvent)

      return try Event(name: name, data: encryptedEventData, channel: channel)
    } catch {
      throw PusherError(from: error)
    }
  }

  // MARK: - Private methods

  /// Encodes event data to its `Data` representation.
  ///
  /// If `eventData` is already encoded as `Data`, it is returned unaltered.
  /// - Parameter eventData: The event data, which conforms to `Encodable`.
  /// - Throws: A `PusherError` if the encoding operation fails for some reason.
  /// - Returns: The event data, encoded as `Data`.
  private static func encodeEventData<EventData: Encodable>(_ eventData: EventData) throws -> Data {

    if let dataEncodedEventData = eventData as? Data {
      return dataEncodedEventData
    }

    return try JSONEncoder().encode(eventData)
  }
}
