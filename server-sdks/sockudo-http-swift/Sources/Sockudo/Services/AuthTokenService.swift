import Foundation

/// Provides functionality for generating authentication signatures for private or presence channel subscriptions.
///
/// The generated authentication signature (and other information depending on the channel configuration)
/// are returned in an authentication token object.
struct AuthenticationTokenService {

  // MARK: - Error reporting

  /// An error generated during an authentication operation.
  enum Error: LocalizedError {

    /// An authentication attempt for a public channel occured.
    case authenticationAttemptForPublicChannel

    /// Presence channel authentication requires `userData` to be provided.
    case missingUserDataForPresenceChannel

    /// A localized human-readable description of the error.
    public var errorDescription: String? {

      switch self {
      case .authenticationAttemptForPublicChannel:
        return NSLocalizedString(
          "Authenticating public channel subscriptions is not required.",
          comment: "'.publicChannelAuthenticationNotRequired' error text")

      case .missingUserDataForPresenceChannel:
        return NSLocalizedString(
          "Authenticating presence channel subscriptions requires 'userData'.",
          comment: "'.presenceChannelAuthenticationRequiresUserData' error text")
      }
    }
  }

  /// Generate an authentication token.
  /// - Parameters:
  ///   - channel: The `Channel` for which to generate the authentication token.
  ///   - socketId: The socket identifier `String` for the connected user.
  ///   - userData: User data containing information to share about a connected user.
  ///               (This must be set when generating an authentication token for a presence channel).
  ///   - options: Configuration options used to managing the connection.
  /// - Throws: A `SockudoError` if generating the authentication token fails for some reason.
  /// - Returns: An `AuthenticationToken`, which can be encoded onto a `URLRequest`.
  static func authenticationToken(
    for channel: Channel,
    socketId: String,
    userData: PresenceUserData?,
    using options: SockudoClientOptions
  ) throws -> AuthenticationToken {

    guard channel.type != .public else {
      throw Error.authenticationAttemptForPublicChannel
    }

    if channel.type == .presence {
      guard userData != nil else {
        throw Error.missingUserDataForPresenceChannel
      }
    }

    var stringToSign = "\(socketId):\(channel.fullName)"
    var userDataString: String?
    if userData != nil {
      userDataString = try JSONEncoder.iso8601Ordered.encode(userData).toString()
      stringToSign += ":\(userDataString!)"
    }

    let signature = CryptoService.sha256HMAC(
      for: stringToSign.toData(),
      using: options.secret.toData())
    let authSignature = "\(options.key):\(signature.hexEncodedString())"

    var sharedSecret: String?
    if channel.type == .encrypted {
      let stringToDigest = "\(channel.fullName)\(options.encryptionMasterKey)"
      sharedSecret = CryptoService.sha256Digest(data: stringToDigest.toData()).base64EncodedString()
    }

    return AuthenticationToken(
      signature: authSignature,
      userData: userDataString,
      sharedSecret: sharedSecret)
  }
}
