import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// Provides functionality for verifying the authenticity of received Webhook requests,
/// and for decoding the request into a `Webhook` object.
struct WebhookService {

  // MARK: - Error reporting

  /// An error generated during a webhook verification operation.
  enum Error: LocalizedError {

    /// The Webhook request is missing body data.
    case bodyDataMissing

    /// The webhook request is missing a `"X-Sockudo-Key"' header, or its value is invalid.
    case xSockudoKeyHeaderMissingOrInvalid

    /// The webhook request is missing a `"X-Sockudo-Signature"' header, or its value is invalid.
    case xSockudoSignatureHeaderMissingOrInvalid

    /// A localized human-readable description of the error.
    public var errorDescription: String? {

      switch self {
      case .bodyDataMissing:
        return NSLocalizedString(
          "Body data is missing on the Webhook request.",
          comment: "'.bodyDataMissing' error text")

      case .xSockudoKeyHeaderMissingOrInvalid:
        return NSLocalizedString(
          """
          The '\(xSockudoKeyHeader)' header is missing or invalid \
          on the Webhook request.
          """,
          comment: "'.xSockudoKeyHeaderMissingOrInvalid' error text")

      case .xSockudoSignatureHeaderMissingOrInvalid:
        return NSLocalizedString(
          """
          The '\(xSockudoSignatureHeader)' header is missing or invalid \
          on the Webhook request.
          """,
          comment: "'.xSockudoSignatureHeaderMissingOrInvalid' error text")
      }
    }
  }

  /// The `X-Sockudo-Key` header.
  static let xSockudoKeyHeader = "X-Sockudo-Key"

  /// The `X-Sockudo-Signature` header.
  static let xSockudoSignatureHeader = "X-Sockudo-Signature"

  /// Verify that the key and signature header values of the received Webhook request are valid.
  /// - Parameters:
  ///   - request: The received `URLRequest` representing a Webhook.
  ///   - callback: A closure containing a `Result` that is populated with an error
  ///   - options: Configuration options used to managing the connection.
  /// - Throws: A `SockudoError` if the key or signature is invalid.
  static func verifySignature(
    of request: URLRequest,
    using options: SockudoClientOptions
  ) throws {

    // Verify the Webhook key and signature header values are valid
    guard let xSockudoKeyHeaderValue = request.value(forHTTPHeaderField: xSockudoKeyHeader),
      xSockudoKeyHeaderValue == options.key
    else {
      throw Error.xSockudoKeyHeaderMissingOrInvalid
    }

    guard let bodyData = request.httpBody, bodyData.count > 0 else {
      throw Error.bodyDataMissing
    }

    let expectedSignature = CryptoService.sha256HMAC(
      for: bodyData,
      using: options.secret.toData()
    ).hexEncodedString()
    guard
      let xSockudoSignatureHeaderValue = request.value(forHTTPHeaderField: xSockudoSignatureHeader),
      expectedSignature == xSockudoSignatureHeaderValue
    else {
      throw Error.xSockudoSignatureHeaderMissingOrInvalid
    }
  }

  /// Decodes a `Webhook` from a received Webhook request.
  ///
  /// Any encrypted events on the Webhook request will be decrypted before returning.
  /// - Parameter request: The received `URLRequest` representing a Webhook.
  /// - Parameter options: Configuration options used to managing the connection.
  /// - Throws: A `SockudoError` if the decoding operation failed for some reason.
  /// - Returns: A decoded `Webhook` object.
  static func webhook(from request: URLRequest, using options: SockudoClientOptions) throws
    -> Webhook
  {
    let decoder = JSONDecoder()
    let webhook = try decoder.decode(Webhook.self, from: request.httpBody!)

    let webhookEvents = try webhook.events.map { webhookEvent -> WebhookEvent in
      try webhookEvent.decrypted(using: options)
    }

    return Webhook(createdAt: webhook.createdAt, events: webhookEvents)
  }
}
