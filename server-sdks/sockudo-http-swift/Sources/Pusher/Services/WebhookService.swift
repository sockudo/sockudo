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

    /// The webhook request is missing a `"X-Pusher-Key"' header, or its value is invalid.
    case xPusherKeyHeaderMissingOrInvalid

    /// The webhook request is missing a `"X-Pusher-Signature"' header, or its value is invalid.
    case xPusherSignatureHeaderMissingOrInvalid

    /// A localized human-readable description of the error.
    public var errorDescription: String? {

      switch self {
      case .bodyDataMissing:
        return NSLocalizedString(
          "Body data is missing on the Webhook request.",
          comment: "'.bodyDataMissing' error text")

      case .xPusherKeyHeaderMissingOrInvalid:
        return NSLocalizedString(
          """
          The '\(xPusherKeyHeader)' header is missing or invalid \
          on the Webhook request.
          """,
          comment: "'.xPusherKeyHeaderMissingOrInvalid' error text")

      case .xPusherSignatureHeaderMissingOrInvalid:
        return NSLocalizedString(
          """
          The '\(xPusherSignatureHeader)' header is missing or invalid \
          on the Webhook request.
          """,
          comment: "'.xPusherSignatureHeaderMissingOrInvalid' error text")
      }
    }
  }

  /// The `X-Pusher-Key` header.
  static let xPusherKeyHeader = "X-Pusher-Key"

  /// The `X-Pusher-Signature` header.
  static let xPusherSignatureHeader = "X-Pusher-Signature"

  /// Verify that the key and signature header values of the received Webhook request are valid.
  /// - Parameters:
  ///   - request: The received `URLRequest` representing a Webhook.
  ///   - callback: A closure containing a `Result` that is populated with an error
  ///   - options: Configuration options used to managing the connection.
  /// - Throws: A `PusherError` if the key or signature is invalid.
  static func verifySignature(
    of request: URLRequest,
    using options: PusherClientOptions
  ) throws {

    // Verify the Webhook key and signature header values are valid
    guard let xPusherKeyHeaderValue = request.value(forHTTPHeaderField: xPusherKeyHeader),
      xPusherKeyHeaderValue == options.key
    else {
      throw Error.xPusherKeyHeaderMissingOrInvalid
    }

    guard let bodyData = request.httpBody, bodyData.count > 0 else {
      throw Error.bodyDataMissing
    }

    let expectedSignature = CryptoService.sha256HMAC(
      for: bodyData,
      using: options.secret.toData()
    ).hexEncodedString()
    guard
      let xPusherSignatureHeaderValue = request.value(forHTTPHeaderField: xPusherSignatureHeader),
      expectedSignature == xPusherSignatureHeaderValue
    else {
      throw Error.xPusherSignatureHeaderMissingOrInvalid
    }
  }

  /// Decodes a `Webhook` from a received Webhook request.
  ///
  /// Any encrypted events on the Webhook request will be decrypted before returning.
  /// - Parameter request: The received `URLRequest` representing a Webhook.
  /// - Parameter options: Configuration options used to managing the connection.
  /// - Throws: A `PusherError` if the decoding operation failed for some reason.
  /// - Returns: A decoded `Webhook` object.
  static func webhook(from request: URLRequest, using options: PusherClientOptions) throws
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
