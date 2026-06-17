import Foundation

/// An information record for authentication parameters for Channels API requests.
struct AuthInfo: AuthInfoRecord {

  let bodyMD5: String?
  let key: String
  let signature: String
  let secret: String
  let timestamp: TimeInterval
  let version: String

  /// An array of `URLQueryItem` that can be used to authenticate a `URLRequest` made to the Channels HTTP API.
  ///
  /// This array will also contain the contents of `additionalQueryItems` if it
  /// was set when initializing the receiver.
  var queryItems: [URLQueryItem] {
    var finalQueryItems = queryItemsToSign
    finalQueryItems.append(URLQueryItem(name: "auth_signature", value: signature))

    return finalQueryItems
  }

  /// An array of `URLQueryItem` to use when authenticating an API request.
  private let queryItemsToSign: [URLQueryItem]

  // MARK: - Lifecycle

  /// Creates an `AuthInfo` object that can be used to authenticate Channels API requests.
  /// - Parameters:
  ///   - httpBody: The HTTP body for an API request, which must conform to `Encodable`.
  ///   - httpMethod: The intended HTTP method `String` for an API request (e.g. `"GET"`).
  ///   - path: The path component for an API request.
  ///   - key: The authentication key (i.e. the application key).
  ///   - secret: The secret key used when generating authentication info.
  ///   - additionalQueryItems: The additional query items to include when generating authentication info.
  ///                           (Defaults to `nil`).
  ///   - timestamp: The timestamp value for an API request. (Defaults to `Date().timeIntervalSince1970`).
  ///   - version: The version number `String` for the Channels HTTP API authentication schema. (Defaults to `"1.0"`).
  init<Body>(
    httpBody: Body?,
    httpMethod: String,
    path: String,
    key: String,
    secret: String,
    additionalQueryItems: [URLQueryItem]? = nil,
    timestamp: TimeInterval = Date().timeIntervalSince1970,
    version: String = "1.0"
  ) where Body: Encodable {

    // Generate a MD5 digest of the body (if provided)
    if let httpBody = httpBody, let bodyData = try? JSONEncoder.iso8601Ordered.encode(httpBody) {
      self.bodyMD5 = CryptoService.md5Digest(data: bodyData).hexEncodedString()
    } else {
      self.bodyMD5 = nil
    }

    self.key = key
    self.secret = secret
    self.timestamp = timestamp
    self.version = version

    // Initialize the query items to sign against
    var queryItems = [
      URLQueryItem(name: "auth_key", value: self.key),
      URLQueryItem(name: "auth_timestamp", value: String(Int(self.timestamp))),
      URLQueryItem(name: "auth_version", value: self.version),
    ]

    if let bodyMD5 = self.bodyMD5 {
      queryItems.append(URLQueryItem(name: "body_md5", value: bodyMD5))
    }
    if let additionalQueryItems = additionalQueryItems {
      queryItems.append(contentsOf: additionalQueryItems)
    }
    self.queryItemsToSign = queryItems

    // Initialize the `String` representation of the API request to sign against
    var components = URLComponents()
    components.queryItems = self.queryItemsToSign
      .map { URLQueryItem(name: $0.name.lowercased(), value: $0.value) }
      .sorted { $0.name < $1.name }
    let stringToSign = "\(httpMethod)\n\(path)\n\(components.query!)"

    // Compute the `auth_signature` SHA256 HMAC digest, using the `secret`
    self.signature = CryptoService.sha256HMAC(
      for: stringToSign.toData(),
      using: self.secret.toData()
    ).hexEncodedString()
  }
}
