import Foundation

/// An information record for authentication parameters for REST API requests.
protocol AuthInfoRecord {

  /// The MD5 hash of the request body (in hexadecimal notation).
  var bodyMD5: String? { get }

  /// The authentication key (i.e. the application key).
  var key: String { get }

  /// The authentication signature.
  var signature: String { get }

  /// The secret key used when authenticating an API request.
  var secret: String { get }

  /// The number of seconds since the Unix epoch (i.e. 1970-01-01T00:00:00Z).
  var timestamp: TimeInterval { get }

  /// The version number of the authentication schema.
  var version: String { get }
}
