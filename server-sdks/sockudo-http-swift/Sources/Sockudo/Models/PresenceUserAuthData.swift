import AnyCodable
import Foundation

/// User data required when generating an `AuthenticationToken` for a subscription attempt to a presence channel.
public struct PresenceUserData: Encodable {

  /// The user identifier to send as part of a generated `AuthenticationToken`.
  public let userId: String

  /// An optional additional user data dictionary to send as part of a generated `AuthenticationToken`.
  public let userInfo: [String: AnyEncodable]?

  enum CodingKeys: String, CodingKey {
    case userId = "user_id"
    case userInfo = "user_info"
  }

  // MARK: Lifecycle

  /// Creates an instance of `PresenceUserData` for use when generating an `AuthenticationToken`.
  /// - Parameter userId: The user identifier to send as part of a generated `AuthenticationToken`.
  /// - Parameter userInfo: Optional additional user data to send as part of a generated `AuthenticationToken`.
  public init(userId: String, userInfo: [String: AnyEncodable]? = nil) {
    self.userId = userId
    self.userInfo = userInfo
  }
}
