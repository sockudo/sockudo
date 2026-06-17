import Foundation

/// An information record for a specific user currently subscribed to a presence `Channel`.
public struct User: UserInfoRecord, Decodable {

  /// The user identifier.
  public let id: String
}
