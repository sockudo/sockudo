import Foundation

/// Represents the HTTP API response returning the users occupying a channel.
struct ChannelUsersAPIResponse: Decodable {

  /// The users subscribed to a presence channel.
  let users: [User]
}
