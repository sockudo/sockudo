import Foundation

/// A channel that users can subscribe to.
protocol ChannelDescription {

  /// The user-facing channel name.
  ///
  /// This is the channel name, omitting any required prefixes (e.g. `my-channel`).
  var name: String { get }

  /// The full channel name.
  ///
  /// This is the full name of the channel, including any required prefix
  /// (e.g. `private-encrypted-my-channel`).
  var fullName: String { get }

  /// The type of the channel.
  var type: ChannelType { get }
}
