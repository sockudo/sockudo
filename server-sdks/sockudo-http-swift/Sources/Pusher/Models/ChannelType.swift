import Foundation

/// The available channel types for a Pusher Channels application.
public enum ChannelType: String, Decodable {

  /// An end-to-end encrypted channel.
  ///
  /// The messages published to encrypted channels are encrypted using
  /// [Secretbox standard](https://nacl.cr.yp.to/secretbox.html) from NaCl.
  /// These channels have the same restrictions as a `private` channel for the purposes of subscription attempts.
  case encrypted = "private-encrypted"

  /// A user presence channel.
  ///
  /// Presence channels provide an awareness of which users are subscribed to them.
  /// These channels have the same restrictions as a `private` channel for the purposes of subscription attempts.
  case presence

  /// A private channel.
  ///
  /// The messages published to private channels are considered sensitive.
  /// In order for a user to subscribe to these channels, they must first be authorised
  /// via a call to a authentication endpoint configured by the developer.
  case `private`

  /// A public channel.
  ///
  /// The messages published to public channels are considered publicly-accessible.
  /// These channels do not require any form of authorisation in order to be subscribed to.
  case `public`

  /// Creates a `ChannelType` whose value is inferred based on a full channel name.
  ///
  /// A channel named `"presence-my-channel"` would initialize to a value of `presence`.
  /// Any channel named without a reserved prefix will be initialized to `public`.
  /// (e.g. `"my-important-channel"`).
  /// - Parameter fullName: The full channel name.
  public init(fullName: String) {
    if fullName.hasPrefix("\(Self.encrypted.rawValue)-") {
      self = .encrypted
    } else if fullName.hasPrefix("\(Self.presence.rawValue)-") {
      self = .presence
    } else if fullName.hasPrefix("\(Self.private.rawValue)-") {
      self = .private
    } else {
      self = .public
    }
  }
}
