import Foundation

/// A channel that can be queried for information, or be published to using an `Event`.
public struct Channel: ChannelDescription {

  /// The shortened channel name.
  ///
  /// This is the channel name without the type identifier prefix, e.g. `"my-channel"`.
  /// It therefore does not expose channel type information, which can be inspected using `type`.
  public let name: String

  /// The full channel name.
  ///
  /// This is the channel name including the type identifier prefix, e.g. `"private-my-channel"`.
  /// It is inferred from the `name` and the `type`.
  public let fullName: String

  /// The channel type.
  public let type: ChannelType

  // MARK: - Lifecycle

  /// Creates a `Channel` based on a full name (including type prefix).
  ///
  /// The channel `type` is inferred based on the provided `fullName`.
  /// e.g. A `fullName` of `"private-my-channel"` results in a channel
  /// whose `name` is `"my-channel"` and a `type` of `private`.
  ///
  /// If a shortened name is accidentally used, this results in a channel
  ///  of `type` `public` since no type prefix was provided.
  /// - Parameter fullName: The full channel name.
  public init(fullName: String) {
    self.name = Self.resolveName(from: fullName)
    self.fullName = fullName
    self.type = ChannelType(fullName: fullName)
  }

  /// Creates a `Channel` based on a shortened name and a channel type.
  ///
  /// If a full name is accidentally used, this results in a channel
  /// whose type is determined by the `type` parameter, regardless of
  /// any type prefix that is present in the name string.
  /// - Parameters:
  ///   - name: The shortened channel name.
  ///   - type: The channel type.
  public init(name: String, type: ChannelType) {
    let shortenedName = Self.resolveName(from: name)
    self.name = shortenedName
    self.fullName = Self.resolveFullName(from: shortenedName, type: type)
    self.type = type
  }

  // MARK: - Private methods

  /// Resolve the shortened channel name inferred from the full channel name.
  ///
  /// The full name for a non-public channel, such as `"private-my-channel"`
  /// resolves to `"my-channel"`. Public channels have no type prefix,
  /// and are therefore returned unaltered.
  /// - Parameter fullName: The full channel name.
  /// - Returns: The shortened channel name.
  private static func resolveName(from fullName: String) -> String {
    let type = ChannelType(fullName: fullName)
    switch type {
    case .encrypted, .presence, .private:
      // Drop the channel type prefix (accounting for the trailing '-')
      return String(fullName.dropFirst(type.rawValue.count + 1))

    case .public:
      return fullName
    }
  }

  /// Resolve the full channel name inferred from the shortened channel name, and a
  /// channel type.
  ///
  /// The full name for a channel named `"my-channel"` of type `private` resolves
  /// to `"private-my-channel"`. Public channels have no type prefix, and are therefore
  /// returned unaltered.
  /// - Parameters:
  ///   - name: The shortened channel name.
  ///   - type: The channel type to use to infer the full name.
  /// - Returns: The full channel name.
  private static func resolveFullName(from name: String, type: ChannelType) -> String {
    switch type {
    case .encrypted, .presence, .private:
      return "\(type.rawValue)-\(name)"
    case .public:
      return name
    }
  }
}
