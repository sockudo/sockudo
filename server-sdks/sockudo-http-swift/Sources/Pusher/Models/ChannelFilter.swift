import Foundation

/// A filter that may be applied when fetching information on an occupied `Channel`
/// (or channels) from the HTTP API.
///
/// The filter can either be based on the `ChannelType`, or a user-specified prefix
/// that allows filtering based on custom naming schemes.
public enum ChannelFilter {

  /// A filter that returns any channel that can be subscribed to.
  ///
  /// The channels that are returned by this filter could be any of `ChannelType`.
  case any

  /// A custom filter, based on a provided channel name `prefix` string.
  ///
  /// This can be used as a filter for a particular group of channels, related by a common naming prefix scheme.
  case custom(prefix: String)

  /// A filter that returns all occupied encrypted channels.
  case encrypted

  /// A filter that returns all occupied private channels.
  case `private`

  /// A filter that returns all occupied presence channels.
  case presence
}

extension ChannelFilter {

  /// A channel name prefix `String`, depending on the `ChannelFilter` value.
  private var channelPrefix: String {
    switch self {
    case .any:
      return ""
    case .custom(prefix: let customPrefix):
      return customPrefix

    case .encrypted:
      return "private-encrypted-"
    case .private:
      return "private-"
    case .presence:
      return "presence-"
    }
  }

  /// A `URLQueryItem` that can be used as a filter when requesting info on occupied channels.
  var queryItem: URLQueryItem? {
    switch self {
    case .any:
      return nil

    case .custom(prefix: _), .encrypted, .private, .presence:
      return URLQueryItem(name: "filter_by_prefix", value: channelPrefix)
    }
  }
}
