import Foundation

/// A summarised information record for a specific channel.
protocol ChannelSummaryRecord: ChannelAttributable {

  /// The channel.
  var channel: Channel { get }
}
