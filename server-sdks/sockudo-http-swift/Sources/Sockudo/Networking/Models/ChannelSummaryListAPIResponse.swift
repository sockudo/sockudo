import Foundation

/// Represents the HTTP API response returning an array of channel summaries.
struct ChannelSummaryListAPIResponse: Decodable {

  /// A dictionary representation of `ChannelAttributes` keyed by channel name.
  typealias ChannelAttributesDictionary = [String: ChannelAttributes]

  // MARK: - Properties

  /// An array of `ChannelSummary` for any occupied channels.
  let channelSummaries: [ChannelSummary]

  // MARK: - Decodable conformance

  enum CodingKeys: String, CodingKey {
    case attributesDictionary = "channels"
  }

  // MARK: - Custom Decodable initializer

  init(from decoder: Decoder) throws {

    let container = try decoder.container(keyedBy: CodingKeys.self)
    guard
      let attributesDict = try container.decodeIfPresent(
        ChannelAttributesDictionary.self,
        forKey: .attributesDictionary)
    else {
      self.channelSummaries = []
      return
    }

    self.channelSummaries = attributesDict.map {
      (name: String, attributes: ChannelAttributes) -> ChannelSummary in
      return ChannelSummary(
        channel: Channel(fullName: name),
        attributes: attributes)
    }
  }
}
