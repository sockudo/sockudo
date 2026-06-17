import Foundation

/// Represents the HTTP API response returning an array of channel information records.
struct ChannelInfoListAPIResponse: Decodable {

  // MARK: - Properties

  /// An array of `ChannelInfo` for any occupied channels.
  let channelInfoList: [ChannelInfo]

  // MARK: - Decodable conformance

  enum CodingKeys: String, CodingKey {
    case channelAttributesList = "batch"
  }

  // MARK: - Custom Decodable initializer

  init(from decoder: Decoder) throws {

    let container = try decoder.container(keyedBy: CodingKeys.self)
    guard
      let attributesList = try container.decodeIfPresent(
        [ChannelAttributes].self,
        forKey: .channelAttributesList)
    else {
      self.channelInfoList = []
      return
    }

    self.channelInfoList = attributesList.map { attributes -> ChannelInfo in
      return ChannelInfo(
        isOccupied: (attributes.subscriptionCount ?? 0 > 0 || attributes.userCount ?? 0 > 0),
        attributes: attributes)
    }
  }
}
