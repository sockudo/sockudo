import APIota
import Foundation

/// Fetches the channel summary information for multiple channels.
struct GetChannelsEndpoint: APIotaCodableEndpoint {

  typealias SuccessResponse = ChannelSummaryListAPIResponse
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered

  let headers: HTTPHeaders? = APIClient.defaultHeaders

  let httpBody: String? = nil

  let httpMethod: HTTPMethod = .GET

  var path: String {

    return "/apps/\(options.appId)/channels"
  }

  var queryItems: [URLQueryItem]? {

    // Initialize 'additional' query items
    // (i.e. for filtering based on `ChannelFilter` and requested channel attributes)
    var typeAndAttributesQueryItems = [URLQueryItem]()
    if let channelFilterQueryItem = channelFilter.queryItem {
      typeAndAttributesQueryItems.append(channelFilterQueryItem)
    }
    typeAndAttributesQueryItems.append(contentsOf: attributeOptions.queryItems)

    // Initialize auth info
    let authInfo = AuthInfo(
      httpBody: httpBody,
      httpMethod: httpMethod.rawValue,
      path: path,
      key: options.key,
      secret: options.secret,
      additionalQueryItems: typeAndAttributesQueryItems)

    return authInfo.queryItems
  }

  /// The channel type used to filter results in the API response.
  let channelFilter: ChannelFilter

  /// The channel attributes to fetch that will be present in the API response.
  let attributeOptions: ChannelAttributeFetchOptions

  /// Configuration options which are used when initializing the `URLRequest`.
  let options: SockudoClientOptions
}
