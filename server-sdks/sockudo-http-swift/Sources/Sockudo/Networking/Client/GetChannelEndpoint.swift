import APIota
import Foundation

/// Fetches the information for a specific channel.
struct GetChannelEndpoint: APIotaCodableEndpoint {

  typealias SuccessResponse = ChannelInfo
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered

  let headers: HTTPHeaders? = APIClient.defaultHeaders

  let httpBody: String? = nil

  let httpMethod: HTTPMethod = .GET

  var path: String {

    return "/apps/\(options.appId)/channels/\(channel.fullName)"
  }

  var queryItems: [URLQueryItem]? {

    // Initialize 'additional' query items (i.e. for requested channel attributes)
    let attributesQueryItems = attributeOptions.queryItems

    // Initialize auth info
    let authInfo = AuthInfo(
      httpBody: httpBody,
      httpMethod: httpMethod.rawValue,
      path: path,
      key: options.key,
      secret: options.secret,
      additionalQueryItems: attributesQueryItems)

    return authInfo.queryItems
  }

  /// The Channel to query for information.
  let channel: Channel

  /// The channel attributes to fetch that will be present in the API response.
  let attributeOptions: ChannelAttributeFetchOptions

  /// Configuration options which are used when initializing the `URLRequest`.
  let options: SockudoClientOptions
}
