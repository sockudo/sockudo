import APIota
import Foundation

/// Fetches the information for users currently subscribed to a specific presence channel.
struct GetUsersEndpoint: APIotaCodableEndpoint {

  typealias SuccessResponse = ChannelUsersAPIResponse
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered

  let headers: HTTPHeaders? = APIClient.defaultHeaders

  let httpBody: String? = nil

  let httpMethod: HTTPMethod = .GET

  var path: String {

    return "/apps/\(options.appId)/channels/\(channel.fullName)/users"
  }

  var queryItems: [URLQueryItem]? {

    // Add array of `URLQueryItem` for authenticating the `URLRequest`
    let authInfo = AuthInfo(
      httpBody: httpBody,
      httpMethod: httpMethod.rawValue,
      path: path,
      key: options.key,
      secret: options.secret)

    return authInfo.queryItems
  }

  /// The channel to query for subscribed user information.
  let channel: Channel

  /// Configuration options which are used when initializing the `URLRequest`.
  let options: PusherClientOptions
}
