import APIota
import Foundation

/// Triggers an event on one or more channels.
struct TriggerEventEndpoint: APIotaCodableEndpoint {

  typealias SuccessResponse = ChannelSummaryListAPIResponse
  typealias ErrorResponse = Data
  typealias Body = Event

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered

  var headers: HTTPHeaders? {

    var headers = APIClient.defaultHeaders
    headers.replaceOrAdd(header: .contentType, value: HTTPMediaType.json.toString())

    return headers
  }

  let httpBody: Event?

  let httpMethod: HTTPMethod = .POST

  var path: String {

    return "/apps/\(options.appId)/events"
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

  /// Configuration options which are used when initializing the `URLRequest`.
  let options: PusherClientOptions
}
