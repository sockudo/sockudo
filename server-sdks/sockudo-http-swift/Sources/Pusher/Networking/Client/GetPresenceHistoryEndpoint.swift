import APIota
import Foundation

struct GetPresenceHistoryEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = PresenceHistoryPage
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders? = APIClient.defaultHeaders
  let httpBody: String? = nil
  let httpMethod: HTTPMethod = .GET

  var path: String {
    "/apps/\(options.appId)/channels/\(channel.fullName)/presence/history"
  }

  var queryItems: [URLQueryItem]? {
    let authInfo = AuthInfo(
      httpBody: httpBody,
      httpMethod: httpMethod.rawValue,
      path: path,
      key: options.key,
      secret: options.secret,
      additionalQueryItems: fetchOptions.queryItems)
    return authInfo.queryItems
  }

  let channel: Channel
  let fetchOptions: PresenceHistoryFetchOptions
  let options: PusherClientOptions
}
