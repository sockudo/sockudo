import APIota
import Foundation

struct GetMessageEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = GetMessageResponse
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders? = APIClient.defaultHeaders
  let httpBody: String? = nil
  let httpMethod: HTTPMethod = .GET

  var path: String {
    "/apps/\(options.appId)/channels/\(channel.fullName)/messages/\(messageSerial)"
  }

  var queryItems: [URLQueryItem]? {
    let authInfo = AuthInfo(
      httpBody: httpBody,
      httpMethod: httpMethod.rawValue,
      path: path,
      key: options.key,
      secret: options.secret)
    return authInfo.queryItems
  }

  let channel: Channel
  let messageSerial: String
  let options: SockudoClientOptions
}
