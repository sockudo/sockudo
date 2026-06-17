import AnyCodable
import APIota
import Foundation

struct MutateMessageEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = MutationResponse
  typealias ErrorResponse = Data
  typealias Body = AnyCodable

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders? = {
    var headers = APIClient.defaultHeaders
    headers.replaceOrAdd(header: .contentType, value: HTTPMediaType.json.toString())
    return headers
  }()
  let httpMethod: HTTPMethod = .POST

  var path: String {
    "/apps/\(options.appId)/channels/\(channel.fullName)/messages/\(messageSerial)/\(operation)"
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

  let httpBody: AnyCodable?
  let channel: Channel
  let messageSerial: String
  let operation: String
  let options: SockudoClientOptions
}
