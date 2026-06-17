import APIota
import Foundation

struct GetPresenceSnapshotEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = PresenceSnapshot
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders? = APIClient.defaultHeaders
  let httpBody: String? = nil
  let httpMethod: HTTPMethod = .GET

  var path: String {
    "/apps/\(options.appId)/channels/\(channel.fullName)/presence/history/snapshot"
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
  let fetchOptions: PresenceSnapshotFetchOptions
  let options: SockudoClientOptions
}
