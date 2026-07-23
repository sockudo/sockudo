import APIota
import Foundation

/// Response returned when a user connection management operation succeeds.
public struct UserConnectionResponse: Decodable, Equatable {}

/// Terminates all active connections for a specific user.
struct TerminateUserConnectionsEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = UserConnectionResponse
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders? = {
    var headers = APIClient.defaultHeaders
    headers.replaceOrAdd(header: .contentType, value: HTTPMediaType.json.toString())
    return headers
  }()
  let httpBody: String? = nil
  let httpMethod: HTTPMethod = .POST

  var path: String {
    "/apps/\(options.appId)/users/\(userId)/terminate_connections"
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

  let userId: String
  let options: SockudoClientOptions
}

/// Forces all active connections for a specific user to reconnect (close with code 4200).
struct ForceReconnectUserEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = UserConnectionResponse
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders? = {
    var headers = APIClient.defaultHeaders
    headers.replaceOrAdd(header: .contentType, value: HTTPMediaType.json.toString())
    return headers
  }()
  let httpBody: String? = nil
  let httpMethod: HTTPMethod = .POST

  var path: String {
    "/apps/\(options.appId)/users/\(userId)/force_reconnect"
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

  let userId: String
  let options: SockudoClientOptions
}
