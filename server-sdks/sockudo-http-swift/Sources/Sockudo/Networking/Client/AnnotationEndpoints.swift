import AnyCodable
import APIota
import Foundation

struct PublishAnnotationEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = PublishAnnotationResponse
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
    "/apps/\(options.appId)/channels/\(channel.fullName)/messages/\(messageSerial)/annotations"
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
  let options: SockudoClientOptions
}

struct DeleteAnnotationEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = DeleteAnnotationResponse
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders? = APIClient.defaultHeaders
  let httpBody: String? = nil
  let httpMethod: HTTPMethod = .DELETE

  var path: String {
    "/apps/\(options.appId)/channels/\(channel.fullName)/messages/\(messageSerial)/annotations/\(annotationSerial)"
  }

  var queryItems: [URLQueryItem]? {
    var additional = [URLQueryItem]()
    if let socketID {
      additional.append(URLQueryItem(name: "socket_id", value: socketID))
    }
    let authInfo = AuthInfo(
      httpBody: httpBody,
      httpMethod: httpMethod.rawValue,
      path: path,
      key: options.key,
      secret: options.secret,
      additionalQueryItems: additional)
    return authInfo.queryItems
  }

  let channel: Channel
  let messageSerial: String
  let annotationSerial: String
  let socketID: String?
  let options: SockudoClientOptions
}

struct GetAnnotationsEndpoint: APIotaCodableEndpoint {
  typealias SuccessResponse = AnnotationEventsPage
  typealias ErrorResponse = Data
  typealias Body = String

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders? = APIClient.defaultHeaders
  let httpBody: String? = nil
  let httpMethod: HTTPMethod = .GET

  var path: String {
    "/apps/\(options.appId)/channels/\(channel.fullName)/messages/\(messageSerial)/annotations"
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
  let messageSerial: String
  let fetchOptions: AnnotationEventsFetchOptions
  let options: SockudoClientOptions
}
