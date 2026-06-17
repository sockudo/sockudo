import APIota
import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// A REST API client for the Pusher Channels HTTP API.
struct APIClient: APIotaClient {

  let session = URLSession.shared

  let decoder = JSONDecoder()

  var baseUrlComponents: URLComponents {
    var components = URLComponents()
    components.scheme = options.scheme
    components.host = options.host
    components.port = options.port

    return components
  }

  /// Configuration options which are used when initializing any `URLRequest` made by the receiver.
  let options: PusherClientOptions
}

extension APIClient {

  /// The default Channels HTTP API headers that should be appended to each `URLRequest` made by the receiver.
  static let defaultHeaders: HTTPHeaders = [
    HTTPHeader("X-Pusher-Library"): "pusher-http-swift \(SDKVersion.current)"
  ]
}
