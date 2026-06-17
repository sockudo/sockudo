import Foundation

public struct PushRegistrationOptions {
  public var endpoint: String
  public var headers: [String: String]
  public var headersProvider: (@Sendable () -> [String: String])?

  public init(
    endpoint: String,
    headers: [String: String] = [:],
    headersProvider: (@Sendable () -> [String: String])? = nil
  ) {
    self.endpoint = endpoint
    self.headers = headers
    self.headersProvider = headersProvider
  }
}

public struct PushCursorParams: Sendable, Equatable {
  public let limit: Int?
  public let cursor: String?

  public init(limit: Int? = nil, cursor: String? = nil) {
    self.limit = limit
    self.cursor = cursor
  }

  var queryItems: [URLQueryItem] {
    var items: [URLQueryItem] = []
    if let limit {
      items.append(URLQueryItem(name: "limit", value: String(limit)))
    }
    if let cursor {
      items.append(URLQueryItem(name: "cursor", value: cursor))
    }
    return items
  }
}

public struct PushSubscriptionParams: Sendable, Equatable {
  public let channel: String?
  public let deviceID: String?
  public let limit: Int?
  public let cursor: String?

  public init(
    channel: String? = nil,
    deviceID: String? = nil,
    limit: Int? = nil,
    cursor: String? = nil
  ) {
    self.channel = channel
    self.deviceID = deviceID
    self.limit = limit
    self.cursor = cursor
  }

  var queryItems: [URLQueryItem] {
    var items: [URLQueryItem] = []
    if let channel {
      items.append(URLQueryItem(name: "channel", value: channel))
    }
    if let deviceID {
      items.append(URLQueryItem(name: "deviceId", value: deviceID))
    }
    if let limit {
      items.append(URLQueryItem(name: "limit", value: String(limit)))
    }
    if let cursor {
      items.append(URLQueryItem(name: "cursor", value: cursor))
    }
    return items
  }
}

public final class SockudoPushRegistration: @unchecked Sendable {
  private let endpoint: String
  private let headers: [String: String]
  private let headersProvider: (@Sendable () -> [String: String])?
  private let urlSession: URLSession

  public init(options: PushRegistrationOptions, urlSession: URLSession = .shared) {
    self.endpoint = options.endpoint.replacingOccurrences(
      of: #"/+$"#,
      with: "",
      options: .regularExpression
    )
    self.headers = options.headers
    self.headersProvider = options.headersProvider
    self.urlSession = urlSession
  }

  public func activateDevice(
    _ device: [String: Any],
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestObject(method: "POST", path: "/deviceRegistrations", body: device, completion: completion)
  }

  public func updateDeviceRegistration(
    _ device: [String: Any],
    deviceIdentityToken: String,
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestObject(
      method: "POST",
      path: "/deviceRegistrations",
      body: device,
      requestHeaders: ["X-Sockudo-Device-Identity-Token": deviceIdentityToken],
      completion: completion
    )
  }

  public func listDeviceRegistrations(
    params: PushCursorParams = .init(),
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestObject(
      method: "GET",
      path: "/deviceRegistrations",
      queryItems: params.queryItems,
      completion: completion
    )
  }

  public func getDeviceRegistration(
    deviceID: String,
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestObject(
      method: "GET",
      path: "/deviceRegistrations/\(encodePath(deviceID))",
      completion: completion
    )
  }

  public func deleteDeviceRegistration(
    deviceID: String,
    completion: @escaping @Sendable (Result<Void, Error>) -> Void
  ) {
    requestVoid(
      method: "DELETE",
      path: "/deviceRegistrations/\(encodePath(deviceID))",
      completion: completion
    )
  }

  public func upsertChannelSubscription(
    _ subscription: [String: Any],
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestObject(method: "POST", path: "/channelSubscriptions", body: subscription, completion: completion)
  }

  public func listChannelSubscriptions(
    params: PushSubscriptionParams = .init(),
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestObject(
      method: "GET",
      path: "/channelSubscriptions",
      queryItems: params.queryItems,
      completion: completion
    )
  }

  public func deleteChannelSubscriptions(
    params: PushSubscriptionParams,
    completion: @escaping @Sendable (Result<Void, Error>) -> Void
  ) {
    requestVoid(
      method: "DELETE",
      path: "/channelSubscriptions",
      queryItems: params.queryItems,
      completion: completion
    )
  }

  public func publish(
    _ request: [String: Any],
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    var payload = request
    payload["sync"] = false
    requestObject(method: "POST", path: "/publish", body: payload, completion: completion)
  }

  public func publishBatch(
    _ requests: [[String: Any]],
    completion: @escaping @Sendable (Result<Any, Error>) -> Void
  ) {
    let payload = requests.map { request -> [String: Any] in
      var normalized = request
      normalized["sync"] = false
      return normalized
    }
    requestValue(method: "POST", path: "/batch/publish", body: payload, completion: completion)
  }

  public func schedulePublish(
    _ request: [String: Any],
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    publish(request, completion: completion)
  }

  public func getPublishStatus(
    publishID: String,
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestObject(
      method: "GET",
      path: "/publish/\(encodePath(publishID))/status",
      completion: completion
    )
  }

  public func cancelScheduledPublish(
    publishID: String,
    completion: @escaping @Sendable (Result<Void, Error>) -> Void
  ) {
    requestVoid(
      method: "DELETE",
      path: "/scheduled/\(encodePath(publishID))",
      completion: completion
    )
  }

  public func postDeliveryStatus(
    _ event: [String: Any],
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestObject(method: "POST", path: "/deliveryStatus", body: event, completion: completion)
  }

  private func requestObject(
    method: String,
    path: String,
    body: Any? = nil,
    queryItems: [URLQueryItem] = [],
    requestHeaders: [String: String] = [:],
    completion: @escaping @Sendable (Result<[String: Any], Error>) -> Void
  ) {
    requestValue(
      method: method,
      path: path,
      body: body,
      queryItems: queryItems,
      requestHeaders: requestHeaders
    ) { result in
      completion(
        result.flatMap { value in
          .success(value as? [String: Any] ?? [:])
        })
    }
  }

  private func requestVoid(
    method: String,
    path: String,
    body: Any? = nil,
    queryItems: [URLQueryItem] = [],
    requestHeaders: [String: String] = [:],
    completion: @escaping @Sendable (Result<Void, Error>) -> Void
  ) {
    requestValue(
      method: method,
      path: path,
      body: body,
      queryItems: queryItems,
      requestHeaders: requestHeaders
    ) { result in
      completion(result.map { _ in () })
    }
  }

  private func requestValue(
    method: String,
    path: String,
    body: Any? = nil,
    queryItems: [URLQueryItem] = [],
    requestHeaders: [String: String] = [:],
    completion: @escaping @Sendable (Result<Any, Error>) -> Void
  ) {
    guard var components = URLComponents(string: "\(endpoint)\(path)") else {
      completion(.failure(SockudoError.invalidURL("Invalid push endpoint \(endpoint)\(path)")))
      return
    }
    if queryItems.isEmpty == false {
      components.queryItems = queryItems
    }
    guard let url = components.url else {
      completion(.failure(SockudoError.invalidURL("Invalid push endpoint \(endpoint)\(path)")))
      return
    }

    do {
      var request = URLRequest(url: url)
      request.httpMethod = method
      if let body {
        request.httpBody = try JSON.encodeData(body)
      }
      request.setValue("application/json", forHTTPHeaderField: "Content-Type")
      for (name, value) in headers.merging(headersProvider?() ?? [:], uniquingKeysWith: { _, new in new }) {
        request.setValue(value, forHTTPHeaderField: name)
      }
      for (name, value) in requestHeaders {
        request.setValue(value, forHTTPHeaderField: name)
      }

      urlSession.dataTask(with: request) { data, response, error in
        Task { @MainActor in
          if let error {
            completion(.failure(error))
            return
          }
          let status = (response as? HTTPURLResponse)?.statusCode ?? -1
          guard (200..<300).contains(status) else {
            completion(.failure(SockudoError.invalidOptions("Sockudo push request failed with HTTP \(status)")))
            return
          }
          guard status != 204 else {
            completion(.success([String: Any]()))
            return
          }
          guard let data, data.isEmpty == false else {
            completion(.success([String: Any]()))
            return
          }
          do {
            completion(.success(try JSON.decode(data)))
          } catch {
            completion(.failure(error))
          }
        }
      }.resume()
    } catch {
      completion(.failure(error))
    }
  }

  private func encodePath(_ value: String) -> String {
    value.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? value
  }
}
