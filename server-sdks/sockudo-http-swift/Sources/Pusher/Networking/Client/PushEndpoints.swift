import AnyCodable
import APIota
import Foundation

enum PushCapability: String {
  case admin = "push-admin"
  case subscribe = "push-subscribe"
}

struct PushIgnoredResponse: Decodable {}

struct PushEndpoint<SuccessResponse: Decodable, Body: Encodable>: APIotaCodableEndpoint {
  typealias ErrorResponse = Data

  let encoder: JSONEncoder = JSONEncoder.iso8601Ordered
  let headers: HTTPHeaders?
  let httpBody: Body?
  let httpMethod: HTTPMethod
  let path: String
  let options: PusherClientOptions
  private let additionalQueryItems: [URLQueryItem]

  var queryItems: [URLQueryItem]? {
    let authInfo = AuthInfo(
      httpBody: httpBody,
      httpMethod: httpMethod.rawValue,
      path: path,
      key: options.key,
      secret: options.secret,
      additionalQueryItems: additionalQueryItems)
    return authInfo.queryItems
  }

  init(
    path: String,
    httpMethod: HTTPMethod,
    httpBody: Body? = nil,
    queryItems: [URLQueryItem] = [],
    capability: PushCapability,
    deviceIdentityToken: String? = nil,
    options: PusherClientOptions
  ) {
    var resolvedHeaders = APIClient.defaultHeaders
    if httpBody != nil {
      resolvedHeaders.replaceOrAdd(header: .contentType, value: HTTPMediaType.json.toString())
    }
    resolvedHeaders.replaceOrAdd(
      header: HTTPHeader("X-Sockudo-Push-Capability"),
      value: capability.rawValue)
    if let deviceIdentityToken {
      resolvedHeaders.replaceOrAdd(
        header: HTTPHeader("X-Sockudo-Device-Identity-Token"),
        value: deviceIdentityToken)
    }

    self.headers = resolvedHeaders
    self.httpBody = httpBody
    self.httpMethod = httpMethod
    self.path = path
    self.options = options
    self.additionalQueryItems = queryItems
  }
}

enum PushEndpointFactory {
  static func activateDevice(
    body: [String: Any],
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, AnyCodable> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/deviceRegistrations",
      httpMethod: .POST,
      httpBody: AnyCodable(body),
      capability: .admin,
      options: options)
  }

  static func updateDeviceRegistration(
    body: [String: Any],
    deviceIdentityToken: String,
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, AnyCodable> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/deviceRegistrations",
      httpMethod: .POST,
      httpBody: AnyCodable(body),
      capability: .subscribe,
      deviceIdentityToken: deviceIdentityToken,
      options: options)
  }

  static func listDeviceRegistrations(
    fetchOptions: PushCursorFetchOptions,
    options: PusherClientOptions
  ) -> PushEndpoint<PushListResponse<PushObjectResponse>, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/deviceRegistrations",
      httpMethod: .GET,
      queryItems: fetchOptions.queryItems,
      capability: .admin,
      options: options)
  }

  static func getDeviceRegistration(
    deviceID: String,
    deviceIdentityToken: String?,
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/deviceRegistrations/\(deviceID)",
      httpMethod: .GET,
      capability: deviceIdentityToken == nil ? .admin : .subscribe,
      deviceIdentityToken: deviceIdentityToken,
      options: options)
  }

  static func deleteDeviceRegistration(
    deviceID: String,
    deviceIdentityToken: String?,
    options: PusherClientOptions
  ) -> PushEndpoint<PushIgnoredResponse, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/deviceRegistrations/\(deviceID)",
      httpMethod: .DELETE,
      capability: deviceIdentityToken == nil ? .admin : .subscribe,
      deviceIdentityToken: deviceIdentityToken,
      options: options)
  }

  static func removeDeviceRegistrationsByClient(
    clientID: String,
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/deviceRegistrations",
      httpMethod: .DELETE,
      queryItems: [URLQueryItem(name: "clientId", value: clientID)],
      capability: .admin,
      options: options)
  }

  static func upsertChannelPushSubscription(
    body: [String: Any],
    deviceIdentityToken: String?,
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, AnyCodable> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/channelSubscriptions",
      httpMethod: .POST,
      httpBody: AnyCodable(body),
      capability: deviceIdentityToken == nil ? .admin : .subscribe,
      deviceIdentityToken: deviceIdentityToken,
      options: options)
  }

  static func listChannelPushSubscriptions(
    fetchOptions: PushSubscriptionFetchOptions,
    deviceIdentityToken: String?,
    options: PusherClientOptions
  ) -> PushEndpoint<PushListResponse<PushObjectResponse>, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/channelSubscriptions",
      httpMethod: .GET,
      queryItems: fetchOptions.queryItems,
      capability: deviceIdentityToken == nil ? .admin : .subscribe,
      deviceIdentityToken: deviceIdentityToken,
      options: options)
  }

  static func deleteChannelPushSubscriptions(
    fetchOptions: PushSubscriptionFetchOptions,
    deviceIdentityToken: String?,
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/channelSubscriptions",
      httpMethod: .DELETE,
      queryItems: fetchOptions.queryItems,
      capability: deviceIdentityToken == nil ? .admin : .subscribe,
      deviceIdentityToken: deviceIdentityToken,
      options: options)
  }

  static func listChannelPushSubscriptionChannels(
    fetchOptions: PushCursorFetchOptions,
    options: PusherClientOptions
  ) -> PushEndpoint<PushListResponse<String>, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/channelSubscriptions/channels",
      httpMethod: .GET,
      queryItems: fetchOptions.queryItems,
      capability: .admin,
      options: options)
  }

  static func listPushCredentials(
    fetchOptions: PushCursorFetchOptions,
    options: PusherClientOptions
  ) -> PushEndpoint<PushListResponse<PushObjectResponse>, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/credentials",
      httpMethod: .GET,
      queryItems: fetchOptions.queryItems,
      capability: .admin,
      options: options)
  }

  static func putPushCredential(
    provider: String,
    credential: [String: Any],
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, AnyCodable> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/credentials/\(provider)",
      httpMethod: .POST,
      httpBody: AnyCodable(credential),
      capability: .admin,
      options: options)
  }

  static func publishPush(
    request: [String: Any],
    options: PusherClientOptions
  ) -> PushEndpoint<PushPublishAcceptedResponse, AnyCodable> {
    var request = request
    request["sync"] = false
    return PushEndpoint(
      path: "/apps/\(options.appId)/push/publish",
      httpMethod: .POST,
      httpBody: AnyCodable(request),
      capability: .admin,
      options: options)
  }

  static func publishPushBatch(
    requests: [[String: Any]],
    options: PusherClientOptions
  ) -> PushEndpoint<PushItemsResponse<PushPublishAcceptedResponse>, AnyCodable> {
    let payload = requests.map { request -> [String: Any] in
      var request = request
      request["sync"] = false
      return request
    }
    return PushEndpoint(
      path: "/apps/\(options.appId)/push/batch/publish",
      httpMethod: .POST,
      httpBody: AnyCodable(payload),
      capability: .admin,
      options: options)
  }

  static func getPublishStatus(
    publishID: String,
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/publish/\(publishID)/status",
      httpMethod: .GET,
      capability: .admin,
      options: options)
  }

  static func cancelScheduledPush(
    publishID: String,
    options: PusherClientOptions
  ) -> PushEndpoint<PushIgnoredResponse, String> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/scheduled/\(publishID)",
      httpMethod: .DELETE,
      capability: .admin,
      options: options)
  }

  static func postPushDeliveryStatus(
    event: [String: Any],
    options: PusherClientOptions
  ) -> PushEndpoint<PushObjectResponse, AnyCodable> {
    PushEndpoint(
      path: "/apps/\(options.appId)/push/deliveryStatus",
      httpMethod: .POST,
      httpBody: AnyCodable(event),
      capability: .admin,
      options: options)
  }
}
