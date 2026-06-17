import Foundation

public struct ChannelAuthorizationData: Sendable, Equatable {
  public let auth: String
  public let channelData: String?
  public let sharedSecret: String?

  public init(auth: String, channelData: String? = nil, sharedSecret: String? = nil) {
    self.auth = auth
    self.channelData = channelData
    self.sharedSecret = sharedSecret
  }
}

public struct UserAuthenticationData: Sendable, Equatable {
  public let auth: String
  public let userData: String

  public init(auth: String, userData: String) {
    self.auth = auth
    self.userData = userData
  }
}

public struct ChannelAuthorizationRequest: Sendable, Equatable {
  public let socketID: String
  public let channelName: String

  public init(socketID: String, channelName: String) {
    self.socketID = socketID
    self.channelName = channelName
  }
}

public struct UserAuthenticationRequest: Sendable, Equatable {
  public let socketID: String

  public init(socketID: String) {
    self.socketID = socketID
  }
}

public typealias ChannelAuthorizationHandler =
  (
    ChannelAuthorizationRequest,
    @escaping @Sendable (Result<ChannelAuthorizationData, Error>) -> Void
  ) -> Void
public typealias UserAuthenticationHandler =
  (
    UserAuthenticationRequest,
    @escaping @Sendable (Result<UserAuthenticationData, Error>) -> Void
  ) -> Void

public struct ChannelAuthorizationOptions {
  public var endpoint: String
  public var headers: [String: String]
  public var params: [String: AuthValue]
  public var headersProvider: (@Sendable () -> [String: String])?
  public var paramsProvider: (@Sendable () -> [String: AuthValue])?
  public var customHandler: ChannelAuthorizationHandler?

  public init(
    endpoint: String = "/sockudo/auth",
    headers: [String: String] = [:],
    params: [String: AuthValue] = [:],
    headersProvider: (@Sendable () -> [String: String])? = nil,
    paramsProvider: (@Sendable () -> [String: AuthValue])? = nil,
    customHandler: ChannelAuthorizationHandler? = nil
  ) {
    self.endpoint = endpoint
    self.headers = headers
    self.params = params
    self.headersProvider = headersProvider
    self.paramsProvider = paramsProvider
    self.customHandler = customHandler
  }
}

public struct UserAuthenticationOptions {
  public var endpoint: String
  public var headers: [String: String]
  public var params: [String: AuthValue]
  public var headersProvider: (@Sendable () -> [String: String])?
  public var paramsProvider: (@Sendable () -> [String: AuthValue])?
  public var customHandler: UserAuthenticationHandler?

  public init(
    endpoint: String = "/sockudo/user-auth",
    headers: [String: String] = [:],
    params: [String: AuthValue] = [:],
    headersProvider: (@Sendable () -> [String: String])? = nil,
    paramsProvider: (@Sendable () -> [String: AuthValue])? = nil,
    customHandler: UserAuthenticationHandler? = nil
  ) {
    self.endpoint = endpoint
    self.headers = headers
    self.params = params
    self.headersProvider = headersProvider
    self.paramsProvider = paramsProvider
    self.customHandler = customHandler
  }
}

public struct PresenceHistoryOptions {
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

public struct VersionedMessagesOptions {
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
