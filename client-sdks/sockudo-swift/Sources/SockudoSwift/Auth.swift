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

public typealias ChannelAuthorizationHandler = @Sendable (
  ChannelAuthorizationRequest,
  @escaping @Sendable (Result<ChannelAuthorizationData, Error>) -> Void
) -> Void
public typealias UserAuthenticationHandler = @Sendable (
  UserAuthenticationRequest,
  @escaping @Sendable (Result<UserAuthenticationData, Error>) -> Void
) -> Void
public typealias CapabilityTokenProvider =
  @Sendable (@escaping @Sendable (Result<String, Error>) -> Void) -> Void
public typealias CapabilityTokenAsyncProvider = @Sendable () async throws -> String

public struct ChannelAuthorizationOptions: Sendable {
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

public struct UserAuthenticationOptions: Sendable {
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

public struct CapabilityTokenAuthData: Sendable, Equatable {
  public let clientID: String?
  public let jti: String?
  public let exp: Int64?

  public init(clientID: String? = nil, jti: String? = nil, exp: Int64? = nil) {
    self.clientID = clientID
    self.jti = jti
    self.exp = exp
  }
}

public struct CapabilityTokenExpiredData: Sendable, Equatable {
  public let code: Int?
  public let reason: String?

  public init(code: Int? = nil, reason: String? = nil) {
    self.code = code
    self.reason = reason
  }
}

public struct CapabilityTokenOptions: Sendable {
  public var token: String?
  public var provider: CapabilityTokenProvider?

  public init(
    token: String? = nil,
    provider: CapabilityTokenProvider? = nil
  ) {
    self.token = token
    self.provider = provider
  }

  public init(
    token: String? = nil,
    asyncProvider: @escaping CapabilityTokenAsyncProvider
  ) {
    self.token = token
    self.provider = { completion in
      Task {
        do {
          completion(.success(try await asyncProvider()))
        } catch {
          completion(.failure(error))
        }
      }
    }
  }
}

public struct PresenceHistoryOptions: Sendable {
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

public struct VersionedMessagesOptions: Sendable {
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
