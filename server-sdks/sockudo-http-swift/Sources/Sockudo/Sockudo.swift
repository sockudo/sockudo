import AnyCodable
import APIota
import Foundation
import Security

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// Manages operations when interacting with the Sockudo Channels HTTP API.
public class Sockudo {

  // MARK: - Properties

  /// The API client used for querying app state via the Channels HTTP API.
  private let apiClient: APIotaClient

  /// Configuration options used to managing the connection.
  private let options: SockudoClientOptions

  /// A compact base64url-encoded random identifier (16 chars from 12 random bytes),
  /// generated once per client instance. Used as the prefix for deterministic idempotency keys.
  private let baseId: String

  /// A monotonically increasing counter that advances before each trigger call.
  /// Combined with `baseId` to form deterministic idempotency keys (`{baseId}:{publishSerial}`).
  private var publishSerial: UInt64 = 0

  /// A lock protecting `publishSerial` for thread-safe access.
  private let serialLock = NSLock()

  private let maxRetries = 3

  /// When `true` (the default), every `trigger` call that does not already carry
  /// an explicit idempotency key will have one auto-generated in the format
  /// `{baseId}:{publishSerial}`. The serial increments before each call so
  /// retries reuse the same key.
  public var autoIdempotencyKey: Bool = true

  // MARK: - Lifecycle

  /// Creates a Sockudo Channels HTTP API client configured using some `SockudoClientOptions`.
  /// - Parameter options: Configuration options used to managing the connection.
  public init(options: SockudoClientOptions) {
    self.apiClient = APIClient(options: options)
    self.options = options
    self.baseId = Sockudo.generateBaseId()
  }

  /// Creates a Sockudo Channels HTTP API client from a type conforming to `APIotaClient`.
  /// - Parameter apiClient: The API client used to manage the connection.
  /// - Parameter options: Configuration options used to managing the connection.
  init(apiClient: APIotaClient, options: SockudoClientOptions) {
    self.apiClient = apiClient
    self.options = options
    self.baseId = Sockudo.generateBaseId()
  }

  private static func generateBaseId() -> String {
    var bytes = [UInt8](repeating: 0, count: 12)
    _ = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
    return Data(bytes).base64EncodedString()
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "=", with: "")
  }

  // MARK: - Application state queries

  /// Fetches an array of `ChannelSummary` records for any occupied channels.
  /// - Parameters:
  ///   - filter: A filter to apply to the returned results.
  ///   - attributeOptions: A set of attributes that should be returned in each `ChannelSummary`.
  ///   - callback: A closure that returns a `Result` containing an array of `ChannelSummary`
  ///               instances, or a `SockudoError` if the operation fails for some reason.
  public func channels(
    withFilter filter: ChannelFilter = .any,
    attributeOptions: ChannelAttributeFetchOptions = [],
    callback: @escaping (Result<[ChannelSummary], SockudoError>) -> Void
  ) {

    apiClient.sendRequest(
      for: GetChannelsEndpoint(
        channelFilter: filter,
        attributeOptions: attributeOptions,
        options: options)
    ) { result in

      // Map the API response to `[ChannelSummary]` when running the callback
      // and map the API client error to an equivalent `SockudoError`
      callback(
        result
          .map { $0.channelSummaries }
          .mapError({ SockudoError(from: $0) }))
    }
  }

  /// Fetches the `ChannelInfo` for a given occupied channel.
  /// - Parameters:
  ///   - channel: The channel to inspect.
  ///   - attributeOptions: A set of attributes that should be returned for the `channel`.
  ///   - callback: A closure that returns a `Result` containing a `ChannelInfo` instance,
  ///               or a `SockudoError` if the operation fails for some reason.
  public func channelInfo(
    for channel: Channel,
    attributeOptions: ChannelAttributeFetchOptions = [],
    callback: @escaping (Result<ChannelInfo, SockudoError>) -> Void
  ) {

    apiClient.sendRequest(
      for: GetChannelEndpoint(
        channel: channel,
        attributeOptions: attributeOptions,
        options: options)
    ) { result in

      // Map the API client error to an equivalent `SockudoError`
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  /// Fetches an array of `User` records currently subscribed to a given occupied presence `Channel`.
  ///
  /// Users can only be fetched from presence channels. Using a channel with a `ChannelType`
  /// other than `presence` is invalid and will result in an error.
  /// - Parameters:
  ///   - channel: The presence channel to inspect.
  ///   - callback: A closure that returns a `Result` containing an array of `User` instances
  ///               subscribed to the `channel`, or a `SockudoError` if the operation fails
  ///               for some reason.
  public func users(
    for channel: Channel,
    callback: @escaping (Result<[User], SockudoError>) -> Void
  ) {

    apiClient.sendRequest(
      for: GetUsersEndpoint(
        channel: channel,
        options: options)
    ) { result in

      // Map the API response to `[User]` when running the callback
      // and map the API client error to an equivalent `SockudoError`
      callback(
        result
          .map { $0.users }
          .mapError({ SockudoError(from: $0) }))
    }
  }

  public func terminateUserConnections(
    userId: String,
    callback: @escaping (Result<UserConnectionResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: TerminateUserConnectionsEndpoint(userId: userId, options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func forceReconnectUser(
    userId: String,
    callback: @escaping (Result<UserConnectionResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: ForceReconnectUserEndpoint(userId: userId, options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func history(
    for channel: Channel,
    options fetchOptions: ChannelHistoryFetchOptions = .init(),
    callback: @escaping (Result<ChannelHistoryPage, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetChannelHistoryEndpoint(
        channel: channel,
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func getMessage(
    for channel: Channel,
    messageSerial: String,
    callback: @escaping (Result<GetMessageResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetMessageEndpoint(
        channel: channel,
        messageSerial: messageSerial,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func getMessageVersions(
    for channel: Channel,
    messageSerial: String,
    options fetchOptions: MessageVersionsFetchOptions = .init(),
    callback: @escaping (Result<MessageVersionsPage, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetMessageVersionsEndpoint(
        channel: channel,
        messageSerial: messageSerial,
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func updateMessage(
    for channel: Channel,
    messageSerial: String,
    body: [String: Any],
    callback: @escaping (Result<MutationResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: MutateMessageEndpoint(
        httpBody: AnyCodable(body),
        channel: channel,
        messageSerial: messageSerial,
        operation: "update",
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func deleteMessage(
    for channel: Channel,
    messageSerial: String,
    body: [String: Any] = [:],
    callback: @escaping (Result<MutationResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: MutateMessageEndpoint(
        httpBody: AnyCodable(body),
        channel: channel,
        messageSerial: messageSerial,
        operation: "delete",
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func appendMessage(
    for channel: Channel,
    messageSerial: String,
    body: [String: Any],
    callback: @escaping (Result<MutationResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: MutateMessageEndpoint(
        httpBody: AnyCodable(body),
        channel: channel,
        messageSerial: messageSerial,
        operation: "append",
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func publishAnnotation(
    for channel: Channel,
    messageSerial: String,
    body: [String: Any],
    callback: @escaping (Result<PublishAnnotationResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PublishAnnotationEndpoint(
        httpBody: AnyCodable(body),
        channel: channel,
        messageSerial: messageSerial,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func deleteAnnotation(
    for channel: Channel,
    messageSerial: String,
    annotationSerial: String,
    socketID: String? = nil,
    callback: @escaping (Result<DeleteAnnotationResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: DeleteAnnotationEndpoint(
        channel: channel,
        messageSerial: messageSerial,
        annotationSerial: annotationSerial,
        socketID: socketID,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func listAnnotations(
    for channel: Channel,
    messageSerial: String,
    options fetchOptions: AnnotationEventsFetchOptions = .init(),
    callback: @escaping (Result<AnnotationEventsPage, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetAnnotationsEndpoint(
        channel: channel,
        messageSerial: messageSerial,
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func presenceHistory(
    for channel: Channel,
    options fetchOptions: PresenceHistoryFetchOptions = .init(),
    callback: @escaping (Result<PresenceHistoryPage, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetPresenceHistoryEndpoint(
        channel: channel,
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func presenceSnapshot(
    for channel: Channel,
    options fetchOptions: PresenceSnapshotFetchOptions = .init(),
    callback: @escaping (Result<PresenceSnapshot, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetPresenceSnapshotEndpoint(
        channel: channel,
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  // MARK: - Push helpers

  public func activateDevice(
    body: [String: Any],
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(for: PushEndpointFactory.activateDevice(body: body, options: options)) {
      result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func createDeviceActivation(
    body: [String: Any],
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    activateDevice(body: body, callback: callback)
  }

  public func updateDeviceRegistration(
    body: [String: Any],
    deviceIdentityToken: String,
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.updateDeviceRegistration(
        body: body,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func listDeviceRegistrations(
    options fetchOptions: PushCursorFetchOptions = .init(),
    callback: @escaping (Result<PushListResponse<PushObjectResponse>, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.listDeviceRegistrations(fetchOptions: fetchOptions, options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func getDeviceRegistration(
    deviceID: String,
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.getDeviceRegistration(
        deviceID: deviceID,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func deleteDeviceRegistration(
    deviceID: String,
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushNoContentResponse, SockudoError>) -> Void
  ) {
    sendNoContentRequest(
      endpoint: PushEndpointFactory.deleteDeviceRegistration(
        deviceID: deviceID,
        deviceIdentityToken: deviceIdentityToken,
        options: options),
      callback: callback)
  }

  public func removeDeviceRegistrationsByClient(
    clientID: String,
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.removeDeviceRegistrationsByClient(
        clientID: clientID,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func upsertChannelPushSubscription(
    body: [String: Any],
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.upsertChannelPushSubscription(
        body: body,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func listChannelPushSubscriptions(
    options fetchOptions: PushSubscriptionFetchOptions = .init(),
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushListResponse<PushObjectResponse>, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.listChannelPushSubscriptions(
        fetchOptions: fetchOptions,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func deleteChannelPushSubscriptions(
    options fetchOptions: PushSubscriptionFetchOptions,
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.deleteChannelPushSubscriptions(
        fetchOptions: fetchOptions,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func listChannelPushSubscriptionChannels(
    options fetchOptions: PushCursorFetchOptions = .init(),
    callback: @escaping (Result<PushListResponse<String>, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.listChannelPushSubscriptionChannels(
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func listPushCredentials(
    options fetchOptions: PushCursorFetchOptions = .init(),
    callback: @escaping (Result<PushListResponse<PushObjectResponse>, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.listPushCredentials(fetchOptions: fetchOptions, options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func putPushCredential(
    provider: String,
    credential: [String: Any],
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.putPushCredential(
        provider: provider,
        credential: credential,
        options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func publishPush(
    request: [String: Any],
    callback: @escaping (Result<PushPublishAcceptedResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(for: PushEndpointFactory.publishPush(request: request, options: options)) {
      result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func publishPushDirect(
    request: [String: Any],
    callback: @escaping (Result<PushPublishAcceptedResponse, SockudoError>) -> Void
  ) {
    publishPush(request: request, callback: callback)
  }

  public func publishPushBatch(
    requests: [[String: Any]],
    callback: @escaping (Result<PushItemsResponse<PushPublishAcceptedResponse>, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.publishPushBatch(requests: requests, options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func schedulePush(
    request: [String: Any],
    callback: @escaping (Result<PushPublishAcceptedResponse, SockudoError>) -> Void
  ) {
    guard request["notBeforeMs"] != nil else {
      callback(.failure(SockudoError(from: PushHelperError.scheduledPushRequiresNotBeforeMs)))
      return
    }

    publishPush(request: request, callback: callback)
  }

  public func getPublishStatus(
    publishID: String,
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.getPublishStatus(publishID: publishID, options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  public func cancelScheduledPush(
    publishID: String,
    callback: @escaping (Result<PushNoContentResponse, SockudoError>) -> Void
  ) {
    sendNoContentRequest(
      endpoint: PushEndpointFactory.cancelScheduledPush(publishID: publishID, options: options),
      callback: callback)
  }

  public func postPushDeliveryStatus(
    event: [String: Any],
    callback: @escaping (Result<PushObjectResponse, SockudoError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.postPushDeliveryStatus(event: event, options: options)
    ) { result in
      callback(result.mapError({ SockudoError(from: $0) }))
    }
  }

  // MARK: - Idempotency

  /// Generates a unique idempotency key using a UUID string.
  /// - Returns: A UUID string suitable for use as an idempotency key.
  public static func generateIdempotencyKey() -> String {
    return UUID().uuidString
  }

  // MARK: - Triggering events

  /// Triggers an `Event` on one or more `Channel` instances.
  ///
  /// The channel (or channels) that the event should be triggered on, (as well as the
  /// attributes to fetch for each channel) are specified when initializing `event`.
  /// - Parameters:
  ///   - event: The event to trigger.
  ///   - callback: A closure that returns a `Result` containing an array of `ChannelSummary`
  ///               instances, or a `SockudoError` if the operation fails for some reason.
  ///               If the `attributeOptions` on the `event` are not set, an empty channel
  ///               array will be returned.
  public func trigger(
    event: Event,
    callback: @escaping (Result<[ChannelSummary], SockudoError>) -> Void
  ) {

    do {
      var eventToTrigger = event

      let serial = nextSerial()
      if autoIdempotencyKey && event.idempotencyKey == nil {
        eventToTrigger = try event.withIdempotencyKey("\(baseId):\(serial)")
      }

      eventToTrigger = try eventToTrigger.encrypted(using: options)
      sendWithRetry(
        endpoint: TriggerEventEndpoint(httpBody: eventToTrigger, options: options),
        attempt: 0
      ) { result in
        callback(
          result
            .map { $0.channelSummaries }
            .mapError({ SockudoError(from: $0) }))
      }
    } catch {
      callback(.failure(SockudoError(from: error)))
    }
  }

  /// Triggers multiple events, each on a specific `Channel`.
  ///
  /// The channel that the event should be triggered on, (as well as the
  /// attributes to fetch for the each channel) are specified when initializing `event`.
  ///
  /// Any event in `events` that specifies more than one channel to trigger on will result in
  /// an error. Providing an array of more than 10 events to trigger will also result in an error.
  /// - Parameters:
  ///   - events: An array of events to trigger.
  ///   - callback: A closure that returns a `Result` containing an array of `ChannelInfo` instances
  ///               (where the instance at index `i` corresponds to the channel for `events[i]`,
  ///               or a `SockudoError` if the operation fails for some reason.
  ///               If the `attributeOptions` on the `event` are not set, an empty information
  ///               array will be returned.
  public func trigger(
    events: [Event],
    idempotencyKey: String? = nil,
    callback: @escaping (Result<[ChannelInfo], SockudoError>) -> Void
  ) {

    do {
      var eventsToTrigger: [Event]
      let serial = nextSerial()

      if autoIdempotencyKey {
        eventsToTrigger = try events.enumerated().map { index, event in
          var modified = event
          if event.idempotencyKey == nil {
            modified = try event.withIdempotencyKey("\(baseId):\(serial):\(index)")
          }
          return try modified.encrypted(using: options)
        }
      } else {
        eventsToTrigger = try events.map { try $0.encrypted(using: options) }
      }

      sendWithRetry(
        endpoint: TriggerBatchEventsEndpoint(
          events: eventsToTrigger,
          options: options,
          idempotencyKey: idempotencyKey),
        attempt: 0
      ) { result in
        callback(
          result
            .map { $0.channelInfoList }
            .mapError({ SockudoError(from: $0) }))
      }
    } catch {
      callback(.failure(SockudoError(from: error)))
    }
  }

  // MARK: - Webhook verification

  /// Verifies that a received Webhook request is genuine and was received from Sockudo.
  ///
  /// Webhook endpoints are accessible to the global internet. Therefore, verifying that
  /// a Webhook request originated from Sockudo is important. Valid Webhooks contain special headers
  /// that contain a copy of your application key and a HMAC signature of the Webhook payload (i.e. its body).
  /// - Parameters:
  ///   - request: The received Webhook request.
  ///   - callback: A closure that returns a `Result` containing a verified `Webhook` and the
  ///               events that were sent with it (which are decrypted if needed), or a `SockudoError`
  ///               if the operation fails for some reason.
  public func verifyWebhook(
    request: URLRequest,
    callback: @escaping (Result<Webhook, SockudoError>) -> Void
  ) {

    // Verify request key and signature and then decode into a `Webhook`
    do {
      try WebhookService.verifySignature(of: request, using: options)
      let webhook = try WebhookService.webhook(from: request, using: options)

      callback(.success(webhook))
    } catch {
      callback(.failure(SockudoError(from: error)))
    }
  }

  // MARK: - Private helpers

  /// Atomically captures the current serial and increments it for next use.
  private func nextSerial() -> UInt64 {
    serialLock.lock()
    let current = publishSerial
    publishSerial += 1
    serialLock.unlock()
    return current
  }

  /// Sends an API request with up to `maxRetries` attempts on network or 5xx errors.
  private func sendWithRetry<E: APIotaCodableEndpoint>(
    endpoint: E,
    attempt: Int,
    callback: @escaping (Result<E.SuccessResponse, Error>) -> Void
  ) {
    apiClient.sendRequest(for: endpoint) { [weak self] result in
      guard let self = self else {
        callback(result)
        return
      }

      switch result {
      case .success:
        callback(result)
      case .failure(let error):
        let isRetryable: Bool
        if let apiError = error as? APIotaClientError<Data> {
          switch apiError {
          case .failedResponse(statusCode: let statusCode, errorResponseBody: _) where statusCode.rawValue >= 500:
            isRetryable = true
          case .internalError:
            isRetryable = true
          default:
            isRetryable = false
          }
        } else {
          isRetryable = false
        }

        if isRetryable && attempt + 1 < self.maxRetries {
          let delayMs = 100 * (attempt + 1)
          DispatchQueue.global().asyncAfter(deadline: .now() + .milliseconds(delayMs)) {
            self.sendWithRetry(endpoint: endpoint, attempt: attempt + 1, callback: callback)
          }
        } else {
          callback(result)
        }
      }
    }
  }

  private func sendNoContentRequest<E: APIotaCodableEndpoint>(
    endpoint: E,
    callback: @escaping (Result<PushNoContentResponse, SockudoError>) -> Void
  ) where E.ErrorResponse == Data {
    let request: URLRequest
    do {
      request = try endpoint.request(baseUrlComponents: apiClient.baseUrlComponents)
    } catch {
      callback(.failure(SockudoError(from: error)))
      return
    }

    apiClient.session.dataTask(with: request) { data, response, error in
      if let error {
        callback(.failure(SockudoError(from: APIotaClientError<Data>.internalError(error))))
        return
      }

      guard
        let httpResponse = response as? HTTPURLResponse,
        let statusCode = HTTPStatusCode(rawValue: httpResponse.statusCode)
      else {
        callback(.failure(SockudoError(from: APIotaClientError<Data>.unexpectedResponse)))
        return
      }

      guard statusCode.category == .successful else {
        callback(.failure(SockudoError(from: APIotaClientError<Data>.failedResponse(
          statusCode: statusCode,
          errorResponseBody: data ?? Data()))))
        return
      }

      callback(.success(PushNoContentResponse()))
    }.resume()
  }

  private enum PushHelperError: LocalizedError {
    case scheduledPushRequiresNotBeforeMs

    var errorDescription: String? {
      switch self {
      case .scheduledPushRequiresNotBeforeMs:
        return "scheduled push requires notBeforeMs"
      }
    }
  }

  // MARK: - Private and presence channel subscription authentication

  /// Generates an authentication token that can be returned to a user client that is attempting
  /// to subscribe to a private or presence `Channel`, which requires authentication with the server.
  /// - Parameters:
  ///   - channel: The channel for which to generate the authentication token.
  ///   - socketId: The socket identifier for the connected user.
  ///   - userData: The data to generate an authentication token for a subscription attempt
  ///               to a presence channel.
  ///               (This is required when autenticating a presence channel, and should otherwise
  ///               be `nil`).
  ///   - callback: A closure that returns a `Result` containing a `AuthenticationToken` for subscribing
  ///               to a private or presence channel, or a `SockudoError` if the operation fails
  ///               for some reason.
  public func authenticate(
    channel: Channel,
    socketId: String,
    userData: PresenceUserData? = nil,
    callback: @escaping (Result<AuthenticationToken, SockudoError>) -> Void
  ) {

    do {
      let token = try AuthenticationTokenService.authenticationToken(
        for: channel,
        socketId: socketId,
        userData: userData,
        using: options)
      callback(.success(token))
    } catch {
      callback(.failure(SockudoError(from: error)))
    }
  }
}
