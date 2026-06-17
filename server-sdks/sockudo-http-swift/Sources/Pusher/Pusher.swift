import APIota
import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// Manages operations when interacting with the Pusher Channels HTTP API.
public class Pusher {

  // MARK: - Properties

  /// The API client used for querying app state via the Channels HTTP API.
  private let apiClient: APIotaClient

  /// Configuration options used to managing the connection.
  private let options: PusherClientOptions

  // MARK: - Lifecycle

  /// Creates a Pusher Channels HTTP API client configured using some `PusherClientOptions`.
  /// - Parameter options: Configuration options used to managing the connection.
  public init(options: PusherClientOptions) {
    self.apiClient = APIClient(options: options)
    self.options = options
  }

  /// Creates a Pusher Channels HTTP API client from a type conforming to `APIotaClient`.
  /// - Parameter apiClient: The API client used to manage the connection.
  /// - Parameter options: Configuration options used to managing the connection.
  init(apiClient: APIotaClient, options: PusherClientOptions) {
    self.apiClient = apiClient
    self.options = options
  }

  // MARK: - Application state queries

  /// Fetches an array of `ChannelSummary` records for any occupied channels.
  /// - Parameters:
  ///   - filter: A filter to apply to the returned results.
  ///   - attributeOptions: A set of attributes that should be returned in each `ChannelSummary`.
  ///   - callback: A closure that returns a `Result` containing an array of `ChannelSummary`
  ///               instances, or a `PusherError` if the operation fails for some reason.
  public func channels(
    withFilter filter: ChannelFilter = .any,
    attributeOptions: ChannelAttributeFetchOptions = [],
    callback: @escaping (Result<[ChannelSummary], PusherError>) -> Void
  ) {

    apiClient.sendRequest(
      for: GetChannelsEndpoint(
        channelFilter: filter,
        attributeOptions: attributeOptions,
        options: options)
    ) { result in

      // Map the API response to `[ChannelSummary]` when running the callback
      // and map the API client error to an equivalent `PusherError`
      callback(
        result
          .map { $0.channelSummaries }
          .mapError({ PusherError(from: $0) }))
    }
  }

  /// Fetches the `ChannelInfo` for a given occupied channel.
  /// - Parameters:
  ///   - channel: The channel to inspect.
  ///   - attributeOptions: A set of attributes that should be returned for the `channel`.
  ///   - callback: A closure that returns a `Result` containing a `ChannelInfo` instance,
  ///               or a `PusherError` if the operation fails for some reason.
  public func channelInfo(
    for channel: Channel,
    attributeOptions: ChannelAttributeFetchOptions = [],
    callback: @escaping (Result<ChannelInfo, PusherError>) -> Void
  ) {

    apiClient.sendRequest(
      for: GetChannelEndpoint(
        channel: channel,
        attributeOptions: attributeOptions,
        options: options)
    ) { result in

      // Map the API client error to an equivalent `PusherError`
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  /// Fetches an array of `User` records currently subscribed to a given occupied presence `Channel`.
  ///
  /// Users can only be fetched from presence channels. Using a channel with a `ChannelType`
  /// other than `presence` is invalid and will result in an error.
  /// - Parameters:
  ///   - channel: The presence channel to inspect.
  ///   - callback: A closure that returns a `Result` containing an array of `User` instances
  ///               subscribed to the `channel`, or a `PusherError` if the operation fails
  ///               for some reason.
  public func users(
    for channel: Channel,
    callback: @escaping (Result<[User], PusherError>) -> Void
  ) {

    apiClient.sendRequest(
      for: GetUsersEndpoint(
        channel: channel,
        options: options)
    ) { result in

      // Map the API response to `[User]` when running the callback
      // and map the API client error to an equivalent `PusherError`
      callback(
        result
          .map { $0.users }
          .mapError({ PusherError(from: $0) }))
    }
  }

  /// Fetches durable history for a given channel.
  public func history(
    for channel: Channel,
    options fetchOptions: ChannelHistoryFetchOptions = .init(),
    callback: @escaping (Result<ChannelHistoryPage, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetChannelHistoryEndpoint(
        channel: channel,
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func presenceHistory(
    for channel: Channel,
    options fetchOptions: PresenceHistoryFetchOptions = .init(),
    callback: @escaping (Result<PresenceHistoryPage, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetPresenceHistoryEndpoint(
        channel: channel,
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func presenceSnapshot(
    for channel: Channel,
    options fetchOptions: PresenceSnapshotFetchOptions = .init(),
    callback: @escaping (Result<PresenceSnapshot, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: GetPresenceSnapshotEndpoint(
        channel: channel,
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  // MARK: - Push helpers

  public func activateDevice(
    body: [String: Any],
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(for: PushEndpointFactory.activateDevice(body: body, options: options)) {
      result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func createDeviceActivation(
    body: [String: Any],
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    activateDevice(body: body, callback: callback)
  }

  public func updateDeviceRegistration(
    body: [String: Any],
    deviceIdentityToken: String,
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.updateDeviceRegistration(
        body: body,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func listDeviceRegistrations(
    options fetchOptions: PushCursorFetchOptions = .init(),
    callback: @escaping (Result<PushListResponse<PushObjectResponse>, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.listDeviceRegistrations(fetchOptions: fetchOptions, options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func getDeviceRegistration(
    deviceID: String,
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.getDeviceRegistration(
        deviceID: deviceID,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func deleteDeviceRegistration(
    deviceID: String,
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushNoContentResponse, PusherError>) -> Void
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
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.removeDeviceRegistrationsByClient(
        clientID: clientID,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func upsertChannelPushSubscription(
    body: [String: Any],
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.upsertChannelPushSubscription(
        body: body,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func listChannelPushSubscriptions(
    options fetchOptions: PushSubscriptionFetchOptions = .init(),
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushListResponse<PushObjectResponse>, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.listChannelPushSubscriptions(
        fetchOptions: fetchOptions,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func deleteChannelPushSubscriptions(
    options fetchOptions: PushSubscriptionFetchOptions,
    deviceIdentityToken: String? = nil,
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.deleteChannelPushSubscriptions(
        fetchOptions: fetchOptions,
        deviceIdentityToken: deviceIdentityToken,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func listChannelPushSubscriptionChannels(
    options fetchOptions: PushCursorFetchOptions = .init(),
    callback: @escaping (Result<PushListResponse<String>, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.listChannelPushSubscriptionChannels(
        fetchOptions: fetchOptions,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func listPushCredentials(
    options fetchOptions: PushCursorFetchOptions = .init(),
    callback: @escaping (Result<PushListResponse<PushObjectResponse>, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.listPushCredentials(fetchOptions: fetchOptions, options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func putPushCredential(
    provider: String,
    credential: [String: Any],
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.putPushCredential(
        provider: provider,
        credential: credential,
        options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func publishPush(
    request: [String: Any],
    callback: @escaping (Result<PushPublishAcceptedResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(for: PushEndpointFactory.publishPush(request: request, options: options)) {
      result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func publishPushDirect(
    request: [String: Any],
    callback: @escaping (Result<PushPublishAcceptedResponse, PusherError>) -> Void
  ) {
    publishPush(request: request, callback: callback)
  }

  public func publishPushBatch(
    requests: [[String: Any]],
    callback: @escaping (Result<PushItemsResponse<PushPublishAcceptedResponse>, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.publishPushBatch(requests: requests, options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func schedulePush(
    request: [String: Any],
    callback: @escaping (Result<PushPublishAcceptedResponse, PusherError>) -> Void
  ) {
    guard request["notBeforeMs"] != nil else {
      callback(.failure(PusherError(from: PushHelperError.scheduledPushRequiresNotBeforeMs)))
      return
    }

    publishPush(request: request, callback: callback)
  }

  public func getPublishStatus(
    publishID: String,
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.getPublishStatus(publishID: publishID, options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  public func cancelScheduledPush(
    publishID: String,
    callback: @escaping (Result<PushNoContentResponse, PusherError>) -> Void
  ) {
    sendNoContentRequest(
      endpoint: PushEndpointFactory.cancelScheduledPush(publishID: publishID, options: options),
      callback: callback)
  }

  public func postPushDeliveryStatus(
    event: [String: Any],
    callback: @escaping (Result<PushObjectResponse, PusherError>) -> Void
  ) {
    apiClient.sendRequest(
      for: PushEndpointFactory.postPushDeliveryStatus(event: event, options: options)
    ) { result in
      callback(result.mapError({ PusherError(from: $0) }))
    }
  }

  // MARK: - Triggering events

  /// Triggers an `Event` on one or more `Channel` instances.
  ///
  /// The channel (or channels) that the event should be triggered on, (as well as the
  /// attributes to fetch for each channel) are specified when initializing `event`.
  /// - Parameters:
  ///   - event: The event to trigger.
  ///   - callback: A closure that returns a `Result` containing an array of `ChannelSummary`
  ///               instances, or a `PusherError` if the operation fails for some reason.
  ///               If the `attributeOptions` on the `event` are not set, an empty channel
  ///               array will be returned.
  public func trigger(
    event: Event,
    callback: @escaping (Result<[ChannelSummary], PusherError>) -> Void
  ) {

    do {
      // Encrypt the `eventData` (if necessary) before triggering the event
      let eventToTrigger = try event.encrypted(using: options)
      apiClient.sendRequest(
        for: TriggerEventEndpoint(
          httpBody: eventToTrigger,
          options: options)
      ) { result in

        // Map the API client error to an equivalent `PusherError`
        callback(
          result
            .map { $0.channelSummaries }
            .mapError({ PusherError(from: $0) }))
      }
    } catch {
      callback(.failure(PusherError(from: error)))
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
  ///               or a `PusherError` if the operation fails for some reason.
  ///               If the `attributeOptions` on the `event` are not set, an empty information
  ///               array will be returned.
  public func trigger(
    events: [Event],
    callback: @escaping (Result<[ChannelInfo], PusherError>) -> Void
  ) {

    do {
      let eventsToTrigger = try events.map { try $0.encrypted(using: options) }
      apiClient.sendRequest(
        for: TriggerBatchEventsEndpoint(
          events: eventsToTrigger,
          options: options)
      ) { result in

        // Map the API client error to an equivalent `PusherError`
        callback(
          result
            .map { $0.channelInfoList }
            .mapError({ PusherError(from: $0) }))
      }
    } catch {
      callback(.failure(PusherError(from: error)))
    }
  }

  // MARK: - Webhook verification

  /// Verifies that a received Webhook request is genuine and was received from Pusher.
  ///
  /// Webhook endpoints are accessible to the global internet. Therefore, verifying that
  /// a Webhook request originated from Pusher is important. Valid Webhooks contain special headers
  /// that contain a copy of your application key and a HMAC signature of the Webhook payload (i.e. its body).
  /// - Parameters:
  ///   - request: The received Webhook request.
  ///   - callback: A closure that returns a `Result` containing a verified `Webhook` and the
  ///               events that were sent with it (which are decrypted if needed), or a `PusherError`
  ///               if the operation fails for some reason.
  public func verifyWebhook(
    request: URLRequest,
    callback: @escaping (Result<Webhook, PusherError>) -> Void
  ) {

    // Verify request key and signature and then decode into a `Webhook`
    do {
      try WebhookService.verifySignature(of: request, using: options)
      let webhook = try WebhookService.webhook(from: request, using: options)

      callback(.success(webhook))
    } catch {
      callback(.failure(PusherError(from: error)))
    }
  }

  private func sendNoContentRequest<E: APIotaCodableEndpoint>(
    endpoint: E,
    callback: @escaping (Result<PushNoContentResponse, PusherError>) -> Void
  ) where E.ErrorResponse == Data {
    let request: URLRequest
    do {
      request = try endpoint.request(baseUrlComponents: apiClient.baseUrlComponents)
    } catch {
      callback(.failure(PusherError(from: error)))
      return
    }

    apiClient.session.dataTask(with: request) { data, response, error in
      if let error {
        callback(.failure(PusherError(from: APIotaClientError<Data>.internalError(error))))
        return
      }

      guard
        let httpResponse = response as? HTTPURLResponse,
        let statusCode = HTTPStatusCode(rawValue: httpResponse.statusCode)
      else {
        callback(.failure(PusherError(from: APIotaClientError<Data>.unexpectedResponse)))
        return
      }

      guard statusCode.category == .successful else {
        callback(.failure(PusherError(from: APIotaClientError<Data>.failedResponse(
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
  ///               to a private or presence channel, or a `PusherError` if the operation fails
  ///               for some reason.
  public func authenticate(
    channel: Channel,
    socketId: String,
    userData: PresenceUserData? = nil,
    callback: @escaping (Result<AuthenticationToken, PusherError>) -> Void
  ) {

    do {
      let token = try AuthenticationTokenService.authenticationToken(
        for: channel,
        socketId: socketId,
        userData: userData,
        using: options)
      callback(.success(token))
    } catch {
      callback(.failure(PusherError(from: error)))
    }
  }
}
