import 'dart:convert';

import 'package:http/http.dart' as http;

import 'support.dart';

typedef PushHeadersProvider = Map<String, String> Function();

class PushRegistrationOptions {
  const PushRegistrationOptions({
    required this.endpoint,
    this.headers = const <String, String>{},
    this.headersProvider,
  });

  final String endpoint;
  final Map<String, String> headers;
  final PushHeadersProvider? headersProvider;
}

class PushCursorParams {
  const PushCursorParams({this.limit, this.cursor});

  final int? limit;
  final String? cursor;

  Map<String, String> toQueryParameters() => <String, String>{
    'limit': ?(limit == null ? null : '$limit'),
    'cursor': ?cursor,
  };
}

class PushSubscriptionParams extends PushCursorParams {
  const PushSubscriptionParams({
    this.channel,
    this.deviceId,
    super.limit,
    super.cursor,
  });

  final String? channel;
  final String? deviceId;

  @override
  Map<String, String> toQueryParameters() => <String, String>{
    'channel': ?channel,
    'deviceId': ?deviceId,
    ...super.toQueryParameters(),
  };
}

class SockudoPushRegistration {
  SockudoPushRegistration(this.options, {http.Client? httpClient})
    : _httpClient = httpClient ?? http.Client(),
      _endpoint = options.endpoint.replaceAll(RegExp(r'/+$'), '');

  final PushRegistrationOptions options;
  final http.Client _httpClient;
  final String _endpoint;

  Future<Map<Object?, Object?>> activateDevice(Map<String, Object?> device) =>
      _requestObject('POST', '/deviceRegistrations', body: device);

  Future<Map<Object?, Object?>> updateDeviceRegistration(
    Map<String, Object?> device,
    String deviceIdentityToken,
  ) => _requestObject(
    'POST',
    '/deviceRegistrations',
    body: device,
    requestHeaders: <String, String>{
      'X-Sockudo-Device-Identity-Token': deviceIdentityToken,
    },
  );

  Future<Map<Object?, Object?>> listDeviceRegistrations([
    PushCursorParams params = const PushCursorParams(),
  ]) => _requestObject(
    'GET',
    '/deviceRegistrations',
    queryParameters: params.toQueryParameters(),
  );

  Future<Map<Object?, Object?>> getDeviceRegistration(String deviceId) =>
      _requestObject(
        'GET',
        '/deviceRegistrations/${Uri.encodeComponent(deviceId)}',
      );

  Future<void> deleteDeviceRegistration(String deviceId) => _requestVoid(
    'DELETE',
    '/deviceRegistrations/${Uri.encodeComponent(deviceId)}',
  );

  Future<Map<Object?, Object?>> upsertChannelSubscription(
    Map<String, Object?> subscription,
  ) => _requestObject('POST', '/channelSubscriptions', body: subscription);

  Future<Map<Object?, Object?>> listChannelSubscriptions([
    PushSubscriptionParams params = const PushSubscriptionParams(),
  ]) => _requestObject(
    'GET',
    '/channelSubscriptions',
    queryParameters: params.toQueryParameters(),
  );

  Future<void> deleteChannelSubscriptions(PushSubscriptionParams params) =>
      _requestVoid(
        'DELETE',
        '/channelSubscriptions',
        queryParameters: params.toQueryParameters(),
      );

  Future<Map<Object?, Object?>> publish(Map<String, Object?> request) =>
      _requestObject(
        'POST',
        '/publish',
        body: <String, Object?>{...request, 'sync': false},
      );

  Future<Object?> publishBatch(List<Map<String, Object?>> requests) =>
      _requestValue(
        'POST',
        '/batch/publish',
        body: requests
            .map((request) => <String, Object?>{...request, 'sync': false})
            .toList(growable: false),
      );

  Future<Map<Object?, Object?>> schedulePublish(Map<String, Object?> request) =>
      publish(request);

  Future<Map<Object?, Object?>> getPublishStatus(String publishId) =>
      _requestObject(
        'GET',
        '/publish/${Uri.encodeComponent(publishId)}/status',
      );

  Future<void> cancelScheduledPublish(String publishId) =>
      _requestVoid('DELETE', '/scheduled/${Uri.encodeComponent(publishId)}');

  Future<Map<Object?, Object?>> postDeliveryStatus(
    Map<String, Object?> event,
  ) => _requestObject('POST', '/deliveryStatus', body: event);

  Future<Map<Object?, Object?>> _requestObject(
    String method,
    String path, {
    Object? body,
    Map<String, String> queryParameters = const <String, String>{},
    Map<String, String> requestHeaders = const <String, String>{},
  }) async {
    final value = await _requestValue(
      method,
      path,
      body: body,
      queryParameters: queryParameters,
      requestHeaders: requestHeaders,
    );
    return value as Map<Object?, Object?>? ?? const <Object?, Object?>{};
  }

  Future<void> _requestVoid(
    String method,
    String path, {
    Object? body,
    Map<String, String> queryParameters = const <String, String>{},
    Map<String, String> requestHeaders = const <String, String>{},
  }) async {
    await _requestValue(
      method,
      path,
      body: body,
      queryParameters: queryParameters,
      requestHeaders: requestHeaders,
    );
  }

  Future<Object?> _requestValue(
    String method,
    String path, {
    Object? body,
    Map<String, String> queryParameters = const <String, String>{},
    Map<String, String> requestHeaders = const <String, String>{},
  }) async {
    final uri = Uri.parse('$_endpoint$path').replace(
      queryParameters: queryParameters.isEmpty ? null : queryParameters,
    );
    final request = http.Request(method, uri)
      ..headers.addAll(<String, String>{
        'Content-Type': 'application/json',
        ...options.headers,
        ...?options.headersProvider?.call(),
        ...requestHeaders,
      });
    if (body != null) {
      request.body = jsonEncode(body);
    }
    final response = await _httpClient.send(request);
    final text = await response.stream.bytesToString();
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw SockudoException(
        'Sockudo push request failed with HTTP ${response.statusCode}',
        statusCode: response.statusCode,
      );
    }
    if (response.statusCode == 204 || text.isEmpty) {
      return const <Object?, Object?>{};
    }
    return JsonSupport.decode(text);
  }
}
