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

enum ApnsChannelStoragePolicy { noStorage, mostRecent }

enum ApnsLiveActivityEvent { start, update, end }

enum ApnsLiveActivityPriority { lowPower, conservePower, immediate }

enum ApnsLiveActivityTokenKind { pushToStart, update }

/// A token rotation emitted by the app's ActivityKit bridge.
///
/// Upload this value to an authenticated backend immediately. Live Activity
/// tokens are credentials and must not be logged or stored in analytics.
class ApnsLiveActivityTokenUpdate {
  const ApnsLiveActivityTokenUpdate._({
    required this.kind,
    required this.token,
    this.activityId,
  });

  factory ApnsLiveActivityTokenUpdate.pushToStart(String token) =>
      ApnsLiveActivityTokenUpdate._(
        kind: ApnsLiveActivityTokenKind.pushToStart,
        token: token,
      );

  factory ApnsLiveActivityTokenUpdate.activity({
    required String activityId,
    required String token,
  }) => ApnsLiveActivityTokenUpdate._(
    kind: ApnsLiveActivityTokenKind.update,
    activityId: activityId,
    token: token,
  );

  final ApnsLiveActivityTokenKind kind;
  final String token;
  final String? activityId;

  Map<String, Object?> toJson() {
    if (token.trim().isEmpty) {
      throw ArgumentError.value(token, 'token', 'must not be empty');
    }
    if (kind == ApnsLiveActivityTokenKind.update &&
        (activityId == null || activityId!.trim().isEmpty)) {
      throw ArgumentError.value(
        activityId,
        'activityId',
        'is required for activity update tokens',
      );
    }
    return <String, Object?>{
      'kind': kind.name,
      'token': token,
      if (activityId != null) 'activityId': activityId,
    };
  }

  static String encodeHex(Iterable<int> bytes) {
    final output = StringBuffer();
    for (final byte in bytes) {
      if (byte < 0 || byte > 255) {
        throw ArgumentError.value(byte, 'bytes', 'must contain byte values');
      }
      output.write(byte.toRadixString(16).padLeft(2, '0'));
    }
    return output.toString();
  }
}

sealed class ApnsLiveActivityRecipient {
  const ApnsLiveActivityRecipient();

  bool get isBroadcast;

  Map<String, Object?> toJson();
}

final class ApnsLiveActivityTokenRecipient extends ApnsLiveActivityRecipient {
  const ApnsLiveActivityTokenRecipient(this.activityToken);

  final String activityToken;

  @override
  bool get isBroadcast => false;

  @override
  Map<String, Object?> toJson() {
    if (activityToken.trim().isEmpty) {
      throw ArgumentError.value(
        activityToken,
        'activityToken',
        'must not be empty',
      );
    }
    return <String, Object?>{
      'transportType': 'apnsLiveActivity',
      'activityToken': activityToken,
    };
  }
}

final class ApnsLiveActivityBroadcastRecipient
    extends ApnsLiveActivityRecipient {
  const ApnsLiveActivityBroadcastRecipient({
    required this.channelId,
    this.storagePolicy = ApnsChannelStoragePolicy.noStorage,
  });

  final String channelId;
  final ApnsChannelStoragePolicy storagePolicy;

  @override
  bool get isBroadcast => true;

  @override
  Map<String, Object?> toJson() {
    if (channelId.trim().isEmpty) {
      throw ArgumentError.value(channelId, 'channelId', 'must not be empty');
    }
    return <String, Object?>{
      'transportType': 'apnsLiveActivityBroadcast',
      'channelId': channelId,
      'storagePolicy': storagePolicy.name,
    };
  }
}

class ApnsLiveActivityPayload {
  const ApnsLiveActivityPayload({
    required this.event,
    required this.timestamp,
    required this.contentState,
    this.attributesType,
    this.attributes,
    this.alert,
    this.staleDate,
    this.dismissalDate,
    this.relevanceScore,
    this.inputPushToken = false,
    this.inputPushChannel,
    this.priority = ApnsLiveActivityPriority.conservePower,
  });

  final ApnsLiveActivityEvent event;
  final int timestamp;
  final Map<String, Object?> contentState;
  final String? attributesType;
  final Map<String, Object?>? attributes;
  final Map<String, Object?>? alert;
  final int? staleDate;
  final int? dismissalDate;
  final double? relevanceScore;
  final bool inputPushToken;
  final String? inputPushChannel;
  final ApnsLiveActivityPriority priority;

  Map<String, Object?> toJson({required bool broadcast}) {
    _validate(broadcast: broadcast);
    return <String, Object?>{
      'event': event.name,
      'timestamp': timestamp,
      'contentState': contentState,
      if (attributesType != null) 'attributesType': attributesType,
      if (attributes != null) 'attributes': attributes,
      if (alert != null) 'alert': alert,
      if (staleDate != null) 'staleDate': staleDate,
      if (dismissalDate != null) 'dismissalDate': dismissalDate,
      if (relevanceScore != null) 'relevanceScore': relevanceScore,
      if (inputPushToken) 'inputPushToken': true,
      if (inputPushChannel != null) 'inputPushChannel': inputPushChannel,
      'priority': priority.name,
    };
  }

  void _validate({required bool broadcast}) {
    if (timestamp <= 0) {
      throw ArgumentError.value(timestamp, 'timestamp', 'must be positive');
    }
    if (inputPushToken && inputPushChannel != null) {
      throw ArgumentError(
        'inputPushToken and inputPushChannel are mutually exclusive',
      );
    }
    if (inputPushChannel != null && inputPushChannel!.trim().isEmpty) {
      throw ArgumentError.value(
        inputPushChannel,
        'inputPushChannel',
        'must not be empty',
      );
    }
    if (priority == ApnsLiveActivityPriority.lowPower && !broadcast) {
      throw ArgumentError('lowPower priority is broadcast-only');
    }
    if (event == ApnsLiveActivityEvent.start) {
      if (broadcast) {
        throw ArgumentError('broadcast notifications cannot start an activity');
      }
      if (attributesType == null || attributesType!.trim().isEmpty) {
        throw ArgumentError('attributesType is required for start events');
      }
      if (attributes == null || alert == null) {
        throw ArgumentError(
          'attributes and alert are required for start events',
        );
      }
      if (staleDate != null || dismissalDate != null) {
        throw ArgumentError(
          'start events cannot include stale or dismissal dates',
        );
      }
    } else if (event == ApnsLiveActivityEvent.update) {
      if (attributesType != null ||
          attributes != null ||
          dismissalDate != null ||
          inputPushToken ||
          inputPushChannel != null) {
        throw ArgumentError('update event contains start or end-only fields');
      }
    } else if (attributesType != null ||
        attributes != null ||
        staleDate != null ||
        inputPushToken ||
        inputPushChannel != null) {
      throw ArgumentError('end event contains start or update-only fields');
    }
    if (relevanceScore != null &&
        (!relevanceScore!.isFinite || relevanceScore!.isNegative)) {
      throw ArgumentError.value(
        relevanceScore,
        'relevanceScore',
        'must be finite and nonnegative',
      );
    }
  }
}

class ApnsLiveActivityPublishRequest {
  const ApnsLiveActivityPublishRequest({
    required this.recipient,
    required this.liveActivity,
    this.publishId,
    this.notBeforeMs,
    this.expiresAtMs,
  });

  final ApnsLiveActivityRecipient recipient;
  final ApnsLiveActivityPayload liveActivity;
  final String? publishId;
  final int? notBeforeMs;
  final int? expiresAtMs;

  Map<String, Object?> toJson() => <String, Object?>{
    if (publishId != null) 'publishId': publishId,
    'recipients': <Map<String, Object?>>[
      <String, Object?>{'type': 'recipient', 'recipient': recipient.toJson()},
    ],
    'payload': const <String, Object?>{},
    'liveActivity': liveActivity.toJson(broadcast: recipient.isBroadcast),
    if (notBeforeMs != null) 'notBeforeMs': notBeforeMs,
    if (expiresAtMs != null) 'expiresAtMs': expiresAtMs,
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

  Future<Map<Object?, Object?>> publishLiveActivity(
    ApnsLiveActivityPublishRequest request,
  ) => publish(request.toJson());

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
