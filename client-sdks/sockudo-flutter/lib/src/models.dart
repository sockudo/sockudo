import 'filter.dart';

typedef EventCallback = void Function(dynamic data, EventMetadata? metadata);
typedef GlobalEventCallback = void Function(String eventName, dynamic data);

enum SockudoTransport { ws, wss }

enum SockudoWireFormat {
  json,
  messagepack,
  protobuf;

  bool get isBinary => this != SockudoWireFormat.json;

  String get queryValue => switch (this) {
    SockudoWireFormat.json => 'json',
    SockudoWireFormat.messagepack => 'messagepack',
    SockudoWireFormat.protobuf => 'protobuf',
  };
}

enum SockudoAppendMode {
  delta,
  full;

  String get queryValue => switch (this) {
    SockudoAppendMode.delta => 'delta',
    SockudoAppendMode.full => 'full',
  };
}

const Set<int> allowedAppendRollupWindows = <int>{0, 20, 40, 100, 500};

enum ConnectionState {
  initialized,
  connecting,
  connected,
  disconnected,
  unavailable,
  failed,
}

enum DeltaAlgorithm { fossil, xdelta3 }

class EventMetadata {
  const EventMetadata({this.userId});

  final String? userId;
}

class PresenceHistoryParams {
  const PresenceHistoryParams({
    this.direction,
    this.limit,
    this.cursor,
    this.startSerial,
    this.endSerial,
    this.startTimeMs,
    this.endTimeMs,
    this.start,
    this.end,
  });

  final String? direction;
  final int? limit;
  final String? cursor;
  final int? startSerial;
  final int? endSerial;
  final int? startTimeMs;
  final int? endTimeMs;
  final int? start;
  final int? end;

  Map<String, Object> toJson() => <String, Object>{
    'direction': ?direction,
    'limit': ?limit,
    'cursor': ?cursor,
    'start_serial': ?startSerial,
    'end_serial': ?endSerial,
    'start_time_ms': ?(startTimeMs ?? start),
    'end_time_ms': ?(endTimeMs ?? end),
  };
}

class PresenceSnapshotParams {
  const PresenceSnapshotParams({this.atTimeMs, this.at, this.atSerial});

  final int? atTimeMs;
  final int? at;
  final int? atSerial;

  Map<String, Object> toJson() => <String, Object>{
    'at_time_ms': ?(atTimeMs ?? at),
    'at_serial': ?atSerial,
  };
}

class PresenceHistoryItem {
  const PresenceHistoryItem({
    required this.streamId,
    required this.serial,
    required this.publishedAtMs,
    required this.event,
    required this.cause,
    required this.userId,
    required this.connectionId,
    required this.deadNodeId,
    required this.payloadSizeBytes,
    required this.presenceEvent,
  });

  final String streamId;
  final int serial;
  final int publishedAtMs;
  final String event;
  final String cause;
  final String userId;
  final String? connectionId;
  final String? deadNodeId;
  final int payloadSizeBytes;
  final Map<Object?, Object?> presenceEvent;
}

class PresenceHistoryBounds {
  const PresenceHistoryBounds({
    required this.startSerial,
    required this.endSerial,
    required this.startTimeMs,
    required this.endTimeMs,
  });

  final int? startSerial;
  final int? endSerial;
  final int? startTimeMs;
  final int? endTimeMs;
}

class PresenceHistoryContinuity {
  const PresenceHistoryContinuity({
    required this.streamId,
    required this.oldestAvailableSerial,
    required this.newestAvailableSerial,
    required this.oldestAvailablePublishedAtMs,
    required this.newestAvailablePublishedAtMs,
    required this.retainedEvents,
    required this.retainedBytes,
    required this.degraded,
    required this.complete,
    required this.truncatedByRetention,
  });

  final String? streamId;
  final int? oldestAvailableSerial;
  final int? newestAvailableSerial;
  final int? oldestAvailablePublishedAtMs;
  final int? newestAvailablePublishedAtMs;
  final int retainedEvents;
  final int retainedBytes;
  final bool degraded;
  final bool complete;
  final bool truncatedByRetention;
}

class PresenceSnapshotMember {
  const PresenceSnapshotMember({
    required this.userId,
    required this.lastEvent,
    required this.lastEventSerial,
    required this.lastEventAtMs,
  });

  final String userId;
  final String lastEvent;
  final int lastEventSerial;
  final int lastEventAtMs;
}

class PresenceSnapshot {
  const PresenceSnapshot({
    required this.channel,
    required this.members,
    required this.memberCount,
    required this.eventsReplayed,
    required this.snapshotSerial,
    required this.snapshotTimeMs,
    required this.continuity,
  });

  final String channel;
  final List<PresenceSnapshotMember> members;
  final int memberCount;
  final int eventsReplayed;
  final int? snapshotSerial;
  final int? snapshotTimeMs;
  final PresenceHistoryContinuity continuity;
}

class PresenceHistoryPage {
  PresenceHistoryPage({
    required this.items,
    required this.direction,
    required this.limit,
    required this.hasMore,
    required this.nextCursor,
    required this.bounds,
    required this.continuity,
    Future<PresenceHistoryPage> Function(String cursor)? fetchNext,
  }) : _fetchNext = fetchNext;

  final List<PresenceHistoryItem> items;
  final String direction;
  final int limit;
  final bool hasMore;
  final String? nextCursor;
  final PresenceHistoryBounds bounds;
  final PresenceHistoryContinuity continuity;
  final Future<PresenceHistoryPage> Function(String cursor)? _fetchNext;

  bool hasNext() => hasMore && nextCursor != null;

  Future<PresenceHistoryPage> next() {
    final cursor = nextCursor;
    final fetchNext = _fetchNext;
    if (cursor == null || fetchNext == null || !hasNext()) {
      throw StateError('No more pages available');
    }
    return fetchNext(cursor);
  }
}

class ChannelHistoryParams {
  const ChannelHistoryParams({
    this.direction,
    this.limit,
    this.cursor,
    this.startSerial,
    this.endSerial,
    this.startTimeMs,
    this.endTimeMs,
    this.untilAttach,
  });

  final String? direction;
  final int? limit;
  final String? cursor;
  final Object? startSerial;
  final Object? endSerial;
  final Object? startTimeMs;
  final Object? endTimeMs;
  final bool? untilAttach;

  Map<String, Object> toJson() => <String, Object>{
    'direction': ?direction,
    'limit': ?limit,
    'cursor': ?cursor,
    'start_serial': ?startSerial,
    'end_serial': ?endSerial,
    'start_time_ms': ?startTimeMs,
    'end_time_ms': ?endTimeMs,
    'until_attach': ?untilAttach,
  };
}

class ChannelHistoryPageProxy {
  ChannelHistoryPageProxy({
    required this.items,
    required this.direction,
    required this.limit,
    required this.hasMore,
    required this.nextCursor,
    required this.bounds,
    required this.continuity,
    Future<ChannelHistoryPageProxy> Function(String cursor)? fetchNext,
  }) : _fetchNext = fetchNext;

  final List<Map<Object?, Object?>> items;
  final String direction;
  final int limit;
  final bool hasMore;
  final String? nextCursor;
  final Map<Object?, Object?> bounds;
  final Map<Object?, Object?> continuity;
  final Future<ChannelHistoryPageProxy> Function(String cursor)? _fetchNext;

  bool hasNext() => hasMore && nextCursor != null;

  Future<ChannelHistoryPageProxy> next() {
    final cursor = nextCursor;
    final fetchNext = _fetchNext;
    if (cursor == null || fetchNext == null || !hasNext()) {
      throw StateError('No more pages available');
    }
    return fetchNext(cursor);
  }
}

class MessageVersionsParams {
  const MessageVersionsParams({this.direction, this.limit, this.cursor});

  final String? direction;
  final int? limit;
  final String? cursor;

  Map<String, Object> toJson() => <String, Object>{
    'direction': ?direction,
    'limit': ?limit,
    'cursor': ?cursor,
  };
}

class MessageVersionsPage {
  MessageVersionsPage({
    required this.channel,
    required this.items,
    required this.direction,
    required this.limit,
    required this.hasMore,
    required this.nextCursor,
    Future<MessageVersionsPage> Function(String cursor)? fetchNext,
  }) : _fetchNext = fetchNext;

  final String channel;
  final List<Map<Object?, Object?>> items;
  final String direction;
  final int limit;
  final bool hasMore;
  final String? nextCursor;
  final Future<MessageVersionsPage> Function(String cursor)? _fetchNext;

  bool hasNext() => hasMore && nextCursor != null;

  Future<MessageVersionsPage> next() {
    final cursor = nextCursor;
    final fetchNext = _fetchNext;
    if (cursor == null || fetchNext == null || !hasNext()) {
      throw StateError('No more pages available');
    }
    return fetchNext(cursor);
  }
}

class VersionedMessageCreateRequest {
  const VersionedMessageCreateRequest({
    this.name,
    this.data,
    this.extras,
    this.messageId,
    this.messageSerial,
    this.clientId,
    this.socketId,
    this.opId,
  });

  final String? name;
  final Object? data;
  final Object? extras;
  final String? messageId;
  final String? messageSerial;
  final String? clientId;
  final String? socketId;
  final String? opId;

  Map<String, Object?> toJson({
    required String defaultName,
    required String defaultMessageId,
  }) => <String, Object?>{
    'name': name ?? defaultName,
    'data': data,
    'extras': extras,
    'messageId': messageId ?? defaultMessageId,
    'messageSerial': messageSerial,
    'clientId': clientId,
    'socketId': socketId,
    'opId': opId,
  };
}

class VersionedMessageMutation {
  const VersionedMessageMutation({
    this.data,
    this.name,
    this.extras,
    this.clearFields,
    this.opId,
    this.clientId,
    this.socketId,
    this.description,
    this.metadata,
  });

  final Object? data;
  final String? name;
  final Object? extras;
  final List<String>? clearFields;
  final String? opId;
  final String? clientId;
  final String? socketId;
  final String? description;
  final Object? metadata;

  Map<String, Object?> toJson({Object? dataOverride}) => <String, Object?>{
    'data': dataOverride ?? data,
    'name': name,
    'extras': extras,
    'clearFields': clearFields,
    'opId': opId,
    'clientId': clientId,
    'socketId': socketId,
    'description': description,
    'metadata': metadata,
  };
}

class MessageAck {
  const MessageAck({
    required this.messageSerial,
    required this.historySerial,
    this.deliverySerial,
    this.versionSerial,
    this.status,
  });

  final String messageSerial;
  final Object historySerial;
  final Object? deliverySerial;
  final String? versionSerial;
  final String? status;
}

class PublishAnnotationRequest {
  const PublishAnnotationRequest({
    required this.type,
    this.name,
    this.clientId,
    this.socketId,
    this.count,
    this.data,
    this.encoding,
  });

  final String type;
  final String? name;
  final String? clientId;
  final String? socketId;
  final int? count;
  final Object? data;
  final String? encoding;

  Map<String, Object?> toJson() => <String, Object?>{
    'type': type,
    'name': ?name,
    'clientId': ?clientId,
    'socketId': ?socketId,
    'count': ?count,
    'data': ?data,
    'encoding': ?encoding,
  };
}

class PublishAnnotationResponse {
  const PublishAnnotationResponse({required this.annotationSerial});

  final String annotationSerial;
}

class DeleteAnnotationResponse {
  const DeleteAnnotationResponse({
    required this.annotationSerial,
    required this.deletedAnnotationSerial,
  });

  final String annotationSerial;
  final String deletedAnnotationSerial;
}

class AnnotationEventsParams {
  const AnnotationEventsParams({
    this.type,
    this.limit,
    this.fromSerial,
    this.socketId,
  });

  final String? type;
  final int? limit;
  final String? fromSerial;
  final String? socketId;

  Map<String, Object> toJson() => <String, Object>{
    'type': ?type,
    'limit': ?limit,
    'fromSerial': ?fromSerial,
    'socketId': ?socketId,
  };
}

class AnnotationEventsPage {
  AnnotationEventsPage({
    required this.channel,
    required this.messageSerial,
    required this.limit,
    required this.hasMore,
    required this.nextCursor,
    required this.items,
    Future<AnnotationEventsPage> Function(String cursor)? fetchNext,
  }) : _fetchNext = fetchNext;

  final String channel;
  final String messageSerial;
  final int limit;
  final bool hasMore;
  final String? nextCursor;
  final List<Map<Object?, Object?>> items;
  final Future<AnnotationEventsPage> Function(String cursor)? _fetchNext;

  bool hasNext() => hasMore && nextCursor != null;

  Future<AnnotationEventsPage> next() {
    final cursor = nextCursor;
    final fetchNext = _fetchNext;
    if (cursor == null || fetchNext == null || !hasNext()) {
      throw StateError('No more pages available');
    }
    return fetchNext(cursor);
  }
}

sealed class AuthValue {
  const AuthValue();

  String get stringValue;
}

class AuthString extends AuthValue {
  const AuthString(this.value);

  final String value;

  @override
  String get stringValue => value;
}

class AuthInt extends AuthValue {
  const AuthInt(this.value);

  final int value;

  @override
  String get stringValue => value.toString();
}

class AuthDouble extends AuthValue {
  const AuthDouble(this.value);

  final double value;

  @override
  String get stringValue => value.toString();
}

class AuthBool extends AuthValue {
  const AuthBool(this.value);

  final bool value;

  @override
  String get stringValue => value ? 'true' : 'false';
}

class ChannelDeltaSettings {
  const ChannelDeltaSettings({this.enabled, this.algorithm});

  final bool? enabled;
  final DeltaAlgorithm? algorithm;

  Object toSubscriptionValue() {
    if (enabled == null && algorithm != null) {
      return algorithm!.name;
    }
    if (enabled == false && algorithm == null) {
      return false;
    }
    if (enabled == true && algorithm == null) {
      return true;
    }
    return <String, Object>{
      ...?enabled == null ? null : <String, Object>{'enabled': enabled!},
      ...?algorithm == null
          ? null
          : <String, Object>{'algorithm': algorithm!.name},
    };
  }
}

class MessageExtras {
  const MessageExtras({
    this.headers,
    this.ephemeral,
    this.idempotencyKey,
    this.echo,
    this.raw = const <String, Object?>{},
  });

  final Map<String, Object>? headers;
  final bool? ephemeral;
  final String? idempotencyKey;
  final bool? echo;
  final Map<String, Object?> raw;

  Map<String, Object?>? get ai {
    final value = raw['ai'];
    return value is Map<String, Object?> ? value : null;
  }

  Object? operator [](String key) => toJson()[key];

  Map<String, dynamic> toJson() {
    final map = <String, dynamic>{...raw};
    if (headers != null) map['headers'] = headers;
    if (ephemeral != null) map['ephemeral'] = ephemeral;
    if (idempotencyKey != null) map['idempotencyKey'] = idempotencyKey;
    if (echo != null) map['echo'] = echo;
    return map;
  }
}

class SubscriptionOptions {
  const SubscriptionOptions({
    this.filter,
    this.delta,
    this.events,
    this.expression,
    this.rewind,
    this.annotationSubscribe = false,
  });

  final FilterNode? filter;
  final ChannelDeltaSettings? delta;
  final List<String>? events;
  final SubscriptionExpression? expression;
  final SubscriptionRewind? rewind;
  final bool annotationSubscribe;
}

class SubscriptionExpression {
  const SubscriptionExpression(this.source) : language = null;

  const SubscriptionExpression.descriptor({
    required this.source,
    this.language = 'jmespath',
  });

  final String source;
  final String? language;

  Object toSubscriptionValue() => language == null
      ? source
      : <String, String>{'language': language!, 'source': source};
}

class SubscriptionRewind {
  const SubscriptionRewind.count(this.count)
    : seconds = null,
      assert(count != null);

  const SubscriptionRewind.seconds(this.seconds)
    : count = null,
      assert(seconds != null);

  final int? count;
  final int? seconds;

  Object toSubscriptionValue() {
    if (count != null) {
      return count!;
    }
    return <String, Object>{'seconds': seconds!};
  }
}

class RecoveryPosition {
  const RecoveryPosition({
    this.streamId,
    required this.serial,
    this.lastMessageId,
  });

  final String? streamId;
  final Object serial;
  final String? lastMessageId;

  Map<String, Object?> toJson() => <String, Object?>{
    'serial': serial,
    if (streamId != null) 'stream_id': streamId,
    if (lastMessageId != null) 'last_message_id': lastMessageId,
  };
}

class ResumeRecoveredChannel {
  const ResumeRecoveredChannel({
    required this.channel,
    required this.source,
    required this.replayed,
    this.position,
  });

  final String channel;
  final String source;
  final int replayed;
  final RecoveryPosition? position;
}

class ResumeFailedChannel {
  const ResumeFailedChannel({
    required this.channel,
    required this.code,
    required this.reason,
    this.expectedStreamId,
    this.currentStreamId,
    this.oldestAvailableSerial,
    this.newestAvailableSerial,
  });

  final String channel;
  final String code;
  final String reason;
  final String? expectedStreamId;
  final String? currentStreamId;
  final int? oldestAvailableSerial;
  final int? newestAvailableSerial;
}

class ResumeSuccessData {
  const ResumeSuccessData({required this.recovered, required this.failed});

  final List<ResumeRecoveredChannel> recovered;
  final List<ResumeFailedChannel> failed;
}

class RewindCompleteData {
  const RewindCompleteData({
    required this.historicalCount,
    required this.liveCount,
    required this.complete,
    required this.truncatedByRetention,
    required this.truncatedByLimit,
  });

  final int historicalCount;
  final int liveCount;
  final bool complete;
  final bool truncatedByRetention;
  final bool truncatedByLimit;
}

class DeltaOptions {
  const DeltaOptions({
    this.enabled,
    this.algorithms = const <DeltaAlgorithm>[
      DeltaAlgorithm.fossil,
      DeltaAlgorithm.xdelta3,
    ],
    this.debug = false,
    this.onStats,
    this.onError,
  });

  final bool? enabled;
  final List<DeltaAlgorithm> algorithms;
  final bool debug;
  final void Function(DeltaStats stats)? onStats;
  final void Function(Object error)? onError;
}

class ChannelDeltaStats {
  const ChannelDeltaStats({
    required this.channelName,
    required this.conflationKey,
    required this.conflationGroupCount,
    required this.deltaCount,
    required this.fullMessageCount,
    required this.totalMessages,
  });

  final String channelName;
  final String? conflationKey;
  final int conflationGroupCount;
  final int deltaCount;
  final int fullMessageCount;
  final int totalMessages;
}

class DeltaStats {
  const DeltaStats({
    required this.totalMessages,
    required this.deltaMessages,
    required this.fullMessages,
    required this.totalBytesWithoutCompression,
    required this.totalBytesWithCompression,
    required this.bandwidthSaved,
    required this.bandwidthSavedPercent,
    required this.errors,
    required this.channelCount,
    required this.channels,
  });

  final int totalMessages;
  final int deltaMessages;
  final int fullMessages;
  final int totalBytesWithoutCompression;
  final int totalBytesWithCompression;
  final int bandwidthSaved;
  final double bandwidthSavedPercent;
  final int errors;
  final int channelCount;
  final List<ChannelDeltaStats> channels;
}

class PresenceMember {
  const PresenceMember({required this.id, required this.info});

  final String id;
  final Object? info;
}
