import 'support.dart';

enum MutableMessageAction {
  create('message.create'),
  update('message.update'),
  delete('message.delete'),
  append('message.append');

  const MutableMessageAction(this.value);

  final String value;

  static MutableMessageAction? fromValue(String value) {
    for (final action in MutableMessageAction.values) {
      if (action.value == value) return action;
    }
    return null;
  }
}

class MutableMessageVersionInfo {
  const MutableMessageVersionInfo({
    required this.action,
    required this.event,
    required this.messageSerial,
    this.versionSerial,
    this.historySerial,
    this.versionTimestampMs,
  });

  final MutableMessageAction action;
  final String event;
  final String messageSerial;
  final String? versionSerial;
  final int? historySerial;
  final int? versionTimestampMs;
}

class MutableMessageState {
  const MutableMessageState({
    required this.messageSerial,
    required this.action,
    required this.data,
    required this.event,
    this.serial,
    this.streamId,
    this.messageId,
    this.versionSerial,
    this.historySerial,
    this.versionTimestampMs,
  });

  final String messageSerial;
  final MutableMessageAction action;
  final Object? data;
  final String event;
  final int? serial;
  final String? streamId;
  final String? messageId;
  final String? versionSerial;
  final int? historySerial;
  final int? versionTimestampMs;
}

int? _parseNumericHeader(Object? value) {
  if (value is int) return value;
  if (value is double) {
    final i = value.truncate();
    if (value == i.toDouble()) return i;
    return null;
  }
  if (value is String) {
    final d = double.tryParse(value.trim());
    if (d != null) {
      final i = d.truncate();
      if (d == i.toDouble()) return i;
    }
  }
  return null;
}

bool isMutableMessageEvent(SockudoEvent event) =>
    getMutableMessageInfo(event) != null;

MutableMessageVersionInfo? getMutableMessageInfo(SockudoEvent event) {
  final headers = event.extras?.headers;
  if (headers == null) return null;

  final actionRaw = headers['sockudo_action'];
  final messageSerialRaw = headers['sockudo_message_serial'];
  if (actionRaw is! String || messageSerialRaw is! String) return null;

  final action = MutableMessageAction.fromValue(actionRaw);
  if (action == null) return null;

  final versionSerialRaw = headers['sockudo_version_serial'];
  final versionSerial = versionSerialRaw is String ? versionSerialRaw : null;

  final historySerial = _parseNumericHeader(headers['sockudo_history_serial']);
  final versionTimestampMs = _parseNumericHeader(
    headers['sockudo_version_timestamp_ms'],
  );

  return MutableMessageVersionInfo(
    action: action,
    event: event.event,
    messageSerial: messageSerialRaw,
    versionSerial: versionSerial,
    historySerial: historySerial,
    versionTimestampMs: versionTimestampMs,
  );
}

MutableMessageState reduceMutableMessageEvent(
  MutableMessageState? current,
  SockudoEvent event,
) {
  final info = getMutableMessageInfo(event);
  if (info == null) {
    throw StateError('Event is not a mutable-message event');
  }

  if (current != null && current.messageSerial != info.messageSerial) {
    throw StateError(
      "Mutable-message reducer expected message_serial '${current.messageSerial}'"
      " but received '${info.messageSerial}'",
    );
  }

  final Object? nextData;
  switch (info.action) {
    case MutableMessageAction.append:
      final base = current?.data;
      if (base is! String) {
        throw StateError(
          'message.append requires an existing string base;'
          ' seed state from a create/update payload or latest-view history first',
        );
      }
      final fragment = event.data;
      if (fragment is! String) {
        throw StateError(
          'message.append payload must be a string fragment when applying'
          ' client-side concatenation',
        );
      }
      nextData = base + fragment;
    case MutableMessageAction.delete:
    case MutableMessageAction.create:
    case MutableMessageAction.update:
      nextData = event.data;
  }

  return MutableMessageState(
    messageSerial: info.messageSerial,
    action: info.action,
    data: nextData,
    event: info.event,
    serial: event.serial,
    streamId: event.streamId,
    messageId: event.messageId,
    versionSerial: info.versionSerial,
    historySerial: info.historySerial,
    versionTimestampMs: info.versionTimestampMs,
  );
}

MutableMessageState? reduceMutableMessageEvents(List<SockudoEvent> events) {
  MutableMessageState? state;
  for (final event in events) {
    state = reduceMutableMessageEvent(state, event);
  }
  return state;
}
