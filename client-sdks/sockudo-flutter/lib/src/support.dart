import 'dart:convert';

import 'package:fixnum/fixnum.dart';

import 'models.dart';

class EventBindingToken {
  EventBindingToken() : id = (++_nextId).toString();

  static int _nextId = 0;
  final String id;
}

class SockudoException implements Exception {
  const SockudoException(this.message, {this.statusCode});

  final String message;
  final int? statusCode;

  @override
  String toString() => 'SockudoException($message)';
}

class EventDispatcher {
  EventDispatcher({this.failThrough});

  final void Function(String eventName, Object? data)? failThrough;
  final Map<String, Map<String, EventCallback>> _callbacks =
      <String, Map<String, EventCallback>>{};
  final Map<String, GlobalEventCallback> _globalCallbacks =
      <String, GlobalEventCallback>{};

  EventBindingToken bind(String eventName, EventCallback callback) {
    final token = EventBindingToken();
    final listeners = _callbacks.putIfAbsent(
      eventName,
      () => <String, EventCallback>{},
    );
    listeners[token.id] = callback;
    return token;
  }

  EventBindingToken bindGlobal(GlobalEventCallback callback) {
    final token = EventBindingToken();
    _globalCallbacks[token.id] = callback;
    return token;
  }

  void unbind({String? eventName, EventBindingToken? token}) {
    if (eventName != null) {
      if (token == null) {
        _callbacks.remove(eventName);
      } else {
        final listeners = _callbacks[eventName];
        listeners?.remove(token.id);
        if (listeners != null && listeners.isEmpty) {
          _callbacks.remove(eventName);
        }
      }
      return;
    }

    if (token != null) {
      for (final listeners in _callbacks.values) {
        listeners.remove(token.id);
      }
      _callbacks.removeWhere((_, listeners) => listeners.isEmpty);
      _globalCallbacks.remove(token.id);
      return;
    }

    _callbacks.clear();
    _globalCallbacks.clear();
  }

  void emit(String eventName, Object? data, {EventMetadata? metadata}) {
    for (final callback in _globalCallbacks.values) {
      callback(eventName, data);
    }

    final listeners = _callbacks[eventName];
    if (listeners == null || listeners.isEmpty) {
      failThrough?.call(eventName, data);
      return;
    }

    for (final callback in listeners.values) {
      callback(data, metadata);
    }
  }
}

class JsonSupport {
  static String encode(Object? value) => jsonEncode(value);

  static Object? decode(String raw) => jsonDecode(raw);
}

class SockudoLogger {
  static bool logToConsole = false;
  static void Function(String message)? customLog;

  static void debug(Object message) => _log('DEBUG', message);
  static void warn(Object message) => _log('WARN', message);
  static void error(Object message) => _log('ERROR', message);

  static void _log(String level, Object message) {
    final line = '[Sockudo:$level] $message';
    customLog?.call(line);
    if (logToConsole) {
      // ignore: avoid_print
      print(line);
    }
  }
}

class SockudoEvent {
  const SockudoEvent({
    required this.event,
    required this.rawMessage,
    this.channel,
    this.data,
    this.userId,
    this.streamId,
    this.messageId,
    this.sequence,
    this.conflationKey,
    this.serial,
    this.extras,
  });

  final String event;
  final String rawMessage;
  final String? channel;
  final Object? data;
  final String? userId;
  final String? streamId;
  final String? messageId;
  final Object? sequence;
  final String? conflationKey;
  final Object? serial;
  final MessageExtras? extras;
}

final BigInt _maxSafeJsonInteger = BigInt.from(9007199254740991);
final BigInt _minSafeJsonInteger = BigInt.from(-9007199254740991);
final RegExp _integerStringPattern = RegExp(r'^-?\d+$');
final RegExp _jsonSerialFieldPattern = RegExp(
  r'("[-_A-Za-z0-9]*serial[-_A-Za-z0-9]*"\s*:\s*)(-?\d+)',
);

String preserveUnsafeJsonSerials(String raw) {
  return raw.replaceAllMapped(_jsonSerialFieldPattern, (match) {
    final literal = match.group(2)!;
    final normalized = normalizeWireSerial(literal);
    if (normalized is String) {
      return '${match.group(1)!}${jsonEncode(normalized)}';
    }
    return match.group(0)!;
  });
}

Object? normalizeWireSerial(Object? value) {
  if (value == null) {
    return null;
  }
  if (value is Int64) {
    return _normalizeBigInt(BigInt.parse(value.toString()));
  }
  if (value is BigInt) {
    return _normalizeBigInt(value);
  }
  if (value is int) {
    return _normalizeBigInt(BigInt.from(value));
  }
  if (value is double) {
    if (!value.isFinite || value.truncateToDouble() != value) {
      return null;
    }
    final integer = value.toInt();
    return _normalizeBigInt(BigInt.from(integer));
  }
  if (value is String) {
    final trimmed = value.trim();
    if (!_integerStringPattern.hasMatch(trimmed)) {
      return null;
    }
    return _normalizeBigInt(BigInt.parse(trimmed));
  }
  return null;
}

int? wireSerialAsInt(Object? value) {
  final normalized = normalizeWireSerial(value);
  return normalized is int ? normalized : null;
}

Object? _normalizeBigInt(BigInt value) {
  if (value <= _maxSafeJsonInteger && value >= _minSafeJsonInteger) {
    return value.toInt();
  }
  return value.toString();
}

String encodeQuery(Map<String, Object?> params) {
  final entries = params.entries.toList()
    ..sort((a, b) => a.key.compareTo(b.key));
  return entries
      .map(
        (entry) =>
            '${Uri.encodeQueryComponent(entry.key)}=${Uri.encodeQueryComponent('${entry.value ?? ''}')}',
      )
      .join('&');
}
