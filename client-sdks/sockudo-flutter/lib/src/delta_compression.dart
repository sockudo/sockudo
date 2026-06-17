import 'dart:convert';
import 'dart:typed_data';

import 'fossil_delta.dart';
import 'models.dart';
import 'protocol_prefix.dart';
import 'support.dart';
import 'vcdiff_decoder.dart';

class DeltaCompressionManager {
  DeltaCompressionManager(this.options, this.sendEvent, {this.prefix});

  final DeltaOptions options;
  final bool Function(String eventName, Object data) sendEvent;
  final ProtocolPrefix? prefix;

  final Map<String, _ChannelState> _channelStates = <String, _ChannelState>{};
  bool _enabled = false;
  DeltaAlgorithm _defaultAlgorithm = DeltaAlgorithm.fossil;
  int _totalMessages = 0;
  int _deltaMessages = 0;
  int _fullMessages = 0;
  int _rawBytes = 0;
  int _wireBytes = 0;
  int _errors = 0;

  void enable() {
    if (_enabled) {
      return;
    }
    final p = prefix ?? ProtocolPrefix(2);
    sendEvent(p.event('enable_delta_compression'), <String, Object>{
      'algorithms': options.algorithms
          .map((algorithm) => algorithm.name)
          .toList(),
    });
  }

  void disable() {
    _enabled = false;
  }

  void handleEnabled(Object? data) {
    final payload = data as Map<Object?, Object?>?;
    _enabled = payload?['enabled'] as bool? ?? true;
    final algorithm = payload?['algorithm'] as String?;
    if (algorithm != null) {
      _defaultAlgorithm = DeltaAlgorithm.values.firstWhere(
        (value) => value.name == algorithm,
        orElse: () => _defaultAlgorithm,
      );
    }
  }

  void handleCacheSync(String channel, Object? data) {
    final payload = data as Map<Object?, Object?>?;
    if (payload == null) {
      return;
    }
    final state = _channelStates.putIfAbsent(
      channel,
      () => _ChannelState(channel),
    );
    state.initialize(payload);
  }

  SockudoEvent? handleDeltaMessage(String channel, Object? data) {
    final payload = data as Map<Object?, Object?>?;
    if (payload == null) {
      return null;
    }

    final eventName = payload['event'] as String?;
    final delta = payload['delta'] as String?;
    final sequence = payload['seq'] as int?;
    if (eventName == null || delta == null || sequence == null) {
      _recordError(const SockudoException('Invalid delta payload'));
      return null;
    }

    final state = _channelStates[channel];
    if (state == null) {
      return null;
    }

    final algorithmName = payload['algorithm'] as String?;
    final algorithm = DeltaAlgorithm.values.firstWhere(
      (value) => value.name == algorithmName,
      orElse: () => _defaultAlgorithm,
    );
    final conflationKey = payload['conflation_key'] as String?;
    final baseIndex = payload['base_index'] as int?;
    final baseMessage = state.baseMessage(conflationKey, baseIndex);
    if (baseMessage == null) {
      _requestResync(channel);
      return null;
    }

    try {
      final deltaBytes = base64Decode(delta);
      final reconstructed = switch (algorithm) {
        DeltaAlgorithm.fossil => utf8.decode(
          FossilDelta.apply(
            Uint8List.fromList(utf8.encode(baseMessage)),
            Uint8List.fromList(deltaBytes),
          ),
        ),
        DeltaAlgorithm.xdelta3 => utf8.decode(
          decodeVcdiff(
            Uint8List.fromList(deltaBytes),
            Uint8List.fromList(utf8.encode(baseMessage)),
          ),
        ),
      };

      state.update(reconstructed, sequence, conflationKey);
      state.deltaCount += 1;
      _totalMessages += 1;
      _deltaMessages += 1;
      _rawBytes += reconstructed.length;
      _wireBytes += deltaBytes.length;
      _emitStats();

      final parsed = _tryDecodeJson(reconstructed);
      final eventData =
          parsed is Map<Object?, Object?> && parsed.containsKey('data')
          ? parsed['data']
          : parsed;

      return SockudoEvent(
        event: eventName,
        channel: channel,
        data: eventData,
        rawMessage: reconstructed,
        sequence: sequence,
        conflationKey: conflationKey,
      );
    } catch (error) {
      _recordError(error);
      return null;
    }
  }

  void handleFullMessage(
    String channel,
    String rawMessage,
    int? sequence,
    String? conflationKey,
  ) {
    if (sequence == null) {
      return;
    }
    final state = _channelStates.putIfAbsent(
      channel,
      () => _ChannelState(channel),
    );
    state.update(rawMessage, sequence, conflationKey);
    state.fullMessageCount += 1;
    _totalMessages += 1;
    _fullMessages += 1;
    _rawBytes += rawMessage.length;
    _wireBytes += rawMessage.length;
    _emitStats();
  }

  DeltaStats getStats() {
    final saved = _rawBytes - _wireBytes;
    final percent = _rawBytes == 0 ? 0.0 : (saved / _rawBytes) * 100;
    return DeltaStats(
      totalMessages: _totalMessages,
      deltaMessages: _deltaMessages,
      fullMessages: _fullMessages,
      totalBytesWithoutCompression: _rawBytes,
      totalBytesWithCompression: _wireBytes,
      bandwidthSaved: saved,
      bandwidthSavedPercent: percent,
      errors: _errors,
      channelCount: _channelStates.length,
      channels:
          _channelStates.values
              .map((state) => state.stats())
              .toList(growable: false)
            ..sort((a, b) => a.channelName.compareTo(b.channelName)),
    );
  }

  void resetStats() {
    _totalMessages = 0;
    _deltaMessages = 0;
    _fullMessages = 0;
    _rawBytes = 0;
    _wireBytes = 0;
    _errors = 0;
    _emitStats();
  }

  void clearChannelState([String? channel]) {
    if (channel == null) {
      _channelStates.clear();
    } else {
      _channelStates.remove(channel);
    }
  }

  void _requestResync(String channel) {
    final p = prefix ?? ProtocolPrefix(2);
    sendEvent(p.event('delta_sync_error'), <String, String>{
      'channel': channel,
    });
    _channelStates.remove(channel);
  }

  void _recordError(Object error) {
    _errors += 1;
    options.onError?.call(error);
  }

  void _emitStats() {
    options.onStats?.call(getStats());
  }
}

class _ChannelState {
  _ChannelState(this.channelName);

  final String channelName;
  String? conflationKey;
  int maxMessagesPerKey = 30;
  final Map<String, List<_CachedMessage>> conflationCaches =
      <String, List<_CachedMessage>>{};
  String? base;
  int? baseSequence;
  int? lastSequence;
  int deltaCount = 0;
  int fullMessageCount = 0;

  void initialize(Map<Object?, Object?> syncData) {
    conflationKey = syncData['conflation_key'] as String?;
    maxMessagesPerKey = ((syncData['max_messages_per_key'] as int?) ?? 10) < 30
        ? 30
        : (syncData['max_messages_per_key'] as int?) ?? 10;
    conflationCaches.clear();
    final states = syncData['states'] as Map<Object?, Object?>?;
    if (states == null) {
      return;
    }

    for (final entry in states.entries) {
      final key = '${entry.key}';
      final values = entry.value as List<Object?>? ?? const <Object?>[];
      conflationCaches[key] = values
          .whereType<Map<Object?, Object?>>()
          .map(
            (item) => _CachedMessage(
              item['content'] as String? ?? '',
              item['seq'] as int? ?? 0,
            ),
          )
          .toList(growable: true);
    }
  }

  String? baseMessage(String? conflationValue, int? baseIndex) {
    if (conflationKey == null) {
      return base;
    }
    if (baseIndex == null) {
      return null;
    }
    return conflationCaches[conflationValue ?? '']
        ?.where((message) => message.sequence == baseIndex)
        .map((message) => message.content)
        .firstOrNull;
  }

  void update(String message, int sequence, String? conflationValue) {
    if (conflationKey != null || conflationValue != null) {
      final key = conflationValue ?? '';
      final cache = List<_CachedMessage>.from(
        conflationCaches[key] ?? const <_CachedMessage>[],
      )..add(_CachedMessage(message, sequence));
      while (cache.length > maxMessagesPerKey) {
        cache.removeAt(0);
      }
      conflationCaches[key] = cache;
      conflationKey ??= 'enabled';
    } else {
      base = message;
      baseSequence = sequence;
    }
    lastSequence = sequence;
  }

  ChannelDeltaStats stats() => ChannelDeltaStats(
    channelName: channelName,
    conflationKey: conflationKey,
    conflationGroupCount: conflationCaches.length,
    deltaCount: deltaCount,
    fullMessageCount: fullMessageCount,
    totalMessages: deltaCount + fullMessageCount,
  );
}

class _CachedMessage {
  const _CachedMessage(this.content, this.sequence);

  final String content;
  final int sequence;
}

Object? _tryDecodeJson(String value) {
  try {
    return JsonSupport.decode(value);
  } catch (_) {
    return value;
  }
}

extension on Iterable<String> {
  String? get firstOrNull => isEmpty ? null : first;
}
