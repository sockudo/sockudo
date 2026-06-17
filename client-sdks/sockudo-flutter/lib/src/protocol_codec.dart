import 'dart:convert';
import 'dart:typed_data';

import 'package:fixnum/fixnum.dart';
import 'package:messagepack/messagepack.dart';

import 'generated/wire.pb.dart';
import 'models.dart';
import 'support.dart';

class DecodedEnvelope {
  const DecodedEnvelope({required this.envelope, required this.rawMessage});

  final Map<String, Object?> envelope;
  final String rawMessage;
}

const List<String> _messagePackEnvelopeFields = <String>[
  'event',
  'channel',
  'data',
  'name',
  'user_id',
  'tags',
  'sequence',
  'conflation_key',
  'message_id',
  'stream_id',
  'serial',
  'idempotency_key',
  'extras',
  '__delta_seq',
  '__conflation_key',
];

class SockudoProtocolCodec {
  static Object encodeEnvelope(
    Map<String, Object?> envelope,
    SockudoWireFormat format,
  ) {
    switch (format) {
      case SockudoWireFormat.json:
        return jsonEncode(envelope);
      case SockudoWireFormat.messagepack:
        return _encodeMessagePack(envelope);
      case SockudoWireFormat.protobuf:
        return _encodeProtobuf(envelope);
    }
  }

  static DecodedEnvelope decodeEnvelope(
    Object? rawMessage,
    SockudoWireFormat format,
  ) {
    switch (format) {
      case SockudoWireFormat.json:
        final rawText = rawMessage is String
            ? rawMessage
            : utf8.decode(_asBytes(rawMessage));
        return DecodedEnvelope(
          envelope: _normalizeMap(jsonDecode(rawText)),
          rawMessage: rawText,
        );
      case SockudoWireFormat.messagepack:
        final envelope = _decodeMessagePackEnvelope(_asBytes(rawMessage));
        return DecodedEnvelope(
          envelope: envelope,
          rawMessage: jsonEncode(envelope),
        );
      case SockudoWireFormat.protobuf:
        final envelope = _decodeProtobuf(_asBytes(rawMessage));
        return DecodedEnvelope(
          envelope: envelope,
          rawMessage: jsonEncode(envelope),
        );
    }
  }

  static SockudoEvent decodeEvent(
    Object? rawMessage,
    SockudoWireFormat format,
  ) {
    final decoded = decodeEnvelope(rawMessage, format);
    final envelope = decoded.envelope;
    final eventName = envelope['event'] as String?;
    if (eventName == null) {
      throw const SockudoException('Unable to decode event envelope');
    }

    final rawData = envelope['data'];
    Object? data = rawData;
    if (rawData is String) {
      try {
        data = jsonDecode(rawData);
      } catch (_) {
        data = rawData;
      }
    }

    return SockudoEvent(
      event: eventName,
      channel: envelope['channel'] as String?,
      data: data,
      userId: envelope['user_id'] as String?,
      streamId: envelope['stream_id'] as String?,
      rawMessage: decoded.rawMessage,
      messageId: envelope['message_id'] as String?,
      sequence: _asInt(envelope['__delta_seq'] ?? envelope['sequence']),
      conflationKey:
          (envelope['__conflation_key'] ?? envelope['conflation_key'])
              as String?,
      serial: _asInt(envelope['serial']),
      extras: _decodeExtras(envelope['extras']),
    );
  }

  static Uint8List _encodeMessagePack(Map<String, Object?> envelope) {
    final packer = Packer();
    _packValue(packer, <Object?>[
      envelope['event'],
      envelope['channel'],
      _encodeMessagePackData(envelope['data']),
      envelope['name'],
      envelope['user_id'],
      envelope['tags'],
      envelope['sequence'],
      envelope['conflation_key'],
      envelope['message_id'],
      envelope['stream_id'],
      envelope['serial'],
      envelope['idempotency_key'],
      _encodeMessagePackExtras(envelope['extras']),
      envelope['__delta_seq'],
      envelope['__conflation_key'],
    ]);
    return packer.takeBytes();
  }

  static Object? _encodeMessagePackData(Object? value) {
    if (value == null) {
      return null;
    }
    if (value is String) {
      return <Object>['string', value];
    }
    return <Object>['json', jsonEncode(value)];
  }

  static Object? _encodeMessagePackExtras(Object? rawExtras) {
    final extras = _decodeExtras(rawExtras);
    if (extras == null) {
      return null;
    }
    return <String, Object?>{
      if (extras.headers != null)
        'headers': extras.headers!.map(
          (key, value) => MapEntry(key, _encodeMessagePackHeaderValue(value)),
        ),
      if (extras.ephemeral != null) 'ephemeral': extras.ephemeral,
      if (extras.idempotencyKey != null)
        'idempotency_key': extras.idempotencyKey,
      if (extras.echo != null) 'echo': extras.echo,
    };
  }

  static Object _encodeMessagePackHeaderValue(Object value) {
    if (value is bool) {
      return <Object>['bool', value];
    }
    if (value is num) {
      return <Object>['number', value];
    }
    return <Object>['string', '$value'];
  }

  static Uint8List _encodeProtobuf(Map<String, Object?> envelope) {
    final message = ProtoPusherMessage();
    final event = envelope['event'] as String?;
    if (event != null) {
      message.event = event;
    }
    final channel = envelope['channel'] as String?;
    if (channel != null) {
      message.channel = channel;
    }
    final userId = envelope['user_id'] as String?;
    if (userId != null) {
      message.userId = userId;
    }

    final serial = _asInt(envelope['serial']);
    if (serial != null) {
      message.serial = Int64(serial);
    }

    final messageId = envelope['message_id'] as String?;
    if (messageId != null) {
      message.messageId = messageId;
    }

    final streamId = envelope['stream_id'] as String?;
    if (streamId != null) {
      message.streamId = streamId;
    }

    final sequence = _asInt(envelope['sequence']);
    if (sequence != null) {
      message.sequence = Int64(sequence);
    }

    final deltaSequence = _asInt(envelope['__delta_seq']);
    if (deltaSequence != null) {
      message.deltaSequence = Int64(deltaSequence);
    }

    final conflationKey = envelope['conflation_key'] as String?;
    if (conflationKey != null) {
      message.conflationKey = conflationKey;
    }

    final deltaConflationKey = envelope['__conflation_key'] as String?;
    if (deltaConflationKey != null) {
      message.deltaConflationKey = deltaConflationKey;
    }

    final extras = _encodeExtras(envelope['extras']);
    if (extras != null) {
      message.extras = extras;
    }

    final data = envelope['data'];
    if (data != null) {
      if (data is String) {
        message.data = ProtoMessageData()..stringValue = data;
      } else {
        message.data = ProtoMessageData()..jsonValue = jsonEncode(data);
      }
    }

    return Uint8List.fromList(message.writeToBuffer());
  }

  static Map<String, Object?> _decodeProtobuf(Uint8List bytes) {
    final message = ProtoPusherMessage.fromBuffer(bytes);
    final envelope = <String, Object?>{
      'event': message.hasEvent() ? message.event : null,
      'channel': message.hasChannel() ? message.channel : null,
      'user_id': message.hasUserId() ? message.userId : null,
      'message_id': message.hasMessageId() ? message.messageId : null,
      'stream_id': message.hasStreamId() ? message.streamId : null,
      'serial': message.hasSerial() ? message.serial.toInt() : null,
      '__delta_seq': message.hasDeltaSequence()
          ? message.deltaSequence.toInt()
          : null,
      '__conflation_key': message.hasDeltaConflationKey()
          ? message.deltaConflationKey
          : null,
      'sequence': message.hasSequence() ? message.sequence.toInt() : null,
      'conflation_key': message.hasConflationKey()
          ? message.conflationKey
          : null,
    };

    if (message.hasData()) {
      switch (message.data.whichKind()) {
        case ProtoMessageData_Kind.stringValue:
          envelope['data'] = message.data.stringValue;
        case ProtoMessageData_Kind.jsonValue:
          final raw = message.data.jsonValue;
          try {
            envelope['data'] = jsonDecode(raw);
          } catch (_) {
            envelope['data'] = raw;
          }
        case ProtoMessageData_Kind.structured:
          envelope['data'] = <String, Object?>{
            if (message.data.structured.hasChannelData())
              'channel_data': message.data.structured.channelData,
            if (message.data.structured.hasChannel())
              'channel': message.data.structured.channel,
            if (message.data.structured.hasUserData())
              'user_data': message.data.structured.userData,
            if (message.data.structured.extra.isNotEmpty)
              'extra': Map<String, String>.from(message.data.structured.extra),
          };
        case ProtoMessageData_Kind.notSet:
          break;
      }
    }

    if (message.hasExtras()) {
      envelope['extras'] = _decodeExtrasMap(message.extras);
    }

    envelope.removeWhere((_, value) => value == null);
    return envelope;
  }

  static ProtoMessageExtras? _encodeExtras(Object? rawExtras) {
    if (rawExtras == null) {
      return null;
    }
    final extras = rawExtras is MessageExtras
        ? rawExtras
        : _decodeExtras(rawExtras);
    if (extras == null) {
      return null;
    }

    final message = ProtoMessageExtras();
    final headers = extras.headers;
    if (headers != null) {
      for (final entry in headers.entries) {
        final value = entry.value;
        final protoValue = ProtoExtrasValue();
        if (value is num) {
          protoValue.numberValue = value.toDouble();
        } else if (value is bool) {
          protoValue.boolValue = value;
        } else {
          protoValue.stringValue = '$value';
        }
        message.headers[entry.key] = protoValue;
      }
    }
    if (extras.ephemeral != null) {
      message.ephemeral = extras.ephemeral!;
    }
    if (extras.idempotencyKey != null) {
      message.idempotencyKey = extras.idempotencyKey!;
    }
    if (extras.echo != null) {
      message.echo = extras.echo!;
    }
    return message;
  }

  static MessageExtras? _decodeExtras(Object? rawExtras) {
    if (rawExtras == null) {
      return null;
    }
    if (rawExtras is MessageExtras) {
      return rawExtras;
    }
    if (rawExtras is ProtoMessageExtras) {
      return MessageExtras(
        headers: rawExtras.headers.isEmpty
            ? null
            : rawExtras.headers.map(
                (key, value) => MapEntry(key, _decodeExtrasValue(value)),
              ),
        ephemeral: rawExtras.hasEphemeral() ? rawExtras.ephemeral : null,
        idempotencyKey: rawExtras.hasIdempotencyKey()
            ? rawExtras.idempotencyKey
            : null,
        echo: rawExtras.hasEcho() ? rawExtras.echo : null,
      );
    }
    final extras = _normalizeMap(rawExtras);
    return MessageExtras(
      headers: _normalizeExtrasHeaders(extras['headers']),
      ephemeral: extras['ephemeral'] as bool?,
      idempotencyKey:
          (extras['idempotency_key'] ?? extras['idempotencyKey']) as String?,
      echo: extras['echo'] as bool?,
    );
  }

  static Map<String, Object?> _decodeExtrasMap(ProtoMessageExtras extras) {
    return <String, Object?>{
      if (extras.headers.isNotEmpty)
        'headers': extras.headers.map(
          (key, value) => MapEntry(key, _decodeExtrasValue(value)),
        ),
      if (extras.hasEphemeral()) 'ephemeral': extras.ephemeral,
      if (extras.hasIdempotencyKey()) 'idempotency_key': extras.idempotencyKey,
      if (extras.hasEcho()) 'echo': extras.echo,
    };
  }

  static Object _decodeExtrasValue(ProtoExtrasValue value) {
    switch (value.whichKind()) {
      case ProtoExtrasValue_Kind.numberValue:
        return value.numberValue;
      case ProtoExtrasValue_Kind.boolValue:
        return value.boolValue;
      case ProtoExtrasValue_Kind.stringValue:
      case ProtoExtrasValue_Kind.notSet:
        return value.stringValue;
    }
  }

  static Map<String, Object>? _normalizeExtrasHeaders(Object? rawHeaders) {
    if (rawHeaders == null) {
      return null;
    }
    final headers = _normalizeMap(rawHeaders);
    final normalized = <String, Object>{};
    for (final entry in headers.entries) {
      final value = entry.value;
      if (value != null) {
        normalized[entry.key] = value;
      }
    }
    return normalized;
  }

  static Map<String, Object?> _decodeMessagePackEnvelope(Uint8List bytes) {
    final unpacker = Unpacker(bytes);
    final unpacked = _isMessagePackList(bytes)
        ? unpacker.unpackList()
        : unpacker.unpackMap();
    if (unpacked is List) {
      final envelope = <String, Object?>{};
      for (var index = 0; index < _messagePackEnvelopeFields.length; index++) {
        if (index >= unpacked.length) {
          break;
        }
        final value = _decodeMessagePackValue(unpacked[index]);
        if (value != null) {
          envelope[_messagePackEnvelopeFields[index]] = value;
        }
      }
      return envelope;
    }
    return _normalizeMap(_decodeMessagePackValue(unpacked));
  }

  static bool _isMessagePackList(Uint8List bytes) {
    if (bytes.isEmpty) {
      throw const SockudoException('Unable to decode event envelope');
    }
    final first = bytes.first;
    return (first & 0xf0) == 0x90 || first == 0xdc || first == 0xdd;
  }

  static Object? _decodeMessagePackValue(Object? value) {
    if (value is List) {
      if (value.length == 2 && value.first is String) {
        final kind = value.first as String;
        final payload = value[1];
        switch (kind) {
          case 'string':
          case 'json':
          case 'number':
          case 'bool':
            return payload;
          case 'structured':
            return _decodeMessagePackValue(payload);
        }
      }
      return value.map(_decodeMessagePackValue).toList(growable: false);
    }
    if (value is Map) {
      return value.map(
        (key, entryValue) =>
            MapEntry('$key', _decodeMessagePackValue(entryValue)),
      );
    }
    return value;
  }

  static void _packValue(Packer packer, Object? value) {
    if (value == null) {
      packer.packNull();
      return;
    }
    if (value is bool) {
      packer.packBool(value);
      return;
    }
    if (value is int) {
      packer.packInt(value);
      return;
    }
    if (value is double) {
      packer.packDouble(value);
      return;
    }
    if (value is String) {
      packer.packString(value);
      return;
    }
    if (value is Uint8List) {
      packer.packBinary(value);
      return;
    }
    if (value is List<int>) {
      packer.packBinary(value);
      return;
    }
    if (value is MessageExtras) {
      _packValue(packer, <String, Object?>{
        if (value.headers != null) 'headers': value.headers,
        if (value.ephemeral != null) 'ephemeral': value.ephemeral,
        if (value.idempotencyKey != null)
          'idempotency_key': value.idempotencyKey,
        if (value.echo != null) 'echo': value.echo,
      });
      return;
    }
    if (value is Map) {
      packer.packMapLength(value.length);
      for (final entry in value.entries) {
        packer.packString('${entry.key}');
        _packValue(packer, entry.value);
      }
      return;
    }
    if (value is Iterable) {
      final list = value.toList(growable: false);
      packer.packListLength(list.length);
      for (final item in list) {
        _packValue(packer, item);
      }
      return;
    }
    if (value is num) {
      packer.packDouble(value.toDouble());
      return;
    }
    throw SockudoException('Unsupported wire value type: ${value.runtimeType}');
  }

  static Uint8List _asBytes(Object? rawMessage) {
    if (rawMessage is Uint8List) {
      return rawMessage;
    }
    if (rawMessage is ByteBuffer) {
      return rawMessage.asUint8List();
    }
    if (rawMessage is List<int>) {
      return Uint8List.fromList(rawMessage);
    }
    if (rawMessage is String) {
      return Uint8List.fromList(utf8.encode(rawMessage));
    }
    throw SockudoException(
      'Unsupported socket payload type: ${rawMessage.runtimeType}',
    );
  }

  static Map<String, Object?> _normalizeMap(Object? value) {
    if (value is! Map) {
      throw const SockudoException('Expected object payload');
    }
    return value.map(
      (key, entryValue) => MapEntry('$key', _normalizeValue(entryValue)),
    );
  }

  static Object? _normalizeValue(Object? value) {
    if (value is Map) {
      return _normalizeMap(value);
    }
    if (value is List) {
      return value.map(_normalizeValue).toList(growable: false);
    }
    return value;
  }

  static int? _asInt(Object? value) {
    if (value is int) {
      return value;
    }
    if (value is Int64) {
      return value.toInt();
    }
    if (value is num) {
      return value.toInt();
    }
    return null;
  }
}
