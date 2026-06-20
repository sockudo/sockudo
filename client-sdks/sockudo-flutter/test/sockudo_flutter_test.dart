import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:sockudo_flutter/sockudo_flutter.dart';
import 'package:sockudo_flutter/src/delta_compression.dart';
import 'package:sockudo_flutter/src/fossil_delta.dart';
import 'package:sockudo_flutter/src/protocol_codec.dart';
import 'package:sodium/sodium.dart' as sodium;
import 'package:sodium_libs/sodium_libs.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  test('validates nested filters', () {
    final filter = Filter.or(<FilterNode>[
      Filter.eq('sport', 'football'),
      Filter.and(<FilterNode>[
        Filter.eq('type', 'goal'),
        Filter.gte('xg', '0.8'),
      ]),
    ]);

    expect(validateFilter(filter), isNull);
  });

  test('serializes delta settings', () {
    expect(
      const ChannelDeltaSettings(enabled: true).toSubscriptionValue(),
      isTrue,
    );
    expect(
      const ChannelDeltaSettings(enabled: false).toSubscriptionValue(),
      isFalse,
    );
    expect(
      const ChannelDeltaSettings(
        algorithm: DeltaAlgorithm.fossil,
      ).toSubscriptionValue(),
      'fossil',
    );
    expect(const SubscriptionRewind.count(10).toSubscriptionValue(), 10);
    expect(
      const SubscriptionRewind.seconds(30).toSubscriptionValue(),
      <String, Object>{'seconds': 30},
    );
  });

  test('presence history params normalize ably aliases', () {
    expect(
      const PresenceHistoryParams(
        direction: 'newest_first',
        limit: 50,
        start: 1000,
        end: 2000,
      ).toJson(),
      <String, Object>{
        'direction': 'newest_first',
        'limit': 50,
        'start_time_ms': 1000,
        'end_time_ms': 2000,
      },
    );
  });

  test('presence history page next uses next cursor', () async {
    String? capturedCursor;
    final page = PresenceHistoryPage(
      items: const <PresenceHistoryItem>[],
      direction: 'newest_first',
      limit: 50,
      hasMore: true,
      nextCursor: 'cursor-2',
      bounds: const PresenceHistoryBounds(
        startSerial: null,
        endSerial: null,
        startTimeMs: null,
        endTimeMs: null,
      ),
      continuity: const PresenceHistoryContinuity(
        streamId: null,
        oldestAvailableSerial: null,
        newestAvailableSerial: null,
        oldestAvailablePublishedAtMs: null,
        newestAvailablePublishedAtMs: null,
        retainedEvents: 0,
        retainedBytes: 0,
        degraded: false,
        complete: true,
        truncatedByRetention: false,
      ),
      fetchNext: (cursor) async {
        capturedCursor = cursor;
        return PresenceHistoryPage(
          items: const <PresenceHistoryItem>[],
          direction: 'newest_first',
          limit: 50,
          hasMore: false,
          nextCursor: null,
          bounds: const PresenceHistoryBounds(
            startSerial: null,
            endSerial: null,
            startTimeMs: null,
            endTimeMs: null,
          ),
          continuity: const PresenceHistoryContinuity(
            streamId: null,
            oldestAvailableSerial: null,
            newestAvailableSerial: null,
            oldestAvailablePublishedAtMs: null,
            newestAvailablePublishedAtMs: null,
            retainedEvents: 0,
            retainedBytes: 0,
            degraded: false,
            complete: true,
            truncatedByRetention: false,
          ),
        );
      },
    );

    expect(page.hasNext(), isTrue);
    await page.next();
    expect(capturedCursor, 'cursor-2');
  });

  test(
    'push proxy helpers use backend endpoint and async publish defaults',
    () async {
      final requests = <http.Request>[];
      final client = RecordingHttpClient((request) async {
        requests.add(request);
        if (request.url.path.endsWith('/publish')) {
          return http.Response(
            jsonEncode(<String, Object?>{'publish_id': 'pub_123'}),
            202,
            headers: <String, String>{'content-type': 'application/json'},
          );
        }
        return http.Response(
          jsonEncode(<String, Object?>{
            'items': <Object?>[],
            'has_more': false,
          }),
          200,
          headers: <String, String>{'content-type': 'application/json'},
        );
      });

      final push = SockudoPushRegistration(
        const PushRegistrationOptions(
          endpoint: 'https://api.example.test/push/',
          headers: <String, String>{'Authorization': 'Bearer session'},
        ),
        httpClient: client,
      );

      final publish = await push.publish(<String, Object?>{
        'recipients': <Map<String, Object?>>[
          <String, Object?>{'type': 'channel', 'channel': 'orders'},
        ],
        'payload': <String, Object?>{'title': 'Order', 'body': 'Updated'},
      });
      await push.updateDeviceRegistration(<String, Object?>{
        'id': 'device-1',
        'formFactor': 'phone',
        'platform': 'android',
        'timezone': 'UTC',
        'locale': 'en',
        'push': <String, Object?>{
          'recipient': <String, Object?>{
            'transportType': 'gcm',
            'registrationToken': 'rotated',
          },
        },
      }, 'identity');
      await push.listChannelSubscriptions(
        const PushSubscriptionParams(
          deviceId: 'device-1',
          limit: 10,
          cursor: 'c1',
        ),
      );

      expect(publish, <Object?, Object?>{'publish_id': 'pub_123'});
      expect(
        requests[0].url.toString(),
        'https://api.example.test/push/publish',
      );
      expect(requests[0].method, 'POST');
      expect(jsonDecode(requests[0].body)['sync'], isFalse);
      expect(requests[0].headers['Authorization'], 'Bearer session');

      expect(
        requests[1].headers['X-Sockudo-Device-Identity-Token'],
        'identity',
      );
      expect(
        requests[2].url.toString(),
        'https://api.example.test/push/channelSubscriptions?deviceId=device-1&limit=10&cursor=c1',
      );
    },
  );

  test('applies insert-only fossil delta', () {
    final result = FossilDelta.apply(
      Uint8List(0),
      Uint8List.fromList('5\n5:hello3NPMmh;'.codeUnits),
    );

    expect(String.fromCharCodes(result), 'hello');
  });

  test('reconstructs fossil delta events', () {
    final manager = DeltaCompressionManager(
      const DeltaOptions(),
      (_, _) => true,
    );

    manager.handleFullMessage('ticker:1', '{"data":{"price":101}}', 1, null);

    final event = manager.handleDeltaMessage('ticker:1', <String, Object>{
      'event': 'price-updated',
      'delta': 'TQpNOnsiZGF0YSI6eyJwcmljZSI6MTAyfX0yQml0bVg7',
      'seq': 2,
      'algorithm': 'fossil',
    });

    expect(event, isNotNull);
    expect(event!.event, 'price-updated');
    expect(event.data, <String, Object>{'price': 102});
  });

  test('reconstructs xdelta3 events', () {
    final manager = DeltaCompressionManager(
      const DeltaOptions(),
      (_, _) => true,
    );

    manager.handleFullMessage('ticker:1', '{"data":{"price":101}}', 1, null);

    final event = manager.handleDeltaMessage('ticker:1', <String, Object>{
      'event': 'price-updated',
      'delta': '1sPEAAABFgAdFgAWAgB7ImRhdGEiOnsicHJpY2UiOjEwMn19ARY=',
      'seq': 2,
      'algorithm': 'xdelta3',
    });

    expect(event, isNotNull);
    expect(event!.event, 'price-updated');
    expect(event.data, <String, Object>{'price': 102});
  });

  test('encodes websocket URL with v2 format query', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() async {
      await server.close(force: true);
    });

    final queryBox = ValueBox<Map<String, String>>();
    unawaited(() async {
      final request = await server.first;
      queryBox.value = request.uri.queryParameters;
      final socket = await WebSocketTransformer.upgrade(request);
      await socket.close();
    }());

    final client = SockudoClient(
      'app-key',
      SockudoOptions(
        cluster: 'local',
        forceTls: false,
        protocolVersion: 2,
        enabledTransports: const <SockudoTransport>[SockudoTransport.ws],
        wsHost: '127.0.0.1',
        wsPort: server.port,
        wssPort: server.port,
        wireFormat: SockudoWireFormat.messagepack,
      ),
    );
    addTearDown(client.close);

    client.connect();

    final query = await _waitForValue(() => queryBox.value);
    expect(query['protocol'], '2');
    expect(query['format'], 'messagepack');
  });

  test('uses v1 by default and omits the format query', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() async {
      await server.close(force: true);
    });

    final queryBox = ValueBox<Map<String, String>>();
    unawaited(() async {
      final request = await server.first;
      queryBox.value = request.uri.queryParameters;
      final socket = await WebSocketTransformer.upgrade(request);
      await socket.close();
    }());

    final client = SockudoClient(
      'app-key',
      SockudoOptions(
        cluster: 'local',
        forceTls: false,
        enabledTransports: const <SockudoTransport>[SockudoTransport.ws],
        wsHost: '127.0.0.1',
        wsPort: server.port,
        wssPort: server.port,
        wireFormat: SockudoWireFormat.messagepack,
      ),
    );
    addTearDown(client.close);

    client.connect();

    final query = await _waitForValue(() => queryBox.value);
    expect(query['protocol'], '7');
    expect(query.containsKey('format'), isFalse);
  });

  test('round trips messagepack envelopes', () {
    final encoded = SockudoProtocolCodec.encodeEnvelope(<String, Object?>{
      'event': 'sockudo:test',
      'channel': 'chat:room-1',
      'data': <String, Object?>{'hello': 'world', 'count': 3},
      'stream_id': 'stream-1',
      'message_id': 'msg-1',
      'serial': 7,
      '__delta_seq': 7,
      '__conflation_key': 'room',
    }, SockudoWireFormat.messagepack);

    final decoded = SockudoProtocolCodec.decodeEvent(
      encoded,
      SockudoWireFormat.messagepack,
    );

    expect(decoded.event, 'sockudo:test');
    expect(decoded.channel, 'chat:room-1');
    expect(decoded.data, <String, Object?>{'hello': 'world', 'count': 3});
    expect(decoded.streamId, 'stream-1');
    expect(decoded.messageId, 'msg-1');
    expect(decoded.serial, 7);
    expect(decoded.sequence, 7);
    expect(decoded.conflationKey, 'room');
  });

  test('round trips protobuf envelopes', () {
    final encoded = SockudoProtocolCodec.encodeEnvelope(<String, Object?>{
      'event': 'sockudo:test',
      'channel': 'chat:room-1',
      'data': <String, Object?>{'hello': 'world'},
      'stream_id': 'stream-2',
      'message_id': 'msg-2',
      'serial': 9,
      '__delta_seq': 11,
      '__conflation_key': 'btc',
      'extras': <String, Object?>{
        'headers': <String, Object>{'region': 'eu', 'ttl': 5, 'replay': true},
        'echo': false,
      },
    }, SockudoWireFormat.protobuf);

    final decoded = SockudoProtocolCodec.decodeEvent(
      encoded,
      SockudoWireFormat.protobuf,
    );

    expect(decoded.event, 'sockudo:test');
    expect(decoded.channel, 'chat:room-1');
    expect(decoded.data, <String, Object?>{'hello': 'world'});
    expect(decoded.streamId, 'stream-2');
    expect(decoded.messageId, 'msg-2');
    expect(decoded.serial, 9);
    expect(decoded.sequence, 11);
    expect(decoded.conflationKey, 'btc');
    expect(decoded.extras?.headers, <String, Object>{
      'region': 'eu',
      'ttl': 5.0,
      'replay': true,
    });
    expect(decoded.extras?.echo, isFalse);
  });

  test('live public integration receives published event', () async {
    if (!_liveTestsEnabled()) {
      return;
    }

    final connected = ValueBox<bool>();
    final subscribed = ValueBox<bool>();
    final received = ValueBox<Map<Object?, Object?>>();

    final client = SockudoClient(
      'app-key',
      SockudoOptions(
        cluster: 'local',
        forceTls: false,
        protocolVersion: 2,
        enabledTransports: <SockudoTransport>[SockudoTransport.ws],
        wsHost: '127.0.0.1',
        wsPort: 6001,
        wssPort: 6001,
        wireFormat: _liveWireFormat(),
      ),
    );

    final channel = client.subscribe('public-updates');
    client.bind('connected', (_, _) => connected.value = true);
    channel.bind('sockudo:subscription_succeeded', (_, _) {
      subscribed.value = true;
    });
    channel.bind('integration-event', (data, _) {
      received.value = data as Map<Object?, Object?>?;
    });

    client.connect();
    await _waitFor(() => connected.value == true);
    await _waitFor(() => subscribed.value == true);

    await _publishToLocalSockudo(
      channel: 'public-updates',
      eventName: 'integration-event',
      payload: <String, Object>{
        'message': 'hello from flutter',
        'item_id': 'flutter-client',
        'padding': List<String>.filled(140, 'x').join(),
      },
    );

    final payload = await _waitForValue(() => received.value);
    expect(payload['message'], 'hello from flutter');
    client.close();
  });

  test('live raw v2 idle heartbeat uses control frames', () async {
    if (!_liveTestsEnabled()) {
      return;
    }

    final socket = await WebSocket.connect(_rawSocketUrl(2));
    final messages = <Object?>[];
    final subscription = socket.listen(messages.add);
    addTearDown(() async {
      await subscription.cancel();
      await socket.close();
    });

    final handshake =
        jsonDecode(
              await _waitForValue(
                () =>
                    messages.isNotEmpty ? messages.removeAt(0) as String : null,
              ),
            )
            as Map<String, Object?>;
    expect(handshake['event'], 'sockudo:connection_established');

    await Future<void>.delayed(const Duration(seconds: 8));
    expect(messages, isEmpty);
  });

  test('live raw v2 fallback pong has no metadata', () async {
    if (!_liveTestsEnabled()) {
      return;
    }

    final socket = await WebSocket.connect(_rawSocketUrl(2));
    final messages = <Object?>[];
    final subscription = socket.listen(messages.add);
    addTearDown(() async {
      await subscription.cancel();
      await socket.close();
    });

    final handshake =
        jsonDecode(
              await _waitForValue(
                () =>
                    messages.isNotEmpty ? messages.removeAt(0) as String : null,
              ),
            )
            as Map<String, Object?>;
    expect(handshake['event'], 'sockudo:connection_established');

    socket.add(
      jsonEncode(<String, Object>{
        'event': 'sockudo:ping',
        'data': <String, Object>{},
      }),
    );
    final pong =
        jsonDecode(
              await _waitForValue(
                () =>
                    messages.isNotEmpty ? messages.removeAt(0) as String : null,
              ),
            )
            as Map<String, Object?>;
    expect(pong['event'], 'sockudo:pong');
    expect(pong.containsKey('message_id'), isFalse);
    expect(pong.containsKey('serial'), isFalse);
    expect(pong.containsKey('stream_id'), isFalse);
  });

  test('live raw v1 heartbeat still uses protocol ping', () async {
    if (!_liveTestsEnabled()) {
      return;
    }

    final socket = await WebSocket.connect(_rawSocketUrl(7));
    var done = false;
    final messages = <Object?>[];
    final subscription = socket.listen(messages.add, onDone: () => done = true);
    addTearDown(() async {
      await subscription.cancel();
      if (socket.closeCode == null) {
        await socket.close();
      }
    });

    final handshake =
        jsonDecode(
              await _waitForValue(
                () =>
                    messages.isNotEmpty ? messages.removeAt(0) as String : null,
              ),
            )
            as Map<String, Object?>;
    expect(handshake['event'], 'pusher:connection_established');

    final ping =
        jsonDecode(
              await _waitForValue(
                () =>
                    messages.isNotEmpty ? messages.removeAt(0) as String : null,
                timeout: const Duration(seconds: 6),
              ),
            )
            as Map<String, Object?>;
    expect(ping['event'], 'pusher:ping');

    socket.add(
      jsonEncode(<String, Object>{
        'event': 'pusher:pong',
        'data': <String, Object>{},
      }),
    );
    await Future<void>.delayed(const Duration(milliseconds: 1500));
    expect(done, isFalse);
  });

  test('live delta integration reconstructs compressed event', () async {
    if (!_liveTestsEnabled()) {
      return;
    }

    final connected = ValueBox<bool>();
    final subscribed = ValueBox<bool>();
    final prices = <int>[];

    final client = SockudoClient(
      'app-key',
      SockudoOptions(
        cluster: 'local',
        forceTls: false,
        protocolVersion: 2,
        enabledTransports: <SockudoTransport>[SockudoTransport.ws],
        wsHost: '127.0.0.1',
        wsPort: 6001,
        wssPort: 6001,
        deltaCompression: DeltaOptions(enabled: true),
        wireFormat: _liveWireFormat(),
      ),
    );

    final channel = client.subscribe(
      'price:integration',
      options: const SubscriptionOptions(
        delta: ChannelDeltaSettings(enabled: true),
      ),
    );
    client.bind('connected', (_, _) => connected.value = true);
    channel.bind('sockudo:subscription_succeeded', (_, _) {
      subscribed.value = true;
    });
    channel.bind('price-updated', (data, _) {
      final map = data as Map<Object?, Object?>?;
      final price = map?['price'] as int?;
      if (price != null) {
        prices.add(price);
      }
    });

    client.connect();
    await _waitFor(() => connected.value == true);
    await _waitFor(() => subscribed.value == true);

    await _publishToLocalSockudo(
      channel: 'price:integration',
      eventName: 'price-updated',
      payload: <String, Object>{
        'item_id': 'delta-item',
        'price': 101,
        'padding': List<String>.filled(180, 'y').join(),
      },
    );
    await _publishToLocalSockudo(
      channel: 'price:integration',
      eventName: 'price-updated',
      payload: <String, Object>{
        'item_id': 'delta-item',
        'price': 102,
        'padding': List<String>.filled(180, 'y').join(),
      },
    );

    await _waitFor(() => prices.contains(102));
    expect(prices.last, 102);
    client.close();
  });

  test('live encrypted integration decrypts payload', () async {
    if (!_liveTestsEnabled()) {
      return;
    }

    sodium.Sodium sodiumInstance;
    try {
      sodiumInstance = await SodiumInit.init();
    } catch (_) {
      return;
    }
    final secretBytes = Uint8List.fromList(
      List<int>.generate(32, (index) => index + 1),
    );
    final connected = ValueBox<bool>();
    final subscribed = ValueBox<bool>();
    final received = ValueBox<Map<Object?, Object?>>();

    final client = SockudoClient(
      'app-key',
      SockudoOptions(
        cluster: 'local',
        forceTls: false,
        protocolVersion: 2,
        enabledTransports: const <SockudoTransport>[SockudoTransport.ws],
        wsHost: '127.0.0.1',
        wsPort: 6001,
        wssPort: 6001,
        channelAuthorization: ChannelAuthorizationOptions(
          customHandler: (request) async {
            return ChannelAuthorizationData(
              auth:
                  'app-key:${_hmacSha256Hex('${request.socketId}:${request.channelName}', 'app-secret')}',
              sharedSecret: base64Encode(secretBytes),
            );
          },
        ),
      ),
    );

    final channel = client.subscribe('private-encrypted-live');
    client.bind('connected', (_, _) => connected.value = true);
    channel.bind('sockudo:subscription_succeeded', (_, _) {
      subscribed.value = true;
    });
    channel.bind('encrypted-event', (data, _) {
      received.value = data as Map<Object?, Object?>?;
    });

    client.connect();
    await _waitFor(() => connected.value == true);
    await _waitFor(() => subscribed.value == true);

    final payload = await _encryptPayload(
      sodiumInstance,
      secretBytes,
      '{"message":"secret hello","item_id":"enc-item"}',
    );
    await _publishRawToLocalSockudo(
      channel: 'private-encrypted-live',
      eventName: 'encrypted-event',
      payload: payload,
    );

    final decrypted = await _waitForValue(() => received.value);
    expect(decrypted['message'], 'secret hello');
    client.close();
  });
}

class RecordingHttpClient extends http.BaseClient {
  RecordingHttpClient(this._handler);

  final Future<http.Response> Function(http.Request request) _handler;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    final streamed = request.finalize();
    final bytes = await streamed.toBytes();
    final copy = http.Request(request.method, request.url)
      ..headers.addAll(request.headers)
      ..bodyBytes = bytes;
    final response = await _handler(copy);
    return http.StreamedResponse(
      Stream<List<int>>.value(response.bodyBytes),
      response.statusCode,
      headers: response.headers,
      request: request,
    );
  }
}

class ValueBox<T> {
  T? value;
}

bool _liveTestsEnabled() => Platform.environment['SOCKUDO_LIVE_TESTS'] == '1';

String _rawSocketUrl(int protocolVersion) {
  final parameters = <String, String>{
    'protocol': '$protocolVersion',
    'client': 'flutter-live',
    'version': '1.0.0',
    if (protocolVersion == 2) 'format': 'json',
  };

  return Uri(
    scheme: 'ws',
    host: '127.0.0.1',
    port: 6001,
    path: '/app/app-key',
    queryParameters: parameters,
  ).toString();
}

SockudoWireFormat _liveWireFormat() {
  switch (Platform.environment['SOCKUDO_WIRE_FORMAT']?.toLowerCase()) {
    case 'messagepack':
    case 'msgpack':
      return SockudoWireFormat.messagepack;
    case 'protobuf':
    case 'proto':
      return SockudoWireFormat.protobuf;
    default:
      return SockudoWireFormat.json;
  }
}

Future<void> _waitFor(
  bool Function() predicate, {
  Duration timeout = const Duration(seconds: 8),
}) async {
  await _waitForValue<bool>(() => predicate() ? true : null, timeout: timeout);
}

Future<T> _waitForValue<T>(
  T? Function() supplier, {
  Duration timeout = const Duration(seconds: 8),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    final value = supplier();
    if (value != null) {
      return value;
    }
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
  throw StateError('Timed out waiting for value');
}

Future<void> _publishToLocalSockudo({
  required String channel,
  required String eventName,
  required Map<String, Object> payload,
}) {
  return _publishBodyToLocalSockudo(<String, Object>{
    'name': eventName,
    'channels': <String>[channel],
    'data': jsonEncode(payload),
  });
}

Future<void> _publishRawToLocalSockudo({
  required String channel,
  required String eventName,
  required Map<String, String> payload,
}) {
  return _publishBodyToLocalSockudo(<String, Object>{
    'name': eventName,
    'channels': <String>[channel],
    'data': jsonEncode(payload),
  });
}

Future<void> _publishBodyToLocalSockudo(Map<String, Object> bodyObject) async {
  const path = '/apps/app-id/events';
  final body = utf8.encode(jsonEncode(bodyObject));
  final bodyMd5 = md5.convert(body).toString();
  final timestamp = (DateTime.now().millisecondsSinceEpoch ~/ 1000).toString();
  final query = <String, String>{
    'auth_key': 'app-key',
    'auth_timestamp': timestamp,
    'auth_version': '1.0',
    'body_md5': bodyMd5,
  };
  final canonicalQuery = query.entries.toList()
    ..sort((a, b) => a.key.compareTo(b.key));
  final canonical = canonicalQuery
      .map((entry) => '${entry.key}=${entry.value}')
      .join('&');
  final signature = _hmacSha256Hex('POST\n$path\n$canonical', 'app-secret');
  final uri = Uri.parse(
    'http://127.0.0.1:6001$path?$canonical&auth_signature=$signature',
  );

  final result = await Process.run('curl', <String>[
    '-s',
    '-o',
    '/tmp/sockudo_flutter_publish_body.txt',
    '-w',
    '%{http_code}',
    '-X',
    'POST',
    '-H',
    'Content-Type: application/json',
    uri.toString(),
    '--data-binary',
    utf8.decode(body),
  ]);

  final statusCode = int.tryParse('${result.stdout}'.trim()) ?? 0;
  expect(<int>[200, 202], contains(statusCode));
}

String _hmacSha256Hex(String value, String secret) {
  final hmac = Hmac(sha256, utf8.encode(secret));
  return hmac.convert(utf8.encode(value)).toString();
}

Future<Map<String, String>> _encryptPayload(
  sodium.Sodium sodiumInstance,
  Uint8List secretBytes,
  String payload,
) async {
  final nonce = Uint8List.fromList(
    List<int>.generate(24, (index) => (index * 3 + 7) & 0xff),
  );
  final key = sodium.SecureKey.fromList(sodiumInstance, secretBytes);
  try {
    final cipherText = sodiumInstance.crypto.secretBox.easy(
      message: Uint8List.fromList(utf8.encode(payload)),
      nonce: nonce,
      key: key,
    );
    return <String, String>{
      'ciphertext': base64Encode(cipherText),
      'nonce': base64Encode(nonce),
    };
  } finally {
    key.dispose();
  }
}
