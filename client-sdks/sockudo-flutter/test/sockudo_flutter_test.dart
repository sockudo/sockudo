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
import 'package:sockudo_flutter/src/support.dart';
import 'package:sodium/sodium.dart' as sodium;

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

  test('channel history params include until attach and string serials', () {
    expect(
      const ChannelHistoryParams(
        direction: 'newest_first',
        limit: 100,
        startSerial: '9007199254740993',
        endSerial: '9007199254740994',
        untilAttach: true,
      ).toJson(),
      <String, Object>{
        'direction': 'newest_first',
        'limit': 100,
        'start_serial': '9007199254740993',
        'end_serial': '9007199254740994',
        'until_attach': true,
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

  test(
    'versioned message proxy helpers send actions and decode acks',
    () async {
      final requests = <Map<Object?, Object?>>[];
      final httpClient = RecordingHttpClient((request) async {
        final body = jsonDecode(request.body) as Map<Object?, Object?>;
        requests.add(body);
        final action = body['action'];
        if (action == 'publish_create') {
          return http.Response(
            jsonEncode(<String, Object?>{
              'channels': <String, Object?>{
                'chat:room-1': <String, Object?>{
                  'message_serial': 'msg:1',
                  'history_serial': 9007199254740993,
                  'delivery_serial': 9007199254740994,
                  'version_serial': 'ver:1',
                  'status': 'applied',
                },
              },
            }),
            200,
            headers: <String, String>{'content-type': 'application/json'},
          );
        }
        return http.Response(
          jsonEncode(<String, Object?>{
            'messageSerial': body['messageSerial'],
            'historySerial': '9007199254740995',
            'deliverySerial': '9007199254740996',
            'versionSerial': 'ver:2',
            'status': 'applied',
          }),
          200,
          headers: <String, String>{'content-type': 'application/json'},
        );
      });

      final client = SockudoClient(
        'app-key',
        const SockudoOptions(
          cluster: 'local',
          protocolVersion: 2,
          versionedMessages: VersionedMessagesOptions(
            endpoint: 'https://api.example.test/versioned',
            headers: <String, String>{'Authorization': 'Bearer session'},
          ),
        ),
        httpClient: httpClient,
      );
      addTearDown(client.close);

      final channel = client.subscribe('chat:room-1');
      final created = await channel.createMessage(
        const VersionedMessageCreateRequest(data: 'hello'),
      );
      final appended = await channel.appendMessage(
        'msg:1',
        ' world',
        const VersionedMessageMutation(opId: 'op-append'),
      );
      await channel.updateMessage(
        'msg:1',
        const VersionedMessageMutation(
          data: <String, Object?>{'text': 'updated'},
          clearFields: <String>['extras'],
        ),
      );
      await channel.deleteMessage(
        'msg:1',
        const VersionedMessageMutation(description: 'redacted'),
      );

      expect(created.messageSerial, 'msg:1');
      expect(created.historySerial, '9007199254740993');
      expect(created.deliverySerial, '9007199254740994');
      expect(created.versionSerial, 'ver:1');
      expect(created.status, 'applied');
      expect(appended.historySerial, '9007199254740995');

      expect(requests.map((request) => request['action']).toList(), <String>[
        'publish_create',
        'message_append',
        'message_update',
        'message_delete',
      ]);
      expect(requests[0]['name'], 'sockudo:message.create');
      expect(requests[0]['messageId'], isA<String>());
      expect(requests[1]['messageSerial'], 'msg:1');
      expect(requests[1]['data'], ' world');
      expect(requests[1]['opId'], 'op-append');
      expect(requests[2]['clearFields'], <Object?>['extras']);
      expect(requests[3]['description'], 'redacted');
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
        appendMode: SockudoAppendMode.full,
        appendRollupWindow: 40,
        token: 'initial-token',
      ),
    );
    addTearDown(client.close);

    client.connect();

    final query = await _waitForValue(() => queryBox.value);
    expect(query['protocol'], '2');
    expect(query['format'], 'messagepack');
    expect(query['append_mode'], 'full');
    expect(query['append_rollup_window'], '40');
    expect(query['token'], 'initial-token');
  });

  test('validates append rollup window values locally', () {
    expect(
      () => SockudoClient(
        'app-key',
        const SockudoOptions(
          cluster: 'local',
          protocolVersion: 2,
          appendRollupWindow: 60,
        ),
      ),
      throwsA(isA<SockudoException>()),
    );
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
    expect(query.containsKey('append_mode'), isFalse);
  });

  test(
    'auth callback supplies initial token and refreshes on expiry',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() async {
        await server.close(force: true);
      });

      final queryBox = ValueBox<Map<String, String>>();
      final authFrameBox = ValueBox<Map<Object?, Object?>>();
      unawaited(() async {
        final request = await server.first;
        queryBox.value = request.uri.queryParameters;
        final socket = await WebSocketTransformer.upgrade(request);
        socket.listen((raw) {
          final frame = jsonDecode(raw as String) as Map<Object?, Object?>;
          authFrameBox.value = frame;
          if (frame['event'] == 'sockudo:auth') {
            socket.add(
              jsonEncode(<String, Object?>{
                'event': 'sockudo:auth_success',
                'data': <String, Object?>{
                  'client_id': 'client-1',
                  'jti': 'jti-2',
                  'exp': 123,
                },
              }),
            );
          }
        });
        socket.add(
          jsonEncode(<String, Object?>{
            'event': 'sockudo:connection_established',
            'data': <String, Object?>{'socket_id': '1.1'},
          }),
        );
        await Future<void>.delayed(const Duration(milliseconds: 50));
        socket.add(
          jsonEncode(<String, Object?>{
            'event': 'sockudo:token_expired',
            'data': <String, Object?>{'code': 40142, 'reason': 'expired'},
          }),
        );
      }());

      var tokenCalls = 0;
      final expiredBox = ValueBox<Map<Object?, Object?>>();
      final successBox = ValueBox<Map<Object?, Object?>>();
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
          authCallback: () async => 'token-${++tokenCalls}',
        ),
      );
      addTearDown(client.close);
      client.bind('sockudo:token_expired', (data, _) {
        expiredBox.value = data as Map<Object?, Object?>;
      });
      client.bind('sockudo:auth_success', (data, _) {
        successBox.value = data as Map<Object?, Object?>;
      });

      client.connect();

      final query = await _waitForValue(() => queryBox.value);
      expect(query['token'], 'token-1');

      final authFrame = await _waitForValue(() => authFrameBox.value);
      expect(authFrame['event'], 'sockudo:auth');
      expect((authFrame['data'] as Map<Object?, Object?>)['token'], 'token-2');
      expect((await _waitForValue(() => expiredBox.value))['code'], 40142);
      expect((await _waitForValue(() => successBox.value))['jti'], 'jti-2');
      expect(tokenCalls, 2);
    },
  );

  test(
    'auth callback jwt proactively refreshes at 80 percent lifetime',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() async {
        await server.close(force: true);
      });

      final queryBox = ValueBox<Map<String, String>>();
      final authFrameBox = ValueBox<Map<Object?, Object?>>();
      unawaited(() async {
        final request = await server.first;
        queryBox.value = request.uri.queryParameters;
        final socket = await WebSocketTransformer.upgrade(request);
        socket.listen((raw) {
          final frame = jsonDecode(raw as String) as Map<Object?, Object?>;
          if (frame['event'] == 'sockudo:auth') {
            authFrameBox.value = frame;
          }
        });
        socket.add(
          jsonEncode(<String, Object?>{
            'event': 'sockudo:connection_established',
            'data': <String, Object?>{'socket_id': '1.1'},
          }),
        );
      }());

      var tokenCalls = 0;
      final now = DateTime.now().millisecondsSinceEpoch / 1000.0;
      final proactiveToken = _fakeJwt(iat: now - 0.1, exp: now + 0.4);
      const refreshedToken = 'opaque-refresh-token';
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
          authCallback: () async {
            tokenCalls += 1;
            return tokenCalls == 1 ? proactiveToken : refreshedToken;
          },
        ),
      );
      addTearDown(client.close);

      client.connect();

      final query = await _waitForValue(() => queryBox.value);
      expect(query['token'], proactiveToken);
      final authFrame = await _waitForValue(
        () => authFrameBox.value,
        timeout: const Duration(seconds: 2),
      );
      expect(authFrame['event'], 'sockudo:auth');
      expect(
        (authFrame['data'] as Map<Object?, Object?>)['token'],
        refreshedToken,
      );
      expect(tokenCalls, 2);
    },
  );

  test(
    'static jwt token without auth callback does not proactively refresh',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() async {
        await server.close(force: true);
      });

      final authFrameBox = ValueBox<Map<Object?, Object?>>();
      unawaited(() async {
        final request = await server.first;
        final socket = await WebSocketTransformer.upgrade(request);
        socket.listen((raw) {
          final frame = jsonDecode(raw as String) as Map<Object?, Object?>;
          if (frame['event'] == 'sockudo:auth') {
            authFrameBox.value = frame;
          }
        });
        socket.add(
          jsonEncode(<String, Object?>{
            'event': 'sockudo:connection_established',
            'data': <String, Object?>{'socket_id': '1.1'},
          }),
        );
      }());

      final now = DateTime.now().millisecondsSinceEpoch / 1000.0;
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
          token: _fakeJwt(iat: now - 0.1, exp: now + 0.4),
        ),
      );
      addTearDown(client.close);

      client.connect();
      await Future<void>.delayed(const Duration(milliseconds: 700));

      expect(authFrameBox.value, isNull);
    },
  );

  test('disconnect cancels proactive auth refresh timer', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() async {
      await server.close(force: true);
    });

    final queryBox = ValueBox<Map<String, String>>();
    final authFrameBox = ValueBox<Map<Object?, Object?>>();
    unawaited(() async {
      final request = await server.first;
      queryBox.value = request.uri.queryParameters;
      final socket = await WebSocketTransformer.upgrade(request);
      socket.listen((raw) {
        final frame = jsonDecode(raw as String) as Map<Object?, Object?>;
        if (frame['event'] == 'sockudo:auth') {
          authFrameBox.value = frame;
        }
      });
      socket.add(
        jsonEncode(<String, Object?>{
          'event': 'sockudo:connection_established',
          'data': <String, Object?>{'socket_id': '1.1'},
        }),
      );
    }());

    var tokenCalls = 0;
    final now = DateTime.now().millisecondsSinceEpoch / 1000.0;
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
        authCallback: () async {
          tokenCalls += 1;
          return _fakeJwt(iat: now - 0.1, exp: now + 0.4);
        },
      ),
    );
    addTearDown(client.close);

    client.connect();
    await _waitForValue(() => queryBox.value);
    client.disconnect();
    await Future<void>.delayed(const Duration(milliseconds: 700));

    expect(authFrameBox.value, isNull);
    expect(tokenCalls, 1);
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

  test('round trips native binary data', () {
    final data = Uint8List.fromList(<int>[0, 1, 2, 255]);

    for (final wireFormat in <SockudoWireFormat>[
      SockudoWireFormat.messagepack,
      SockudoWireFormat.protobuf,
    ]) {
      final payload = SockudoProtocolCodec.encodeEnvelope(<String, Object?>{
        'event': 'sockudo:binary',
        'channel': 'binary:room-1',
        'data': data,
      }, wireFormat);

      final decoded = SockudoProtocolCodec.decodeEvent(payload, wireFormat);

      expect(decoded.data, orderedEquals(data));
      expect(decoded.data, isA<Uint8List>());
    }
  });

  test('subscription expressions preserve both wire variants', () {
    expect(
      const SubscriptionExpression('data.total >= `100`').toSubscriptionValue(),
      'data.total >= `100`',
    );
    expect(
      const SubscriptionExpression.descriptor(
        source: 'headers.priority == `"high"`',
      ).toSubscriptionValue(),
      <String, String>{
        'language': 'jmespath',
        'source': 'headers.priority == `"high"`',
      },
    );
  });

  test('resume success applies authoritative channel position', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() async {
      await server.close(force: true);
    });

    unawaited(() async {
      final request = await server.first;
      final socket = await WebSocketTransformer.upgrade(request);
      socket.add(
        jsonEncode(<String, Object?>{
          'event': 'sockudo:connection_established',
          'data': <String, Object?>{'socket_id': '1.1'},
        }),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));
      socket.add(
        jsonEncode(<String, Object?>{
          'event': 'sockudo:resume_success',
          'data': <String, Object?>{
            'recovered': <Object?>[
              <String, Object?>{
                'channel': 'orders',
                'source': 'durable',
                'replayed': 0,
                'position': <String, Object?>{
                  'stream_id': 'stream-2',
                  'serial': 42,
                  'last_message_id': 'message-42',
                },
              },
            ],
            'failed': <Object?>[],
          },
        }),
      );
    }());

    final client = SockudoClient(
      'app-key',
      SockudoOptions(
        cluster: 'local',
        forceTls: false,
        protocolVersion: 2,
        connectionRecovery: true,
        enabledTransports: const <SockudoTransport>[SockudoTransport.ws],
        wsHost: '127.0.0.1',
        wsPort: server.port,
        wssPort: server.port,
      ),
    );
    addTearDown(client.close);

    client.connect();
    final position = await _waitForValue(
      () => client.getRecoveryPosition('orders'),
    );

    expect(position.serial, 42);
    expect(position.streamId, 'stream-2');
    expect(position.lastMessageId, 'message-42');
  });

  test('preserves forward-compatible extras and serials while decoding', () {
    final futureFrame = SockudoProtocolCodec.decodeEvent(
      _forwardCompatFixture('future-v2-frame.json'),
      SockudoWireFormat.json,
    );
    final safeFrame = SockudoProtocolCodec.decodeEvent(
      jsonEncode(<String, Object?>{
        'event': 'sockudo:test',
        'channel': 'private-ai-forward',
        'serial': 2147483648,
      }),
      SockudoWireFormat.json,
    );
    final extrasFrame = SockudoProtocolCodec.decodeEvent(
      _forwardCompatFixture('unknown-ai-extras.json'),
      SockudoWireFormat.json,
    );
    final futureVersionEnvelope = SockudoProtocolCodec.decodeEnvelope(
      _forwardCompatFixture('future-versioned-action.json'),
      SockudoWireFormat.json,
    ).envelope;

    expect(futureFrame.serial, '9007199254740993');
    expect(safeFrame.serial, 2147483648);
    expect(futureVersionEnvelope['history_serial'], '9007199254740993');
    expect(futureVersionEnvelope['delivery_serial'], '9007199254740994');
    expect(extrasFrame.extras?.ai, <String, Object?>{
      'transport': <String, Object?>{
        'turn-id': 'turn-1',
        'status': 'streaming',
      },
      'codec': <String, Object?>{
        'provider-future-key': 'opaque',
        'x-custom': 'opaque',
      },
    });
    expect(extrasFrame.extras?['futureExtrasField'], isTrue);
  });

  test('preserves large messagepack serials and raw extras', () {
    final encoded = SockudoProtocolCodec.encodeEnvelope(<String, Object?>{
      'event': 'sockudo:test',
      'channel': 'private-ai-forward',
      'data': 'content',
      'stream_id': 'stream-1',
      'serial': 9007199254740993,
      'extras': <String, Object?>{
        'headers': <String, Object?>{
          'sockudo_history_serial': 9007199254740994,
        },
        'ai': <String, Object?>{
          'transport': <String, Object?>{'turn-id': 'turn-1'},
        },
      },
    }, SockudoWireFormat.messagepack);

    final decoded = SockudoProtocolCodec.decodeEvent(
      encoded,
      SockudoWireFormat.messagepack,
    );

    expect(decoded.serial, '9007199254740993');
    expect(
      decoded.extras?.headers?['sockudo_history_serial'],
      '9007199254740994',
    );
    expect(decoded.extras?.ai, <String, Object?>{
      'transport': <String, Object?>{'turn-id': 'turn-1'},
    });
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
        'ai': <String, Object?>{
          'transport': <String, Object?>{
            'turn-id': 'turn-1',
            'status': 'streaming',
          },
          'codec': <String, Object?>{'x-custom': 'opaque'},
        },
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
    expect(decoded.extras?.ai, <String, Object?>{
      'transport': <String, String>{'turn-id': 'turn-1', 'status': 'streaming'},
      'codec': <String, String>{'x-custom': 'opaque'},
    });
  });

  test('unknown mutable message actions are ignored by helpers', () {
    final event = SockudoProtocolCodec.decodeEvent(
      jsonEncode(<String, Object?>{
        'event': 'sockudo:message.future',
        'channel': 'private-ai-forward',
        'data': 'opaque',
        'extras': <String, Object?>{
          'headers': <String, Object?>{
            'sockudo_action': 'message.future',
            'sockudo_message_serial': 'future-message',
            'sockudo_history_serial': 9007199254740993,
          },
        },
      }),
      SockudoWireFormat.json,
    );

    expect(isMutableMessageEvent(event), isFalse);
    expect(getMutableMessageInfo(event), isNull);
  });

  test(
    'replays forward-compatible realtime fixtures through dispatch',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() async {
        await server.close(force: true);
      });

      unawaited(() async {
        final request = await server.first;
        final socket = await WebSocketTransformer.upgrade(request);
        socket.add(
          jsonEncode(<String, Object?>{
            'event': 'sockudo:connection_established',
            'data': <String, Object?>{'socket_id': '1.1'},
          }),
        );
        await Future<void>.delayed(const Duration(milliseconds: 50));
        for (final fixtureName in <String>[
          'future-v2-frame.json',
          'future-versioned-action.json',
          'future-webhook-events.json',
          'unknown-ai-extras.json',
        ]) {
          socket.add(_forwardCompatFixture(fixtureName));
        }
      }());

      final received = <String, Object?>{};
      final errors = <Object?>[];
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
        ),
      );
      addTearDown(client.close);

      client.bindGlobal((eventName, data) {
        received[eventName] = data;
      });
      client.bind('error', (data, _) {
        errors.add(data);
      });

      client.connect();

      await _waitFor(() => received.containsKey('sockudo:future_event'));
      await _waitFor(() => received.containsKey('sockudo:message.future'));
      await _waitFor(() => received.containsKey('ai-output'));

      expect(received['sockudo:future_event'], <String, Object?>{
        'known': true,
        'futureObject': <String, Object?>{'nested': 'ignored'},
      });
      expect(received['sockudo:message.future'], 'opaque');
      expect(received['ai-output'], 'content');
      expect(errors, isEmpty);
    },
  );

  test(
    'future presence events and malformed member data do not mutate members',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() async {
        await server.close(force: true);
      });

      unawaited(() async {
        final request = await server.first;
        final socket = await WebSocketTransformer.upgrade(request);
        socket.add(
          jsonEncode(<String, Object?>{
            'event': 'sockudo:connection_established',
            'data': <String, Object?>{'socket_id': '1.1'},
          }),
        );
        await Future<void>.delayed(const Duration(milliseconds: 50));
        for (final frame in <Map<String, Object?>>[
          <String, Object?>{
            'event': 'sockudo_internal:subscription_succeeded',
            'channel': 'presence-ai-forward',
            'data': <String, Object?>{
              'presence': <String, Object?>{
                'hash': <String, Object?>{
                  'user-1': <String, Object?>{'role': 'reader'},
                },
                'count': 1,
              },
            },
          },
          <String, Object?>{
            'event': 'sockudo_internal:member_updated',
            'channel': 'presence-ai-forward',
            'data': <String, Object?>{
              'user_id': 'user-1',
              'user_info': <String, Object?>{'role': 'writer'},
            },
          },
          <String, Object?>{
            'event': 'sockudo_internal:member_added',
            'channel': 'presence-ai-forward',
            'data': <String, Object?>{
              'user_info': <String, Object?>{'malformed': true},
            },
          },
        ]) {
          socket.add(jsonEncode(frame));
        }
      }());

      final internalEvents = <String>[];
      final addedMembers = <PresenceMember>[];
      final errors = <Object?>[];
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
          channelAuthorization: const ChannelAuthorizationOptions(
            customHandler: _presenceAuth,
          ),
        ),
      );
      addTearDown(client.close);

      final channel =
          client.subscribe('presence-ai-forward') as PresenceChannel;
      channel.bindGlobal((eventName, _) {
        internalEvents.add(eventName);
      });
      channel.bind('sockudo:member_added', (data, _) {
        addedMembers.add(data as PresenceMember);
      });
      client.bind('error', (data, _) {
        errors.add(data);
      });

      client.connect();

      await _waitFor(
        () => internalEvents.contains('sockudo_internal:member_updated'),
      );

      expect(channel.members.count, 1);
      expect(channel.members.member('user-1')?.info, <String, Object?>{
        'role': 'reader',
      });
      expect(channel.members.member('undefined'), isNull);
      expect(addedMembers, isEmpty);
      expect(errors, isEmpty);
    },
  );

  test('presence update sends v2 frame and updates member state', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() async {
      await server.close(force: true);
    });

    final presenceUpdateFrameBox = ValueBox<Map<Object?, Object?>>();
    unawaited(() async {
      final request = await server.first;
      final socket = await WebSocketTransformer.upgrade(request);
      socket.listen((raw) {
        final frame = jsonDecode(raw as String) as Map<Object?, Object?>;
        if (frame['event'] == 'sockudo:subscribe') {
          socket.add(
            jsonEncode(<String, Object?>{
              'event': 'sockudo_internal:subscription_succeeded',
              'channel': 'presence-agent',
              'data': <String, Object?>{
                'attach_serial': 9007199254740993,
                'presence': <String, Object?>{
                  'hash': <String, Object?>{
                    'user-1': <String, Object?>{'status': 'idle'},
                  },
                  'count': 1,
                },
              },
            }),
          );
        }
        if (frame['event'] == 'sockudo:presence_update') {
          presenceUpdateFrameBox.value = frame;
          socket.add(
            jsonEncode(<String, Object?>{
              'event': 'sockudo_internal:presence_update',
              'channel': 'presence-agent',
              'data': <String, Object?>{
                'user_id': 'user-1',
                'user_info': <String, Object?>{'status': 'thinking'},
              },
            }),
          );
        }
      });
      socket.add(
        jsonEncode(<String, Object?>{
          'event': 'sockudo:connection_established',
          'data': <String, Object?>{'socket_id': '1.1'},
        }),
      );
    }());

    final subscribedBox = ValueBox<bool>();
    final updateBox = ValueBox<PresenceMember>();
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
        channelAuthorization: const ChannelAuthorizationOptions(
          customHandler: _presenceAuth,
        ),
      ),
    );
    addTearDown(client.close);

    final channel = client.subscribe('presence-agent') as PresenceChannel;
    channel.bind('sockudo:subscription_succeeded', (_, _) {
      subscribedBox.value = true;
    });
    channel.bind('sockudo:presence_update', (data, _) {
      updateBox.value = data as PresenceMember;
    });

    client.connect();
    await _waitFor(() => subscribedBox.value == true);
    expect(channel.attachSerial, '9007199254740993');
    expect(channel.members.member('user-1')?.info, <String, Object?>{
      'status': 'idle',
    });

    expect(channel.update(<String, Object?>{'status': 'thinking'}), isTrue);

    final updateFrame = await _waitForValue(() => presenceUpdateFrameBox.value);
    expect(updateFrame['event'], 'sockudo:presence_update');
    expect(updateFrame['data'], <String, Object?>{
      'channel': 'presence-agent',
      'data': <String, Object?>{'status': 'thinking'},
    });

    final updatedMember = await _waitForValue(() => updateBox.value);
    expect(updatedMember.id, 'user-1');
    expect(updatedMember.info, <String, Object?>{'status': 'thinking'});
    expect(channel.members.member('user-1')?.info, <String, Object?>{
      'status': 'thinking',
    });
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
      sodiumInstance = await sodium.SodiumInit.init();
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

String _forwardCompatFixture(String name) {
  return File(
    '../../tests/ai-conformance/fixtures/forward-compat/$name',
  ).readAsStringSync();
}

Future<ChannelAuthorizationData> _presenceAuth(
  ChannelAuthorizationRequest request,
) async {
  return ChannelAuthorizationData(
    auth: 'test-auth',
    channelData: jsonEncode(<String, Object?>{'user_id': 'user-1'}),
  );
}

String _fakeJwt({double? iat, double? exp}) {
  String encodePart(Object value) {
    return base64Url.encode(utf8.encode(jsonEncode(value))).replaceAll('=', '');
  }

  return [
    encodePart(<String, Object?>{'alg': 'none', 'typ': 'JWT'}),
    encodePart(<String, Object?>{'iat': ?iat, 'exp': ?exp}),
    'signature',
  ].join('.');
}

String _rawSocketUrl(int protocolVersion) {
  final parameters = <String, String>{
    'protocol': '$protocolVersion',
    'client': 'flutter-live',
    'version': '2.1.0',
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
