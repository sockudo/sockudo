# sockudo_flutter

Official Flutter and Dart client for Sockudo.

`sockudo_flutter` is a Pusher-compatible realtime client for Flutter applications and pure Dart runtimes. It keeps the subscribe/bind/channel model familiar to existing Pusher users while exposing Sockudo-native features such as filter subscriptions, delta reconstruction, and encrypted channels.

## Features

- WebSocket-backed `SockudoClient`
- Public, private, presence, and encrypted channels
- Proxy-backed presence history and presence snapshot helpers
- Channel authorization and user authentication
- Filter-aware subscriptions
- Fossil and Xdelta3/VCDIFF delta reconstruction in pure Dart
- Encrypted channel payload decryption with libsodium
- User sign-in and watchlist event handling
- Continuity-aware connection recovery and subscribe-time rewind on Protocol V2
- CI validation and planned `pub.dev` publishing workflow
- Live integration tests against Sockudo on `127.0.0.1:6001`

## Installation

Clone the Sockudo monorepo and use a local path dependency until the package is published on
`pub.dev`:

```yaml
dependencies:
  sockudo_flutter:
    path: ../sockudo/client-sdks/sockudo-flutter
```

Then import it:

```dart
import 'package:sockudo_flutter/sockudo_flutter.dart';
```

## Quick Start

```dart
import 'package:sockudo_flutter/sockudo_flutter.dart';

final client = SockudoClient(
  'app-key',
  const SockudoOptions(
    cluster: 'local',
    forceTls: false,
    enabledTransports: <SockudoTransport>[SockudoTransport.ws],
    wsHost: '127.0.0.1',
    wsPort: 6001,
    wssPort: 6001,
  ),
);

final channel = client.subscribe('public-updates');
channel.bind('price-updated', (data, _) {
  print(data);
});

client.connect();
```

The default client mode is Protocol V1 compatibility (`protocol=7`). Opt into Protocol V2 explicitly when you want Sockudo-native event prefixes and V2-only features.

```dart
final v2Client = SockudoClient(
  'app-key',
  const SockudoOptions(
    cluster: 'local',
    forceTls: false,
    wsHost: '127.0.0.1',
    wsPort: 6001,
    protocolVersion: 2,
  ),
);
```

Protocol V2 heartbeat behavior:

- Sockudo servers use native WebSocket ping/pong frames for automatic heartbeat traffic
- Flutter/Dart runtimes may still use lightweight `sockudo:ping` / `sockudo:pong` fallback messages for client-side activity checks where native ping APIs are not exposed consistently
- fallback heartbeat messages are intentionally excluded from V2 recovery metadata such as `message_id`, `serial`, and `stream_id`

## Advanced Usage

### Channel Authorization

```dart
final client = SockudoClient(
  'app-key',
  SockudoOptions(
    cluster: 'local',
    forceTls: false,
    wsHost: '127.0.0.1',
    wsPort: 6001,
    channelAuthorization: ChannelAuthorizationOptions(
      customHandler: (request) async {
        return const ChannelAuthorizationData(
          auth: 'signed-auth-token',
          channelData: '{"user_id":"42"}',
        );
      },
    ),
  ),
);
```

### Filters and Delta Compression

```dart
final channel = client.subscribe(
  'price:btc',
  options: const SubscriptionOptions(
    filter: FilterNode(key: 'market', cmp: 'eq', val: 'spot'),
    delta: ChannelDeltaSettings(
      enabled: true,
      algorithm: DeltaAlgorithm.xdelta3,
    ),
  ),
);
```

### Recovery And Rewind

```dart
final client = SockudoClient(
  'app-key',
  const SockudoOptions(
    cluster: 'local',
    protocolVersion: 2,
    forceTls: false,
    wsHost: '127.0.0.1',
    wsPort: 6001,
    connectionRecovery: true,
  ),
);

final channel = client.subscribe(
  'market:BTC',
  options: const SubscriptionOptions(
    rewind: SubscriptionRewind.seconds(30),
  ),
);

channel.bind('message', (_, __) {
  print(client.getRecoveryPosition('market:BTC'));
});

client.bind('sockudo:resume_success', (data, _) {
  print(data);
});

channel.bind('sockudo:rewind_complete', (data, _) {
  print(data);
});
```

### Mutable Messages (Release 4.3)

Protocol V2 mutable messages use:

- `sockudo:message.update`
- `sockudo:message.delete`
- `sockudo:message.append`

Client rule:

- `message.update` replaces local content with the full event payload
- `message.delete` is the latest visible version and may carry `null` data
- `message.append` concatenates onto the current local string state

If you receive `message.append` before you have a string base, fetch the latest visible message first and seed local state before applying more appends.

For historical inspection, use:

- `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}` for the latest visible version
- `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}/versions` for preserved versions in `version_serial` order

```dart
import 'package:sockudo_flutter/sockudo_flutter.dart';

final client = SockudoClient(
  'app-key',
  const SockudoOptions(
    cluster: 'local',
    forceTls: false,
    wsHost: '127.0.0.1',
    wsPort: 6001,
    protocolVersion: 2,
  ),
);

MutableMessageState? state;

final channel = client.subscribe('chat:room-1');
channel.bindGlobal((eventName, data) {
  if (data is! SockudoEvent || !isMutableMessageEvent(data)) return;
  try {
    state = reduceMutableMessageEvent(state, data);
    print('${state?.messageSerial} ${state?.action} ${state?.data}');
  } catch (e) {
    print('mutable message reduction failed: $e');
  }
});

client.connect();
```

### Presence History

Client-side presence history is proxy-backed. The Flutter/Dart client does not sign the server REST API directly; configure `presenceHistory` with a backend endpoint that accepts `{channel, params, action}` and forwards the request using server credentials.

```dart
final client = SockudoClient(
  'app-key',
  SockudoOptions(
    cluster: 'local',
    forceTls: false,
    wsHost: '127.0.0.1',
    wsPort: 6001,
    presenceHistory: const PresenceHistoryOptions(
      endpoint: 'https://api.example.com/sockudo/presence-history',
    ),
  ),
);

final channel = client.subscribe('presence-lobby') as PresenceChannel;
final page = await channel.history(
  const PresenceHistoryParams(limit: 50, direction: 'newest_first'),
);
if (page.hasNext()) {
  final nextPage = await page.next();
}

final snapshot = await channel.snapshot(
  const PresenceSnapshotParams(atSerial: 4),
);
```

### Push Proxy Helpers

Push registration and publish helpers are HTTP/proxy surfaces. Keep Sockudo app secrets on your backend and point the client helper at your own proxy/admin endpoint.

- `publish()` and `publishBatch()` always send `sync: false`
- publish calls should expect `202 Accepted` plus a `publish_id`
- list helpers use `limit` and `cursor` query parameters

```dart
import 'package:sockudo_flutter/sockudo_flutter.dart';

final push = SockudoPushRegistration(
  const PushRegistrationOptions(
    endpoint: 'https://api.example.com/sockudo/push',
    headers: <String, String>{'Authorization': 'Bearer session-token'},
  ),
);

final publish = await push.publish(<String, Object?>{
  'recipients': <Map<String, Object?>>[
    <String, Object?>{'type': 'channel', 'channel': 'orders'},
  ],
  'payload': <String, Object?>{
    'title': 'Order updated',
    'body': 'Ready for pickup',
  },
});
print(publish['publish_id']);

final page = await push.listChannelSubscriptions(
  const PushSubscriptionParams(deviceId: 'device-1', limit: 20),
);
print(page['next_cursor']);
```

### Encrypted Channels

`private-encrypted-*` channels use the `sharedSecret` returned by your auth endpoint or custom handler. Payload decryption is handled automatically.

## Testing

Static checks:

```bash
flutter analyze
flutter test
```

Live integration tests against a local Sockudo server on port `6001`:

```bash
SOCKUDO_LIVE_TESTS=1 flutter test
```

The live suite covers:

- public subscribe + publish round-trip
- delta-enabled delivery
- encrypted channel decryption

## Publishing

Dry-run publish validation:

```bash
flutter pub publish --dry-run
```

Publish:

```bash
flutter pub publish
```

GitHub Actions:

- CI: `.github/workflows/ci.yml`
- Publish: `.github/workflows/publish.yml`

## Status

The package now covers the core Sockudo client feature set, including pure-Dart VCDIFF decoding and encrypted channel handling, and is suitable for publishing as the official Flutter SDK.
