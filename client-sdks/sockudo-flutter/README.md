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
- Protocol V2 capability-token initial auth and reactive refresh
- Continuity-aware connection recovery and subscribe-time rewind on Protocol V2
- Presence member updates for agent state channels
- Proxy-backed channel history and mutable-message write helpers
- AI extras and unsafe serial preservation for V2 frames
- Root GitHub Actions CI and automated `pub.dev` publishing
- Live integration tests against Sockudo on `127.0.0.1:6001`

## Installation

For apps, install the published package:

```bash
dart pub add sockudo_flutter
# or, in Flutter apps:
flutter pub add sockudo_flutter
```

For local monorepo development, use a local path dependency:

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

Protocol V2 capability tokens:

```dart
final client = SockudoClient(
  'app-key',
  SockudoOptions(
    cluster: 'local',
    protocolVersion: 2,
    token: 'initial-token',
    authCallback: () async => fetchFreshSockudoToken(),
  ),
);
```

`token` is sent as the initial WebSocket query parameter. When `authCallback` is available and the current token looks like a JWT with an `exp` claim, the client decodes the payload without validating the signature and schedules refresh at 80% of the token lifetime. If `iat` is absent, the lifetime starts at the local decode time. Opaque tokens, JWTs without `exp`, and static-only `token` usage do not schedule proactive refreshes.

When the server emits `sockudo:token_expired` with code `40142`, `authCallback` is called and the client sends `sockudo:auth` with the fresh token. Revocation code `40160` is emitted to listeners and left to the server close path.

AI append rollup can be requested for Protocol V2 connections with `appendRollupWindow`; accepted values are `0`, `20`, `40`, `100`, and `500`.

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

final history = await channel.channelHistory(
  const ChannelHistoryParams(untilAttach: true, limit: 100),
);
print(channel.attachSerial);

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

Proxy-backed writes:

```dart
final channel = client.subscribe('chat:room-1');

final created = await channel.createMessage(
  const VersionedMessageCreateRequest(data: 'hello'),
);
await channel.appendMessage(created.messageSerial, ' world');
await channel.updateMessage(
  created.messageSerial,
  const VersionedMessageMutation(data: {'text': 'hello world'}),
);
await channel.deleteMessage(created.messageSerial);
```

These helpers call your configured `versionedMessages.endpoint` with `publish_create`, `message_append`, `message_update`, or `message_delete` and expect a 10 second acknowledgement containing `messageSerial` and `historySerial`.

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

### Presence Updates

Protocol V2 presence channels can update member data without a leave/rejoin cycle.

```dart
final channel = client.subscribe('presence-agent') as PresenceChannel;
channel.bind('sockudo:subscription_succeeded', (_, __) {
  channel.update(<String, Object?>{'status': 'thinking'});
});
channel.bind('sockudo:presence_update', (member, _) {
  print((member as PresenceMember).info);
});
```

Inbound `sockudo_internal:presence_update` updates `channel.members` and emits `sockudo:presence_update` with the updated `PresenceMember`.

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

GitHub Actions are managed from the monorepo root:

- CI: `.github/workflows/sdk-ci.yml`
- Publish: `.github/workflows/sdk-release.yml` with tag `client-flutter-vX.Y.Z`
- Setup: see `docs/sdk-publishing-2026.md` for pub.dev automated publishing.

## Status

The package now covers the core Sockudo client feature set, including pure-Dart VCDIFF decoding and encrypted channel handling, and is suitable for publishing as the official Flutter SDK.

On Dart web, serial values that arrive as JSON numbers may already have been rounded by the JavaScript runtime before Dart sees them. The SDK preserves unsafe serials as strings when decoding raw JSON, MessagePack, protobuf, and proxy responses where the original literal is still available. Prefer string serials in app-level web APIs when values may exceed `2^53 - 1`.
