# SDK Ecosystem Audit And Compatibility Baseline

Generated: 2026-06-27 11:46:02 +03:00.

Baseline server SHA: `1d3f871a911487ffffef5057dd83be3d63a56418`.

Important qualifier: the checkout was dirty before this audit began. The baseline below records the
observed local workspace at that SHA, not a pristine main-branch checkout. The original E0 audit did
not change SDK production code; this document now also records the later E2 tolerant-reader fixes and
E3 server HTTP SDK hardening.

Machine-readable artifact: `docs/specs/sdk-ecosystem-baseline.json`.

## Summary

- The in-tree ecosystem is 7 client packages and 9 server HTTP SDKs, not 5 client SDKs.
- No SDK directories are git submodules in this checkout. They are imported package directories
  governed by root workflows such as `.github/workflows/sdk-ci.yml`.
- Baseline suite status: 12 green suites, 1 repository-level red suite, 3 local/tooling-blocked
  suites, 1 package-manager policy caveat, and 1 third-party V1 canary failure.
- Filed issues:
  - https://github.com/sockudo/sockudo/issues/297 for the PHP server SDK PHPUnit warning exit.
  - https://github.com/sockudo/sockudo/issues/298 for vanilla Pusher publish-to-deliver canary
    failure.
- The highest-risk realtime additive-change breakpoints were handled in E2. The highest-risk server
  HTTP webhook parser breakpoints were handled in E3 for Node types, Python, .NET, Go, Java, Rust,
  and Swift; PHP/Ruby were already dynamic and now have fixture coverage.
- E4 client platform-primitive enablement is now landed for `client-sdks/sockudo-dotnet`,
  `client-sdks/sockudo-flutter`, `client-sdks/sockudo-kotlin`, `client-sdks/sockudo-python`,
  and `client-sdks/sockudo-swift`. The remaining client-side reference dependency is
  `client-sdks/sockudo-js`, whose full P1 primitive port is still not present in this checkout.

## Inventory

Published versions were checked against the public registries on 2026-06-27. This release-prep
branch targets SDK `2.1.0` across local package metadata; registry publishes will make those target
versions current. CI setup is rooted in `.github/workflows/sdk-ci.yml`, with release lanes in
`.github/workflows/sdk-release.yml`.

### Client Packages

| SDK | Package id and release target | Local metadata | Toolchain and runtime targets | Repo and CI status | Baseline commands |
|---|---|---|---|---|---|
| `client-sdks/sockudo-js` | npm `@sockudo/client` 2.1.0 | `client-sdks/sockudo-js/package.json:2`, `client-sdks/sockudo-js/package.json:3` | TypeScript, Bun, Node `>=22`, browser, Node, worker, React, Vue, React Native, NativeScript | Imported directory, no submodule; root CI matrix at `.github/workflows/sdk-ci.yml:83` | `bun install --frozen-lockfile`; `bun run lint`; `bun run build`; `bun run typecheck`; `bun test` |
| `client-sdks/sockudo-ai-transport-js` | npm `@sockudo/ai-transport` 2.1.0 | `client-sdks/sockudo-ai-transport-js/package.json:2`, `client-sdks/sockudo-ai-transport-js/package.json:3` | TypeScript, pnpm 10.17.0, Node `>=20`, framework subpaths for React, Vue, Svelte, Vercel AI SDK | Imported directory, no submodule; root CI matrix at `.github/workflows/sdk-ci.yml:93` | `pnpm lint`; `pnpm typecheck`; `pnpm test`; `pnpm build`; release gate in package scripts |
| `client-sdks/sockudo-python` | PyPI `sockudo-python` 2.1.0 | `client-sdks/sockudo-python/pyproject.toml:6`, `client-sdks/sockudo-python/pyproject.toml:10` | Python `>=3.10`, asyncio/websockets, httpx | Imported directory, no submodule; root CI matrix at `.github/workflows/sdk-ci.yml:174` | `python -m pip install -e .[dev]`; `ruff check .`; `python -m pytest` |
| `client-sdks/sockudo-dotnet` | NuGet `Sockudo.Client` 2.1.0 | `client-sdks/sockudo-dotnet/src/Sockudo.Client/Sockudo.Client.csproj` | .NET, browser/server capable WebSocket client | Imported directory, no submodule; root CI matrix at `.github/workflows/sdk-ci.yml:245` | `dotnet restore`; `dotnet build --configuration Release`; `dotnet test tests/Sockudo.Client.Tests/Sockudo.Client.Tests.csproj --configuration Release` |
| `client-sdks/sockudo-flutter` | pub.dev `sockudo_flutter` 2.1.0 | `client-sdks/sockudo-flutter/pubspec.yaml:1`, `client-sdks/sockudo-flutter/pubspec.yaml:3` | Dart/Flutter, Android, iOS, web, desktop depending on Flutter targets | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:217` | `flutter pub get`; `dart format --set-exit-if-changed .`; `flutter analyze`; `flutter test` |
| `client-sdks/sockudo-kotlin` | Maven `io.sockudo:sockudo-kotlin` 2.1.0 | `client-sdks/sockudo-kotlin/lib/build.gradle.kts:11`, `client-sdks/sockudo-kotlin/lib/build.gradle.kts:12` | Kotlin/JVM, OkHttp/Kotlinx serialization | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:314` | `./gradlew --no-daemon :lib:check`; `./gradlew --no-daemon :lib:test` |
| `client-sdks/sockudo-swift` | SwiftPM `SockudoSwift`, tag v2.1.0 | `client-sdks/sockudo-swift/Package.swift:1`, `client-sdks/sockudo-swift/Package.swift:7` | Swift 6.2 package, iOS 13, macOS 10.15, tvOS 13, watchOS 6, visionOS 1 | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:348` | `swift format lint --recursive Sources Tests`; `swiftlint lint --strict`; `swift test` |

### Server HTTP SDKs

| SDK | Package id and release target | Local metadata | Toolchain and runtime targets | Repo and CI status | Baseline commands |
|---|---|---|---|---|---|
| `server-sdks/sockudo-http-node` | npm `sockudo` 2.1.0 | `server-sdks/sockudo-http-node/package.json:2`, `server-sdks/sockudo-http-node/package.json:3` | TypeScript/Node HTTP server SDK | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:75` | `npm install`; `npm run local-test` |
| `server-sdks/sockudo-http-python` | PyPI `sockudo-http-python` 2.1.0 | `server-sdks/sockudo-http-python/pyproject.toml:6`, `server-sdks/sockudo-http-python/pyproject.toml:10` | Python `>=3.9`, httpx/PyNaCl | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:166` | `python -m pip install -e .[dev]`; `ruff check .`; `python -m pytest` |
| `server-sdks/sockudo-http-dotnet` | NuGet `SockudoServer` 2.1.0 | `server-sdks/sockudo-http-dotnet/SockudoServer/SockudoServer.csproj` | .NET server SDK, plus Pusher-compatible solution | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:236` | `dotnet test sockudo-dotnet-server.sln`; `dotnet test pusher-dotnet-server.sln` |
| `server-sdks/sockudo-http-go` | Go module `github.com/sockudo/sockudo/server-sdks/sockudo-http-go/v2` v2.1.0 | `server-sdks/sockudo-http-go/go.mod:1`, `server-sdks/sockudo-http-go/go.mod:3` | Go HTTP server SDK | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:425` | `go test ./...` |
| `server-sdks/sockudo-http-java` | Maven `io.sockudo:sockudo-http-java` 2.1.0 | `server-sdks/sockudo-http-java/build.gradle:26`, `server-sdks/sockudo-http-java/build.gradle:94` | Java 17-compatible library | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:306` | `./gradlew --no-daemon test` |
| `server-sdks/sockudo-http-php` | Packagist `sockudo/sockudo-php-server` v2.1.0 | `server-sdks/sockudo-http-php/composer.json:2` | PHP `^8.2` HTTP server SDK | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:370` | `composer install --no-interaction --prefer-dist`; `vendor/bin/phpunit` |
| `server-sdks/sockudo-http-ruby` | RubyGems `sockudo` 2.1.0 | `server-sdks/sockudo-http-ruby/sockudo.gemspec:6`, `server-sdks/sockudo-http-ruby/sockudo.gemspec:16` | Ruby HTTP server SDK | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:398` | `bundle install`; `bundle exec rake spec` |
| `server-sdks/sockudo-http-rust` | crates.io `sockudo-http` 2.1.0 | `server-sdks/sockudo-http-rust/Cargo.toml:2`, `server-sdks/sockudo-http-rust/Cargo.toml:3` | Rust HTTP server SDK | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:449` | `cargo test --all-features` |
| `server-sdks/sockudo-http-swift` | SwiftPM `Sockudo`, tag v2.1.0 | `server-sdks/sockudo-http-swift/Package.swift:1`, `server-sdks/sockudo-http-swift/Package.swift:10` | Swift server SDK | Imported directory, no submodule; root CI at `.github/workflows/sdk-ci.yml:340` | `swift format lint --recursive Sources Tests`; `swiftlint lint --strict`; `swift test` |

## Client Protocol Coverage

| Client SDK | Implemented protocol surface | Missing or risky for U1-U7 |
|---|---|---|
| `@sockudo/client` | V1/V2 prefix handling in `client-sdks/sockudo-js/src/core/protocol_prefix.ts`; connection options in `client-sdks/sockudo-js/src/core/options.ts:26`; channel history params/page in `client-sdks/sockudo-js/src/core/channels/channel.ts:30` and `client-sdks/sockudo-js/src/core/channels/channel.ts:247`; consume-side mutable-message reducer in `client-sdks/sockudo-js/src/core/versioned_messages.ts:44`; presence add/remove in `client-sdks/sockudo-js/src/core/channels/presence_channel.ts:101`; E2 tolerance hardening in `client-sdks/sockudo-js/src/core/connection/protocol/protocol.ts`, `client-sdks/sockudo-js/src/core/serial.ts`, `client-sdks/sockudo-js/src/core/channels/channel.ts`, `client-sdks/sockudo-js/src/core/channels/presence_channel.ts`, `client-sdks/sockudo-js/src/core/channels/members.ts`, and `client-sdks/sockudo-js/test/forward-compat.test.ts`; push proxy support exists under package subpaths. | No capability-token connection auth, no re-auth frame, no `append_rollup_window`, no `until_attach`/`attach_serial`, no publish-side mutation ack API. E2 has resolved realtime reader tolerance for `extras.ai`, unknown extras in JSON/msgpack, unknown internal events, malformed presence member events, and u64 serial preservation; U1-U5 remain gated until server features land. |
| `@sockudo/ai-transport` | Adapter package, not a socket client. It models `Serial = number \| string`, `MessageAck`, `AppendRollupWindow`, `untilAttach`, `attachSerial`, presence update hooks, and mutation methods in `client-sdks/sockudo-ai-transport-js/src/realtime/types.ts:5` and `client-sdks/sockudo-ai-transport-js/src/realtime/adapter.ts:224`. E2 adapter tolerance is in `client-sdks/sockudo-ai-transport-js/src/realtime/adapter.ts`, `client-sdks/sockudo-ai-transport-js/src/core/transport/client-transport.ts`, `client-sdks/sockudo-ai-transport-js/src/realtime/adapter.test.ts`, and `client-sdks/sockudo-ai-transport-js/src/core/transport/client-transport.test.ts`: shared E1 fixture replay, `extras.ai` passthrough, event-less frame drops, future internal mutable actions as no-op summaries, malformed presence member skipping, and BigInt/string serial comparison. | Depends on underlying `@sockudo/client` and server methods that are not yet present for U1-U5. E2 adapter-level tolerance is complete after `@sockudo/client` E2; this is not independent socket-decoder coverage and does not make U1-U5 shippable. Normal `pnpm` still hits the active minimum-release-age policy for fresh AI SDK 7 dependencies unless the lockfile is explicitly trusted or the packages age past policy. |
| `Sockudo.Client` (.NET) | V1/V2, channel auth, user auth, recovery, delta, presence, presence history, channel history, annotations, push proxy, consume-side mutable messages, E2 tolerant-reader hardening, and E4 primitives. Evidence: options and params in `client-sdks/sockudo-dotnet/src/Sockudo.Client/Models.cs`, protocol decode in `client-sdks/sockudo-dotnet/src/Sockudo.Client/ProtocolCodec.cs`, dispatch/auth/rollup/history/presence in `client-sdks/sockudo-dotnet/src/Sockudo.Client/SockudoClient.cs`, mutable helpers in `client-sdks/sockudo-dotnet/src/Sockudo.Client/VersionedMessages.cs`, and tests in `client-sdks/sockudo-dotnet/tests/Sockudo.Client.Tests/ProtocolTests.cs`. | E4 added capability-token URL auth/provider refresh with 80% JWT scheduling, `sockudo:auth`, `sockudo:token_expired`, presence update, attach serial, `until_attach`, rollup validation, and proxy-backed mutation helpers. Waivers: opaque/static tokens are reactive/manual only, and U4 does not add unsupported websocket mutation frames. |
| `sockudo_flutter` | V1/V2, channel auth, recovery, rewind, delta, tag filter, presence add/remove, presence history, channel history, annotations, push proxy, consume-side mutable-message helpers, E2 tolerant-reader hardening, and E4 primitives. Evidence: options/auth/dispatch in `client-sdks/sockudo-flutter/lib/src/client.dart`, auth types in `client-sdks/sockudo-flutter/lib/src/auth.dart`, models in `client-sdks/sockudo-flutter/lib/src/models.dart`, protocol decode in `client-sdks/sockudo-flutter/lib/src/protocol_codec.dart`, protobuf schema in `client-sdks/sockudo-flutter/proto/wire.proto`, and tests in `client-sdks/sockudo-flutter/test/sockudo_flutter_test.dart`. | E4 added token/authCallback auth with 80% JWT scheduling, `sockudo:auth`, presence update, attach serial, `until_attach`, rollup validation, and proxy-backed mutation helpers. Waivers: opaque/static tokens are reactive/manual only, `40160` revocation is surfaced and left to the server close path, and U4 does not add unsupported websocket mutation frames. |
| `sockudo-kotlin` | V1/V2, channel auth, recovery, rewind, delta, tag filter, presence add/remove, presence history, channel history, annotations, push-related helpers, consume-side mutable-message helpers, E2 tolerant-reader hardening, and E4 primitives. Evidence: auth in `client-sdks/sockudo-kotlin/lib/src/main/kotlin/io/sockudo/client/Auth.kt`, options in `Models.kt`, connection/auth/rollup in `SockudoClient.kt`, channel/history/presence/mutation helpers in `Channels.kt`, mutable reducer in `VersionedMessages.kt`, and tests in `client-sdks/sockudo-kotlin/lib/src/test/kotlin/io/sockudo/client/PlatformPrimitiveTest.kt` plus `ForwardCompatTest.kt`. | E4 added provider token auth with 80% JWT scheduling, `sockudo:auth`, presence update, attach serial, `until_attach`, rollup validation, and proxy-backed mutation helpers. Waivers: opaque/static tokens are reactive/manual only, and U4 does not add unsupported websocket mutation frames. |
| `sockudo-python` | V1/V2 prefix handling, channel auth/user auth, recovery, rewind, delta, tag filtering, presence add/remove, presence history/snapshot, channel history, E2 tolerant-reader hardening, and E4 primitives. Evidence: `client-sdks/sockudo-python/src/sockudo_python/client.py`, exports in `client-sdks/sockudo-python/src/sockudo_python/__init__.py`, and tests in `client-sdks/sockudo-python/tests/test_client.py`, `test_protocol.py`, and `test_forward_compat.py`. | E4 added async token/authCallback auth with 80% JWT/result scheduling, `sockudo:auth`, presence update, attach serial, `until_attach` channel history, rollup validation, and proxy-backed mutation helpers. Waivers: opaque tokens are reactive-only, and U4 does not add unsupported websocket mutation frames. |
| `SockudoSwift` | V1/V2, channel auth, recovery, delta, presence add/remove, presence history, channel history, annotations, push proxy, consume-side mutable-message helpers, E2 tolerant-reader hardening, and E4 primitives. Evidence: auth in `client-sdks/sockudo-swift/Sources/SockudoSwift/Auth.swift`, options/auth/rollup in `SockudoClient.swift`, channel/history/presence helpers in `Channels.swift`, raw extras/serial helpers in `Support.swift`, mutable helpers in `VersionedMessages.swift`, and tests in `client-sdks/sockudo-swift/Tests/SockudoSwiftTests/SockudoSwiftTests.swift`. | E4 added capability-token auth with 80% JWT scheduling, `sockudo:auth`, presence update, attach serial, `untilAttach`, rollup validation, and proxy-backed mutation helpers. Waivers: opaque/static tokens are reactive/manual only, websocket channel-history request correlation remains unimplemented, and U4 does not add unsupported websocket mutation frames. |

## Server SDK Protocol Coverage

| Server SDK | Implemented HTTP surface | Missing or risky for planned additive work |
|---|---|---|
| Node | Trigger/batch/auth/channel info/history style helpers, dynamic JSON response parsing, TypeScript types for webhook/event data in `server-sdks/sockudo-http-node/src/types.ts` and `index.d.ts`. | E3 widened public webhook typings to accept future event names/fields and optional mutation ack serial fields; runtime already preserves nested webhook JSON values. Coverage: `server-sdks/sockudo-http-node/tests/integration/webhook.js`. |
| Python | HTTP trigger/auth helpers and webhook parser in `server-sdks/sockudo-http-python/src/sockudo_http_python/client.py`. `Webhook.parse` now builds a typed projection from a raw mapping and preserves the full raw event payload. | E3 resolved strict `WebhookEvent(**event)` construction; future webhook fields and nested object/array/bool/null values are accepted and exposed through `raw`. Coverage: `server-sdks/sockudo-http-python/tests/test_client.py`. |
| .NET | Trigger/batch/auth/channel helpers and webhook validation. Webhook events now keep `RawEvents` plus a legacy string projection in both `SockudoServer` and `PusherServer`. | E3 resolved nested webhook value rejection while preserving the existing `Events` API. `GetRawEvents()` in `server-sdks/sockudo-http-dotnet/*Server/WebHookExtensions.cs` exposes future payloads without stringifying them. Coverage: `server-sdks/sockudo-http-dotnet/PusherServer.Tests/UnitTests/WebHook.cs`. |
| Go | Trigger/batch/channel/history/versioned/annotation/push/auth/webhook helpers. `server-sdks/sockudo-http-go/webhook.go` preserves raw field values and raw `data`; mutation responses include optional `history_serial` and `delivery_serial`. | E3 resolved strict non-string webhook `data` decoding and added u64-safe mutation response serial coverage. Coverage: `server-sdks/sockudo-http-go/webhook_test.go` and `response_parsing_test.go`. |
| Java | Gson writer and raw/string result style in `server-sdks/sockudo-http-java/src/main/java/io/sockudo/rest/SockudoAbstract.java:33`; trigger/batch at `server-sdks/sockudo-http-java/src/main/java/io/sockudo/rest/SockudoAbstract.java:376`; history generic params at `server-sdks/sockudo-http-java/src/main/java/io/sockudo/rest/SockudoAbstract.java:529`; tolerant webhook parser in `server-sdks/sockudo-http-java/src/main/java/io/sockudo/rest/data/Webhook.java` and `server-sdks/sockudo-http-java/src/main/java/io/sockudo/rest/data/WebhookEvent.java`. | E3 complete: `parseWebhook` preserves unknown event names, future fields, nested object/array/bool/null JSON values, and serial-like fields as strings/raw Gson values. Responses remain raw `Result` strings, so additive response fields are ignored. Coverage: `server-sdks/sockudo-http-java/src/test/java/io/sockudo/rest/WebhookTest.java`. |
| PHP | Trigger/batch/channel/webhook helpers. Webhook uses dynamic `json_decode(..., false, JSON_THROW_ON_ERROR)` and `stdClass` in `server-sdks/sockudo-http-php/src/Sockudo.php:780`; wrapper is dynamic at `server-sdks/sockudo-http-php/src/Webhook.php:5`. | Parser already tolerant; E3 added shared fixture and nested-value replay in `server-sdks/sockudo-http-php/tests/unit/WebhookTest.php`. Full local suite still has baseline issue #297. |
| Ruby | Dynamic trigger/channel/webhook helpers. Webhook JSON parse and event access are in `server-sdks/sockudo-http-ruby/lib/sockudo/webhook.rb:27`. | Parser already tolerant; E3 added shared fixture and nested-value replay in `server-sdks/sockudo-http-ruby/spec/web_hook_spec.rb`, but local execution remains blocked by old Ruby/Bundler. |
| Rust | Trigger/history/webhook helpers. Webhook data still exposes `Vec<HashMap<String,String>>` for compatibility, with a custom deserializer that accepts nested future values. | E3 resolved nested webhook value rejection by keeping the legacy string projection and adding raw JSON access through `Webhook::get_raw_json_events()` in `server-sdks/sockudo-http-rust/src/webhook.rs`. |
| Swift | Trigger/channel/history/webhook helpers. Webhook event types now preserve unknown raw event names and events keep `rawPayload`. | E3 resolved strict enum decode failures for `member_updated`, `ai_*`, and future webhook event names in both Sockudo and Pusher modules. Coverage: `server-sdks/sockudo-http-swift/Tests/*/WebhookTests.swift`. |

## Parser Strictness Risk Register

| SDK | Decode site | Strictness | Break risk vs planned additive changes | Required E2/E3 fix |
|---|---|---|---|---|
| `@sockudo/client` | `client-sdks/sockudo-js/src/core/connection/protocol/protocol.ts`, `client-sdks/sockudo-js/src/core/serial.ts`, `client-sdks/sockudo-js/src/core/channels/channel.ts`, `client-sdks/sockudo-js/src/core/channels/presence_channel.ts`, `client-sdks/sockudo-js/src/core/channels/members.ts`, `client-sdks/sockudo-js/src/core/versioned_messages.ts`, and `client-sdks/sockudo-js/test/forward-compat.test.ts` | Tolerant after E2: unknown event names route through channel/global binders or are dropped when not realtime frames; unknown internal events are logged and emitted without state mutation; malformed presence member payloads are ignored; JSON/msgpack/protobuf serial decoding preserves u64 values as `number \| string`; `extras.ai` and unknown JSON/msgpack extras pass through. | E2 break risk resolved for realtime reader tolerance. U1-U5 feature enablement remains absent/gated; protobuf cannot preserve future unknown fields outside its schema, but the known `ai` tier is now decoded. | E2 complete for `sockudo-js`; fixture replay is `client-sdks/sockudo-js/test/forward-compat.test.ts`. |
| `@sockudo/ai-transport` | `client-sdks/sockudo-ai-transport-js/src/realtime/adapter.ts`, `client-sdks/sockudo-ai-transport-js/src/core/transport/client-transport.ts`, `client-sdks/sockudo-ai-transport-js/src/realtime/adapter.test.ts`, and `client-sdks/sockudo-ai-transport-js/src/core/transport/client-transport.test.ts` | Tolerant after E2 adapter verification: shared E1 fixtures replay through the adapted channel handoff, `extras.ai` and future extras pass through, event-less webhook-shaped fixtures drop, future internal/mutable actions normalize to no-op summaries, malformed presence members are skipped, and serials remain `number \| string` with BigInt/string-safe comparison. | E2 adapter break risk resolved after `@sockudo/client` E2. U1-U5 feature enablement remains absent/gated and this package is not a socket decoder. | E2 complete for adapter layer; normal pnpm install/check remains subject to minimum-release-age policy for fresh AI SDK 7 lockfile entries. |
| .NET client | `client-sdks/sockudo-dotnet/src/Sockudo.Client/ProtocolCodec.cs`, `client-sdks/sockudo-dotnet/src/Sockudo.Client/Models.cs`, `client-sdks/sockudo-dotnet/src/Sockudo.Client/SockudoClient.cs`, `client-sdks/sockudo-dotnet/src/Sockudo.Client/VersionedMessages.cs`, and `client-sdks/sockudo-dotnet/tests/Sockudo.Client.Tests/ProtocolTests.cs` | Tolerant after E2 and primitive-enabled after E4: `long`/string serial decode, raw JSON/msgpack extras passthrough, known protobuf `extras.ai`, unknown realtime event/action/status tolerance, token auth, presence update, history additions, rollup, and proxy mutation helpers. | E2 reader risk resolved; E4 enablement complete under the proxy-backed mutation boundary. | E2 fixture replay and E4 primitive tests live in `ProtocolTests.cs`. |
| Flutter client | `client-sdks/sockudo-flutter/lib/src/protocol_codec.dart`, `client-sdks/sockudo-flutter/lib/src/support.dart`, `client-sdks/sockudo-flutter/lib/src/models.dart`, `client-sdks/sockudo-flutter/lib/src/client.dart`, `client-sdks/sockudo-flutter/proto/wire.proto`, and `client-sdks/sockudo-flutter/test/sockudo_flutter_test.dart` | Tolerant after E2 and primitive-enabled after E4: unsafe JSON serials preserved as strings, safe large serials remain ints, JSON/msgpack/protobuf known `extras.ai`, raw JSON/msgpack extras passthrough, token auth, presence update, history additions, rollup, and proxy mutation helpers. | E2 reader risk resolved; E4 enablement complete under the proxy-backed mutation boundary. | E2 fixture replay and E4 primitive tests live in `sockudo_flutter_test.dart`. |
| Kotlin client | `client-sdks/sockudo-kotlin/lib/src/main/kotlin/io/sockudo/client/ProtocolCodec.kt`, `client-sdks/sockudo-kotlin/lib/src/main/kotlin/io/sockudo/client/Support.kt`, `client-sdks/sockudo-kotlin/lib/src/main/kotlin/io/sockudo/client/Models.kt`, `client-sdks/sockudo-kotlin/lib/src/main/kotlin/io/sockudo/client/Channels.kt`, `client-sdks/sockudo-kotlin/lib/src/main/kotlin/io/sockudo/client/VersionedMessages.kt`, and `client-sdks/sockudo-kotlin/lib/src/test/kotlin/io/sockudo/client/ForwardCompatTest.kt` | Tolerant after E2 and primitive-enabled after E4: integer-string serial parsing avoids `Double`, raw extras passthrough, known protobuf `extras.ai`, token auth, presence update, history additions, rollup, and proxy mutation helpers. | E2 reader risk resolved; E4 enablement complete under the proxy-backed mutation boundary. | E2 fixture replay is in `ForwardCompatTest.kt`; E4 primitive tests are in `PlatformPrimitiveTest.kt`. |
| Python client | `client-sdks/sockudo-python/src/sockudo_python/client.py` and `client-sdks/sockudo-python/tests/test_forward_compat.py` | Tolerant after E2 and primitive-enabled after E4: raw extras passthrough, `extras.ai`, safe event/channel coercion, malformed/no-event frame drop, Python int/string serial decode, token auth, presence update, channel history, rollup, and proxy mutation helpers. | E2 reader risk resolved; E4 enablement complete under the proxy-backed mutation boundary. | E2 fixture replay is in `test_forward_compat.py`; E4 primitive coverage is in `test_client.py` and `test_protocol.py`. |
| Swift client | `client-sdks/sockudo-swift/Sources/SockudoSwift/ProtocolCodec.swift`, `client-sdks/sockudo-swift/Sources/SockudoSwift/Support.swift`, `client-sdks/sockudo-swift/Sources/SockudoSwift/Channels.swift`, `client-sdks/sockudo-swift/Sources/SockudoSwift/SockudoClient.swift`, `client-sdks/sockudo-swift/Sources/SockudoSwift/VersionedMessages.swift`, and `client-sdks/sockudo-swift/Tests/SockudoSwiftTests/SockudoSwiftTests.swift` | Tolerant after E2 and primitive-enabled after E4: exact full-width numeric/string serial coercion, raw extras passthrough, known protobuf/msgpack `extras.ai`, token auth, presence update, history additions, rollup, and proxy mutation helpers. | E2 reader risk resolved; E4 enablement complete under the proxy-backed mutation boundary. | E2 fixture replay and E4 primitive tests live in `SockudoSwiftTests.swift`. |
| Node server | Runtime `JSON.parse` and dynamic request/response handling; TypeScript declarations in `server-sdks/sockudo-http-node/src/types.ts` and `index.d.ts` | Tolerant runtime, widened E3 declarations | Compile-time break risk resolved for future webhook names/fields and mutation ack serials | E3 complete; shared fixture and nested object/array/bool/null replay in `tests/integration/webhook.js` |
| Python server | `server-sdks/sockudo-http-python/src/sockudo_http_python/client.py` | Tolerant after E3: typed projection plus preserved raw mapping | Strict unknown-field construction risk resolved | E3 complete; shared fixture and nested object/array/bool/null replay in `tests/test_client.py` |
| .NET server | `server-sdks/sockudo-http-dotnet/SockudoServer/WebHookData.cs` and `PusherServer/WebHookData.cs` | Tolerant after E3: raw event dictionaries plus legacy string projection | Nested webhook value rejection resolved | E3 complete; shared fixture and nested object/array/bool/null tests in `PusherServer.Tests/UnitTests/WebHook.cs` |
| Go server | `server-sdks/sockudo-http-go/webhook.go` and `response_parsing.go` | Tolerant after E3: raw event fields/data preserved; u64 serial response fields decoded | Nested webhook data and mutation response serial gaps resolved | E3 complete; shared fixture, nested object/array/bool/null, and >2^53 serial tests added |
| Java server | `server-sdks/sockudo-http-java/src/main/java/io/sockudo/rest/SockudoAbstract.java`, `server-sdks/sockudo-http-java/src/main/java/io/sockudo/rest/data/Webhook.java`, and `server-sdks/sockudo-http-java/src/main/java/io/sockudo/rest/data/WebhookEvent.java` | Tolerant after E3: raw Gson webhook body/event payloads preserved, event names are strings, and response bodies remain raw strings | Unknown event names/future fields and nested values are accepted; serial-like fields are exposed as strings/raw JSON to avoid precision loss | E3 complete; shared fixture and nested object/array/bool/null replay added in `server-sdks/sockudo-http-java/src/test/java/io/sockudo/rest/WebhookTest.java` |
| PHP server | `server-sdks/sockudo-http-php/src/Sockudo.php:780` | Tolerant dynamic `stdClass` | Low break risk; fixture coverage now guards future webhook event names/fields and nested values | E3 fixture and nested-value replay added; E5 fields/tests later |
| Ruby server | `server-sdks/sockudo-http-ruby/lib/sockudo/webhook.rb:27` | Tolerant dynamic hash | Low break risk; fixture coverage added but local execution blocked | E3 fixture and nested-value replay added; rerun when Bundler 4.0.11-capable Ruby is available |
| Rust server | `server-sdks/sockudo-http-rust/src/webhook.rs` | Tolerant after E3 custom deserializer; legacy `HashMap<String,String>` projection retained and raw JSON events exposed through `get_raw_json_events()` | Nested webhook value rejection and raw preservation gap resolved | E3 complete; shared fixture and nested object/array/bool/null replay added |
| Swift server | `server-sdks/sockudo-http-swift/Sources/*/Models/WebhookEventType.swift` | Tolerant after E3 unknown raw event case and `rawPayload` preservation | Unknown event enum decode risk resolved | E3 complete; shared fixture and nested object/array/bool/null replay added for Sockudo and Pusher modules |

## Baseline Runs

Current server was built with `cargo build -p sockudo --features ai-transport` before the V1 canary.
Full AI Transport U1-U7 integration coverage is not available yet; these are compatibility
baselines against the current build and current SDK suites.

E3 used package-local webhook/response fixture replay. The E1 two-profile SDK matrix was not rerun
for this pass because `scripts/sdk-compat-full-matrix.mjs` is all-lanes only and this checkout still
has known PHP full-suite warning failures and a Ruby Bundler blocker.

| Package | Command | Result | Notes |
|---|---|---|---|
| `client-sdks/sockudo-js` | `bun run check`; `bun run build` | Pass | E2 rerun: 13 suites passed, 47 tests passed; typecheck/lint passed with existing prefer-const warnings; web build passed. |
| `client-sdks/sockudo-ai-transport-js` | Direct local binaries: `tsc -p tsconfig.json --noEmit`; `oxlint --config .oxlintrc.json src test scripts demo/lib demo/server demo/*.ts *.ts`; `vitest run --config vitest.config.ts --coverage.enabled=false`; `vitest run --config vitest.conformance.config.ts`; `node scripts/check-doc-snippets.mjs`; `prettier --check CHANGELOG.md`; `tsc -p tsconfig.build.json`; all Vite library builds | Pass with package-manager caveat | E2 adapter rerun: 24 unit files passed, 164 tests passed; conformance passed 12 tests with 1 skipped; snippets, typecheck, lint, declaration build, and Vite subpath builds passed. Plain `pnpm exec`/install still fails the active supply-chain policy for `@ai-sdk/mcp@2.0.1` and `@ai-sdk/react@4.0.3`; local verification used direct binaries after `pnpm install --frozen-lockfile --trust-lockfile --config.auto-install-peers=false` linked dependencies, which itself exited nonzero on unapproved build scripts. |
| `client-sdks/sockudo-python` | Python 3.13 venv: `python -m pytest tests`; `python -m ruff check src tests` | Pass | E4 rerun: 40 tests passed; ruff passed. System `/usr/bin/python3` remains Python 3.9.6. |
| `client-sdks/sockudo-dotnet` | `dotnet test tests/Sockudo.Client.Tests/Sockudo.Client.Tests.csproj --configuration Release --verbosity minimal`; `dotnet format src/Sockudo.Client/Sockudo.Client.csproj --verify-no-changes` | Pass | E4 rerun: 39 tests passed; format verify passed. |
| `client-sdks/sockudo-flutter` | `dart format --set-exit-if-changed .`; `flutter analyze`; `flutter test` | Pass | E4 rerun: analyze passed; 31 tests passed. |
| `client-sdks/sockudo-kotlin` | `./gradlew --no-daemon :lib:check` | Pass | E4 rerun passed; only non-failing existing/JDK warnings. |
| `client-sdks/sockudo-swift` | `swift test`; `swift format lint --recursive Sources Tests` | Pass with warnings | E4 rerun: 31 Swift Testing tests passed. `swift format lint` exits 0 but reports existing style-warning classes; `swiftlint lint --strict` remains known red in this checkout from the E2 baseline and was not rerun for E4. |
| `server-sdks/sockudo-http-node` | `npm run local-test` | Pass | E3 rerun: 154 tests passed, including shared future-webhook fixture and nested-value replay. |
| `server-sdks/sockudo-http-python` | Python 3.13 venv: `python -m pytest tests/test_client.py`; `python -m ruff check .` | Pass | E3 rerun: 22 tests passed; ruff passed. System Python still lacks package-local dev deps. No package changelog exists. |
| `server-sdks/sockudo-http-dotnet` | `dotnet test sockudo-dotnet-server.sln` and `dotnet test pusher-dotnet-server.sln` | Pass | E3 rerun: Sockudo solution 142 passed, 54 skipped; Pusher solution 135 passed, 54 skipped. Existing nullable warnings only. |
| `server-sdks/sockudo-http-go` | `go test ./...` | Pass | E3 rerun green, including shared future-webhook fixture replay and >2^53 mutation serial coverage. |
| `server-sdks/sockudo-http-java` | `./gradlew --no-daemon test --tests io.sockudo.rest.WebhookTest`; `./gradlew --no-daemon test` | Pass | E3 rerun includes shared future-webhook fixture, nested-value replay, unknown event names, raw-body signature validation, additive response-field tolerance, and full package tests. Gradle/JDK deprecation warnings only. |
| `server-sdks/sockudo-http-php` | `vendor/bin/phpunit --filter WebhookTest tests/unit/WebhookTest.php`; `vendor/bin/phpunit` | Partial | E3 focused replay passed: 5 tests, 18 assertions, with the existing PHPUnit deprecation. Full suite executed 158 tests and 172 assertions but exits red on 21 baseline warnings plus 1 deprecation from issue #297. |
| `server-sdks/sockudo-http-ruby` | `bundle exec rspec spec/web_hook_spec.rb` | Blocked | E3 fixture test added, but local Ruby 2.6.10/Bundler 1.17.2 cannot satisfy Gemfile.lock Bundler 4.0.11. |
| `server-sdks/sockudo-http-rust` | `cargo fmt`; `cargo test --all-features` | Pass | E3 rerun: 46 tests passed; doc tests 0. No package changelog exists. |
| `server-sdks/sockudo-http-swift` | `swift test` | Pass | E3 rerun: 101 tests passed, 35 skipped live tests due unset live credentials. |

## E4 Client SDK Enablement

E4a-E4e were implemented for the five non-JS realtime client SDKs on 2026-06-27. Server gates were
present for capability tokens (`token`, `sockudo:auth`, `sockudo:auth_success`,
`sockudo:token_expired`), presence update, channel history with `until_attach`, subscription
`attach_serial`, and `append_rollup_window`. Publish-side mutable-message websocket frames are still
not a server behavior; E4 therefore uses each SDK's existing proxy-backed HTTP/versioned-message
pattern for create/append/update/delete helpers rather than inventing unsupported websocket
ack-correlation.

The local `client-sdks/sockudo-js` P1 reference implementation is still incomplete for U1-U5. These
ports used `docs/specs/ai-transport-wire-protocol.md` and the server code as their wire reference,
plus each SDK's existing style and proxy conventions. This is recorded as a dependency waiver for
the common spec's "port from JS" sequencing rule.

| SDK | E4 file refs | Landed E4 surface | Waivers |
|---|---|---|---|
| .NET | `client-sdks/sockudo-dotnet/src/Sockudo.Client/Models.cs`; `SockudoClient.cs`; `VersionedMessages.cs`; `tests/Sockudo.Client.Tests/ProtocolTests.cs`; `README.md` | Token/provider auth with unsigned JWT lifetime parsing and 80% proactive refresh, `sockudo:auth` refresh, token-expired handling, presence update, attach serial, `until_attach`, rollup validation/query, typed proxy mutation acks. | Opaque/static tokens are reactive/manual only; no websocket mutation ack frames; no live AI-enabled E1 lane run. |
| Flutter | `client-sdks/sockudo-flutter/lib/src/auth.dart`; `client.dart`; `models.dart`; `test/sockudo_flutter_test.dart`; `README.md`; `CHANGELOG.md` | Token/authCallback auth with 80% JWT refresh timer, `sockudo:auth`, token-expired handling, presence update, attach serial, `untilAttach`, rollup validation/query, typed proxy mutation acks. | Opaque/static tokens are reactive/manual only; revocation is surfaced and left to server close; no websocket mutation ack frames; no live AI-enabled E1 lane run. |
| Kotlin | `client-sdks/sockudo-kotlin/lib/src/main/kotlin/io/sockudo/client/Auth.kt`; `Models.kt`; `SockudoClient.kt`; `Channels.kt`; `VersionedMessages.kt`; `lib/src/test/kotlin/io/sockudo/client/PlatformPrimitiveTest.kt`; `README.md`; `CHANGELOG.md` | Provider auth with 80% JWT refresh job, `sockudo:auth`, token-expired handling, presence update, attach serial, `untilAttach`, rollup validation/query, typed proxy mutation acks. | Opaque/static tokens are reactive/manual only; no websocket mutation ack frames; no live AI-enabled E1 lane run. |
| Python | `client-sdks/sockudo-python/src/sockudo_python/client.py`; `src/sockudo_python/__init__.py`; `tests/test_client.py`; `tests/test_protocol.py`; `tests/test_forward_compat.py`; `README.md` | Async token/authCallback auth with 80% JWT/result refresh scheduling, `sockudo:auth`, token-expired handling, presence update, attach serial, channel history with `until_attach`, rollup validation/query, typed proxy mutation acks. | Opaque/no-exp tokens are reactive only; no websocket mutation ack frames; no live AI-enabled E1 lane run. |
| Swift | `client-sdks/sockudo-swift/Sources/SockudoSwift/Auth.swift`; `SockudoClient.swift`; `Channels.swift`; `Support.swift`; `VersionedMessages.swift`; `Tests/SockudoSwiftTests/SockudoSwiftTests.swift`; `README.md`; `CHANGELOG.md` | Capability-token auth with 80% JWT refresh scheduling, `sockudo:auth`, token-expired handling, presence update, attach serial, `untilAttach`, rollup validation/query, typed proxy mutation acks. | Opaque/static tokens are reactive/manual only; websocket channel-history correlation remains unimplemented; no websocket mutation ack frames; no live AI-enabled E1 lane run. |

## Third-Party V1 Canary

Canary packages:

- npm `pusher-js` 8.5.0
- npm `pusher` 5.3.4
- `ws@latest`

Server command:

```bash
cargo build -p sockudo --features ai-transport
./target/debug/sockudo --config config/config.toml
```

Observed:

- Vanilla `pusher-js` connected to the current server.
- `pusher:subscription_succeeded` was delivered on `public-canary`.
- Official Node `pusher` returned `{"size":0,"timeout":0}` for trigger.
- The subscribed client did not receive the application event within 10 seconds.

Diagnostic output:

```text
CONNECTED
CHANNEL_GLOBAL pusher:subscription_succeeded {}
SUBSCRIBED
TRIGGER_RESULT {"size":0,"timeout":0}
timeout connected=true subscribed=true triggered=true seen=[subscription_succeeded only]
```

Status: V1 canary failed publish-to-deliver. Filed #298.

## Gaps Feeding Later Prompts

### Client SDK U1-U7 Gaps

| SDK | U1 auth | U2 presence update | U3 history/until_attach | U4 mutation acks | U5 rollup | U6 extras passthrough | U7 serial safety |
|---|---|---|---|---|---|---|---|
| `@sockudo/client` | absent | absent | partial history, missing `until_attach`/`attach_serial` | consume only, publish absent | absent | E2 tolerant for realtime JSON/msgpack/protobuf known `ai` tier plus unknown JSON/msgpack extras | E2 tolerant with `number \| string` u64 serials and >2^53 fixture coverage |
| `@sockudo/ai-transport` | passthrough only | adapter-shaped, underlying absent | adapter-shaped | adapter-shaped | adapter-shaped | E2 adapter tolerant for `extras.ai`, future extras, and E1 fixture replay after underlying `@sockudo/client` E2 | E2 adapter tolerant with `number \| string` serials, BigInt/string comparison, and >2^53 fixture coverage after underlying `@sockudo/client` E2 |
| .NET client | E4 complete for provider/JWT tokens; opaque/static reactive-only | E4 complete | E4 complete for proxy history params and attach serial | E4 proxy-backed HTTP helpers only | E4 complete | E2/E4 tolerant for raw JSON/msgpack extras plus known protobuf `extras.ai` | E2/E4 tolerant with `long`/string-safe serials and >2^53 coverage |
| Flutter client | E4 complete for authCallback/JWT tokens; opaque/static reactive-only | E4 complete | E4 complete for proxy history params and attach serial | E4 proxy-backed HTTP helpers only | E4 complete | E2/E4 tolerant for raw JSON/msgpack extras plus known protobuf `extras.ai` | E2/E4 tolerant with `int \| String` serials and >2^53 coverage |
| Kotlin client | E4 complete for provider/JWT tokens; opaque/static reactive-only | E4 complete | E4 complete for proxy history params and attach serial | E4 proxy-backed HTTP helpers only | E4 complete | E2/E4 tolerant for raw extras passthrough plus known protobuf `extras.ai` | E2/E4 tolerant with `Long`/integer-string serials and >2^53 coverage |
| Python client | E4 complete for async callback/JWT/result tokens; opaque/no-exp reactive-only | E4 complete | E4 complete, including channel history API | E4 proxy-backed HTTP helpers only | E4 complete | E2/E4 tolerant for raw JSON/msgpack extras plus known protobuf `extras.ai` | E2/E4 tolerant with Python int/string serials and >2^53 coverage |
| Swift client | E4 complete for provider/JWT tokens; opaque/static reactive-only | E4 complete | E4 complete for proxy history params and attach serial | E4 proxy-backed HTTP helpers only | E4 complete | E2/E4 tolerant for raw extras plus known protobuf/msgpack `extras.ai` | E2/E4 tolerant with exact `Int64`/string serials and >2^53 coverage |

For the five E4 SDKs, U4 intentionally means proxy-backed HTTP helpers because the server does not
support websocket mutation request/ack frames. `@sockudo/client` remains the key realtime-client
gap for U1-U5 and still needs the full P1 primitive port before `@sockudo/ai-transport` can depend
on those methods directly.

### Server SDK E3/E5 Gaps

| SDK | E3 parser hardening | E5 enablement notes |
|---|---|---|
| Node | E3 complete: public types widened, fixture replay added, nested values preserved by runtime JSON parse | Add optional `extras.ai`, history pagination/until_attach helpers, and any additional mutation helper fields only after server support lands |
| Python | E3 complete: strict `WebhookEvent(**event)` replaced with raw event preservation, including nested values | Add optional trigger/mutation/history/webhook AI fields after server support lands |
| .NET | E3 complete: raw webhook event values preserved beyond `Dictionary<string,string>` | Add optional trigger/mutation/history/webhook AI fields |
| Go | E3 complete: webhook data preserves raw JSON values and mutation response serials decode as `uint64` pointers | Add `extras.ai`, until_attach, and mutation helper fields after server support lands |
| Java | E3 complete: tolerant `parseWebhook` helper added with raw event/body access and fixture replay | Add optional typed helpers/examples while keeping raw responses |
| PHP | E3 complete by fixture and nested-value coverage; parser already tolerant | Add optional fields/helpers; fix suite warning baseline first |
| Ruby | E3 fixture and nested-value coverage added; local execution blocked by Bundler | Add optional fields/helpers once local modern Ruby baseline is green |
| Rust | E3 complete: nested webhook JSON accepted into the legacy string projection and preserved through `get_raw_json_events()` | Add `history_serial`, `extras.ai`, until_attach, and mutation helpers |
| Swift | E3 complete: unknown webhook event enum/raw string fallback and raw payload preservation | Add optional fields/helpers after server support lands |

## Plan Corrections

`plans/ai-transport/03-existing-sdks-prompts.md` was updated to reflect:

- 7 client packages in tree, not 5.
- SDK directories are imported monorepo package directories, not submodules.
- `sockudo-python` E2 coverage is complete and E4 added channel history plus U1-U5 enablement
  under the same proxy-backed mutation boundary as the other non-JS clients.
- `@sockudo/ai-transport` is an adapter package with its own CI/release lane and should be
  tracked alongside, but not treated as a standalone realtime socket implementation. Its E2
  adapter-level tolerance verification is complete after `@sockudo/client` E2; U1-U5 remain gated.
