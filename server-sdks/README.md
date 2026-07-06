# Server SDKs

HTTP/server SDKs live here as imported monorepo packages. Directory names
preserve the original repository names to keep package metadata, docs, release
scripts, and issue references recognizable.

SDK CI and package publishing are managed from the monorepo root through
`.github/workflows/sdk-ci.yml` and `.github/workflows/sdk-release.yml`. See the
[2026 SDK publishing runbook](../docs/sdk-publishing-2026.md) before publishing
or changing registry setup. Most registry-native packages publish from this
monorepo directly; the PHP SDK is mirrored to `sockudo/sockudo-http-php`
because public Packagist expects `composer.json` at the repository root.

## Packages

| Package | Directory |
| --- | --- |
| `SockudoServer` | `server-sdks/sockudo-http-dotnet` |
| `github.com/sockudo/sockudo/server-sdks/sockudo-http-go/v2` | `server-sdks/sockudo-http-go` |
| `io.sockudo:sockudo-http-java` | `server-sdks/sockudo-http-java` |
| `sockudo` | `server-sdks/sockudo-http-node` |
| `sockudo/sockudo-php-server` | `server-sdks/sockudo-http-php` |
| `sockudo-http-python` | `server-sdks/sockudo-http-python` |
| `sockudo` | `server-sdks/sockudo-http-ruby` |
| `sockudo-http` | `server-sdks/sockudo-http-rust` |
| `Sockudo` via SwiftPM | `server-sdks/sockudo-http-swift` |
