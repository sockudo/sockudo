# Client SDKs

Client SDKs live here as imported monorepo packages. Directory names preserve
the original repository names to keep package metadata, docs, release scripts,
and issue references recognizable.

SDK CI and package publishing are managed from the monorepo root through
`.github/workflows/sdk-ci.yml` and `.github/workflows/sdk-release.yml`. See the
[2026 SDK publishing runbook](../docs/sdk-publishing-2026.md) before publishing
or changing registry setup. SwiftPM distribution uses the root `Package.swift`
in this monorepo.

## Packages

| Package | Directory |
| --- | --- |
| `@sockudo/ai-transport` | `client-sdks/sockudo-ai-transport-js` |
| `Sockudo.Client` | `client-sdks/sockudo-dotnet` |
| `sockudo_flutter` | `client-sdks/sockudo-flutter` |
| `@sockudo/client` | `client-sdks/sockudo-js` |
| `io.sockudo:sockudo-kotlin` | `client-sdks/sockudo-kotlin` |
| `sockudo-python` | `client-sdks/sockudo-python` |
| `SockudoSwift` via SwiftPM | `client-sdks/sockudo-swift` |
