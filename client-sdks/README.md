# Client SDKs

Client SDKs live here as imported monorepo packages. Directory names preserve
the original repository names to keep package metadata, docs, release scripts,
and issue references recognizable.

SDK CI and package publishing are managed from the monorepo root through
`.github/workflows/sdk-ci.yml` and `.github/workflows/sdk-release.yml`. See the
[2026 SDK publishing runbook](../docs/sdk-publishing-2026.md) before publishing
or changing registry setup. VCS-tagged ecosystems such as SwiftPM still require
package repository/subtree-split handling unless their public package URL is
changed intentionally.

## Imports

| Directory | Current source | Legacy repo |
| --- | --- | --- |
| `sockudo-ai-transport-js` | `client-sdks/sockudo-ai-transport-js` | <https://github.com/sockudo/sockudo-ai-transport-js> imported at `d87253f74613` |
| `sockudo-dotnet` | `client-sdks/sockudo-dotnet` | <https://github.com/sockudo/sockudo-dotnet> imported at `b4abfe67e195` |
| `sockudo-flutter` | `client-sdks/sockudo-flutter` | <https://github.com/sockudo/sockudo-flutter> imported at `dc1b886b191f` |
| `sockudo-js` | `client-sdks/sockudo-js` | <https://github.com/sockudo/sockudo-js> imported at `a304b0df04d7` |
| `sockudo-kotlin` | `client-sdks/sockudo-kotlin` | <https://github.com/sockudo/sockudo-kotlin> imported at `adb338e1ef7a` |
| `sockudo-python` | `client-sdks/sockudo-python` | <https://github.com/sockudo/sockudo-python> imported at `6a9f6de7830e` |
| `sockudo-swift` | `client-sdks/sockudo-swift` | <https://github.com/sockudo/sockudo-swift> imported at `cb3fc23d0e94` |
