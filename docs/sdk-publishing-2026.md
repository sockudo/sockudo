# Sockudo SDK Publishing Runbook 2026

This is the source-of-truth release process for every imported Sockudo client and server SDK.

## CI/CD Ownership

- Root workflow: `.github/workflows/sdk-ci.yml`
- Root release workflow: `.github/workflows/sdk-release.yml`
- Old package-local `.github/workflows/*` files are intentionally removed.
- SDK validation runs on SDK/workflow changes only.
- Publishing is opt-in, environment-protected, and dry-runnable.

## Required GitHub Environments

Create these environments in GitHub repository settings and require reviewer approval for all of
them:

| Environment | Used for |
| --- | --- |
| `npm` | npm trusted publishing for `@sockudo/client`, `@sockudo/ai-transport`, and `sockudo` |
| `pypi` | PyPI trusted publishing for Python SDKs |
| `nuget` | NuGet trusted publishing for .NET SDKs |
| `maven-central` | Maven Central token/GPG guarded publishing |
| `pub.dev` | pub.dev automated publishing for `sockudo_flutter` |
| `rubygems` | RubyGems trusted publishing |
| `crates.io` | crates.io trusted publishing |
| `packagist` | Packagist release review gate |

## Required Repository Secrets

OIDC registries do not need long-lived package tokens, but a few ecosystems still require account
or signing credentials:

| Secret | Required for | Notes |
| --- | --- | --- |
| `NUGET_SOCKUDO_USER` | NuGet | nuget.org username/profile name that owns `Sockudo.Client` and `SockudoServer` |
| `NUGET_PUSHER_USER` | NuGet | optional nuget.org username/profile name that owns `PusherServer` |
| `MAVEN_CENTRAL_USERNAME` | Maven Central | Central Portal user token username |
| `MAVEN_CENTRAL_PASSWORD` | Maven Central | Central Portal user token password |
| `MAVEN_GPG_KEY_ID` | Maven Central | signing key id |
| `MAVEN_GPG_PRIVATE_KEY` | Maven Central | ASCII-armored private key |
| `MAVEN_GPG_PASSPHRASE` | Maven Central | signing key passphrase |

## Browser Setup Checklist

Use these exact trusted publisher values unless you intentionally rename the workflow:

- Repository owner: `sockudo`
- Repository name: `sockudo`
- Workflow file: `sdk-release.yml`

### npm

Packages: `@sockudo/client`, `@sockudo/ai-transport`, `sockudo`

Important 2026 constraint: npm trusted publishing is configured from package settings, so a package
must exist before you can attach the trusted publisher. For brand-new npm packages, do one initial
interactive publish or staged publish with an owner account, then enable trusted publishing and use
the GitHub workflow for every later release.

1. Log in to npm.
2. Open each package settings page.
3. Configure Trusted publishing with GitHub Actions.
4. Use environment `npm`.
5. Restrict legacy token publishing after a successful OIDC publish.

The package `repository.url` must point at `https://github.com/sockudo/sockudo` for npm OIDC
validation. The workflow installs a current npm CLI and publishes with provenance.

### PyPI

Packages: `sockudo-python`, `sockudo-http-python`

1. Log in to PyPI.
2. For an existing project, add a trusted publisher.
3. For a first release, create a pending trusted publisher.
4. Use environment `pypi`.

### NuGet

Packages: `Sockudo.Client`, `SockudoServer`, `PusherServer`

1. Log in to nuget.org.
2. Open Trusted Publishing.
3. Add a GitHub Actions policy for `sdk-release.yml`.
4. Use environment `nuget`.
5. Set `NUGET_SOCKUDO_USER` in GitHub secrets for Sockudo-owned packages.
6. Set `NUGET_PUSHER_USER` only if the publishing account is allowed to publish `PusherServer`.

The workflow uses `NuGet/login@v1` to mint a temporary API key at publish time.

### Maven Central

Packages: `io.sockudo:sockudo-kotlin`, `io.sockudo:sockudo-http-java`

1. Log in to Central Portal.
2. Verify the `io.sockudo` namespace.
3. Create a Central Portal user token.
4. Add the Maven and GPG secrets listed above.
5. Keep the `maven-central` GitHub environment protected.

Maven Central still requires signing. The Gradle builds publish through the Central Portal OSSRH
Staging API compatibility endpoint.

### pub.dev

Package: `sockudo_flutter`

1. Publish the first version manually if the package does not yet exist.
2. Open `pub.dev/packages/sockudo_flutter/admin`.
3. Enable automated publishing from GitHub Actions.
4. Use tag pattern `client-flutter-v{{version}}`.
5. Require environment `pub.dev`.

pub.dev only accepts automated GitHub Actions publishing from tag-triggered workflows.

### RubyGems

Package: `sockudo`

1. Log in to RubyGems.org.
2. Open the gem's Trusted publishers page, or create a pending trusted publisher for a new gem.
3. Use environment `rubygems`.

The workflow uses RubyGems trusted publishing and does not need a RubyGems API token.

### crates.io

Package: `sockudo-http`

1. Publish the first crate version manually if the crate does not yet exist.
2. Open the crate owner settings on crates.io.
3. Add a trusted publisher for `sdk-release.yml`.
4. Use environment `crates.io`.

The workflow exchanges GitHub OIDC for a short-lived crates.io token.

### Packagist

Package: `sockudo/sockudo-php-server`

Packagist publishes from VCS tags and repository hooks, not from an uploaded artifact. Public
Packagist.org does not publish packages from a subdirectory, so this monorepo has a root
`composer.json` whose autoload paths point at `server-sdks/sockudo-http-php`.

Submit this repository URL to Packagist:

```text
https://github.com/sockudo/sockudo
```

Stable Packagist versions come from Composer-compatible root tags such as `v5.0.0`. Before pushing a
root PHP release tag, run the PHP release dry run:

```bash
gh workflow run sdk-release.yml -f package=server-php -f dry_run=true
```

### Go Modules

Package: `github.com/sockudo/sockudo-http-go/v5`

Go modules publish from semantic VCS tags. The current module path points at the legacy package
repository, so push matching `v5.x.y` tags to that repository or intentionally change the module
path in a separate breaking change.

### SwiftPM

Packages: `SockudoSwift`, `Sockudo`

SwiftPM packages publish from Git tags on repositories whose root contains `Package.swift`. These
SDKs currently live under monorepo subdirectories, so publish via package repositories/subtree
splits or intentionally migrate consumers to a new package URL in a separate breaking change.

## Package Commands

| Package | CI gate | Release trigger |
| --- | --- | --- |
| `@sockudo/client` | Bun format/typecheck/lint/test/build and `npm pack --dry-run` | `client-js-vX.Y.Z` |
| `@sockudo/ai-transport` | pnpm release check and `npm pack --dry-run` | `client-ai-transport-js-vX.Y.Z` |
| `Sockudo.Client` | `dotnet format`, build, test, pack | `client-dotnet-vX.Y.Z` |
| `sockudo_flutter` | `dart format`, analyze, test, publish dry-run | `client-flutter-vX.Y.Z` |
| `io.sockudo:sockudo-kotlin` | Gradle check and `publishToMavenLocal` | `client-kotlin-vX.Y.Z` |
| `sockudo-python` | Ruff format/check, pytest, build, twine check | `client-python-vX.Y.Z` |
| `SockudoSwift` | Swift build/test and SwiftLint | `client-swift-vX.Y.Z` |
| `sockudo` Node server SDK | npm lint/typecheck/local-test and `npm pack --dry-run` | `server-node-vX.Y.Z` |
| `sockudo-http-python` | Ruff format/check, pytest, build, twine check | `server-python-vX.Y.Z` |
| `sockudo/sockudo-php-server` | Composer validate, PHP-CS-Fixer, PHPLint, PHPUnit | `vX.Y.Z` |
| `sockudo` Ruby gem | RuboCop, RSpec, gem build | `server-ruby-vX.Y.Z` |
| `github.com/sockudo/sockudo-http-go/v5` | gofmt, go vet, go test | `server-go-vX.Y.Z` |
| `sockudo-http` | cargo fmt, clippy, test, package | `server-rust-vX.Y.Z` |
| `io.sockudo:sockudo-http-java` | Gradle check and `publishToMavenLocal` | `server-java-vX.Y.Z` |
| `SockudoServer`, `PusherServer` | `dotnet format`, build, test, pack | `server-dotnet-vX.Y.Z` |
| `Sockudo` Swift server SDK | Swift build/test and SwiftLint | `server-swift-vX.Y.Z` |

## Release Procedure

1. Update the package version and changelog in the package directory.
2. Run the local package checks listed above.
3. Push a PR and wait for `SDK CI`.
4. Dry-run the release:

```bash
gh workflow run sdk-release.yml -f package=client-js -f dry_run=true
```

5. Publish one package by pushing the package tag:

```bash
git tag client-js-v1.3.1
git push origin client-js-v1.3.1
```

6. For a controlled multi-package publish, use workflow dispatch with `package=all` and
   `dry_run=false`, then approve each protected environment deliberately.

Prefer one package at a time unless the versions were prepared and reviewed as one coordinated SDK
release train.
