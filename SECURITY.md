# Security Policy

## Reporting a vulnerability

Please do not report security vulnerabilities in public GitHub issues, discussions, or pull
requests.

Report them privately through GitHub's private vulnerability reporting:

1. Open the repository's **Security** tab.
2. Choose **Report a vulnerability**.
3. Fill in the advisory form.

Direct link: <https://github.com/sockudo/sockudo/security/advisories/new>

Include as much of the following as you can:

- affected version or commit, and whether current `master` is affected
- the component (server crate, client SDK, server SDK, dashboard, Helm chart, migrations)
- configuration needed to reproduce, with secrets removed
- steps to reproduce or a proof of concept
- the impact you expect, for example who can do what to whom

## What to expect

- We acknowledge a report within 3 business days.
- We send an initial assessment within 10 business days.
- We keep you updated while we work on a fix, and agree a disclosure date with you.
- We publish a GitHub security advisory with the fix, and credit you unless you would rather not
  be named.

## Supported versions

Security fixes go to the latest minor release line of Sockudo and to `master`. Older release
lines get fixes only when the issue is severe and the backport is practical.

| Version | Supported |
| ------- | --------- |
| 5.2.x   | Yes       |
| < 5.2   | No        |

Client and server SDKs in this repository follow the same policy for their latest published
version.

## Scope

In scope: the Sockudo server, the official client and server SDKs, the dashboard, and the
deployment assets in this repository (Docker images, Helm charts, Compose files, migrations).

Out of scope: demo credentials that are documented as development-only, such as
`ops/migrations/*/900_dev_seed_app.sql` and the sample `config/config.toml`, unless a supported
production path applies them.
