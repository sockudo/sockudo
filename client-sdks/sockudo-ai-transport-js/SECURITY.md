# Security Policy

## Supported Versions

Security fixes are prepared for the current `0.1.x` release line until the next minor release is
available.

## Reporting A Vulnerability

Do not open public issues for vulnerabilities. Email `security@sockudo.app` with:

- affected package and version
- impact and exploit preconditions
- minimal reproduction or proof of concept
- whether credentials, tokens, or customer data may be exposed

We aim to acknowledge reports within 3 business days, provide an initial assessment within 7
business days, and coordinate disclosure timing with the reporter.

## Supply Chain

Release candidates run `pnpm audit`, OSV lockfile scanning, package dry-run installation from a
Verdaccio registry, API snapshot checks, and export smoke tests. Published npm artifacts use
provenance-enabled OIDC publishing.
