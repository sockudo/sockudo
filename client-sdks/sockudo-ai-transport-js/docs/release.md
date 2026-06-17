# Release Guide

`@sockudo/ai-transport` releases are changesets-driven and start at `0.1.0`.

## Release Candidate Gate

Run:

```bash
pnpm release:check
pnpm release:dry-run
```

The dry run starts Verdaccio locally, publishes the packed tarball to that registry, installs a
fresh app from the registry package, and imports all four public entry points from the installed
tarball.

## Publishing

Publishing is done only from the GitHub release workflow after maintainer approval. The workflow
uses npm OIDC provenance:

```bash
npm publish --access public --provenance
```

Do not publish from a workstation.

## CDN/UMD Distribution

Vite builds UMD artifacts for all public entry points:

- `dist/index.umd.cjs`
- `dist/react/index.umd.cjs`
- `dist/vercel/index.umd.cjs`
- `dist/vercel/react/index.umd.cjs`

The release workflow uploads `dist/**` as a GitHub release artifact. A separate CDN promotion step
is intentionally skipped for `0.1.0` because Sockudo does not yet operate a public CDN bucket for
SDK artifacts. The uploaded UMD artifacts are the canonical CDN-ready payloads.

## Dependency Policy

Dependencies are pinned in `pnpm-lock.yaml`. Routine dependency PRs should wait at least 7 days
after upstream release unless they fix a security advisory or a release-blocking defect. Security
updates may bypass the age window after audit.
