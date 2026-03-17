# Sockudo Documentation

> Official documentation for [Sockudo](https://github.com/sockudo/sockudo) — a production-ready, drop-in Pusher replacement built in Rust.

**Live site →** [sockudo.io](https://sockudo.io)

## What is Sockudo?

Sockudo is a high-performance WebSocket server that implements the Pusher protocol. It lets you own your realtime infrastructure with enterprise features like delta compression, tag filtering, and multi-region scaling — while keeping full compatibility with existing Pusher clients and server SDKs.

This repository contains the source for the Sockudo documentation site.

## Local Development

**Prerequisites:** [Bun](https://bun.sh) (or Node.js 22+)

```bash
# Clone the repo
git clone https://github.com/sockudo/sockudo-docs.git
cd sockudo-docs

# Install dependencies
bun install

# Start the dev server
bun run dev
```

The site will be running at `http://localhost:3000`.

## Project Structure

```
sockudo-docs/
├── content/
│   ├── index.md                  # Homepage
│   ├── 1.getting-started/        # Installation, first connection, auth, migration
│   ├── 2.server/                 # Configuration, scaling, security, HTTP API, etc.
│   ├── 3.client/                 # @sockudo/client usage, features, runtime targets
│   ├── 4.integrations/           # Laravel Echo, pusher-js, backend SDKs, recipes
│   └── 5.reference/              # Protocol spec, HTTP endpoints, config reference
├── components/
│   └── OgImage/                  # Custom Open Graph image components
├── public/                       # Static assets (logos, favicons, diagrams)
├── app.config.ts                 # Docus theme & UI configuration
├── nuxt.config.ts                # Nuxt / site configuration
└── netlify.toml                  # Deployment config
```

All documentation content lives in `content/` as Markdown files. Docus uses the folder numbering prefix (e.g. `1.getting-started`) for navigation ordering — the numbers are stripped from the final URLs.

## Writing Docs

- Pages are written in Markdown with [MDC syntax](https://content.nuxt.com/usage/markdown) for embedding Vue components.
- Frontmatter at the top of each `.md` file controls the page title, description, and navigation behavior.
- Navigation sections are configured via `.navigation.yml` files in each content subdirectory.
- Static assets like images and diagrams go in `public/`.

## Building for Production

```bash
bun run build
```

The output is generated as a static site (configured via `NITRO_PRESET=netlify_static`) and deployed to Netlify.

## Contributing

Contributions are welcome! Whether it's fixing a typo, improving an explanation, or adding a new guide:

1. Fork this repository
2. Create a branch (`git checkout -b fix/typo-in-scaling-docs`)
3. Make your changes in `content/`
4. Run `bun run dev` and verify locally
5. Open a pull request

For larger changes (new sections, restructuring), please open an issue first to discuss.

## Related Repositories

| Repo | Description |
|------|-------------|
| [sockudo/sockudo](https://github.com/sockudo/sockudo) | The Sockudo server (Rust) |
| [sockudo/sockudo-js](https://github.com/sockudo/sockudo-js) | Official JavaScript/TypeScript client (`@sockudo/client`) |

## License

[MIT](https://opensource.org/licenses/MIT)