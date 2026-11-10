# Sockudo docs

The Sockudo website and documentation, built with [Astro](https://astro.build).

```bash
bun install
bun run dev          # http://localhost:4321
bun run build        # static site in dist/ + Pagefind search index
bun run preview      # serve the production build (search works here)
bun run types:check  # astro check
```

Search is powered by Pagefind and only exists after `bun run build`.

## Layout

| Path                         | Purpose                                                                 |
| ---------------------------- | ----------------------------------------------------------------------- |
| `content/docs/**/*.mdx`      | Documentation pages. Sidebar order comes from each folder's `meta.json`. |
| `content/blog/*.mdx`         | Blog posts (`date`, `author`, `category`, `tags` frontmatter).          |
| `src/pages/index.astro`      | Homepage, composed from `src/components/home/*`.                        |
| `src/pages/docs/[...slug]`   | Docs page shell (sidebar, TOC, prev/next, copy-as-markdown).            |
| `src/components/mdx/*`       | Components available in every MDX page.                                 |
| `src/scripts/three/*`        | Three.js scenes: hero, scroll-driven fanout diagram, CTA globe.         |
| `src/scripts/site.ts`        | Theme, smooth scroll, search, reveals, TOC tracking, Mermaid, 3D mounts. |
| `src/styles/global.css`      | Design tokens (purple accent, glass surfaces) and shared styles.        |
| `ec.config.mjs`              | Expressive Code (code block) theme.                                     |

## Writing pages

Plain Markdown works everywhere. These components are available without imports:

```mdx
<Cards>
  <Card title="Installation" href="/docs/getting-started/installation" description="Run Sockudo locally." />
</Cards>

<Callout type="tip" title="Heads up">Body text.</Callout>   {/* note | tip | warn */}

<Steps>

1. First step
2. Second step

</Steps>

<FanoutDiagram start={1} caption="Interactive 3D publish → fanout → deliver → recover model." />
```

` ```mermaid ` fences are rendered at build time by `src/plugins/remark-diagrams.mjs`:

- `sequenceDiagram` → animated HTML (supports `participant … as …`, `->>`, `-->>`, `--x`, `-x`, `->`, `-->`, `-)`, `--)`, `Note over|left of|right of`)
- `flowchart` / `graph` → SVG laid out with ELK (`src/plugins/flowchart.mjs`); supports the usual node shapes, labelled/dotted/thick/bidirectional edges, `&` chains and subgraphs (including edges to a subgraph)

Anything outside that subset falls back to client-side Mermaid. Check pages before pushing:

```bash
node scripts/check-mdx.mjs content/docs/**/*.mdx
```

## Agent-friendly output

- `/llms.txt` — index of every page
- `/llms-full.txt` — the whole documentation in one file
- `/docs/<slug>.md` — raw Markdown for any page (also `/llms.mdx/docs/<slug>/content.md`)

The GitHub star count is fetched at build time (`GITHUB_TOKEN` is used when set) and refreshed in the browser.
