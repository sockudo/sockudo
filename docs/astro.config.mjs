// @ts-check
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import expressiveCode from 'astro-expressive-code';
import { rehypeHeadingIds } from '@astrojs/markdown-remark';
import { remarkDiagrams } from './src/plugins/remark-diagrams.mjs';
import { rehypeProse } from './src/plugins/rehype-prose.mjs';

export default defineConfig({
  site: 'https://sockudo.io',
  trailingSlash: 'ignore',
  prefetch: { prefetchAll: true, defaultStrategy: 'hover' },
  redirects: {
    '/install.sh': 'https://github.com/sockudo/sockudo/releases/latest/download/install.sh',
  },
  markdown: {
    remarkPlugins: [remarkDiagrams],
    rehypePlugins: [rehypeHeadingIds, rehypeProse],
  },
  integrations: [
    expressiveCode(),
    mdx(),
    sitemap(),
  ],
  vite: {
    build: { chunkSizeWarningLimit: 1600 },
    define: { __DOCS_ROOT__: JSON.stringify(fileURLToPath(new URL('.', import.meta.url))) },
  },
});
