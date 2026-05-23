import { defineCollections, defineConfig, defineDocs } from 'fumadocs-mdx/config';
import { metaSchema, pageSchema } from 'fumadocs-core/source/schema';
import { z } from 'zod';

// You can customize Zod schemas for frontmatter and `meta.json` here
// see https://fumadocs.dev/docs/mdx/collections
export const docs = defineDocs({
  dir: 'content/docs',
  docs: {
    schema: pageSchema,
    postprocess: {
      includeProcessedMarkdown: true,
    },
  },
  meta: {
    schema: metaSchema,
  },
});

export const blog = defineCollections({
  type: 'doc',
  dir: 'content/blog',
  schema: pageSchema.extend({
    date: z.string(),
    author: z.string().default('Sockudo Team'),
    category: z.string().default('Engineering'),
    tags: z.array(z.string()).default([]),
  }),
  postprocess: {
    includeProcessedMarkdown: true,
  },
});

export default defineConfig({
  mdxOptions: {
    // MDX options
  },
});
