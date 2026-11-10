// Fast syntax check for docs content without a full site build:
//   node scripts/check-mdx.mjs content/docs/server/webhooks.mdx [...more]
// Compiles each file as MDX and renders its sequence diagrams.
import { readFileSync } from 'node:fs';
import { compile } from '@mdx-js/mdx';
import remarkGfm from 'remark-gfm';
import { remarkDiagrams } from '../src/plugins/remark-diagrams.mjs';

// Fail on flowchart syntax the build-time renderer can't handle.
process.env.DIAGRAMS_STRICT = '1';

// Run the diagram renderer (so bad sequence syntax fails here), then drop its raw
// HTML output, which plain @mdx-js/mdx cannot serialise but Astro can.
const diagramsThenStrip = () => async (tree) => {
  await remarkDiagrams()(tree);
  const walk = (n) => n.children && (n.children = n.children.filter((c) => c.type !== 'html').map((c) => (walk(c), c)));
  walk(tree);
};

const known = new Set(['Callout', 'Card', 'Cards', 'DocsHub', 'FanoutDiagram', 'Steps']);
let failed = 0;
for (const file of process.argv.slice(2)) {
  const src = readFileSync(file, 'utf8');
  try {
    await compile(src, { remarkPlugins: [remarkGfm, diagramsThenStrip] });
    const body = src.replace(/```[\s\S]*?```/g, '').replace(/`[^`\n]*`/g, '');
    const unknown = [...body.matchAll(/<([A-Z][A-Za-z0-9]*)/g)].map((m) => m[1]).filter((n) => !known.has(n));
    if (unknown.length) throw new Error(`unknown components: ${[...new Set(unknown)].join(', ')}`);
    if (!/^---\n(?:[\s\S]*?\n)?title: /.test(src)) throw new Error('missing frontmatter title');
    console.log(`ok    ${file}`);
  } catch (err) {
    failed++;
    console.log(`FAIL  ${file}\n      ${String(err.message ?? err).split('\n')[0]}`);
  }
}
process.exit(failed ? 1 : 0);
