import { hrefOf, loadDocs, slugOf, type DocEntry } from './docs';

const site = 'https://sockudo.io';

/** Raw page source with MDX-only syntax stripped, for agents and copy-as-markdown. */
export function toMarkdown(entry: DocEntry): string {
  const body = (entry.body ?? '')
    .replace(/^import .*$/gm, '')
    .replace(/<Card\s+title="([^"]*)"\s+href="([^"]*)"\s+description="([^"]*)"\s*\/>/g, '- [$1]($2): $3')
    .replace(/<\/?(Cards|Steps)>/g, '')
    .replace(/<(DocsHub|FanoutDiagram)\b[^>]*\/>/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return `# ${entry.data.title} (${site}${hrefOf(slugOf(entry))})\n\n${entry.data.description ? `> ${entry.data.description}\n\n` : ''}${body}\n`;
}

export async function llmsIndex(): Promise<string> {
  const { groups, docs } = await loadDocs();
  const bySlug = new Map(docs.map((d) => [slugOf(d), d]));
  const lines = [
    '# Sockudo',
    '',
    '> Sockudo is a self-hosted Rust realtime server with Pusher-compatible Protocol V1, opt-in Ably REST/WebSocket compatibility, Protocol V2 durability (history, recovery, mutable messages, annotations), horizontal fanout, push notifications, and AI Transport.',
    '',
    `Full documentation in one file: ${site}/llms-full.txt`,
    '',
  ];
  for (const g of groups) {
    lines.push(`## ${g.title}`, '');
    const links = g.items.flatMap((i) => (i.type === 'section' ? i.items : i.external ? [] : [i]));
    for (const l of links) {
      const d = bySlug.get(l.slug);
      lines.push(`- [${l.title}](${site}${l.href}.md)${d?.data.description ? `: ${d.data.description}` : ''}`);
    }
    lines.push('');
  }
  return lines.join('\n');
}

export async function llmsFull(): Promise<string> {
  const { flat, docs } = await loadDocs();
  const bySlug = new Map(docs.map((d) => [slugOf(d), d]));
  return flat
    .map((l) => bySlug.get(l.slug))
    .filter((d): d is DocEntry => !!d)
    .map(toMarkdown)
    .join('\n\n---\n\n');
}
