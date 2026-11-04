import { getCollection, type CollectionEntry } from 'astro:content';

export type DocEntry = CollectionEntry<'docs'>;

type Meta = { title?: string; icon?: string; root?: boolean; pages?: string[] };

const metas = import.meta.glob<Meta>('/content/docs/**/meta.json', { eager: true, import: 'default' });

export interface NavLink {
  type: 'link';
  title: string;
  href: string;
  slug: string;
  external?: boolean;
}

export interface NavSection {
  type: 'section';
  title: string;
  icon?: string;
  href?: string;
  items: NavLink[];
}

export interface NavGroup {
  title: string;
  items: (NavLink | NavSection)[];
}

/** Normalises a content id ("clients/index", "index") to a URL slug ("clients", ""). */
export function slugOf(entry: DocEntry): string {
  return entry.id.replace(/(^|\/)index$/, '');
}

export function hrefOf(slug: string): string {
  return slug ? `/docs/${slug}` : '/docs';
}

function metaFor(dir: string): Meta {
  return metas[`/content/docs/${dir ? `${dir}/` : ''}meta.json`] ?? {};
}

let cache: { docs: DocEntry[]; groups: NavGroup[]; flat: NavLink[] } | undefined;

export async function loadDocs() {
  if (cache) return cache;
  const docs = await getCollection('docs');
  const bySlug = new Map(docs.map((d) => [slugOf(d), d]));

  const link = (slug: string, fallbackTitle?: string): NavLink | undefined => {
    const doc = bySlug.get(slug);
    if (!doc) return undefined;
    return { type: 'link', title: fallbackTitle ?? doc.data.title, href: hrefOf(slug), slug };
  };

  const root = metaFor('');
  const groups: NavGroup[] = [];
  let current: NavGroup = { title: 'Introduction', items: [] };
  groups.push(current);

  for (const raw of root.pages ?? []) {
    const sep = raw.match(/^---(.*)---$/);
    if (sep) {
      current = { title: sep[1], items: [] };
      groups.push(current);
      continue;
    }
    const ext = raw.match(/^\[(.+)\]\((.+)\)$/);
    if (ext) {
      current.items.push({ type: 'link', title: ext[1], href: ext[2], slug: ext[2], external: true });
      continue;
    }
    if (raw === 'index') {
      const l = link('', 'Overview');
      if (l) current.items.push(l);
      continue;
    }
    const meta = metaFor(raw);
    if (meta.pages || [...bySlug.keys()].some((s) => s.startsWith(`${raw}/`))) {
      const pages = meta.pages ?? [...bySlug.keys()].filter((s) => s.startsWith(`${raw}/`)).map((s) => s.slice(raw.length + 1));
      const items: NavLink[] = [];
      for (const p of pages) {
        const slug = p === 'index' ? raw : `${raw}/${p}`;
        const l = link(slug, p === 'index' ? 'Overview' : undefined);
        if (l) items.push(l);
      }
      current.items.push({
        type: 'section',
        title: meta.title ?? raw,
        icon: meta.icon,
        href: bySlug.has(raw) ? hrefOf(raw) : items[0]?.href,
        items,
      });
    } else {
      const l = link(raw);
      if (l) current.items.push(l);
    }
  }

  const filtered = groups.filter((g) => g.items.length > 0);
  const flat: NavLink[] = [];
  for (const g of filtered)
    for (const item of g.items) {
      if (item.type === 'link' && !item.external) flat.push(item);
      if (item.type === 'section') flat.push(...item.items);
    }

  cache = { docs, groups: filtered, flat };
  return cache;
}

export async function getPrevNext(slug: string) {
  const { flat } = await loadDocs();
  const i = flat.findIndex((l) => l.slug === slug);
  return { prev: i > 0 ? flat[i - 1] : undefined, next: i >= 0 && i < flat.length - 1 ? flat[i + 1] : undefined };
}

export async function getBreadcrumb(slug: string) {
  const { groups } = await loadDocs();
  for (const g of groups)
    for (const item of g.items)
      if (item.type === 'section' && item.items.some((l) => l.slug === slug)) return [g.title, item.title];
      else if (item.type === 'link' && item.slug === slug) return [g.title];
  return [];
}
