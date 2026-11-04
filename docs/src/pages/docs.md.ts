import type { APIRoute } from 'astro';
import { loadDocs, slugOf } from '@/lib/docs';
import { toMarkdown } from '@/lib/llms';

export const GET: APIRoute = async () => {
  const { docs } = await loadDocs();
  const root = docs.find((d) => slugOf(d) === '')!;
  return new Response(toMarkdown(root), { headers: { 'Content-Type': 'text/markdown; charset=utf-8' } });
};
