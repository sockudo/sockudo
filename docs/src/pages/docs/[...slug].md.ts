import type { APIRoute, GetStaticPaths } from 'astro';
import { loadDocs, slugOf, type DocEntry } from '@/lib/docs';
import { toMarkdown } from '@/lib/llms';

export const getStaticPaths: GetStaticPaths = async () => {
  const { docs } = await loadDocs();
  return docs.filter((e) => slugOf(e)).map((entry) => ({ params: { slug: slugOf(entry) }, props: { entry } }));
};

export const GET: APIRoute = ({ props }) =>
  new Response(toMarkdown(props.entry as DocEntry), { headers: { 'Content-Type': 'text/markdown; charset=utf-8' } });
