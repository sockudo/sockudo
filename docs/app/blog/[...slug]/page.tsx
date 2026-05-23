import { getBlogPost, blogPosts } from '@/lib/blog';
import { getMDXComponents } from '@/components/mdx';
import { DocsBody } from 'fumadocs-ui/layouts/docs/page';
import type { Metadata } from 'next';
import { notFound } from 'next/navigation';

export default async function BlogPostPage(props: PageProps<'/blog/[...slug]'>) {
  const params = await props.params;
  const post = getBlogPost(params.slug);
  if (!post) notFound();

  const MDX = post.body;

  return (
    <main className="blog-article">
      <header className="blog-article-header">
        <span className="doc-chip">
          {post.category} · {post.date}
        </span>
        <h1>{post.title}</h1>
        <p>{post.description}</p>
      </header>
      <DocsBody>
        <MDX components={getMDXComponents()} />
      </DocsBody>
    </main>
  );
}

export function generateStaticParams() {
  return blogPosts.map((post) => ({
    slug: post.slug,
  }));
}

export async function generateMetadata(
  props: PageProps<'/blog/[...slug]'>,
): Promise<Metadata> {
  const params = await props.params;
  const post = getBlogPost(params.slug);
  if (!post) notFound();

  return {
    title: post.title,
    description: post.description,
  };
}
