import { blogPosts } from '@/lib/blog';
import type { Metadata } from 'next';
import Link from 'next/link';

export const metadata: Metadata = {
  title: 'Blog',
  description:
    'Engineering notes on Sockudo protocol design, operations, SDKs, and realtime infrastructure.',
};

export default function BlogIndexPage() {
  return (
    <main className="blog-layout">
      <header className="blog-header">
        <span className="doc-chip">Sockudo Blog</span>
        <h1>Realtime systems notes for builders and operators.</h1>
        <p>
          Practical essays on protocol evolution, clustered fanout, SDK design,
          recovery semantics, and operational tradeoffs in Sockudo.
        </p>
      </header>

      <div className="blog-grid">
        {blogPosts.map((post) => (
          <Link className="blog-tile" href={post.url} key={post.url}>
            <div className="blog-meta">
              {post.category} · {post.date}
            </div>
            <h3>{post.title}</h3>
            <p>{post.description}</p>
          </Link>
        ))}
      </div>
    </main>
  );
}
