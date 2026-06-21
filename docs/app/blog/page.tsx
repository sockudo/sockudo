import { blogPosts } from '@/lib/blog';
import { ArrowRight, CalendarDays, Newspaper } from 'lucide-react';
import type { Metadata } from 'next';
import Link from 'next/link';

export const metadata: Metadata = {
  title: 'Blog',
  description:
    'Engineering notes on Sockudo protocol design, operations, SDKs, and realtime infrastructure.',
};

export default function BlogIndexPage() {
  const [featured, ...posts] = blogPosts;

  return (
    <main className="blog-layout blog-index">
      <header className="blog-header">
        <span className="blog-eyebrow">
          <Newspaper className="size-4" />
          Sockudo Blog
        </span>
        <h1>Realtime systems notes for builders and operators.</h1>
        <p>
          Practical essays on protocol evolution, clustered fanout, SDK design,
          recovery semantics, and operational tradeoffs in Sockudo.
        </p>
      </header>

      {featured ? (
        <Link className="blog-featured" href={featured.url}>
          <div>
            <div className="blog-meta">
              <span>{featured.category}</span>
              <span>
                <CalendarDays className="size-4" />
                {featured.date}
              </span>
            </div>
            <h2>{featured.title}</h2>
            <p>{featured.description}</p>
          </div>
          <span className="blog-read-link">
            Read article
            <ArrowRight className="size-4" />
          </span>
        </Link>
      ) : null}

      <div className="blog-grid" aria-label="All blog posts">
        {posts.map((post) => (
          <Link className="blog-tile" href={post.url} key={post.url}>
            <div className="blog-meta">
              <span>{post.category}</span>
              <span>
                <CalendarDays className="size-4" />
                {post.date}
              </span>
            </div>
            <h3>{post.title}</h3>
            <p>{post.description}</p>
            <span className="blog-read-link">
              Read article
              <ArrowRight className="size-4" />
            </span>
          </Link>
        ))}
      </div>
    </main>
  );
}
