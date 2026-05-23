import { blog } from 'collections/server';

export type BlogPost = (typeof blog)[number] & {
  slug: string[];
  url: string;
};

export const blogPosts = blog
  .map((post) => {
    const slug = post.info.path.replace(/\.mdx$/, '').split('/');

    return {
      ...post,
      slug,
      url: `/blog/${slug.join('/')}`,
    } satisfies BlogPost;
  })
  .sort((a, b) => b.date.localeCompare(a.date));

export function getBlogPost(slug?: string[]) {
  const target = slug?.join('/') ?? '';
  return blogPosts.find((post) => post.slug.join('/') === target);
}
