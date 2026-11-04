export const repo = { owner: 'sockudo', name: 'sockudo' };
export const repoUrl = `https://github.com/${repo.owner}/${repo.name}`;

// Used when the build has no network; refreshed client-side on every visit anyway.
const FALLBACK_STARS = 843;

let starsPromise: Promise<number> | undefined;

/** Star count fetched once per build. */
export function getStars(): Promise<number> {
  starsPromise ??= (async () => {
    try {
      const headers: Record<string, string> = { Accept: 'application/vnd.github+json' };
      if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
      const res = await fetch(`https://api.github.com/repos/${repo.owner}/${repo.name}`, {
        headers,
        signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) return FALLBACK_STARS;
      const json = (await res.json()) as { stargazers_count?: number };
      return json.stargazers_count ?? FALLBACK_STARS;
    } catch {
      return FALLBACK_STARS;
    }
  })();
  return starsPromise;
}

export function formatStars(n: number): string {
  return n >= 1000 ? `${(n / 1000).toFixed(n >= 10000 ? 0 : 1)}k` : String(n);
}
