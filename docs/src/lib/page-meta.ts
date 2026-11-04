import { execFileSync } from 'node:child_process';

declare const __DOCS_ROOT__: string;

/** Reading time and last-commit date for a content file, computed at build time. */
export function pageMeta(body: string, filePath?: string) {
  const prose = body.replace(/```[\s\S]*?```/g, ' ').replace(/<[^>]+>/g, ' ');
  const words = prose.split(/\s+/).filter(Boolean).length;
  const minutes = Math.max(1, Math.round(words / 230));
  let updated: string | undefined;
  if (filePath) {
    try {
      const iso = execFileSync('git', ['log', '-1', '--format=%cI', '--', filePath], { encoding: 'utf8', cwd: __DOCS_ROOT__ }).trim();
      if (iso) updated = new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    } catch {
      // Not a git checkout (e.g. a tarball build): omit the date.
    }
  }
  return { minutes, updated };
}
