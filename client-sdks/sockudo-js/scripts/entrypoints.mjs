import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";

const IMPORT_RE = /(?:from\s*|import\s*)["']([^"']+)["']/g;
const BUNDLE_RE = /^dist\/[a-z0-9-]+\/[^/]+\.m?js$/;

/**
 * Every hand-written entrypoint wrapper that ships in the tarball, derived from
 * package.json `files` so a new subpath is covered automatically.
 *
 * @param {string} root package root
 * @returns {string[]} wrapper paths relative to root
 */
export function collectWrapperFiles(root) {
  const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  const wrappers = [];

  for (const entry of pkg.files) {
    if (entry.endsWith(".js")) {
      wrappers.push(entry);
      continue;
    }
    if (entry.includes(".")) continue;

    const dir = join(root, entry);
    if (!existsSync(dir)) continue;
    for (const child of readdirSync(dir, { withFileTypes: true })) {
      if (child.isFile() && child.name.endsWith(".js")) {
        wrappers.push(`${entry}/${child.name}`);
      } else if (child.isDirectory() && existsSync(join(dir, child.name, "index.js"))) {
        wrappers.push(`${entry}/${child.name}/index.js`);
      }
    }
  }

  return wrappers.sort();
}

/**
 * Validate the wrappers against the bundles vite emits into `dist/`.
 *
 * @param {string} root package root
 * @param {{ requireBuilt?: boolean }} [options] also check the bundle exists on disk
 * @returns {{ files: string[], problems: string[] }}
 */
export function checkEntrypoints(root, { requireBuilt = false } = {}) {
  const files = collectWrapperFiles(root);
  const problems = [];

  for (const file of files) {
    const source = readFileSync(join(root, file), "utf8");
    const specifiers = [...source.matchAll(IMPORT_RE)]
      .map((match) => match[1])
      .filter((specifier) => specifier.startsWith("."));

    if (specifiers.length === 0) {
      problems.push(`${file}: imports no relative bundle`);
    }

    for (const specifier of specifiers) {
      const resolved = resolve(root, dirname(file), specifier);
      const rel = relative(root, resolved).split(sep).join("/");
      const base = rel.slice(rel.lastIndexOf("/") + 1);

      if (!BUNDLE_RE.test(rel)) {
        problems.push(`${file}: ${specifier} does not target a dist bundle (${rel})`);
      } else if (base.includes("pusher")) {
        problems.push(`${file}: ${specifier} still targets a stale pusher-* bundle`);
      } else if (requireBuilt && !existsSync(resolved)) {
        problems.push(`${file}: ${specifier} is not emitted by the build`);
      }
    }
  }

  return { files, problems };
}