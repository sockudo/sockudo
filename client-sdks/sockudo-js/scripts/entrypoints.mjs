import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";

const IMPORT_RE = /(?:from\s*|import\s*)["']([^"']+)["']/g;
const BUNDLE_RE = /^dist\/[a-z0-9-]+\/[^/]+\.m?js$/;
// Published directories that hold build output or declarations, not wrappers.
const NON_WRAPPER_DIRS = new Set(["dist", "types"]);

/**
 * Relative specifiers imported or re-exported by a wrapper source.
 *
 * @param {string} source wrapper file contents
 * @returns {string[]}
 */
export function importSpecifiers(source) {
  return [...source.matchAll(IMPORT_RE)]
    .map((match) => match[1])
    .filter((specifier) => specifier.startsWith("."));
}

/**
 * Walk published directories so a wrapper nested at any depth is covered.
 *
 * @param {string} root package root
 * @param {string} dir directory relative to root
 * @param {string[]} out collected wrapper paths
 */
function collectJsFiles(root, dir, out) {
  for (const child of readdirSync(join(root, dir), { withFileTypes: true })) {
    const rel = `${dir}/${child.name}`;
    if (child.isDirectory()) {
      collectJsFiles(root, rel, out);
    } else if (child.isFile() && child.name.endsWith(".js")) {
      out.push(rel);
    }
  }
}

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

  for (const raw of pkg.files) {
    const entry = raw.replace(/^\.\//, "").replace(/\/$/, "");
    if (entry.endsWith(".js")) {
      wrappers.push(entry);
      continue;
    }
    if (entry.includes(".") || NON_WRAPPER_DIRS.has(entry)) continue;

    const dir = join(root, entry);
    if (!existsSync(dir)) continue;
    collectJsFiles(root, entry, wrappers);
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
    const specifiers = importSpecifiers(source);

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
