import { readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const writeMode = process.argv.includes("--write");
const snippetStart = /^\s*\/\/\s*@docs-snippet\s+([a-z0-9-]+)\s*$/u;
const snippetEnd = /^\s*\/\/\s*@docs-snippet-end\s*$/u;

const snippets = new Map();
for (const file of await listFiles(join(root, "demo"))) {
  if (
    !/\.(?:ts|tsx|js|mjs|vue)$/u.test(file) ||
    file.includes("node_modules") ||
    file.includes(`${sep}.next${sep}`) ||
    file.includes(`${sep}dist${sep}`)
  ) {
    continue;
  }
  const text = await readFile(file, "utf8");
  const lines = text.split(/\r?\n/u);
  let active;
  let buffer = [];
  for (const line of lines) {
    const start = snippetStart.exec(line);
    if (start) {
      if (active !== undefined) {
        throw new Error(`nested docs snippet in ${file}`);
      }
      active = start[1];
      buffer = [];
      continue;
    }
    if (snippetEnd.test(line)) {
      if (active === undefined) {
        throw new Error(`orphan docs snippet end in ${file}`);
      }
      if (snippets.has(active)) {
        throw new Error(`duplicate docs snippet id ${active}`);
      }
      snippets.set(active, {
        file: relative(root, file),
        code: trimBlankEdges(buffer).join("\n"),
      });
      active = undefined;
      buffer = [];
      continue;
    }
    if (active !== undefined) {
      buffer.push(line);
    }
  }
  if (active !== undefined) {
    throw new Error(`unclosed docs snippet ${active} in ${file}`);
  }
}

if (snippets.size === 0) {
  throw new Error("no docs snippets found under demo/");
}

const generated = renderGenerated(snippets);
const generatedPath = join(root, "docs/snippets/generated.md");
if (writeMode) {
  await writeFile(generatedPath, generated);
} else {
  const current = await readFile(generatedPath, "utf8");
  if (current !== generated) {
    throw new Error("docs/snippets/generated.md is stale; run pnpm docs:snippets:update");
  }
}

const docsText = await readDocs(join(root, "docs"));
for (const id of snippets.keys()) {
  if (!docsText.includes(`snippet:${id}`)) {
    throw new Error(`snippet ${id} is not referenced from docs/`);
  }
}

function renderGenerated(map) {
  const chunks = [
    "# Generated Demo Snippets",
    "",
    "Do not edit by hand. Run `pnpm docs:snippets:update` after changing demo snippet markers.",
    "",
    "<!-- prettier-ignore-start -->",
  ];
  for (const [id, snippet] of [...map.entries()].sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    const language = snippet.file.endsWith(".tsx")
      ? "tsx"
      : snippet.file.endsWith(".vue")
        ? "vue"
        : "ts";
    chunks.push(
      "",
      `## ${id}`,
      "",
      `Source: \`${snippet.file}\``,
      "",
      `<!-- snippet:${id} -->`,
      "",
      `\`\`\`${language}`,
      snippet.code,
      "```",
    );
  }
  chunks.push("", "<!-- prettier-ignore-end -->", "");
  return chunks.join("\n");
}

async function listFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listFiles(path)));
    } else {
      files.push(path);
    }
  }
  return files;
}

async function readDocs(dir) {
  const parts = [];
  for (const file of await listFiles(dir)) {
    if (file.endsWith(".md") && !file.endsWith("generated.md")) {
      parts.push(await readFile(file, "utf8"));
    }
  }
  return parts.join("\n");
}

function trimBlankEdges(lines) {
  let start = 0;
  let end = lines.length;
  while (start < end && lines[start]?.trim() === "") {
    start += 1;
  }
  while (end > start && lines[end - 1]?.trim() === "") {
    end -= 1;
  }
  return lines.slice(start, end);
}
