import { readFile } from "node:fs/promises";

const files = [
  "dist/index.d.ts",
  "dist/react/index.d.ts",
  "dist/vue/index.d.ts",
  "dist/svelte/index.d.ts",
  "dist/vercel/index.d.ts",
  "dist/vercel/react/index.d.ts",
  "dist/vercel/vue/index.d.ts",
  "dist/vercel/svelte/index.d.ts",
  "dist/providers/index.d.ts",
];

const declarationPattern =
  /(?<doc>\/\*\*[\s\S]*?\*\/\s*)?export\s+(?:declare\s+)?(?<kind>class|interface|enum|function|const|type)\s+(?<name>[A-Za-z_$][\w$]*)/gu;

const missing = [];

for (const file of files) {
  const content = await readFile(file, "utf8");
  for (const match of content.matchAll(declarationPattern)) {
    const doc = match.groups?.doc;
    const kind = match.groups?.kind;
    const name = match.groups?.name;
    if (doc === undefined && kind !== undefined && name !== undefined) {
      missing.push(`${file}: ${kind} ${name}`);
    }
  }
}

if (missing.length > 0) {
  console.error("Exported declarations missing TSDoc:");
  for (const item of missing) {
    console.error(`- ${item}`);
  }
  process.exitCode = 1;
}
