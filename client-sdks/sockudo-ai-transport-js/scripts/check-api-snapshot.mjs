import { readFile } from "node:fs/promises";

const entries = [
  ["core", "dist/index.d.ts"],
  ["react", "dist/react/index.d.ts"],
  ["vue", "dist/vue/index.d.ts"],
  ["svelte", "dist/svelte/index.d.ts"],
  ["vercel", "dist/vercel/index.d.ts"],
  ["vercel/react", "dist/vercel/react/index.d.ts"],
  ["vercel/vue", "dist/vercel/vue/index.d.ts"],
  ["vercel/svelte", "dist/vercel/svelte/index.d.ts"],
  ["providers", "dist/providers/index.d.ts"],
];

const snapshotPath = "etc/api-snapshot.d.ts";

const normalize = (content) => content.replaceAll("\r\n", "\n").trimEnd();

const actualSnapshot = (
  await Promise.all(
    entries.map(async ([name, path]) => {
      const content = await readFile(path, "utf8");
      return `// ${name}\n${normalize(content)}`;
    }),
  )
).join("\n\n");

const expectedSnapshot = await readFile(snapshotPath, "utf8").then(normalize);

if (actualSnapshot !== expectedSnapshot) {
  console.error(
    `Public API snapshot mismatch. Update ${snapshotPath} intentionally if the API changed.`,
  );
  console.error("--- expected");
  console.error(expectedSnapshot);
  console.error("--- actual");
  console.error(actualSnapshot);
  process.exitCode = 1;
}
