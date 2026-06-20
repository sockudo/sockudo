import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const packageJson = require("../package.json");

const entrypoints = [
  "@sockudo/ai-transport",
  "@sockudo/ai-transport/react",
  "@sockudo/ai-transport/vue",
  "@sockudo/ai-transport/svelte",
  "@sockudo/ai-transport/vercel",
  "@sockudo/ai-transport/vercel/react",
  "@sockudo/ai-transport/vercel/vue",
  "@sockudo/ai-transport/vercel/svelte",
  "@sockudo/ai-transport/providers",
];

for (const entrypoint of entrypoints) {
  const esmModule = await import(entrypoint);
  const cjsModule = require(entrypoint);

  if (esmModule.version !== packageJson.version || cjsModule.version !== packageJson.version) {
    throw new Error(`Unexpected version export from ${entrypoint}`);
  }
}
