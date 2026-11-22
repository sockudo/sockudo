// Post-build guard: every wrapper must import a bundle that vite actually emitted.
// Requires `bun run build:all` first; the unit suite covers the static half.
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { checkEntrypoints } from "./entrypoints.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const { files, problems } = checkEntrypoints(root, { requireBuilt: true });

if (problems.length > 0) {
  for (const problem of problems) console.error(`entrypoint: ${problem}`);
  process.exit(1);
}

console.log(`entrypoints ok: ${files.length} wrappers resolve to built bundles`);