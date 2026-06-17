import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["bench/**/*.bench.ts"],
    globals: false,
    pool: "threads",
    testTimeout: 120_000,
    hookTimeout: 120_000,
    isolate: true,
    coverage: { enabled: false },
  },
});
