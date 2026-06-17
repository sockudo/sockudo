import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      enabled: true,
      provider: "v8",
      include: [
        "src/errors.ts",
        "src/event-emitter.ts",
        "src/logger.ts",
        "src/utils.ts",
        "src/constants.ts",
      ],
      reporter: ["text", "lcov"],
      thresholds: {
        branches: 100,
        functions: 100,
        lines: 100,
        statements: 100,
      },
    },
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});
