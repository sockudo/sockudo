import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  define: {
    RUNTIME: JSON.stringify("node"),
    VERSION: JSON.stringify("2.2.0"),
  },
  resolve: {
    alias: {
      runtime: path.resolve(import.meta.dirname, "src/runtimes/node/runtime.ts"),
      core: path.resolve(import.meta.dirname, "src/core"),
      isomorphic: path.resolve(import.meta.dirname, "src/runtimes/isomorphic"),
      web: path.resolve(import.meta.dirname, "src/runtimes/web"),
      worker: path.resolve(import.meta.dirname, "src/runtimes/worker"),
      "react-native": path.resolve(import.meta.dirname, "src/runtimes/react-native"),
      node: path.resolve(import.meta.dirname, "src/runtimes/node"),
    },
  },
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
  },
});
