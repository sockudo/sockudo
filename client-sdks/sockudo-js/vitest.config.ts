import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: {
      runtime: path.resolve(__dirname, "src/runtimes/node/runtime.ts"),
      core: path.resolve(__dirname, "src/core"),
      isomorphic: path.resolve(__dirname, "src/runtimes/isomorphic"),
      web: path.resolve(__dirname, "src/runtimes/web"),
      worker: path.resolve(__dirname, "src/runtimes/worker"),
      "react-native": path.resolve(__dirname, "src/runtimes/react-native"),
      node: path.resolve(__dirname, "src/runtimes/node"),
    },
  },
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
  },
});
