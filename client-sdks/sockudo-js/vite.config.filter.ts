import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        filter: path.resolve(__dirname, "src/filter.ts"),
      },
      name: "PusherFilter",
      formats: ["es"],
      fileName: () => "filter.mjs",
    },
    outDir: "dist/web",
    sourcemap: true,
    minify: false,
  },
  resolve: {
    alias: {
      core: path.resolve(__dirname, "src/core"),
    },
  },
});
