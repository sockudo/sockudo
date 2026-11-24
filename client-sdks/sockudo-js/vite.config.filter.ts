import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        filter: path.resolve(import.meta.dirname, "src/filter.ts"),
      },
      name: "SockudoFilter",
      formats: ["es"],
      fileName: () => "filter.mjs",
    },
    outDir: "dist/web",
    sourcemap: true,
    minify: false,
  },
  resolve: {
    alias: {
      core: path.resolve(import.meta.dirname, "src/core"),
    },
  },
});
