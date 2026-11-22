import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        filter: path.resolve(__dirname, "src/filter.ts"),
      },
      name: "SockudoFilter",
      formats: ["es"],
      fileName: () => "filter.js",
    },
    outDir: "dist/node",
    sourcemap: true,
    minify: false,
    target: "node22",
    ssr: true,
  },
  resolve: {
    alias: {
      core: path.resolve(__dirname, "src/core"),
    },
  },
});
