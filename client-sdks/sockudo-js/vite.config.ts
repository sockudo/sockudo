import { defineConfig } from "vite";
import path from "path";
import pkg from "./package.json" with { type: "json" };

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        sockudo: path.resolve(import.meta.dirname, "src/index.ts"),
      },
      name: "Sockudo",
      formats: ["es"],
      fileName: () => "sockudo.mjs",
    },
    outDir: "dist/web",
    sourcemap: true,
    minify: false,
    rollupOptions: {
      output: {
        exports: "auto",
      },
      onwarn(warning, warn) {
        if (warning.code === "CIRCULAR_DEPENDENCY") return;
        warn(warning);
      },
    },
  },
  resolve: {
    alias: {
      runtime: path.resolve(import.meta.dirname, "src/runtimes/web/runtime.ts"),
      core: path.resolve(import.meta.dirname, "src/core"),
      isomorphic: path.resolve(import.meta.dirname, "src/runtimes/isomorphic"),
      web: path.resolve(import.meta.dirname, "src/runtimes/web"),
    },
  },
  define: {
    global: "window",
    RUNTIME: JSON.stringify("web"),
    VERSION: JSON.stringify(pkg.version),
  },
});
