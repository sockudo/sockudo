import { defineConfig } from "vite";
import path from "path";
import pkg from "./package.json" with { type: "json" };

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        "sockudo-with-encryption.worker": path.resolve(
          import.meta.dirname,
          "src/core/sockudo-with-encryption.ts",
        ),
      },
      name: "Sockudo",
      formats: ["es"],
      fileName: (format, entryName) => `${entryName}.js`,
    },
    outDir: "dist/worker",
    sourcemap: true,
    minify: false,
    rollupOptions: {
      external: ["tweetnacl"],
      onwarn(warning, warn) {
        if (warning.code === "CIRCULAR_DEPENDENCY") return;
        warn(warning);
      },
    },
  },
  resolve: {
    alias: {
      runtime: path.resolve(import.meta.dirname, "src/runtimes/worker/runtime.ts"),
      core: path.resolve(import.meta.dirname, "src/core"),
      isomorphic: path.resolve(import.meta.dirname, "src/runtimes/isomorphic"),
      worker: path.resolve(import.meta.dirname, "src/runtimes/worker"),
    },
  },
  define: {
    RUNTIME: JSON.stringify("worker"),
    VERSION: JSON.stringify(pkg.version),
  },
});
