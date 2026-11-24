import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    lib: {
      entry: {
        sockudo: path.resolve(import.meta.dirname, "src/index.ts"),
      },
      name: "Sockudo",
      formats: ["es"],
      fileName: (format, entryName) => `${entryName}.js`,
    },
    outDir: "dist/nativescript",
    sourcemap: true,
    minify: false,
    rollupOptions: {
      external: ["@valor/nativescript-websockets", "tweetnacl"],
      onwarn(warning, warn) {
        if (warning.code === "CIRCULAR_DEPENDENCY") return;
        warn(warning);
      },
    },
  },
  resolve: {
    alias: {
      runtime: path.resolve(import.meta.dirname, "src/runtimes/nativescript/runtime.ts"),
      core: path.resolve(import.meta.dirname, "src/core"),
      isomorphic: path.resolve(import.meta.dirname, "src/runtimes/isomorphic"),
    },
  },
  define: {
    RUNTIME: JSON.stringify("nativescript"),
    VERSION: JSON.stringify("2.2.0"),
  },
});
