import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        react: path.resolve(import.meta.dirname, "src/framework-react/index.ts"),
      },
      formats: ["es"],
      fileName: () => "react.mjs",
    },
    outDir: "dist/web",
    sourcemap: true,
    minify: false,
    rollupOptions: {
      external: ["react"],
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
    VERSION: JSON.stringify("2.2.0"),
  },
});
