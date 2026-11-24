import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        sockudo: path.resolve(import.meta.dirname, "src/index.ts"),
      },
      name: "Sockudo",
      formats: ["es"],
      fileName: () => "sockudo.js",
    },
    outDir: "dist/node",
    sourcemap: true,
    minify: false,
    target: "node22",
    ssr: true,
    rollupOptions: {
      external: [
        "fossil-delta",
        "@ably/vcdiff-decoder",
        "tweetnacl",
        "crypto",
        "http",
        "https",
        "net",
        "tls",
        "fs",
        "stream",
        "child_process",
        "url",
        "zlib",
        "buffer",
        "events",
      ],
      onwarn(warning, warn) {
        if (warning.code === "CIRCULAR_DEPENDENCY") return;
        warn(warning);
      },
    },
  },
  resolve: {
    alias: {
      runtime: path.resolve(import.meta.dirname, "src/runtimes/node/runtime.ts"),
      core: path.resolve(import.meta.dirname, "src/core"),
      isomorphic: path.resolve(import.meta.dirname, "src/runtimes/isomorphic"),
      node: path.resolve(import.meta.dirname, "src/runtimes/node"),
      worker: path.resolve(import.meta.dirname, "src/runtimes/worker"),
    },
  },
  define: {
    RUNTIME: JSON.stringify("node"),
    VERSION: JSON.stringify("2.2.0"),
  },
});
