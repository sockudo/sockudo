import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        "sockudo-with-encryption.worker": path.resolve(
          __dirname,
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
      runtime: path.resolve(__dirname, "src/runtimes/worker/runtime.ts"),
      core: path.resolve(__dirname, "src/core"),
      isomorphic: path.resolve(__dirname, "src/runtimes/isomorphic"),
      worker: path.resolve(__dirname, "src/runtimes/worker"),
    },
  },
  define: {
    RUNTIME: JSON.stringify("worker"),
    VERSION: JSON.stringify("8.4.9-next.0"),
    CDN_HTTP: JSON.stringify("//js.pusher.com/"),
    CDN_HTTPS: JSON.stringify("//js.pusher.com/"),
    DEPENDENCY_SUFFIX: JSON.stringify(""),
  },
});
