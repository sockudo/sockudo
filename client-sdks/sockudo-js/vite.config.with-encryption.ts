import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        "sockudo-with-encryption": path.resolve(
          __dirname,
          "src/core/sockudo-with-encryption.ts",
        ),
      },
      name: "Sockudo",
      formats: ["es"],
      fileName: () => "sockudo-with-encryption.mjs",
    },
    outDir: "dist/web",
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
      runtime: path.resolve(__dirname, "src/runtimes/web/runtime.ts"),
      core: path.resolve(__dirname, "src/core"),
      isomorphic: path.resolve(__dirname, "src/runtimes/isomorphic"),
      web: path.resolve(__dirname, "src/runtimes/web"),
    },
  },
  define: {
    global: "window",
    RUNTIME: JSON.stringify("web"),
    VERSION: JSON.stringify("8.4.9-next.0"),
    CDN_HTTP: JSON.stringify("//js.pusher.com/"),
    CDN_HTTPS: JSON.stringify("//js.pusher.com/"),
    DEPENDENCY_SUFFIX: JSON.stringify(""),
  },
});
