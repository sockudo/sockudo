import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    emptyOutDir: false,
    lib: {
      entry: {
        react: path.resolve(__dirname, "src/framework-react/index.ts"),
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
