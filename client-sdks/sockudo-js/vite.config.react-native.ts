import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  build: {
    lib: {
      entry: {
        sockudo: path.resolve(__dirname, "src/index.ts"),
      },
      name: "Sockudo",
      formats: ["es"],
      fileName: (format, entryName) => `${entryName}.js`,
    },
    outDir: "dist/react-native",
    sourcemap: true,
    minify: false,
    rollupOptions: {
      external: ["@react-native-community/netinfo", "tweetnacl"],
      onwarn(warning, warn) {
        if (warning.code === "CIRCULAR_DEPENDENCY") return;
        warn(warning);
      },
    },
  },
  resolve: {
    alias: {
      runtime: path.resolve(__dirname, "src/runtimes/react-native/runtime.ts"),
      core: path.resolve(__dirname, "src/core"),
      isomorphic: path.resolve(__dirname, "src/runtimes/isomorphic"),
      "react-native": path.resolve(__dirname, "src/runtimes/react-native"),
    },
  },
  define: {
    RUNTIME: JSON.stringify("react-native"),
    VERSION: JSON.stringify("8.4.9-next.0"),
    CDN_HTTP: JSON.stringify("//js.pusher.com/"),
    CDN_HTTPS: JSON.stringify("//js.pusher.com/"),
    DEPENDENCY_SUFFIX: JSON.stringify(""),
  },
});
