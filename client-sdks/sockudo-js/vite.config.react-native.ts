import { defineConfig } from "vite";
import path from "path";
import pkg from "./package.json" with { type: "json" };

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
      runtime: path.resolve(import.meta.dirname, "src/runtimes/react-native/runtime.ts"),
      core: path.resolve(import.meta.dirname, "src/core"),
      isomorphic: path.resolve(import.meta.dirname, "src/runtimes/isomorphic"),
      "react-native": path.resolve(import.meta.dirname, "src/runtimes/react-native"),
    },
  },
  define: {
    RUNTIME: JSON.stringify("react-native"),
    VERSION: JSON.stringify(pkg.version),
  },
});
