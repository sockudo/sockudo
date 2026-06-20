import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { PluginOption, UserConfig } from "vite";
import { defineConfig } from "vite";

const rootDir = dirname(fileURLToPath(import.meta.url));

const globals = {
  "@sockudo/client": "Sockudo",
  "@sockudo/client/react": "SockudoReact",
  "@sockudo/client/vue": "SockudoVue",
  ai: "AI",
  react: "React",
  svelte: "Svelte",
  "svelte/store": "SvelteStore",
  vue: "Vue",
} as const;

export interface LibraryEntry {
  readonly entry: string;
  readonly fileBase: string;
  readonly name: string;
  readonly plugins?: readonly PluginOption[];
}

export function createLibraryConfig(entry: LibraryEntry): UserConfig {
  return defineConfig({
    plugins: [...(entry.plugins ?? [])],
    define: {
      __SOCKUDO_AI_TRANSPORT_VERSION__: JSON.stringify(
        process.env.npm_package_version ?? "0.0.0-dev",
      ),
    },
    build: {
      emptyOutDir: false,
      lib: {
        entry: resolve(rootDir, entry.entry),
        formats: ["es", "umd"],
        name: entry.name,
        fileName: (format) =>
          format === "es" ? `${entry.fileBase}.js` : `${entry.fileBase}.umd.cjs`,
      },
      outDir: "dist",
      rollupOptions: {
        external: [
          "@sockudo/client",
          "@sockudo/client/react",
          "@sockudo/client/vue",
          "ai",
          "react",
          "svelte",
          "svelte/store",
          "vue",
        ],
        output: {
          globals,
          exports: "named",
        },
      },
      sourcemap: true,
      target: "es2022",
    },
  });
}
