import { fileURLToPath } from "node:url";

export default defineNuxtConfig({
  compatibilityDate: "2026-06-17",
  css: ["~/assets/css/main.css"],
  devtools: { enabled: false },
  ssr: false,
  nitro: {
    experimental: {
      asyncContext: true,
    },
  },
  typescript: {
    strict: true,
    typeCheck: false,
  },
  vite: {
    optimizeDeps: {
      include: ["@ai-sdk/vue", "@sockudo/client", "@sockudo/client/vue"],
    },
    resolve: {
      alias: [
        {
          find: /^@sockudo\/ai-transport$/,
          replacement: fileURLToPath(new URL("../src/index.ts", import.meta.url)),
        },
        {
          find: /^@sockudo\/ai-transport\/vercel$/,
          replacement: fileURLToPath(new URL("../src/vercel/index.ts", import.meta.url)),
        },
        {
          find: /^@sockudo\/ai-transport\/vercel\/vue$/,
          replacement: fileURLToPath(new URL("../src/vercel/vue/index.ts", import.meta.url)),
        },
      ],
    },
  },
});
