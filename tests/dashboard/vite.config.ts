import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import tailwindcss from "@tailwindcss/vite";
import { fileURLToPath, URL } from "node:url";

export default defineConfig({
  plugins: [vue(), tailwindcss()],
  resolve: {
    alias: {
      runtime: fileURLToPath(
        new URL(
          "../../client-sdks/sockudo-js/src/runtimes/web/runtime.ts",
          import.meta.url,
        ),
      ),
      core: fileURLToPath(
        new URL("../../client-sdks/sockudo-js/src/core", import.meta.url),
      ),
      isomorphic: fileURLToPath(
        new URL(
          "../../client-sdks/sockudo-js/src/runtimes/isomorphic",
          import.meta.url,
        ),
      ),
      web: fileURLToPath(
        new URL(
          "../../client-sdks/sockudo-js/src/runtimes/web",
          import.meta.url,
        ),
      ),
      "@sockudo/client": fileURLToPath(
        new URL("../../client-sdks/sockudo-js/src/index.ts", import.meta.url),
      ),
    },
  },
  define: {
    global: "window",
    RUNTIME: JSON.stringify("web"),
    VERSION: JSON.stringify("dashboard-dev"),
    CDN_HTTP: JSON.stringify("//js.pusher.com/"),
    CDN_HTTPS: JSON.stringify("//js.pusher.com/"),
    DEPENDENCY_SUFFIX: JSON.stringify(""),
  },
  server: {
    port: 3456,
    open: true,
    proxy: {
      "/pusher": {
        target: "http://localhost:3457",
        changeOrigin: true,
      },
    },
  },
});
