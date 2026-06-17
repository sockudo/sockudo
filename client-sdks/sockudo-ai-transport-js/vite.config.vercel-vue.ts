import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/vercel/vue/index.ts",
  fileBase: "vercel/vue/index",
  name: "SockudoAITransportVercelVue",
});
