import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/vue/index.ts",
  fileBase: "vue/index",
  name: "SockudoAITransportVue",
});
