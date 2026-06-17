import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/svelte/index.ts",
  fileBase: "svelte/index",
  name: "SockudoAITransportSvelte",
});
