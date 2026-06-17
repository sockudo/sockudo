import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/vercel/svelte/index.ts",
  fileBase: "vercel/svelte/index",
  name: "SockudoAITransportVercelSvelte",
});
