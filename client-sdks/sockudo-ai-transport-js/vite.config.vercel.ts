import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/vercel/index.ts",
  fileBase: "vercel/index",
  name: "SockudoAITransportVercel",
});
