import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/index.ts",
  fileBase: "index",
  name: "SockudoAITransport",
});
