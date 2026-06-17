import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/providers/index.ts",
  fileBase: "providers/index",
  name: "SockudoAITransportProviders",
});
