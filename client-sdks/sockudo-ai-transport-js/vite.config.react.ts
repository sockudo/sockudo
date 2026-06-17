import react from "@vitejs/plugin-react";
import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/react/index.ts",
  fileBase: "react/index",
  name: "SockudoAITransportReact",
  plugins: [react()],
});
