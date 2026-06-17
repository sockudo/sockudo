import react from "@vitejs/plugin-react";
import { createLibraryConfig } from "./vite.config.shared.js";

export default createLibraryConfig({
  entry: "src/vercel/react/index.ts",
  fileBase: "vercel/react/index",
  name: "SockudoAITransportVercelReact",
  plugins: [react()],
});
