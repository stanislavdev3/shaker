import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Build output is embedded by the Go binary (//go:embed web/dist).
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/httpapi/web/dist",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/v1": { target: "http://localhost:8080", changeOrigin: true, ws: true },
    },
  },
});
