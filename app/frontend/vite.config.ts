import { defineConfig } from "vite";

// No @vitejs/plugin-react: the internal npm proxy only serves canary
// react-refresh, so we use Vite's built-in esbuild JSX transform (automatic
// runtime) instead. Dev does full-reload instead of Fast Refresh — fine here.
export default defineConfig({
  esbuild: {
    jsx: "automatic",
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
