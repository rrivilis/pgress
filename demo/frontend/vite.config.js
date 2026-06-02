import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Proxy /graph, /metrics, /op to the FastAPI backend so the frontend
// dev server (port 5173) and backend (port 8000) share the same origin.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/graph':   'http://localhost:8001',
      '/metrics': 'http://localhost:8001',
      '/op':      'http://localhost:8001',
    },
  },
});
