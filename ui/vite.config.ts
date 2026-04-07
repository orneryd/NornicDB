import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

function normalizeViteBasePath(raw?: string): string {
  const value = (raw || '').trim();
  if (value === '' || value === '/') {
    return '/';
  }
  const withLeading = value.startsWith('/') ? value : `/${value}`;
  return withLeading.endsWith('/') ? withLeading : `${withLeading}/`;
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const basePath = normalizeViteBasePath(env.VITE_BASE_PATH);
  
  return {
    plugins: [react()],
    base: basePath, // Handles both /foo and /foo/ input forms
    build: {
      outDir: 'dist',
      assetsDir: 'assets',
    },
    server: {
      port: 5174,
      proxy: {
        // Proxy API requests to NornicDB server
        '/api': {
          target: 'http://localhost:7474',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api/, ''),
        },
        '/health': {
          target: 'http://localhost:7474',
          changeOrigin: true,
        },
        '/status': {
          target: 'http://localhost:7474',
          changeOrigin: true,
        },
        '/db': {
          target: 'http://localhost:7474',
          changeOrigin: true,
        },
        '/auth': {
          target: 'http://localhost:7474',
          changeOrigin: true,
        },
        '/nornicdb': {
          target: 'http://localhost:7474',
          changeOrigin: true,
        },
        '/admin': {
          target: 'http://localhost:7474',
          changeOrigin: true,
        },
      },
    },
  };
});
