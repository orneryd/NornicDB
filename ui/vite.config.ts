import { defineConfig, loadEnv, type Plugin } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';

function normalizeViteBasePath(raw?: string): string {
  const value = (raw || '').trim();
  if (value === '' || value === '/') {
    return '/';
  }
  const withLeading = value.startsWith('/') ? value : `/${value}`;
  return withLeading.endsWith('/') ? withLeading : `${withLeading}/`;
}

// neo4j-driver-bolt-connection ships separate node/ and browser/
// channel implementations; only the browser/ one knows about ws:// URLs.
// The package's "browser" object-map field is the canonical swap, but
// Rolldown (under Vite 8) doesn't honor the object form. We hook the
// resolveId step: when something resolves to .../channel/node/index.js
// inside that package, redirect to the .../channel/browser/index.js
// sibling. Without this every Cypher query fails with
// "Unknown scheme: ws".
function neo4jBrowserChannelPlugin(): Plugin {
  const matcher = /neo4j-driver-bolt-connection\/lib\/channel\/node(?:\/index\.js)?$/;
  return {
    name: 'nornicdb:neo4j-browser-channel',
    enforce: 'pre',
    async resolveId(source, importer, options) {
      if (!matcher.test(source) && !source.endsWith('./node')) {
        return null;
      }
      // Resolve through the default pipeline first so we get an
      // absolute path; then check it lives under the bolt-connection
      // package and redirect to the browser sibling.
      const resolved = await this.resolve(source, importer, { ...options, skipSelf: true });
      if (!resolved || !resolved.id.includes('neo4j-driver-bolt-connection')) {
        return null;
      }
      if (!resolved.id.includes('/channel/node/')) {
        return null;
      }
      const swapped = resolved.id.replace('/channel/node/', '/channel/browser/');
      return { ...resolved, id: swapped };
    },
  };
}

// nodeShimPlugin returns empty stubs for genuinely Node-only built-ins
// (net, tls, fs, ...) that the bolt-connection node channel pulls in.
// After neo4jBrowserChannelPlugin redirects to the browser channel
// these aren't reached, but Rolldown's static analysis still scans the
// node channel's transitive deps when resolving. The shim short-circuits
// those to harmless empty modules so the build doesn't fail on missing
// node built-ins.
//
// IMPORTANT: do NOT shim string_decoder or buffer here — those are
// real npm packages that work in browsers and are imported by
// channel/utf8.js (shared between node and browser channels). Stubbing
// them produces "StringDecoder is not a constructor" at runtime.
function nodeShimPlugin(): Plugin {
  const builtins = new Set(['net', 'tls', 'fs', 'os', 'path', 'crypto']);
  const empty = path.resolve(__dirname, 'src/utils/empty-shim.ts');
  return {
    name: 'nornicdb:node-shim',
    enforce: 'pre',
    resolveId(source) {
      if (builtins.has(source) || (source.startsWith('node:') && builtins.has(source.slice(5)))) {
        return empty;
      }
      return null;
    },
  };
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const basePath = normalizeViteBasePath(env.VITE_BASE_PATH);
  
  return {
    plugins: [neo4jBrowserChannelPlugin(), nodeShimPlugin(), react()],
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
          target: 'http://localhost:7475',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api/, ''),
        },
        '/db': {
          target: 'http://localhost:7475',
          changeOrigin: true,
        },
        '/auth': {
          target: 'http://localhost:7475',
          changeOrigin: true,
        },
        '/nornicdb': {
          target: 'http://localhost:7475',
          changeOrigin: true,
        },
        '/admin': {
          target: 'http://localhost:7475',
          changeOrigin: true,
        },
      },
    },
  };
});
