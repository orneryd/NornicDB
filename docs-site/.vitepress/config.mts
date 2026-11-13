import { defineConfig } from "vitepress";
import fs from "node:fs";
import path from "node:path";
const group = (text: string, dir: string) => ({
  text,
  collapsed: true,
  items: (fs.existsSync(path.resolve(".generated", dir))
    ? fs.readdirSync(path.resolve(".generated", dir))
    : []
  )
    .filter((f) => f.endsWith(".md"))
    .map((f) => ({
      text: f.replace(/\.md$/, "").replaceAll("-", " "),
      link: "/" + dir + "/" + f.replace(/\.md$/, ""),
    })),
});
const start = process.env.DOCS_VERSION
  ? "/getting-started/quick-start"
  : "/start";
const section = (text: string, items: [string, string][]) => ({
  text,
  collapsed: true,
  items: items
    .filter(([, link]) =>
      fs.existsSync(path.resolve(".generated", "." + link + ".md")),
    )
    .map(([text, link]) => ({ text, link })),
});

export default defineConfig({
  title: "NornicDB",
  appearance: "dark",
  description: "NornicDB guides, query reference, and implementation.",
  srcDir: ".generated",
  base: process.env.DOCS_BASE || "/NornicDB/",
  cleanUrls: true,
  ignoreDeadLinks: [
    /^https?:\/\/(localhost|127\.0\.0\.1)(:|\/|$)/,
    (url, file) => {
      const candidate = url.startsWith("/")
        ? path.resolve(".generated/public", "." + url)
        : path.resolve(
            ".generated/public",
            path.relative(path.resolve(".generated"), path.dirname(file)),
            url,
          );
      return (
        candidate.startsWith(path.resolve(".generated/public") + path.sep) &&
        fs.existsSync(candidate) &&
        fs.statSync(candidate).isFile()
      );
    },
  ],
  locales: { root: { label: "English", lang: "en" } },
  vite: {
    server: {
      host: "127.0.0.1",
      port: 4173,
      strictPort: true,
      proxy: { "/NornicDB/api": "http://127.0.0.1:4174" },
    },
  },
  markdown: {
    html: true,
    config(md) {
      const inline = md.renderer.rules.html_inline!,
        block = md.renderer.rules.html_block!;
      const allowed =
        /^(?:<\/?(?:a|img|br|hr|details|summary|div|span|p|table|thead|tbody|tr|td|th|sup|sub|kbd|strong|em|code|pre|ul|li|ol)\b|<!--|<(?:CodeExplorer|SearchPage|InquiryForm)\s*\/?>)/i;
      md.renderer.rules.html_inline = (tokens, i, options, env, self) =>
        allowed.test(tokens[i].content) || !/^\s*</.test(tokens[i].content)
          ? inline(tokens, i, options, env, self)
          : md.utils.escapeHtml(tokens[i].content);
      md.renderer.rules.html_block = (tokens, i, options, env, self) =>
        allowed.test(tokens[i].content) || !/^\s*</.test(tokens[i].content)
          ? block(tokens, i, options, env, self)
          : md.utils.escapeHtml(tokens[i].content);
      const fence = md.renderer.rules.fence!;
      md.renderer.rules.fence = (tokens, idx, options, env, self) =>
        tokens[idx].info.trim() === "mermaid"
          ? `<Diagram source="${md.utils.escapeHtml(tokens[idx].content)}" />`
          : fence(tokens, idx, options, env, self);
    },
  },
  themeConfig: {
    outline: [2, 3],
    nav: [
      { text: "Start", link: start },
      { text: "Guides", link: "/user-guides/index" },
      { text: "Reference", link: "/api-reference/index" },
      { text: "Performance", link: "/performance/index" },
      { text: "Code", link: "/explore" },
    ],
    sidebar: [
      section("Start here", [
        ["Quick start", start],
        ["Install NornicDB", "/getting-started/installation"],
        ["Choose a Docker image", "/getting-started/image-quick-reference"],
        ["Write your first queries", "/getting-started/first-queries"],
      ]),
      section("Query and model data", [
        ["Cypher queries", "/user-guides/cypher-queries"],
        ["Graph traversal", "/user-guides/graph-traversal"],
        ["Property types", "/user-guides/property-data-types"],
        ["Transactions", "/user-guides/transactions"],
        ["Constraints", "/user-guides/managing-constraints"],
        ["Import and export", "/user-guides/data-import-export"],
      ]),
      section("Temporal patterns", [
        ["Overview", "/temporal-patterns/index"],
        ["Epoch records", "/user-guides/canonical-graph-ledger"],
        ["Independent constraints", "/user-guides/managing-constraints"],
        [
          "Retained as-of history",
          "/user-guides/historical-reads-mvcc-retention",
        ],
        [
          "Read and constraint costs",
          "/performance/temporal-read-and-constraint-costs",
        ],
      ]),
      section("Search and retrieval", [
        ["Hybrid search", "/user-guides/hybrid-search"],
        ["BM25 stemming", "/user-guides/bm25-stemmers"],
        ["Vector search", "/user-guides/vector-search"],
        ["Embeddings", "/features/vector-embeddings"],
        ["RAG procedures", "/user-guides/cypher-rag-procedures"],
        ["Search continuation", "/user-guides/search-continuation"],
      ]),
      section("Reference", [
        ["API reference", "/api-reference/index"],
        ["Cypher functions", "/api-reference/cypher-functions/index"],
        ["Bolt protocol", "/api-reference/bolt-protocol"],
        ["Environment variables", "/operations/environment-variables"],
        ["OpenAPI", "/api-reference/openapi"],
      ]),
      section("Operate", [
        ["Configuration", "/operations/configuration"],
        ["Authentication and roles", "/compliance/rbac"],
        ["Monitoring", "/operations/monitoring"],
        ["Durability and recovery", "/operations/durability"],
        ["WAL lifecycle", "/operations/wal-compaction"],
        ["Backup and restore", "/operations/backup-restore"],
        ["Memory accounting and limits", "/operations/memory-management"],
        ["Low-memory profile", "/operations/low-memory-mode"],
        ["Transaction accounting", "/operations/transaction-memory-limits"],
        ["Troubleshooting", "/operations/troubleshooting"],
      ]),
      group("Architecture", "architecture"),
      section("Performance", [
        ["Benchmark reports", "/performance/index"],
        [
          "Database comparison",
          "/performance/benchmarks/database-comparison/sweep",
        ],
        [
          "Shortest path · 3.97M edges",
          "/performance/benchmarks/shortest-path",
        ],
        ["Parser scaling", "/performance/benchmarks/parser-scaling/report"],
        [
          "Northwind parser modes",
          "/performance/benchmarks/database-comparison/parser-modes",
        ],
        ["Retrieval quality", "/performance/retrieval-recall-benchmark"],
      ]),
      section("Development", [
        ["Build an application", "/development/index"],
        ["Embed in Go", "/development/embedding"],
        ["Canonical APIs", "/api-reference/index"],
        ["Contribute", "/development/contributing"],
        ["Build setup", "/development/setup"],
      ]),
      { text: "Full documentation library", link: "/library" },
      ...(fs.existsSync(path.resolve('.generated/managed-services.md'))
        ? [{ text: 'Managed services', link: '/managed-services' }] : []),
    ],
    socialLinks: [
      { icon: "github", link: "https://github.com/orneryd/NornicDB" },
    ],
  },
});
