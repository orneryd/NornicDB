import fs from "node:fs";
import path from "node:path";
const out = path.resolve(import.meta.dirname, "../.generated");
fs.copyFileSync(
  path.join(out, "index.md"),
  path.join(out, "documentation-hub.md"),
);
fs.writeFileSync(
  path.join(out, "index.md"),
  `---
layout: home
hero:
  name: NornicDB
  text: Query, search, and operate.
  tagline: Learn the database, write queries, and trace the implementation.
  actions:
    - theme: brand
      text: Quick start
      link: /start
    - theme: alt
      text: Explore the code
      link: /explore
features:
  - title: Query
    details: Create nodes and relationships. Read them through Cypher and Neo4j-compatible drivers.
    link: /user-guides/cypher-queries
  - title: Search
    details: Combine full-text and vector retrieval. Configure embeddings and inspect results.
    link: /user-guides/hybrid-search
  - title: Operate
    details: Configure authentication, monitor workloads, and back up your data.
    link: /operations/index
---
`,
);
fs.writeFileSync(
  path.join(out, "explore.md"),
  '---\nlayout: page\n---\n\n<div class="explore-page">\n\n# Explore the implementation\n\nFind a symbol, inspect its callers and callees, and follow its documentation references. Arrows point from caller to callee.\n\n<CodeExplorer />\n\n</div>\n',
);
fs.writeFileSync(path.join(out, "search.md"), "<SearchPage />\n");
fs.writeFileSync(
  path.join(out, "404.md"),
  "---\nlayout: page\n---\n\n<SearchPage />\n",
);
if (process.env.DOCS_VERSION) {
  const home = path.join(out, "index.md");
  fs.writeFileSync(
    home,
    fs
      .readFileSync(home, "utf8")
      .replace("link: /start", "link: /getting-started/quick-start"),
  );
}
