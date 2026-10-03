---
type: architecture
title: NornicDB architecture
description: High-performance graph storage with Neo4j-compatible Cypher and Bolt interfaces.
tags: [graph, storage, cypher]
owner: core-team
---

# System design

NornicDB combines graph storage, vector retrieval, and fulltext search. The
[offline administration guide](../operations/admin-import.md "{type: DOCUMENTS, scope: import}")
describes how bundles enter a database namespace.

The [hybrid search design](../features/hybrid-search.md) combines vector and
BM25 retrieval. The [future migration](../drafts/future-migration.md) is
intentionally unresolved so an importer can preserve partial documentation.

The [hybrid-search reference][hybrid-reference] demonstrates a CommonMark
reference-style concept link.

[hybrid-reference]: ../features/hybrid-search.md "{type: REFERENCES, section: retrieval}"

```markdown
[A link in a code example](../operations/admin-import.md) is source text only.
```

External links such as [the project homepage](https://github.com/orneryd/NornicDB)
remain Markdown content rather than graph relationships.
