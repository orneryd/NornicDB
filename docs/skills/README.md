# NornicDB Consumer Skills

This directory holds Claude Code-style skill files that teach an agent how to **use** NornicDB features through its Cypher API. They are consumer-facing — every example is a Cypher statement or `CALL` procedure a user can run, not a reference to repo-internal code.

The skills live alongside the user guides because they are user documentation: a structured, agent-friendly summary of the same surface that humans read in `docs/user-guides/`.

## Skills in this directory

| File | What it covers |
|---|---|
| [`cypher-queries.skill.md`](cypher-queries.skill.md) | Hot-path Cypher cookbook — point lookup, batch retrieval, pagination, search, traversal, batched UNWIND/MERGE writes, cleanup, multi-tenant isolation. Maps query shapes to executor fast paths. |
| [`bolt-client.skill.md`](bolt-client.skill.md) | Driving NornicDB from a Bolt client (`neo4j-go-driver/v5` or any other Neo4j driver) — connection defaults, retry classification, MERGE-under-concurrent-writers, batch sizing. |
| [`grpc.skill.md`](grpc.skill.md) | The Qdrant-compatible gRPC surface plus `NornicSearch.SearchText` — connection, RPC catalog, collection→database mapping, point→node mapping, limits, auth. |
| [`qdrant-migration.skill.md`](qdrant-migration.skill.md) | Migrating from Qdrant to NornicDB through the gRPC compat layer. Phase-by-phase plan, mapping reference, cutover playbook. Pairs with `scripts/migration/qdrant/`. |
| [`neo4j-migration.skill.md`](neo4j-migration.skill.md) | Migrating from Neo4j 5 to NornicDB over Bolt. Schema → nodes → edges with cookbook-pinned UNWIND/MERGE shapes. Pairs with `scripts/migration/neo4j/`. |
| [`knowledge-policies.skill.md`](knowledge-policies.skill.md) | The full DDL surface for decay profiles, parameter bundles, promotion profiles, and promotion policies. Diagnostics via `kp.*`. |
| [`decay-tuning.skill.md`](decay-tuning.skill.md) | Picking `halfLifeSeconds`, `function`, `scoreFloor`, `visibilityThreshold`, `scoreFrom`. Forgetting curves and inverted (idle-time consolidation) curves. |
| [`promotion-policies.skill.md`](promotion-policies.skill.md) | Reinforcement and dampening with `ON ACCESS` mutations, `WHEN` predicates, and Kalman-smoothed behavioral signals. |
| [`managed-embeddings.skill.md`](managed-embeddings.skill.md) | Server-side embeddings: `WITH EMBEDDING`, `db.index.vector.embed`, the `ChunkEmbeddings` storage model, provider config. |
| [`vector-search.skill.md`](vector-search.skill.md) | Vector and full-text indexes — `CREATE/DROP VECTOR INDEX`, `CREATE/DROP FULLTEXT INDEX`, `db.index.vector.queryNodes`, `db.index.fulltext.queryNodes`. |
| [`rag-procedures.skill.md`](rag-procedures.skill.md) | `db.retrieve`, `db.rretrieve`, `db.rerank`, `db.infer` — assembling end-to-end RAG pipelines as Cypher. |

## How to use them with Claude Code

Each file is a self-contained Markdown skill with a YAML frontmatter (`name`, `description`). Make Claude Code aware of them by either:

1. **Project skills:** symlink or copy the desired files into `.claude/skills/<name>/SKILL.md` at the project root. Claude Code auto-loads project skills.
2. **User skills:** copy them into `~/.claude/skills/<name>/SKILL.md` to enable everywhere.
3. **Direct reference:** read or `@`-mention the file from inside any conversation; the YAML frontmatter still tells Claude when each one is relevant.

The mental model in every skill is Neo4j Cypher with NornicDB extensions noted explicitly. Nothing here documents internal storage mechanics — those belong in `docs/architecture/`.

## See also

- [`docs/user-guides/`](../user-guides/README.md) — long-form human documentation for the same features.
- [`docs/features/`](../features/README.md) — feature overviews and architecture references.
- [`docs/operations/configuration.md`](../operations/configuration.md) — environment variable and YAML configuration reference.
