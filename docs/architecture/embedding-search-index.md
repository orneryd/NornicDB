# Embedding Search (Index)

This page is the entry point to NornicDB's embedding and vector-search documentation.

## Start here

- [Embedding Search Overview](embedding-search.md) — overview + key concepts.
- [Embedding & Search Architecture](embedding-search-architecture.md) — data model + execution paths.
- [Embedding & Search Flow Diagrams](embedding-search-flow-diagrams.md) — Mermaid diagrams.
- [Embedding & Search Examples](embedding-search-examples.md) — end-to-end examples.

## User-facing guides

- [Vector Embeddings](../features/vector-embeddings.md) — embedding generation.
- [Vector Search](../user-guides/vector-search.md) — hybrid search and RRF usage.
- [Qdrant gRPC](../user-guides/qdrant-grpc.md) — Qdrant compatibility layer.

## Agent-ready skills

- [Managed Embeddings](../skills/managed-embeddings.skill.md) — `WITH EMBEDDING`, `db.index.vector.embed`, provider config.
- [Vector & Full-Text Search](../skills/vector-search.skill.md) — `CREATE/DROP VECTOR INDEX`, `db.index.vector.queryNodes`.
- [RAG Procedures](../skills/rag-procedures.skill.md) — `db.retrieve`, `db.rerank`, `db.infer`.
- [gRPC (Qdrant + NornicSearch)](../skills/grpc.skill.md) — gRPC surface for vector ingestion and hybrid search.

## Implementation references

- [`pkg/search/search.go`](https://github.com/orneryd/nornicdb/blob/main/pkg/search/search.go) — unified search service + vector pipeline selection.
- [`pkg/qdrantgrpc/points_service.go`](https://github.com/orneryd/nornicdb/blob/main/pkg/qdrantgrpc/points_service.go) — Qdrant Points API mapping.
