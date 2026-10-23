# Search procedure node materialization

Search procedures return logical nodes: ordinary stored properties, labels and
identity remain readable, including a vector stored as a user property. Managed
chunk and named embedding payloads remain internal and are not loaded for results.
Neo4j 5.26.30 returns the full stored node from both vector and fulltext index
procedures; reading `node.embedding` therefore returns a stored property when one
exists, and null when there is only an internal managed embedding.

## Owner and consumers

`pkg/search/search_result_materialization.go` owns the conversion from a stored
hit to a search result and from that result to a typed node. The result retains
the original read-time node metadata. `SearchResult.MaterializedNode` makes one
detached output body with the result's selected properties and labels. This keeps
creation/update times available to later property-visibility checks. Rebuilding a
node from only its ID, labels and properties loses that information and can cause
fresh properties to be treated as old under an enabled decay policy.

The consumers are:

- `db.retrieve` and `db.rretrieve`: regular rows, continuation page rows and grouped
  passages in `pkg/cypher/call_rag.go`.
- `db.retrieveGraph` and `db.rretrieveGraph`: ordinary rows and terminal graph pages
  through `graphSearchEntity` in `pkg/cypher/call_rag_graph.go`.
- Vector-only and fulltext-only hit enrichment through `enrichIndexResults`, and
  hybrid hit enrichment through `enrichResults`. Both use `hitNodes` and
  `searchResultFromNode`.
- Ranked, catalog and grouped continuations: compact retained rows discard the
  source node and property maps. Page hydration reads embedding-free bodies and
  uses the same result owner before the procedure adapter materializes nodes.

`db.index.vector.queryNodes` already reads complete storage nodes after ranking.
`db.index.fulltext.queryNodes` already returns complete nodes from its ranked and
wildcard paths. Neither rebuilds a metadata-free node. Relationship search and
relationship index procedures return relationship values; retrieval graph and
relationship procedures obtain their start/end nodes from storage directly.

`TestSearchProcedureNodeMaterializationOwner` scans production Cypher/search ASTs
and rejects a second node projection in search adapters or a second search-result
constructor carrying labels and properties. The repeatable shared regression
contract covers the public node-returning procedures, ordinary and graph rows,
continuation pages and grouped passages through Execute, Bolt and HTTP.

## Retention and visibility

Final hit reads retain the existing `BatchGetNodesWithoutEmbeddings` /
`GetNodeWithoutEmbeddings` contract. An embedding-bearing fallback read or an
explicit property projection retains only a metadata copy, with managed vectors
and excluded properties removed. The result cache accounts for the retained node
body and embedding metadata. Compact continuations retain no source node body.

This preserves property suppression: the result carries the selected visible
property map, and later normal-query reads still use the canonical visibility
owner with the original timestamps. It does not bypass decay checks or fetch a
newer node after ranking.
