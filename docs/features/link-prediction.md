# Link Prediction Integration Guide

## Overview

NornicDB includes topological link prediction to complement its existing semantic inference engine, in two forms: **GDS's `gds.alpha.linkprediction.*` functions**, with the values GDS 2.13 gives, and **NornicDB's own `gds.linkPrediction.*.stream` procedures**, which are [NornicDB extensions](apoc-functions.md#nornicdb-extensions) (GDS has no such procedures). This document explains the integration, API usage, and how it fits into the broader architecture.

## What Was Added

### 1. Topological Link Prediction Package (`pkg/linkpredict/`)

Five canonical graph algorithms:
- **Common Neighbors**: `|N(u) ∩ N(v)|`
- **Jaccard Coefficient**: `|N(u) ∩ N(v)| / |N(u) ∪ N(v)|`  
- **Adamic-Adar**: `Σ(1 / log(|N(z)|))` for common neighbors  
- **Resource Allocation**: `Σ(1 / |N(z)|)` for common neighbors  
- **Preferential Attachment**: `|N(u)| * |N(v)|`

Plus hybrid scoring that blends topology with semantic similarity.

### 2. GDS's link-prediction functions (`pkg/cypher/gds_functions.go`)

The functions Neo4j GDS 2.13 has, scoring one pair of nodes, with GDS's arguments and values:

```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
RETURN gds.alpha.linkprediction.adamicAdar(a, b) AS adamicAdar,
       gds.alpha.linkprediction.commonNeighbors(a, b, {relationshipQuery: 'KNOWS'}) AS common,
       gds.alpha.linkprediction.resourceAllocation(a, b, {direction: 'OUTGOING'}) AS resourceAllocation,
       gds.alpha.linkprediction.preferentialAttachment(a, b) AS preferentialAttachment,
       gds.alpha.linkprediction.totalNeighbors(a, b) AS total,
       gds.alpha.linkprediction.sameCommunity(a, b, 'community') AS sameCommunity
```

As in GDS: neighbours are distinct other nodes, in both directions and over every relationship type unless the config's `relationshipQuery` (one type) or `direction` (`OUTGOING`, `INCOMING`) narrows them; a degree counts relationships. The scores aren't rescaled. `gds.util.asNode` / `asNodes`, `NaN`, `infinity`, `isFinite` and `isInfinite`, and `RETURN gds.version()`, are there as well.

### 3. NornicDB's link-prediction procedures (`pkg/cypher/linkprediction.go`)

NornicDB's own procedures, which rank the top-K candidates for one source node. They are NornicDB extensions: GDS has no `gds.linkPrediction.*.stream` procedures. Their scores are the functions' values above, not rescaled, from the same code: a degree counts relationships, a parallel one again and a self-loop once.

- **The graph:** `stream('g', {...})` reads the projected graph `g` as projected (its labels, types and relationships at projection time; a graph that doesn't exist is an error); `stream({...})` reads every stored node and relationship. Either way relationships are read in both directions.
- **The config:** `sourceNode` (required; a node that isn't in the graph is an error), `topK` (at least 1, default 10), `targetNodes`, `relationshipTypes`, and for `predict.stream` `algorithm`, `topologyWeight`, `semanticWeight` and `minThreshold`. Keys match whatever their case; another key, or a value of the wrong type, is refused with GDS's messages.

```cypher
CALL gds.linkPrediction.adamicAdar.stream(...)
CALL gds.linkPrediction.commonNeighbors.stream(...)
CALL gds.linkPrediction.resourceAllocation.stream(...)
CALL gds.linkPrediction.preferentialAttachment.stream(...)
CALL gds.linkPrediction.jaccard.stream(...)
CALL gds.linkPrediction.predict.stream(...)  // Hybrid
```

### 4. Integration with Existing Inference Engine

Works alongside `/pkg/inference/inference.go`:

```
┌────────────────────────────────────────────────────────┐
│                  Link Prediction                       │
├────────────────────────────────────────────────────────┤
│  Topological              │  Semantic (Existing)       │
│  (pkg/linkpredict)        │  (pkg/inference)           │
│  • Structure-based        │  • Embedding similarity    │
│  • Fast, deterministic    │  • Co-access patterns      │
│  • GDS's functions        │  • Temporal proximity      │
│                           │  • Transitive inference    │
├────────────────────────────────────────────────────────┤
│         Hybrid Scorer (pkg/linkpredict/hybrid.go)      │
│   α * topology_score + β * semantic_score              │
└────────────────────────────────────────────────────────┘
```

## API Usage

### Via Cypher (NornicDB procedures)

```cypher
-- Basic topological prediction
MATCH (n:Person {name: 'Alice'})
CALL gds.linkPrediction.adamicAdar.stream({
  sourceNode: id(n),
  topK: 10
})
YIELD node1, node2, score
WHERE score > 0.5
RETURN node2, score
ORDER BY score DESC

-- Hybrid (topology + semantics)
MATCH (n:Memory {id: 'memory-123'})
CALL gds.linkPrediction.predict.stream({
  sourceNode: id(n),
  algorithm: 'adamic_adar',
  topologyWeight: 0.6,
  semanticWeight: 0.4,
  topK: 20
})
YIELD node1, node2, score, topology_score, semantic_score, reason
RETURN node2, score, reason
ORDER BY score DESC
```

`predict.stream`'s columns:

- `topology_score` — the topology algorithm's score on its own [0, 1] scale, the same for a pair whatever the other candidates: Jaccard as is; common neighbours `1 - 1/(1 + n/2)`; Adamic-Adar and resource allocation `tanh(score/5)`; preferential attachment `log10(score)/4`, capped at 1; `ensemble` their weighted sum.
- `semantic_score` — the best cosine similarity between the two nodes' embeddings: any chunk of one against any chunk of the other, and each named embedding against the other node's embedding of the same name (vectors of different names, which may come from different models, aren't compared). Past 4,096 pairs in a group, each vector is compared with the other node's mean vector instead, so long documents cost linearly. 0 when the nodes have no comparable embeddings.
- `score` — `topologyWeight * topology_score + semanticWeight * semantic_score`.
- `reason` — both scores in words, each strong (above 0.6), moderate (above 0.3), weak (above 0) or none, for example "Strong structural connection, no semantic match".

### Via Go API

```go
import "github.com/orneryd/nornicdb/pkg/linkpredict"

// Build graph from storage
graph, err := linkpredict.BuildGraphFromEngine(ctx, engine, true)

// Run topological algorithm
predictions := linkpredict.AdamicAdar(graph, sourceNodeID, 10)
for _, pred := range predictions {
    fmt.Printf("→ %s: %.3f\n", pred.TargetID, pred.Score)
}

// Or use hybrid scoring
config := linkpredict.HybridConfig{
    TopologyWeight:    0.6,
    SemanticWeight:    0.4,
    TopologyAlgorithm: "adamic_adar",
}
scorer := linkpredict.NewHybridScorer(config)

scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
    // Get embeddings and compute similarity
    return linkpredict.CosineSimilarity(sourceEmb, targetEmb)
})

hybridPreds := scorer.Predict(ctx, graph, sourceNodeID, 10)
```

## Integration Points

### 1. Cypher Executor (`pkg/cypher/call.go`)

Added procedure routing at line 343-355:

```go
// Neo4j GDS Link Prediction Procedures (topological)
case strings.Contains(upper, "GDS.LINKPREDICTION.ADAMICADAR.STREAM"):
    result, err = e.callGdsLinkPredictionAdamicAdar(cypher)
case strings.Contains(upper, "GDS.LINKPREDICTION.COMMONNEIGHBORS.STREAM"):
    result, err = e.callGdsLinkPredictionCommonNeighbors(cypher)
// ... etc
```

### 2. Storage Layer (`pkg/storage/`)

Leverages existing `Engine` interface:
- `AllNodes()` - Get all nodes for graph construction
- `GetOutgoingEdges(nodeID)` - Build adjacency lists
- `GetNode(id)` - Fetch embeddings for semantic scoring

No changes to storage layer required ✅

### 3. Inference Engine (`pkg/inference/`)

**Complementary, not replacement**:
- Inference engine continues to handle real-time co-access tracking
- Topological algorithms handle batch/on-demand predictions
- Hybrid scorer bridges both worlds

## Configuration

No new config needed. Algorithms are stateless and invoked on-demand via Cypher procedures or Go API.

For hybrid scoring, weights are passed in procedure parameters:

```cypher
CALL gds.linkPrediction.predict.stream({
  sourceNode: id(n),
  topologyWeight: 0.7,    -- Structure matters more
  semanticWeight: 0.3,
  algorithm: 'ensemble'    -- Or specific: 'adamic_adar'
})
```

## Testing

### Unit Tests

```bash
# Test topological algorithms
go test ./pkg/linkpredict/... -v

# Test hybrid scoring
go test ./pkg/linkpredict/... -v -run=TestHybrid

# Test Cypher integration
go test ./pkg/cypher/... -v -run=TestLinkPrediction
```

### Benchmarks

```bash
go test ./pkg/linkpredict/... -bench=. -benchmem
```

### Integration Test Example

```cypher
-- Create test graph
CREATE (a:Person {name: 'Alice'})
CREATE (b:Person {name: 'Bob'})
CREATE (c:Person {name: 'Charlie'})
CREATE (d:Person {name: 'Diana'})
CREATE (a)-[:KNOWS]->(b)
CREATE (a)-[:KNOWS]->(c)
CREATE (b)-[:KNOWS]->(d)
CREATE (c)-[:KNOWS]->(d)

-- Test prediction (should suggest Alice -> Diana)
MATCH (a:Person {name: 'Alice'})
CALL gds.linkPrediction.commonNeighbors.stream({
  sourceNode: id(a),
  topK: 5
})
YIELD node1, node2, score
MATCH (target) WHERE id(target) = node2
RETURN target.name, score
ORDER BY score DESC
```

Expected: Diana ranks high (2 common neighbors: Bob and Charlie)

## Performance Characteristics

### Time Complexity

| Algorithm | Complexity | Notes |
|-----------|-----------|-------|
| Common Neighbors | O(deg(u) * deg(v)) | Fast for low-degree nodes |
| Jaccard | O(deg(u) * deg(v)) | Same as CN |
| Adamic-Adar | O(deg(u) * deg(v)) | Includes log computation |
| Resource Allocation | O(deg(u) * deg(v)) | Similar to AA |
| Preferential Attachment | O(1) per candidate | Fastest |
| Hybrid | O(algorithm + N*embedding_lookup) | Depends on semantic scorer |

### Memory Usage

- **Graph construction**: ~200-500 bytes per node + adjacency
- **Prediction**: Minimal (outputs top-K only)
- **Hybrid**: +embedding storage (1024 floats * 4 bytes = 4KB per node)

### Optimization Tips

1. **Candidate generation**: Algorithms only consider 2-hop neighbors (not all nodes)
2. **Caching**: Build graph once, run multiple algorithms
3. **Batching**: Process multiple source nodes with same graph instance
4. **Sampling**: For very large graphs, sample subgraph first

## Neo4j Compatibility Statement

✅ **GDS's functions**: `gds.alpha.linkprediction.*`, `gds.util.asNode` / `asNodes` / `NaN` / `infinity` / `isFinite` / `isInfinite` and `gds.version()` take GDS 2.13's arguments and give its values  
⚠️ **NornicDB extensions**: the `gds.linkPrediction.*.stream` procedures aren't GDS's (GDS has none by those names); a query using them runs only on NornicDB  
✅ **GDS procedures**: `gds.graph.project`, `gds.graph.list`, `gds.graph.drop`, `gds.graph.exists`, `gds.fastRP.stream` / `stats` and `gds.util.nodeProperty` / `gds.graph.exists()` take GDS 2.13's parameters and give its columns, values and errors; the `.stream` procedures can read a projected graph by name  

❌ **Not Implemented**: the rest of GDS: see [GDS in the feature parity list](../neo4j-migration/feature-parity.md#graph-data-science-gds)

## Use Cases

### 1. Social Networks
**Best**: Adamic-Adar, Common Neighbors  
**Why**: Mutual friends are strong signal

```cypher
CALL gds.linkPrediction.adamicAdar.stream({...})
```

### 2. Knowledge Graphs
**Best**: Hybrid (topology + semantic)  
**Why**: Both structure and content matter

```cypher
CALL gds.linkPrediction.predict.stream({
  topologyWeight: 0.5,
  semanticWeight: 0.5
})
```

### 3. Citation Networks
**Best**: Adamic-Adar, Preferential Attachment  
**Why**: Common citations + popular papers get more citations

### 4. AI Agent Memory
**Best**: Hybrid with high semantic weight  
**Why**: Semantic similarity drives associations, structure is secondary

```cypher
CALL gds.linkPrediction.predict.stream({
  topologyWeight: 0.3,
  semanticWeight: 0.7,
  algorithm: 'adamic_adar'
})
```

## Comparison with Existing Inference

| Feature | Topological (New) | Semantic (Existing) |
|---------|------------------|---------------------|
| **Input** | Graph structure | Embeddings, access logs |
| **Signal** | Neighbors, paths | Meaning, behavior |
| **Speed** | Fast (structure-only) | Moderate (embeddings) |
| **Use When** | Structure matters | Semantics matter |
| **Example** | "Mutual friends" | "Similar interests" |

**Recommendation**: Use hybrid for best results in AI agent memory workloads.

## Future Enhancements

1. **Persistent Graph Projections**: Cache graph structure for faster repeated queries
2. **Incremental Updates**: Update adjacency without full rebuild
3. **GPU Acceleration**: Parallelize neighbor intersection on GPU
4. **More Algorithms**: Total neighbors, Same community, SimRank
5. **Write Mode**: Auto-create edges above threshold

## References

- Neo4j GDS Documentation: https://neo4j.com/docs/graph-data-science/current/algorithms/linkprediction/
- Adamic & Adar (2003): "Friends and neighbors on the Web"
- Liben-Nowell & Kleinberg (2007): "The link-prediction problem for social networks"
- Zhou et al. (2009): "Predicting missing links via local information"

## Summary

- **What it does**: Adds topological link prediction: GDS's link-prediction functions, and NornicDB's own top-K procedures
- **How it integrates**: Complements existing semantic inference, accessible via Cypher
- **Why it matters**: Provides structural signals Neo4j users expect, enables hybrid prediction
- **Status**: Tested, documented, follows NornicDB conventions
