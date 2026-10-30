# Link Prediction Package

Topological link prediction algorithms for NornicDB, providing **Neo4j Graph Data Science (GDS) compatibility**.

## Overview

This package adds canonical graph-based link prediction to complement NornicDB's existing semantic/behavioral inference engine. It implements the standard Neo4j GDS link prediction procedures.

## Neo4j GDS Compatibility

All algorithms are accessible via Neo4j GDS-compatible Cypher procedures:

```cypher
-- Adamic-Adar (weights rare connections)
CALL gds.linkPrediction.adamicAdar.stream({
  sourceNode: id(n),
  topK: 10
})
YIELD node1, node2, score

-- Common Neighbors (simple count)
CALL gds.linkPrediction.commonNeighbors.stream({
  sourceNode: id(n),
  topK: 10
})
YIELD node1, node2, score

-- Resource Allocation (inverse degree weighting)
CALL gds.linkPrediction.resourceAllocation.stream({
  sourceNode: id(n),
  topK: 10
})
YIELD node1, node2, score

-- Preferential Attachment (degree product)
CALL gds.linkPrediction.preferentialAttachment.stream({
  sourceNode: id(n),
  topK: 10
})
YIELD node1, node2, score

-- Jaccard Coefficient (normalized overlap)
CALL gds.linkPrediction.jaccard.stream({
  sourceNode: id(n),
  topK: 10
})
YIELD node1, node2, score

-- Hybrid (topology + semantic)
CALL gds.linkPrediction.predict.stream({
  sourceNode: id(n),
  algorithm: 'adamic_adar',
  topologyWeight: 0.6,
  semanticWeight: 0.4,
  topK: 20
})
YIELD node1, node2, score, topology_score, semantic_score, reason
```

## Algorithms Implemented

### 1. Common Neighbors
- **Formula**: `|N(u) ∩ N(v)|`
- **Use**: Simplest heuristic, counts shared neighbors
- **Good for**: Social networks, collaboration graphs

### 2. Jaccard Coefficient
- **Formula**: `|N(u) ∩ N(v)| / |N(u) ∪ N(v)|`
- **Use**: Normalized similarity, reduces degree bias
- **Good for**: Comparing nodes with different degrees

### 3. Adamic-Adar
- **Formula**: `Σ(1 / log(|N(z)|))` for `z` in common neighbors
- **Use**: Weights rare connections higher
- **Good for**: Citation networks, knowledge graphs
- **Reference**: Adamic & Adar (2003)

### 4. Resource Allocation
- **Formula**: `Σ(1 / |N(z)|)` for `z` in common neighbors
- **Use**: Simpler variant of Adamic-Adar
- **Good for**: Similar to Adamic-Adar

### 5. Preferential Attachment
- **Formula**: `|N(u)| * |N(v)|`
- **Use**: Models "rich get richer" phenomenon
- **Good for**: Scale-free networks, growth models
- **Reference**: Barabási & Albert (1999)

## Hybrid Scoring

Combines topological and semantic signals:

```go
import "github.com/orneryd/nornicdb/pkg/linkpredict"

// Create hybrid scorer
config := linkpredict.HybridConfig{
    TopologyWeight:    0.6,  // Structural signals
    SemanticWeight:    0.4,  // Embedding similarity
    TopologyAlgorithm: "adamic_adar",
    NormalizeScores:   true,
    MinThreshold:      0.3,
}
scorer := linkpredict.NewHybridScorer(config)

// Set semantic scorer (embedding similarity)
scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
    // Return cosine similarity between embeddings
    return linkpredict.CosineSimilarity(sourceEmb, targetEmb)
})

// Get predictions
predictions := scorer.Predict(ctx, graph, sourceNode, 10)
for _, pred := range predictions {
    fmt.Printf("%s: %.3f (topo: %.3f, sem: %.3f)\n",
        pred.TargetID, pred.Score, pred.TopologyScore, pred.SemanticScore)
}
```

## When to Use Each Algorithm

| Algorithm | Social Networks | Knowledge Graphs | Citation Networks | AI Agent Memory |
|-----------|----------------|------------------|-------------------|-----------------|
| Common Neighbors | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| Jaccard | ⭐⭐⭐ | ⭐⭐ | ⭐⭐ | ⭐⭐ |
| Adamic-Adar | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| Resource Allocation | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| Preferential Attachment | ⭐⭐ | ⭐ | ⭐⭐⭐ | ⭐ |
| Hybrid (Topology + Semantic) | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |

## Comparison: Topological vs Semantic

**Topological Link Prediction** (this package):
- Uses only graph structure (neighbors, degrees, paths)
- Fast, deterministic
- Captures social/organizational patterns
- Examples: "You both know Alice", "Popular people connect to popular people"

**Semantic Link Prediction** (existing inference engine):
- Uses embeddings, co-access, temporal signals
- Captures meaning and behavior
- Examples: "Documents about similar topics", "Accessed together frequently"

**Hybrid** (best of both):
- Combines structural and semantic signals
- More robust predictions
- Examples: "Similar interests AND mutual friends", "Related topics AND cited together"

## Performance

Benchmarked on 1000-node graph with average degree 10:

```
BenchmarkCommonNeighbors-8         10000    ~100 µs/op
BenchmarkJaccard-8                  5000    ~200 µs/op
BenchmarkAdamicAdar-8               5000    ~250 µs/op
BenchmarkResourceAllocation-8       5000    ~250 µs/op
BenchmarkPreferentialAttachment-8  20000    ~50 µs/op
BenchmarkHybridPredict-8            3000    ~400 µs/op
```

## Testing

```bash
# Run all tests
cd nornicdb && go test ./pkg/linkpredict/... -v

# Run benchmarks
go test ./pkg/linkpredict/... -bench=. -benchmem

# Test Neo4j GDS compatibility
go test ./pkg/cypher/... -v -run=TestLinkPrediction
```

## Integration with Existing Inference Engine

The topological algorithms complement NornicDB's existing `/pkg/inference/` engine:

```
┌─────────────────────────────────────────────┐
│         Link Prediction Strategy             │
├─────────────────────────────────────────────┤
│  Topological (this pkg)  │  Semantic (inference) │
│  • Adamic-Adar           │  • Embedding similarity│
│  • Jaccard               │  • Co-access patterns │
│  • Common Neighbors      │  • Temporal proximity │
│  • Resource Allocation   │  • Transitive inference│
│  • Preferential Attach   │                      │
├─────────────────────────────────────────────┤
│         Hybrid Scorer (this pkg)             │
│   Blends topology + semantic with weights    │
└─────────────────────────────────────────────┘
```

Use topological when:
- Structure matters (social networks, org charts)
- Fast predictions needed (no embedding lookups)
- Graph topology is well-formed

Use semantic when:
- Meaning matters (documents, knowledge)
- Embeddings are available
- Behavioral patterns are important

Use hybrid when:
- Both dimensions matter (AI agent memory)
- Maximum prediction quality needed
- Willing to pay compute cost

## References

- Liben-Nowell & Kleinberg (2007). "The link-prediction problem for social networks"
- Adamic & Adar (2003). "Friends and neighbors on the Web"
- Zhou et al. (2009). "Predicting missing links via local information"
- Barabási & Albert (1999). "Emergence of scaling in random networks"
- Neo4j GDS Documentation: https://neo4j.com/docs/graph-data-science/

## License

MIT (same as NornicDB parent project)
