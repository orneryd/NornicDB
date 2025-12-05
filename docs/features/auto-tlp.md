# Auto-TLP: Automatic Relationship Inference

## Overview

**Auto-TLP** (Automatic Temporal Link Prediction) is NornicDB's system for automatically inferring and materializing relationships between nodes based on multiple signals. When enabled, the inference engine observes node access patterns and creates `SIMILAR_TO`, `CO_ACCESSED`, and other relationship types automatically.

> ⚠️ **Disabled by Default**: Auto-TLP can create many edges automatically. Enable only when you want the system to actively build your knowledge graph.

## Feature Flag

| Flag | Environment Variable | Default |
|------|---------------------|---------|
| Auto-TLP | `NORNICDB_AUTO_TLP_ENABLED` | ❌ Disabled |

```bash
# Enable automatic relationship inference
export NORNICDB_AUTO_TLP_ENABLED=true
```

## How It Works

Auto-TLP triggers on two events:

### 1. OnStore (Node Creation)

When a new node is created with an embedding:

```
Node Created → OnStore() → Find Similar Nodes → Create SIMILAR_TO edges
```

The inference engine:
1. Receives the new node's embedding
2. Searches for semantically similar existing nodes
3. Creates `SIMILAR_TO` edges with confidence scores
4. Marks edges as `auto_generated: true`

### 2. OnAccess (Node Access)

When a node is read/accessed:

```
Node Accessed → OnAccess() → Track Co-Access → Create CO_ACCESSED edges
```

The inference engine:
1. Records the access event with timestamp
2. Checks for other nodes accessed in the same session
3. Creates `CO_ACCESSED` edges between co-accessed nodes
4. Applies transitive inference (A→B, B→C suggests A→C)

## Inference Signals

Auto-TLP combines multiple signals:

| Signal | Weight | Description |
|--------|--------|-------------|
| **Semantic Similarity** | High | Embedding cosine distance |
| **Co-Access Patterns** | Medium | Nodes accessed together |
| **Temporal Proximity** | Medium | Nodes created/accessed near in time |
| **Transitive Inference** | Low | If A→B and B→C, suggest A→C |

## Auto-Generated Edge Properties

Edges created by Auto-TLP have these properties:

```cypher
// Example auto-generated edge
(a)-[:SIMILAR_TO {
  confidence: 0.85,        // Similarity score
  auto_generated: true,    // Marker for cleanup
  created_at: timestamp(), // When inferred
  updated_at: timestamp(), // Last reinforced
  source: "inference"      // Origin system
}]->(b)
```

## Integration with Edge Decay

Auto-generated edges work with the **Edge Decay** system (enabled by default):

```
Auto-TLP creates edge → Edge Decay monitors → Unused edges decay → Below threshold → Deleted
```

This prevents graph bloat:
- **Reinforced edges** (accessed/used) stay strong
- **Unused edges** decay over time (default: 5% per day)
- **Weak edges** (confidence < 0.3) are automatically removed

See: [Edge Decay Configuration](./feature-flags.md#edge-decay)

## Go API

```go
import (
    "github.com/orneryd/nornicdb/pkg/config"
    "github.com/orneryd/nornicdb/pkg/inference"
)

// Check if Auto-TLP is enabled
if config.IsAutoTLPEnabled() {
    // Relationships are being inferred automatically
}

// Enable/disable at runtime
config.EnableAutoTLP()
config.DisableAutoTLP()

// Scoped enable for testing
cleanup := config.WithAutoTLPEnabled()
defer cleanup()
// ... test code with Auto-TLP enabled ...

// Direct access to inference engine
engine := inference.New(storage, embeddingFunc)

// Manual trigger (works even with Auto-TLP disabled)
suggestions, err := engine.OnStore(ctx, nodeID, embedding)
for _, s := range suggestions {
    fmt.Printf("Suggested: %s -[%s]-> %s (%.2f)\n", 
        s.SourceID, s.Type, s.TargetID, s.Confidence)
}
```

## When to Enable Auto-TLP

### ✅ Good Use Cases

- **Personal knowledge graphs**: Notes, bookmarks, research
- **AI agent memory**: Building associative memory automatically
- **Discovery systems**: "Related items" features
- **Recommendation engines**: Finding similar content

### ❌ Avoid When

- **Strict schemas**: You control all relationships explicitly
- **High-volume writes**: Many nodes created rapidly
- **Audit requirements**: All relationships must be human-created
- **Low memory**: Auto-TLP adds edge storage overhead

## Performance Considerations

| Operation | Impact |
|-----------|--------|
| Node Create | +5-20ms (similarity search) |
| Node Access | +1-5ms (co-access tracking) |
| Edge Storage | ~100-500 bytes per edge |
| Edge Decay | Background, minimal impact |

### Tuning Tips

1. **Similarity threshold**: Adjust inference engine's `MinSimilarity` (default: 0.7)
2. **Co-access window**: Adjust session timeout for co-access detection
3. **Decay rate**: Speed up cleanup with lower `DecayRate` (default: 0.95)
4. **Max edges per node**: Limit auto-generated edges to prevent fan-out

## Comparison: Auto-TLP vs Link Prediction Procedures

| Feature | Auto-TLP | Link Prediction (Cypher) |
|---------|----------|--------------------------|
| **Trigger** | Automatic (OnStore/OnAccess) | Manual (CALL procedure) |
| **Algorithms** | Semantic + temporal + transitive | Topological (Adamic-Adar, Jaccard) |
| **Materializes** | Yes (creates edges) | No (returns suggestions) |
| **Control** | Feature flag | Always available |
| **Best for** | Background graph building | On-demand analysis |

Use **both** for best results:
- Auto-TLP builds the base graph automatically
- Link prediction procedures for explicit queries

## Monitoring

Check inference activity:

```cypher
// Count auto-generated edges
MATCH ()-[r]-() WHERE r.auto_generated = true
RETURN type(r), count(r) as count
ORDER BY count DESC

// Find highest-confidence auto edges
MATCH (a)-[r]->(b) WHERE r.auto_generated = true
RETURN a.id, type(r), b.id, r.confidence
ORDER BY r.confidence DESC
LIMIT 20

// Edges created today
MATCH ()-[r]-() 
WHERE r.auto_generated = true 
  AND r.created_at > datetime() - duration('P1D')
RETURN count(r)
```

## Troubleshooting

### No edges being created

1. Check feature flag: `echo $NORNICDB_AUTO_TLP_ENABLED`
2. Verify embeddings exist on nodes
3. Check similarity threshold isn't too high
4. Ensure inference engine is initialized

### Too many edges

1. Increase similarity threshold
2. Enable edge decay (default: on)
3. Reduce decay grace period
4. Lower co-access session timeout

### Performance degraded

1. Disable during bulk imports
2. Use deferred mode for batch operations
3. Consider disabling and using link prediction procedures instead

## Summary

| Aspect | Details |
|--------|---------|
| **Purpose** | Automatically infer relationships |
| **Flag** | `NORNICDB_AUTO_TLP_ENABLED` |
| **Default** | ❌ Disabled |
| **Triggers** | OnStore (new nodes), OnAccess (reads) |
| **Signals** | Semantic, co-access, temporal, transitive |
| **Cleanup** | Edge Decay (enabled by default) |
| **Alternative** | `CALL gds.linkPrediction.*` procedures |

---

*"The Norns weave your data's destiny"* — When Auto-TLP is enabled, NornicDB automatically discovers and creates the connections that give your data meaning.
