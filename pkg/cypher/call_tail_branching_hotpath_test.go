package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallTailSetBasedSupportsBranchingPathCapAggregation(t *testing.T) {
	exec, store, ctx, rootByKey := setupCallTailTraversalFixture(t)
	const pathCap = 5

	for fanout := 1; fanout <= 3; fanout++ {
		rootKey := fmt.Sprintf("branch-f%d", fanout)
		root := rootByKey[rootKey]
		require.NotNil(t, root)

		t.Run(fmt.Sprintf("fanout_%d", fanout), func(t *testing.T) {
			for depth := 1; depth <= 6; depth++ {
				query := fmt.Sprintf(`
CALL db.index.vector.queryNodes('idx_original_text', $vectorTopK, $query)
YIELD node, score
MATCH p = (node)-[:BENCH_HOP|REL_A|REL_B*1..%d]->(x)
WHERE ALL(n IN nodes(p) WHERE size(labels(n)) > 0)
WITH node, score, p, length(p) AS d
ORDER BY d ASC
WITH node, score, collect(p)[0..$pathCap] AS paths
RETURN elementId(node) AS nodeID, score, size(paths) AS pathCount
LIMIT $topK
`, depth)
				res, err := exec.Execute(ctx, query, map[string]interface{}{
					"vectorTopK": int64(1),
					"topK":       int64(1),
					"pathCap":    int64(pathCap),
					"query":      vectorForFanout(fanout),
				})
				require.NoError(t, err, "fanout=%d depth=%d", fanout, depth)
				require.True(t, exec.LastHotPathTrace().CallTailTraversalFastPath, "fanout=%d depth=%d should use call-tail traversal hot path", fanout, depth)
				require.Equal(t, []string{"nodeID", "score", "pathCount"}, res.Columns)
				require.Len(t, res.Rows, 1)
				assert.Equal(t, storage.NodeElementID("test", root.ID), res.Rows[0][0])
				expectedCount := minInt(pathCap, geometricPathCount(fanout, depth))
				assert.EqualValues(t, expectedCount, toInt64ForTest(t, res.Rows[0][2]), "fanout=%d depth=%d pathCount", fanout, depth)
			}
		})
	}

	_ = store
}

func TestCallTailSetBasedSupportsFrontierReachabilityAggregation(t *testing.T) {
	exec, _, ctx, rootByKey := setupCallTailTraversalFixture(t)

	for fanout := 1; fanout <= 3; fanout++ {
		rootKey := fmt.Sprintf("frontier-f%d", fanout)
		root := rootByKey[rootKey]
		require.NotNil(t, root)

		t.Run(fmt.Sprintf("fanout_%d", fanout), func(t *testing.T) {
			for depth := 1; depth <= 6; depth++ {
				query := fmt.Sprintf(`
CALL db.index.vector.queryNodes('idx_original_text', $vectorTopK, $query)
YIELD node, score
MATCH (node)-[:REL*1..%d]->(x)
WITH node, score, length(shortestPath((node)-[:REL*1..%d]->(x))) AS d
WITH node, score, min(d) AS nearest, count(*) AS reachable
RETURN elementId(node) AS nodeID, score, nearest, reachable
LIMIT $topK
`, depth, depth)
				res, err := exec.Execute(ctx, query, map[string]interface{}{
					"vectorTopK": int64(1),
					"topK":       int64(1),
					"query":      frontierVectorForFanout(fanout),
				})
				require.NoError(t, err, "fanout=%d depth=%d", fanout, depth)
				require.True(t, exec.LastHotPathTrace().CallTailTraversalFastPath, "fanout=%d depth=%d should use call-tail traversal hot path", fanout, depth)
				require.Equal(t, []string{"nodeID", "score", "nearest", "reachable"}, res.Columns)
				require.Len(t, res.Rows, 1)
				assert.Equal(t, storage.NodeElementID("test", root.ID), res.Rows[0][0])
				assert.EqualValues(t, 1, toInt64ForTest(t, res.Rows[0][2]), "fanout=%d depth=%d nearest", fanout, depth)
				assert.EqualValues(t, geometricPathCount(fanout, depth), toInt64ForTest(t, res.Rows[0][3]), "fanout=%d depth=%d reachable", fanout, depth)
			}
		})
	}
}

func TestCallTailSetBasedSupportsConstrainedTraversalAggregation(t *testing.T) {
	exec, _, ctx, rootByKey := setupCallTailTraversalFixture(t)
	strongRoot := rootByKey["constrained-strong"]
	weakRoot := rootByKey["constrained-weak"]
	require.NotNil(t, strongRoot)
	require.NotNil(t, weakRoot)

	for depth := 1; depth <= 6; depth++ {
		query := fmt.Sprintf(`
CALL db.index.vector.queryNodes('idx_original_text', $vectorTopK, $query)
YIELD node, score
MATCH p = (node)-[:REL*1..%d]->(x)
WHERE any(r IN relationships(p) WHERE r.weight >= $minWeight)
  AND any(n IN nodes(p) WHERE n.category IN $cats)
RETURN elementId(node) AS nodeID, score, max(length(p)) AS maxDepth
LIMIT $topK
`, depth)
		res, err := exec.Execute(ctx, query, map[string]interface{}{
			"vectorTopK": int64(2),
			"topK":       int64(2),
			"query":      []interface{}{0.12, 0.84, 0.04},
			"minWeight":  2.5,
			"cats":       []string{"allowed"},
		})
		require.NoError(t, err, "depth=%d", depth)
		require.True(t, exec.LastHotPathTrace().CallTailTraversalFastPath, "depth=%d should use call-tail traversal hot path", depth)
		require.Equal(t, []string{"nodeID", "score", "maxDepth"}, res.Columns)
		require.Len(t, res.Rows, 1, "depth=%d should only return the strong constrained root", depth)
		assert.Equal(t, storage.NodeElementID("test", strongRoot.ID), res.Rows[0][0])
		assert.NotEqual(t, storage.NodeElementID("test", weakRoot.ID), res.Rows[0][0])
		assert.EqualValues(t, depth, toInt64ForTest(t, res.Rows[0][2]), "depth=%d maxDepth", depth)
	}
}

func setupCallTailTraversalFixture(t *testing.T) (*StorageExecutor, storage.Engine, context.Context, map[string]*storage.Node) {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('idx_original_text', 'OriginalText', 'embedding', 3, 'cosine')", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (o:OriginalText {textKey: row.textKey})
SET o.originalText = row.originalText,
    o.embedding = row.embedding,
    o.nodeKey = row.textKey
RETURN count(o) AS prepared
`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"textKey": "branch-f1", "originalText": "branch fanout one", "embedding": vectorForFanout(1)},
			{"textKey": "branch-f2", "originalText": "branch fanout two", "embedding": vectorForFanout(2)},
			{"textKey": "branch-f3", "originalText": "branch fanout three", "embedding": vectorForFanout(3)},
			{"textKey": "frontier-f1", "originalText": "frontier fanout one", "embedding": frontierVectorForFanout(1)},
			{"textKey": "frontier-f2", "originalText": "frontier fanout two", "embedding": frontierVectorForFanout(2)},
			{"textKey": "frontier-f3", "originalText": "frontier fanout three", "embedding": frontierVectorForFanout(3)},
			{"textKey": "constrained-strong", "originalText": "weighted allowed root", "embedding": []interface{}{0.12, 0.84, 0.04}},
			{"textKey": "constrained-weak", "originalText": "filtered weak root", "embedding": []interface{}{0.11, 0.79, 0.10}},
		},
	})
	require.NoError(t, err)

	roots, err := store.GetNodesByLabel("OriginalText")
	require.NoError(t, err)
	rootByKey := make(map[string]*storage.Node, len(roots))
	for _, root := range roots {
		rootByKey[root.Properties["textKey"].(string)] = root
	}

	for fanout := 1; fanout <= 3; fanout++ {
		createBranchingTree(t, store, rootByKey[fmt.Sprintf("branch-f%d", fanout)], fmt.Sprintf("branch-f%d", fanout), fanout, 6, branchingEdgeType, 1.0, "allowed", "BranchHop")
		createBranchingTree(t, store, rootByKey[fmt.Sprintf("frontier-f%d", fanout)], fmt.Sprintf("frontier-f%d", fanout), fanout, 6, frontierEdgeType, 1.0, "allowed", "FrontierHop")
	}
	createBranchingTree(t, store, rootByKey["constrained-strong"], "constrained-strong", 2, 6, frontierEdgeType, 5.0, "allowed", "ConstrainedHop")
	createBranchingTree(t, store, rootByKey["constrained-weak"], "constrained-weak", 2, 6, frontierEdgeType, 0.2, "other", "ConstrainedHop")

	return exec, store, ctx, rootByKey
}

func createBranchingTree(t *testing.T, store storage.Engine, root *storage.Node, rootKey string, fanout, maxDepth int, edgeTypeFn func(depth, childIdx int) string, weight float64, category, label string) {
	t.Helper()
	require.NotNil(t, root)
	type parentRef struct {
		node *storage.Node
		idx  int
	}
	parents := []parentRef{{node: root, idx: 0}}
	nextOrdinal := 0
	for depth := 1; depth <= maxDepth; depth++ {
		nextParents := make([]parentRef, 0, len(parents)*fanout)
		for _, parent := range parents {
			for childIdx := 0; childIdx < fanout; childIdx++ {
				nextOrdinal++
				nodeID := storage.NodeID(fmt.Sprintf("%s:%s:%02d:%04d", rootKey, label, depth, nextOrdinal))
				child := &storage.Node{
					ID:     nodeID,
					Labels: []string{label},
					Properties: map[string]interface{}{
						"nodeKey":    string(nodeID),
						"category":   category,
						"hopDepth":   int64(depth),
						"rootKey":    rootKey,
						"branchSlot": int64(childIdx),
					},
				}
				_, err := store.CreateNode(child)
				require.NoError(t, err)
				edgeType := edgeTypeFn(depth, childIdx)
				require.NoError(t, store.CreateEdge(&storage.Edge{
					ID:         storage.EdgeID(fmt.Sprintf("%s:%s:%02d:%04d", rootKey, edgeType, depth, nextOrdinal)),
					Type:       edgeType,
					StartNode:  parent.node.ID,
					EndNode:    child.ID,
					Properties: map[string]interface{}{"weight": weight, "depth": int64(depth)},
				}))
				nextParents = append(nextParents, parentRef{node: child, idx: childIdx})
			}
		}
		parents = nextParents
	}
}

func branchingEdgeType(depth, childIdx int) string {
	if depth == 1 {
		return "BENCH_HOP"
	}
	if childIdx%2 == 0 {
		return "REL_A"
	}
	return "REL_B"
}

func frontierEdgeType(_ int, _ int) string {
	return "REL"
}

func vectorForFanout(fanout int) []interface{} {
	switch fanout {
	case 1:
		return []interface{}{1.0, 0.0, 0.0}
	case 2:
		return []interface{}{0.0, 1.0, 0.0}
	default:
		return []interface{}{0.0, 0.0, 1.0}
	}
}

func frontierVectorForFanout(fanout int) []interface{} {
	switch fanout {
	case 1:
		return []interface{}{0.75, 0.25, 0.0}
	case 2:
		return []interface{}{0.25, 0.75, 0.0}
	default:
		return []interface{}{0.25, 0.25, 0.5}
	}
}

func geometricPathCount(fanout, depth int) int {
	if fanout == 1 {
		return depth
	}
	return int((math.Pow(float64(fanout), float64(depth+1)) - float64(fanout)) / float64(fanout-1))
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
