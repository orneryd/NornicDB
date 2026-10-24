package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func nodePatternSeekFixture(t *testing.T, indexed bool) *storage.NamespacedEngine {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_seek")
	for _, node := range []*storage.Node{
		{ID: "n1", Labels: []string{"R73Label", "R73Wanted"}, Properties: map[string]interface{}{"id": int64(1), "bucket": int64(1), "flag": false}},
		{ID: "n2", Labels: []string{"R73Label"}, Properties: map[string]interface{}{"id": int64(2), "bucket": int64(1), "flag": true}},
		{ID: "n3", Labels: []string{"R73Label", "R73Wanted"}, Properties: map[string]interface{}{"id": int64(3), "bucket": int64(2), "flag": true}},
		{ID: "end", Labels: []string{"R73End"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	for index := 1; index <= 3; index++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", index)), StartNode: storage.NodeID(fmt.Sprintf("n%d", index)), EndNode: "end", Type: "R73Forward"}))
	}
	if indexed {
		for _, property := range []string{"id", "bucket"} {
			_, err := NewStorageExecutor(store).Execute(context.Background(), "CREATE INDEX seek_"+property+" FOR (n:R73Label) ON (n."+property+")", nil)
			require.NoError(t, err)
		}
	}
	return store
}

// These unbounded seek entrances also feed bounded node reads and traversal
// seeds. The physical index may narrow the population; it cannot waive maps
// or a label conjunction on a candidate it happens to contain.
func TestNodePatternSeekAcceptancePublic(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		store := nodePatternSeekFixture(t, indexed)
		for _, explicit := range []bool{false, true} {
			for _, profile := range []bool{false, true} {
				for _, predicate := range []string{
					"n.id = 1", "n.id IN $ids", "n.id IN [1,2]",
					"n.id = 1 OR n.id = 2", "n.id IN [1] OR n.bucket IN [1]",
					"id(n) = 'n1'", "id(n) = $node", "id(n) IN $native",
				} {
					t.Run(fmt.Sprintf("index=%t/tx=%t/profile=%t/%s", indexed, explicit, profile, predicate), func(t *testing.T) {
						executor := NewStorageExecutor(store)
						ctx := context.Background()
						if explicit {
							_, err := executor.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) }()
						}
						query := "MATCH (n:R73Label:R73Wanted {flag:true}) WHERE " + predicate + " RETURN n.id LIMIT 1"
						if profile {
							query = "PROFILE " + query
						}
						result, err := executor.Execute(ctx, query, map[string]interface{}{"ids": []interface{}{int64(1), int64(2)}, "node": "n1", "native": []interface{}{"n1", "n2"}})
						require.NoError(t, err)
						require.Empty(t, result.Rows, query)
					})
				}
			}
		}
	}
}

func TestNodePatternPinnedRelationshipSeeds(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		store := nodePatternSeekFixture(t, indexed)
		for index := 1; index <= 3; index++ {
			query := fmt.Sprintf("MATCH (n:R73Label:R73Wanted {flag:true})-[r:R73Forward]->(q:R73End) WHERE id(r)='r%d' RETURN n.id", index)
			result, err := NewStorageExecutor(store).Execute(context.Background(), query, nil)
			require.NoError(t, err)
			if index == 3 {
				require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
			} else {
				require.Empty(t, result.Rows, query)
			}
		}
	}
}

func TestNodePatternStreamingTraversalRejectsIncompleteLabelSet(t *testing.T) {
	store := nodePatternSeekFixture(t, false)
	var nodes []*storage.Node
	for _, id := range []storage.NodeID{"n2", "n3"} {
		node, err := store.GetNode(id)
		require.NoError(t, err)
		nodes = append(nodes, node)
	}
	probe := &collectNodesLabelProbeEngine{Engine: store, projectedNodes: nodes}
	result, err := NewStorageExecutor(probe).Execute(context.Background(), "MATCH (n:R73Label:R73Wanted)-[:R73Forward]->(q:R73End) RETURN n.id LIMIT 1", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
	require.Positive(t, probe.projectedLabelCalls)
}

// The legacy parameter adapter also accepts a parameter map without statement
// context. Its candidates must obey the same pattern before callers can cap it.
func TestNodePatternExplicitParameterSeek(t *testing.T) {
	executor := NewStorageExecutor(nodePatternSeekFixture(t, false))
	for _, id := range []string{"n1", "n2", "n3"} {
		nodes, used, err := executor.tryCollectNodesFromIDEqualityParam(context.Background(), nodePatternInfo{variable: "n", labels: []string{"R73Label", "R73Wanted"}, properties: map[string]interface{}{"flag": true}}, "id(n)=$node", map[string]interface{}{"node": id})
		require.NoError(t, err)
		require.True(t, used)
		if id == "n3" {
			require.Len(t, nodes, 1)
		} else {
			require.Empty(t, nodes)
		}
	}
}

func TestNodePatternVectorRelationshipLabelConjunction(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_pattern")
	for _, node := range []*storage.Node{
		{ID: "wrong-start", Labels: []string{"Start"}},
		{ID: "right-start", Labels: []string{"Start", "Wanted"}},
		{ID: "wrong-end", Labels: []string{"End"}},
		{ID: "right-end", Labels: []string{"End", "Wanted"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	for _, edge := range []*storage.Edge{
		{ID: "r1", StartNode: "wrong-start", EndNode: "right-end", Type: "SIMILAR"},
		{ID: "r2", StartNode: "right-start", EndNode: "wrong-end", Type: "SIMILAR"},
		{ID: "r3", StartNode: "right-start", EndNode: "right-end", Type: "SIMILAR"},
	} {
		edge.Properties = map[string]interface{}{"key": string(edge.ID), "embedding": []float64{1, 0}}
		require.NoError(t, store.CreateEdge(edge))
	}
	executor := NewStorageExecutor(store)
	_, err := executor.Execute(context.Background(), "CREATE VECTOR INDEX pattern_vector FOR ()-[r:SIMILAR]-() ON (r.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:Start:Wanted)-[r:SIMILAR]->(q:End:Wanted) RETURN r.key AS key, vector.similarity.cosine(r.embedding,[1.0,0.0]) AS score ORDER BY score DESC LIMIT 3",
		"MATCH (n:Start:Wanted)-[r:SIMILAR]->(q:End:Wanted) WITH n,r,q,vector.similarity.cosine(r.embedding,[1.0,0.0]) AS score WHERE score>0 RETURN r.key AS key,score ORDER BY score DESC LIMIT 3",
	} {
		result, err := executor.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		require.True(t, executor.LastHotPathTrace().CosineVectorIndexFastPath)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "r3", result.Rows[0][0])
	}
}
