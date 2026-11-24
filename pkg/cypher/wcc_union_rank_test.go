package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Union by rank attaches the lower-ranked root under the higher one from
// either side. The hub is created last: in creation order the first leaf
// becomes the root, and each later leaf joins as the lower-ranked side.
// Repeating covers any order storage lists the nodes in.
func TestComputeWCCUnionByRank(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (a:W {id: 1}), (b:W {id: 2}), (c:W {id: 3}), (d:W {id: 4}), (e:W {id: 5})
		CREATE (h:W {id: 0}), (h)-[:R]->(a), (h)-[:R]->(b), (h)-[:R]->(c), (h)-[:R]->(d), (h)-[:R]->(e), (:W {id: 9})`, nil)
	require.NoError(t, err)
	for i := 0; i < 32; i++ {
		nodes, err := exec.algorithmNodes(ctx, "apoc.algo.wcc", []interface{}{"W"})
		require.NoError(t, err)
		components, err := exec.computeWCC(ctx, nodes)
		require.NoError(t, err)
		require.Len(t, components, 7)
		ids := map[int]int{}
		for _, id := range components {
			ids[id]++
		}
		require.Len(t, ids, 2)
	}
}

func TestWCCStreamsCompleteStoredGraph(t *testing.T) {
	engine, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "wcc"))
	other := NewStorageExecutor(storage.NewNamespacedEngine(engine, "other"))
	_, err = other.Execute(t.Context(), `CREATE (:V)-[:X]->(:V)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(t.Context(), `CREATE
		(a:V:Subset {n: 'a'}), (b:V:Subset {n: 'b'}), (c:V {n: 'c'}), (d:V:Subset {n: 'd'}),
		(b)-[:X]->(a), (a)-[:X]->(b), (c)-[:Y]->(b)`, nil)
	require.NoError(t, err)
	query := `CALL apoc.algo.wcc('V') YIELD node, componentId RETURN node.n, componentId ORDER BY node.n`
	rows, reads := profileReaderAccesses(t, exec, query)
	require.Len(t, rows, 4)
	require.Equal(t, rows[0][1], rows[1][1])
	require.Equal(t, rows[0][1], rows[2][1])
	require.NotEqual(t, rows[0][1], rows[3][1])
	require.Equal(t, []gdsAccess{{kind: profileKindAllRelationshipsScan, rows: 3, hits: 3}},
		profileGDSAccessesOfKinds(reads, profileKindAllRelationshipsScan, profileKindExpand))
	profile, err := exec.Execute(t.Context(), "PROFILE "+query, nil)
	require.NoError(t, err)
	require.Equal(t, rows, profile.Rows)
	scans := profilePlanOperators(executionPlanFromResult(t, profile).Root, profileKindAllRelationshipsScan, "")
	require.Len(t, scans, 1)
	require.EqualValues(t, 3, scans[0].DBHits)
	for _, selector := range []string{"'V', ['X']", "'Subset'"} {
		result, err := exec.Execute(t.Context(), `CALL apoc.algo.wcc(`+selector+`) YIELD node, componentId RETURN node.n, componentId ORDER BY node.n`, nil)
		require.NoError(t, err)
		require.Equal(t, result.Rows[0][1], result.Rows[1][1])
		for _, row := range result.Rows[2:] {
			require.NotEqual(t, result.Rows[0][1], row[1])
		}
	}
}
