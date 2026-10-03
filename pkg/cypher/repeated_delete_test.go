package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A statement that deletes the same entity on several rows deletes it once:
// it counts once and the stored counts stay right, auto-commit and in an
// explicit transaction alike (#827; expected values from Neo4j 5.26).
func TestRepeatedDeleteInOneStatementCountsOnce(t *testing.T) {
	for _, tc := range []struct {
		query                          string
		nodesDeleted, relsDeleted      int
		nodesAfter, relationshipsAfter int64
	}{
		{"MATCH ()-[r:R]->() WITH collect(r) + collect(r) AS rs UNWIND rs AS r DELETE r", 0, 2, 3, 0},
		{"MATCH (n:P) WITH collect(n) + collect(n) AS ns UNWIND ns AS n DETACH DELETE n", 3, 2, 0, 0},
		{"MATCH (a:P {id: 'a'})-[r:R]->(b) DETACH DELETE a, r", 1, 1, 2, 1},
	} {
		for _, explicit := range []bool{false, true} {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (a:P {id: 'a'})-[:R]->(b:P {id: 'b'})-[:R]->(c:P {id: 'c'})", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.nodesDeleted, result.Stats.NodesDeleted, "%s explicit=%v", tc.query, explicit)
			require.Equal(t, tc.relsDeleted, result.Stats.RelationshipsDeleted, "%s explicit=%v", tc.query, explicit)
			if explicit {
				_, err = exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			nodes, err := exec.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
			require.NoError(t, err)
			rels, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.nodesAfter}}, nodes.Rows, "%s explicit=%v", tc.query, explicit)
			require.Equal(t, [][]interface{}{{tc.relationshipsAfter}}, rels.Rows, "%s explicit=%v", tc.query, explicit)
		}
	}
}

// The transaction wrapper's BulkDeleteEdges skips relationships that are
// gone, as the engines' does.
func TestTransactionWrapperBulkDeleteEdgesSkipsDeleted(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), "CREATE (:P {id: 'a'})-[:R]->(:P {id: 'b'})", nil)
	require.NoError(t, err)
	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)

	require.NoError(t, base.EnsureNamespaceMVCC("test"))
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("test"))
	wrapper := &transactionStorageWrapper{tx: tx, underlying: store, namespace: "test", separator: ":", mutatedNodeIDs: make(map[string]struct{})}
	require.NoError(t, wrapper.BulkDeleteEdges([]storage.EdgeID{edges[0].ID, edges[0].ID, "missing"}))
	require.NoError(t, tx.Commit())
	count, err := base.EdgeCount()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}
