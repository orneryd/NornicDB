package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// IDs accepted by shipped plain databases can be persisted by clients. A
// composite's real source selection must not remove that plain-store contract.
func TestPlainDatabaseLegacyElementIDRoundTrip(t *testing.T) {
	for _, database := range []string{"test", "nornic", "archive"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", database, explicit), func(t *testing.T) {
				engine, err := storage.NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { _ = engine.Close() })
				store := storage.NewNamespacedEngine(engine, database)
				_, err = store.CreateNode(&storage.Node{ID: "legacy-a", Labels: []string{"LegacyP"}})
				require.NoError(t, err)
				_, err = store.CreateNode(&storage.Node{ID: "legacy-b", Labels: []string{"LegacyQ"}})
				require.NoError(t, err)
				require.NoError(t, store.CreateEdge(&storage.Edge{ID: "legacy-edge", StartNode: "legacy-a", EndNode: "legacy-b", Type: "LEGACY"}))
				exec := NewStorageExecutor(store)
				ctx := context.Background()
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				result, err := exec.Execute(ctx, "MATCH (a:LegacyP)-[r:LEGACY]->(b:LegacyQ) RETURN elementId(a),elementId(r),elementId(b)", nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				t.Logf("shipped public element IDs for database %s: %#v", database, result.Rows[0])
				for _, qualifier := range []string{"nornicdb", database, "historical"} {
					nodeID, edgeID := "4:"+qualifier+":legacy-a", "5:"+qualifier+":legacy-edge"
					for _, query := range []string{"MATCH (a:LegacyP) WHERE elementId(a)=$id RETURN count(a)", "MATCH (a:LegacyP) WHERE elementId(a) IN [$id] RETURN count(a)", "MATCH (a:LegacyP) WHERE id(a)=$id RETURN count(a)"} {
						result, err = exec.Execute(ctx, query, map[string]interface{}{"id": nodeID})
						require.NoError(t, err, query)
						require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, query+" "+nodeID)
					}
					result, err = exec.Execute(ctx, "MATCH ()-[r:LEGACY]->() WHERE elementId(r)=$id RETURN count(r)", map[string]interface{}{"id": edgeID})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, edgeID)
					result, err = exec.Execute(ctx, "MATCH (a:LegacyP), (b:LegacyQ) WHERE elementId(a)=$a AND elementId(b)=$b CREATE (a)-[r:LEGACY_CREATED]->(b) RETURN count(r)", map[string]interface{}{"a": nodeID, "b": "4:" + qualifier + ":legacy-b"})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, nodeID)
				}
			})
		}
	}
}
