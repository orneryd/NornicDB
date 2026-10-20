package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeNativeAggregateSourceBoundary(t *testing.T) {
	for _, graph := range []string{"one", "comp"} {
		for _, mode := range []string{"autocommit", "explicit"} {
			t.Run(graph+"/"+mode, func(t *testing.T) {
				_, manager := newCompositeIdentityExecutor(t, false)
				for _, database := range []string{"one", "two"} {
					store, err := manager.GetStorage(database)
					require.NoError(t, err)
					target, err := store.GetNode("target")
					require.NoError(t, err)
					target.Properties["db"] = database
					require.NoError(t, store.UpdateNode(target))
					_, err = store.CreateNode(&storage.Node{ID: "leaf", Labels: []string{"L"}, Properties: map[string]interface{}{"db": database}})
					require.NoError(t, err)
					_, err = store.CreateNode(&storage.Node{ID: "tail", Labels: []string{"Z"}, Properties: map[string]interface{}{"db": database}})
					require.NoError(t, err)
					require.NoError(t, store.CreateEdge(&storage.Edge{ID: "middle-rel", StartNode: "target", EndNode: "leaf", Type: "S"}))
					require.NoError(t, store.CreateEdge(&storage.Edge{ID: "tail-rel", StartNode: "leaf", EndNode: "tail", Type: "T"}))
				}
				store, err := manager.GetStorage(graph)
				require.NoError(t, err)
				executor := NewStorageExecutor(store)
				executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
				ctx := context.Background()
				if mode == "explicit" {
					_, err = executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
				}
				for _, test := range []struct {
					name, query string
					columns     int
				}{
					{"outgoing", "MATCH (a:P)-[:R]->(b:Q) RETURN a.db AS source,count(b) AS total ORDER BY source", 2},
					{"incoming", "MATCH (b:Q)<-[:R]-(a:P) RETURN b.db AS source,count(a) AS total ORDER BY source", 2},
					{"two hop", "MATCH (a:P)-[:R]->(b:Q)-[:S]->(c:L) RETURN a.db AS leftDb,c.db AS rightDb,count(b) AS total ORDER BY leftDb,rightDb", 3},
					{"three hop", "MATCH (a:P)-[:R]->(b:Q)-[:S]->(c:L)-[:T]->(d:Z) RETURN a.db AS leftDb,d.db AS rightDb,count(DISTINCT b) AS total ORDER BY leftDb,rightDb", 3},
				} {
					t.Run(test.name, func(t *testing.T) {
						query := test.query
						if graph == "comp" {
							body, order, found := strings.Cut(query, " ORDER BY ")
							require.True(t, found)
							columns := "source,total"
							if test.columns == 3 {
								columns = "leftDb,rightDb,total"
							}
							query = "USE comp CALL { USE comp.one " + body + " UNION ALL USE comp.two " + body + " } RETURN " + columns + " ORDER BY " + order
						}
						result, err := executor.Execute(ctx, query, nil)
						require.NoError(t, err)
						databases := []string{"one"}
						if graph == "comp" {
							databases = append(databases, "two")
						}
						var expected [][]interface{}
						for _, database := range databases {
							row := []interface{}{database}
							if test.columns == 3 {
								row = append(row, database)
							}
							expected = append(expected, append(row, int64(1)))
						}
						require.Equal(t, expected, result.Rows, "same local IDs must not join or aggregate across sources")
					})
				}
				if mode == "explicit" {
					_, err = executor.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestOptimizedMutualOpaqueIdentityBoundary(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "opaque")
	for _, id := range []storage.NodeID{"a:b", "c", "c:a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id, Properties: map[string]interface{}{"name": string(id)}})
		require.NoError(t, err)
	}
	// The reverse of a:b -> c is c -> a:b, not c:a -> b.
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "first", StartNode: "a:b", EndNode: "c", Type: "R"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "second", StartNode: "c:a", EndNode: "b", Type: "R"}))
	executor := NewStorageExecutor(store)
	query := "MATCH (a)-[:R]->(b)-[:R]->(a) RETURN a.name,b.name"
	result, used := executor.ExecuteOptimized(context.Background(), query, PatternInfo{Pattern: PatternMutualRelationship, RelType: "R"})
	require.True(t, used)
	require.Empty(t, result.Rows, "opaque endpoint tuples cannot be concatenated into ambiguous pair keys")
	ordinary, err := executor.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Empty(t, ordinary.Rows)
}
