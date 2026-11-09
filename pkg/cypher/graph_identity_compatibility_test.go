package cypher

import (
	"context"
	"fmt"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A qualified element ID identifies one database, including when the selected
// store is not composite. Foreign qualifiers cannot name a local entity.
func TestPlainDatabaseElementIDsRequireExactDatabase(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, database := range []string{"test", "nornic", "archive"} {
				for _, explicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/explicit=%v", database, explicit), func(t *testing.T) {
						engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
						require.NoError(t, err)
						t.Cleanup(func() { _ = engine.Close() })
						store := storage.NewNamespacedEngine(engine, database)
						_, err = store.CreateNode(t.Context(), &storage.Node{ID: "legacy-a", Labels: []string{"LegacyP"}})
						require.NoError(t, err)
						_, err = store.CreateNode(t.Context(), &storage.Node{ID: "legacy-b", Labels: []string{"LegacyQ"}})
						require.NoError(t, err)
						require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "legacy-edge", StartNode: "legacy-a", EndNode: "legacy-b", Type: "LEGACY"}))
						_, err = store.CreateNode(t.Context(), &storage.Node{ID: "4:historical:legacy-a", Labels: []string{"Opaque"}})
						require.NoError(t, err)
						require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "opaque-edge", StartNode: "4:historical:legacy-a", EndNode: "legacy-b", Type: "OPAQUE"}))
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
						assert.Equal(t, []interface{}{"4:" + database + ":legacy-a", "5:" + database + ":legacy-edge", "4:" + database + ":legacy-b"}, result.Rows[0], "emitted IDs retain the selected database")
						var expectedWrites int64
						for _, qualifier := range []string{"nornicdb", database, "historical", "", strings.ToUpper(database)} {
							nodeID, edgeID := "4:"+qualifier+":legacy-a", "5:"+qualifier+":legacy-edge"
							want := int64(0)
							if qualifier == database {
								want = 1
								expectedWrites++
							}
							for _, query := range []string{"MATCH (a:LegacyP) WHERE elementId(a)=$id RETURN count(a)", "MATCH (a:LegacyP) WHERE elementId(a) IN [$id] RETURN count(a)", "MATCH (a:LegacyP) WHERE elementId(a)=$id AND true RETURN count(a)", "MATCH (a:LegacyP) WHERE id(a)=$id RETURN count(a)", "MATCH (a:LegacyP) WHERE elementId(a) IN [$id] OR false RETURN count(a)"} {
								result, err = exec.Execute(ctx, query, map[string]interface{}{"id": nodeID})
								require.NoError(t, err, query)
								assert.Equal(t, [][]interface{}{{want}}, result.Rows, query+" "+nodeID)
							}
							result, err = exec.Execute(ctx, "MATCH ()-[r:LEGACY]->() WHERE elementId(r)=$id RETURN count(r)", map[string]interface{}{"id": edgeID})
							require.NoError(t, err)
							assert.Equal(t, [][]interface{}{{want}}, result.Rows, edgeID)
							result, err = exec.Execute(ctx, "MATCH (a:LegacyP), (b:LegacyQ) WHERE elementId(a)=$a AND elementId(b)=$b CREATE (a)-[r:LEGACY_CREATED]->(b) RETURN count(r)", map[string]interface{}{"a": nodeID, "b": "4:" + qualifier + ":legacy-b"})
							require.NoError(t, err)
							assert.Equal(t, [][]interface{}{{want}}, result.Rows, nodeID)
							result, err = exec.Execute(ctx, "RETURN gds.util.asNode($id) IS NOT NULL, size(gds.util.asNodes([$id]))", map[string]interface{}{"id": nodeID})
							require.NoError(t, err)
							assert.Equal(t, [][]interface{}{{want == 1, want}}, result.Rows, "graph functions "+nodeID)
							if want == 1 || !explicit {
								result, err = exec.Execute(ctx, "CALL apoc.path.subgraphNodes($id,{maxLevel:0}) YIELD node RETURN count(node)", map[string]interface{}{"id": nodeID})
								if want == 0 {
									require.Error(t, err, "foreign path input must not resolve")
								} else {
									require.NoError(t, err)
									assert.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
								}
							}
						}
						result, err = exec.Execute(ctx, "MATCH ()-[r:LEGACY_CREATED]->() RETURN count(r)", nil)
						require.NoError(t, err)
						assert.Equal(t, [][]interface{}{{expectedWrites}}, result.Rows, "foreign IDs caused no writes")
						result, err = exec.Execute(ctx, "MATCH (a:LegacyP) WHERE id(a)='legacy-a' RETURN count(a)", nil)
						require.NoError(t, err)
						assert.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "raw local ID extension is preserved")
						result, err = exec.Execute(ctx, "CALL apoc.algo.dijkstra($source,$target,'OPAQUE','weight') YIELD path RETURN count(path)", map[string]interface{}{"source": "4:historical:legacy-a", "target": "legacy-b"})
						require.NoError(t, err)
						assert.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "a foreign element ID cannot become an opaque local ID")
						result, err = exec.Execute(ctx, "MATCH (a:Opaque),(b:LegacyQ) CALL apoc.algo.dijkstra(a,b,'OPAQUE','weight') YIELD path RETURN count(path)", nil)
						require.NoError(t, err)
						assert.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "native nodes retain opaque local IDs")
						// An invalid procedure argument fails an explicit transaction; test it
						// last so subsequent assertions do not run on that failed transaction.
						if explicit {
							_, err = exec.Execute(ctx, "CALL apoc.path.subgraphNodes($id,{maxLevel:0}) YIELD node RETURN count(node)", map[string]interface{}{"id": "4:historical:legacy-a"})
							require.Error(t, err, "foreign path input must not resolve in a transaction")
						}
					})
				}
			}
		})
	}
}
