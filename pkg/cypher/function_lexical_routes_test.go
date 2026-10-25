package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFunctionLexicalPublicRoutes(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, route := range []string{"direct", "use", "call"} {
				t.Run(fmt.Sprintf("normalize=%t/explicit=%t/%s", normalize, explicit, route), func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, manager.Close()) })
					require.NoError(t, manager.CreateDatabase("review_shard"))
					require.NoError(t, manager.CreateCompositeDatabase("review_cmp", []multidb.ConstituentRef{{Alias: "sh", DatabaseName: "review_shard", Type: "local", AccessMode: "read_write"}}))
					shard, err := manager.GetStorage("review_shard")
					require.NoError(t, err)
					direct := NewStorageExecutor(shard)
					ctx := context.Background()
					_, err = direct.Execute(ctx, "CREATE (:ReviewLabel)", nil)
					require.NoError(t, err)
					store, err := manager.GetStorage("review_cmp")
					require.NoError(t, err)
					composite := NewStorageExecutor(store)
					composite.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
					exec := direct
					if route != "direct" {
						exec = composite
					}
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					for _, sample := range []struct {
						query string
						rows  [][]interface{}
					}{{"UNWIND [1,2] AS x RETURN COUNT /*gap*/ { RETURN count(*) AS n } AS v", [][]interface{}{{int64(1)}, {int64(1)}}},
						{"WITH {and:true} AS n RETURN n . and AND true AS v", [][]interface{}{{true}}},
						{"RETURN toUpper(/* range('bad') */ 'ok') AS v", [][]interface{}{{"OK"}}},
						{"RETURN toUpper(/* percentileCont('bad') shortestPath((a)-->(a)) missing('bad') ) */ 'ok') AS v", [][]interface{}{{"OK"}}},
						{"RETURN toUpper(// range('bad')\n'ok') AS v", [][]interface{}{{"OK"}}},
						{"CREATE (n:LexicalEpoch {v:datetime /* don't, range('bad') */ . fromEpochMillis(0).year}) RETURN n.v AS v", [][]interface{}{{int64(1970)}}},
						{"MERGE (n:LexicalEpoch {v:datetime // don't, range('bad')\n . fromEpochMillis(0).year}) RETURN n.v AS v", [][]interface{}{{int64(1970)}}},
						{"UNWIND [1,2] AS x RETURN x /* count('bad') range('bad') */ AS v", [][]interface{}{{int64(1)}, {int64(2)}}}} {
						query := sample.query
						if route == "use" {
							query = "USE review_cmp.sh " + query
						}
						if route == "call" {
							query = "CALL { USE review_cmp.sh " + query + " } RETURN v"
						}
						result, err := exec.Execute(ctx, query, nil)
						require.NoError(t, err, query)
						require.Equal(t, sample.rows, result.Rows, query)
					}
				})
			}
		}
	}
}
