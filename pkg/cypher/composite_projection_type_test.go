package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestCompositeProjectionTypeRoutes(t *testing.T) {
	raw, err := os.ReadFile("testdata/composite_projection_type_oracle.json")
	require.NoError(t, err)
	var cases []struct {
		ID, Query, Code string
		Rows            json.RawMessage
	}
	require.NoError(t, json.Unmarshal(raw, &cases))
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, route := range []string{"direct", "use", "call"} {
				for _, tc := range cases {
					t.Run(fmt.Sprintf("normalize=%t/explicit=%t/%s/%s", normalize, explicit, route, tc.ID), func(t *testing.T) {
						config.SetCypherQueryNormalizationEnabled(normalize)
						manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, manager.Close()) })
						require.NoError(t, manager.CreateDatabase("review_shard"))
						require.NoError(t, manager.CreateCompositeDatabase("review_cmp", []multidb.ConstituentRef{{Alias: "sh", DatabaseName: "review_shard", Type: "local", AccessMode: "read_write"}}))
						shard, err := manager.GetStorage("review_shard")
						require.NoError(t, err)
						exec := NewStorageExecutor(shard)
						if route != "direct" {
							store, err := manager.GetStorage("review_cmp")
							require.NoError(t, err)
							exec = NewStorageExecutor(store)
							exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
						}
						ctx := context.Background()
						if explicit {
							_, err = exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
						}
						query := tc.Query
						if route == "use" {
							query = "USE review_cmp.sh " + query
						}
						if route == "call" {
							query = "CALL { USE review_cmp.sh " + query + " } RETURN *"
						}
						result, err := exec.Execute(ctx, query, nil)
						if tc.Code != "" {
							if !assert.Error(t, err, "query=%s result=%+v", query, result) {
								return
							}
							code, _ := nornicerrors.Neo4jStatus(err)
							assert.Equal(t, tc.Code, code, "query=%s result=%+v err=%v", query, result, err)
							return
						}
						if assert.NoError(t, err, query) {
							rows := result.Rows
							if rows == nil {
								rows = [][]interface{}{}
							}
							got, err := json.Marshal(rows)
							require.NoError(t, err)
							assert.JSONEq(t, string(tc.Rows), string(got), query)
						}
					})
				}
			}
		}
	}
}
