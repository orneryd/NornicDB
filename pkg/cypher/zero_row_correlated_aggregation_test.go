package cypher

import (
	"context"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Empty row streams must reach the final aggregation without evaluating n in
// a correlated CALL or in later OPTIONAL MATCH property maps.
func TestZeroRowCorrelatedCallAggregation(t *testing.T) {
	for _, fixture := range []string{"row_zero_aggregate.cypher", "row_zero_aggregate_minimal.cypher"} {
		query, err := os.ReadFile("testdata/" + fixture)
		require.NoError(t, err)
		for _, explicit := range []bool{false, true} {
			mode := "auto"
			if explicit {
				mode = "explicit"
			}
			for _, populated := range []bool{false, true} {
				name := "empty"
				if populated {
					name = "nonempty"
				}
				t.Run(fixture+"/"+mode+"/"+name, func(t *testing.T) {
					base := storage.NewMemoryEngine()
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "zero_row_aggregate"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:RowAggregateNode:RowAggregateItem {id:'n0',origin:'automatic',group_id:'group0',source_key:'s0',name:'item',cite_start:[],cite_end:[],identifier_values:[],identifier_schemes:[],identifier_issuers:[],identifier_jurisdictions:[],identifier_periods:[],identifier_categories:[],alias_names:[],alias_types:[]}), (:RowAggregateSource {id:'s0',text:''})", nil)
					require.NoError(t, err)
					params := map[string]interface{}{"active": []interface{}{}, "items": []interface{}{}, "current": true, "group_id": "group0", "source_key": "s0"}
					want := 0
					if populated {
						params["active"] = []interface{}{map[string]interface{}{"targets": []interface{}{"n0"}}}
						params["items"] = []interface{}{map[string]interface{}{"continues": "n0", "own": "fallback"}}
						want = 1
					}
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					result, err := exec.Execute(ctx, string(query), params)
					require.NoError(t, err)
					require.Len(t, result.Rows, 1, "an ungrouped aggregation produces one row on empty input")
					require.Len(t, result.Rows[0], 3)
					subjects := result.Rows[0][0].([]interface{})
					require.Len(t, subjects, want)
					given := result.Rows[0][1].(map[string]interface{})
					require.Len(t, given["subject"], want)
					require.Empty(t, given["party"])
					require.Empty(t, given["role"])
					carry := result.Rows[0][2].(map[string]interface{})
					require.Len(t, carry["ids"], want)
					require.Len(t, carry["carried"], want)
					if populated {
						require.Equal(t, "n0", subjects[0].(map[string]interface{})["id"])
						require.Equal(t, []interface{}{"n0"}, carry["ids"])
						require.Equal(t, []interface{}{"n0"}, carry["carried"])
					}
					if explicit {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}
