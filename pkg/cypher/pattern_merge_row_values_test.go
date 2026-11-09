package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// MERGE retains template syntax while each execution supplies fresh row values.
func TestPatternMergeRetainsRowsAndActions(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "transaction"
		}
		for _, relationship := range []bool{false, true} {
			kind := "node"
			if relationship {
				kind = "relationship"
			}
			t.Run(mode+"/"+kind, func(t *testing.T) {
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_merge"))
				ctx := context.Background()
				run := func(q string, p map[string]interface{}) *ExecuteResult {
					r, err := e.Execute(ctx, q, p)
					require.NoError(t, err, q)
					return r
				}
				if explicit {
					run("BEGIN", nil)
					t.Cleanup(func() { _, _ = e.Execute(ctx, "ROLLBACK", nil) })
				}
				pattern := "(r:PatternMergeValue {k:v.k + $offset})"
				prefix := ""
				read := "MATCH (r:PatternMergeValue) RETURN r.k,r.created,r.matched ORDER BY r.k"
				if relationship {
					run("CREATE (:PatternMergeEndpoint {id:1}), (:PatternMergeEndpoint {id:2})", nil)
					prefix = "MATCH (a:PatternMergeEndpoint {id:1}), (b:PatternMergeEndpoint {id:2}) "
					pattern = "(a)-[r:PatternMergeValue {k:v.k + $offset}]->(b)"
					read = "MATCH ()-[r:PatternMergeValue]->() RETURN r.k,r.created,r.matched ORDER BY r.k"
				}
				q := prefix + "UNWIND $rows AS v MERGE " + pattern + " ON CREATE SET r.created=v.value + $bias ON MATCH SET r.matched=v.value + $bias RETURN count(r),$__nornic_pattern_property_0"
				for _, round := range []int64{0, 1} {
					params := map[string]interface{}{
						"rows":   []interface{}{map[string]interface{}{"k": int64(1), "value": int64(10) + round}, map[string]interface{}{"k": int64(2), "value": int64(20) + round}},
						"offset": round * 100, "bias": int64(1000), "__nornic_pattern_property_0": int64(700) + round,
					}
					require.Equal(t, [][]interface{}{{int64(2), int64(700) + round}}, run(q, params).Rows)
					params["bias"] = int64(2000)
					require.Equal(t, [][]interface{}{{int64(2), int64(700) + round}}, run(q, params).Rows)
				}
				require.Equal(t, [][]interface{}{{int64(1), int64(1010), int64(2010)}, {int64(2), int64(1020), int64(2020)}, {int64(101), int64(1011), int64(2011)}, {int64(102), int64(1021), int64(2021)}}, run(read, nil).Rows)
				if explicit {
					run("COMMIT", nil)
				}
			})
		}
		for _, property := range []string{"v.k + $offset", "$key", "7"} {
			t.Run(mode+"/template/"+property, func(t *testing.T) {
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_template"))
				ctx := context.Background()
				_, err := e.Execute(ctx, "CREATE (:PatternMergeEndpoint {id:1}), (:PatternMergeEndpoint {id:2})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = e.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = e.Execute(ctx, "ROLLBACK", nil) })
				}
				q := "MATCH (a:PatternMergeEndpoint {id:1}), (b:PatternMergeEndpoint {id:2}) UNWIND $rows AS v MERGE (a)-[r:PatternMergeValue {k:" + property + "}]->(b) RETURN r.k ORDER BY r.k"
				for _, offset := range []int64{0, 100} {
					p := map[string]interface{}{"rows": []interface{}{map[string]interface{}{"k": int64(1)}, map[string]interface{}{"k": int64(2)}}, "offset": offset, "key": int64(5) + offset, "__nornic_pattern_property_0": int64(888)}
					want := [][]interface{}{{int64(1) + offset}, {int64(2) + offset}}
					if property == "$key" {
						want = [][]interface{}{{int64(5) + offset}, {int64(5) + offset}}
					}
					if property == "7" {
						want = [][]interface{}{{int64(7)}, {int64(7)}}
					}
					for repeat := 0; repeat < 2; repeat++ {
						r, err := e.Execute(ctx, q, p)
						require.NoError(t, err)
						require.Equal(t, want, r.Rows)
					}
				}
				if explicit {
					_, err = e.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}
