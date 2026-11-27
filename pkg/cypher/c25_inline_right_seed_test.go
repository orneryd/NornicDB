package cypher

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// C25: an inline key at a fixed chain's right end must seed the reverse walk.
// Before the fix, the 8,000-pair form read 72,000 entities versus seven for
// the same-result left-keyed control.
func TestC25InlineRightKeyedFixedChain(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, count := range []int{1_000, 8_000} {
			t.Run(fmt.Sprint(count), func(t *testing.T) {
				ctx := t.Context()
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "c25"))
				_, err := exec.Execute(ctx, "CREATE INDEX c25_id IF NOT EXISTS FOR (n:C25Node) ON (n.id)", nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, "CREATE INDEX c25_bucket IF NOT EXISTS FOR (n:C25Node) ON (n.bucket)", nil)
				require.NoError(t, err)
				empty, err := exec.Execute(ctx, "MATCH (a:C25Node)-[:C25Rel]->(m:C25Node)-[:C25Rel]->(z:C25Node {id:$id}) RETURN count(a)", map[string]interface{}{"id": "absent"})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, empty.Rows)
				_, err = exec.Execute(ctx, `UNWIND range(0, $n - 1) AS i
					CREATE (a:C25Node {id:'a'+toString(i)}),
					       (m:C25Node {id:'m'+toString(i)}),
					       (z:C25Node {id:'z'+toString(i), bucket:'shared'})
					CREATE (a)-[:C25Rel]->(m), (m)-[:C25Rel]->(z)`, map[string]interface{}{"n": count})
				require.NoError(t, err)

				cases := []struct {
					name, query string
					params      map[string]interface{}
					want        int64
					maxHits     int64
				}{
					{"right_keyed", "MATCH (a:C25Node)-[:C25Rel]->(m:C25Node)-[:C25Rel]->(z:C25Node {id:$id}) RETURN count(a)", map[string]interface{}{"id": fmt.Sprintf("z%d", count-1)}, 1, 32},
					{"left_keyed_control", "MATCH (a:C25Node {id:$id})-[:C25Rel]->(m:C25Node)-[:C25Rel]->(z:C25Node) RETURN count(z)", map[string]interface{}{"id": fmt.Sprintf("a%d", count-1)}, 1, 32},
					{"bound_end_control", "MATCH (z:C25Node {id:$id}) MATCH (a:C25Node)-[:C25Rel]->(m:C25Node)-[:C25Rel]->(z) RETURN count(a)", map[string]interface{}{"id": fmt.Sprintf("z%d", count-1)}, 1, 32},
					{"missing_right_key", "MATCH (a:C25Node)-[:C25Rel]->(m:C25Node)-[:C25Rel]->(z:C25Node {id:$id}) RETURN count(a)", map[string]interface{}{"id": "absent"}, 0, 32},
					{"wide_index_posting", "MATCH (a:C25Node)-[:C25Rel]->(m:C25Node)-[:C25Rel]->(z:C25Node {bucket:$bucket}) RETURN count(a)", map[string]interface{}{"bucket": "shared"}, int64(count), 0},
					{"one_hop_control", "MATCH (m)-[:C25Rel]->(z {id:$id}) RETURN count(m)", map[string]interface{}{"id": fmt.Sprintf("z%d", count-1)}, 1, 0},
				}
				for _, test := range cases {
					t.Run(test.name, func(t *testing.T) {
						var elapsed time.Duration
						for rep := 0; rep < 3; rep++ {
							started := time.Now()
							result, err := exec.Execute(ctx, "PROFILE "+test.query, test.params)
							elapsed += time.Since(started)
							require.NoError(t, err)
							require.Len(t, result.Rows, 1)
							require.Len(t, result.Rows[0], 1)
							require.EqualValues(t, test.want, result.Rows[0][0])
							if rep == 0 {
								plan, ok := result.Metadata["plan"].(*ExecutionPlan)
								require.True(t, ok, "PROFILE plan: %T", result.Metadata["plan"])
								require.True(t, plan.TotalDBHitsObserved)
								t.Logf("PROFILE hits=%d plan=%+v", plan.TotalDBHits, Neo4jPlanMap(plan, true))
								if test.maxHits > 0 {
									require.LessOrEqual(t, plan.TotalDBHits, test.maxHits)
								}
							}
						}
						t.Logf("mean of three executions: %s", elapsed/3)
					})
				}
				if count == 1_000 {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					result, err := exec.Execute(ctx, cases[0].query, cases[0].params)
					require.NoError(t, err)
					require.EqualValues(t, 1, result.Rows[0][0])
					_, err = exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			})
		}
	})
}
