package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPDW161Item4KeyedRelationshipShapes(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{1_000, 8_000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			_, err := exec.Execute(ctx, "CREATE INDEX pd_w161_nid IF NOT EXISTS FOR (n:PDW161Node) ON (n.id)", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE INDEX pd_w161_rid IF NOT EXISTS FOR ()-[r:PDW161R]-() ON (r.id)", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, `UNWIND range(0, $n - 1) AS i
				CREATE (a:PDW161Node {id:'a'+toString(i)}),
				       (m:PDW161Node {id:'m'+toString(i)}),
				       (z:PDW161Node {id:'z'+toString(i)})
				CREATE (a)-[:PDW161R {id:'r1-'+toString(i)}]->(m),
				       (m)-[:PDW161R {id:'r2-'+toString(i)}]->(z)`, map[string]interface{}{"n": n})
			require.NoError(t, err)
			first, err := exec.Execute(ctx, "MATCH ()-[r:PDW161R]->() RETURN elementId(r) AS id LIMIT 1", nil)
			require.NoError(t, err)
			require.NotEmpty(t, first.Rows)
			cases := []struct {
				name, query string
				params      map[string]interface{}
			}{
				{"two_hops_keyed_right", "MATCH (a:PDW161Node)-[:PDW161R]->(m:PDW161Node)-[:PDW161R]->(z:PDW161Node {id:$id}) RETURN count(a)", map[string]interface{}{"id": "z" + fmt.Sprint(n-1)}},
				{"unlabelled_keyed_end", "MATCH (a)-[:PDW161R]->(z {id:$id}) RETURN count(a)", map[string]interface{}{"id": "z" + fmt.Sprint(n-1)}},
				{"start_keyed_where", "MATCH (a:PDW161Node)-[:PDW161R]->(m) WHERE a.id=$id RETURN count(m)", map[string]interface{}{"id": "a" + fmt.Sprint(n-1)}},
				{"relationship_property_where", "MATCH (a)-[r:PDW161R]->(z) WHERE r.id=$id RETURN count(r)", map[string]interface{}{"id": "r1-" + fmt.Sprint(n-1)}},
				{"reversed_element_id", "MATCH (a)-[r:PDW161R]->(z) WHERE $id=elementId(r) RETURN count(r)", map[string]interface{}{"id": first.Rows[0][0]}},
			}
			for _, tc := range cases {
				var elapsed time.Duration
				var rows int
				for rep := 0; rep < 3; rep++ {
					start := time.Now()
					res, err := exec.Execute(ctx, "PROFILE "+tc.query, tc.params)
					elapsed += time.Since(start)
					require.NoError(t, err, tc.name)
					rows = len(res.Rows)
					if rep == 0 {
						plan, ok := res.Metadata["plan"].(*ExecutionPlan)
						require.True(t, ok, "profile plan: %T", res.Metadata["plan"])
						t.Logf("PD_W161_CAUSE item=4 shape=%s size=%d result_rows=%d plan=%+v", tc.name, n, rows, Neo4jPlanMap(plan, true))
						if tc.name == "relationship_property_where" {
							require.Equal(t, [][]interface{}{{int64(1)}}, res.Rows)
							require.Less(t, plan.TotalDBHits, int64(500), "indexed relationship equality should seed the one matching edge")
						}
					}
				}
				t.Logf("PD_W161_RESULT item=4 shape=%s size=%d repeats=3 elapsed_ms=%.3f", tc.name, n, float64(elapsed.Microseconds())/3000)
				require.Equal(t, 1, rows)
			}
		})
	}
}
