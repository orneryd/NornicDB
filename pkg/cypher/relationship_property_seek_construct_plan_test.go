package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Inventory: statement bindings; projections; iterations; graph bindings;
// CALL imports/exports/UNION; YIELD; expression subqueries; local expression
// binders; graph comprehensions; update loops; Cypher 25; nested combinations.
// Each case checks an actual public execution, EXPLAIN, and public PROFILE.
func TestRelationshipPropertySeekConstructPlans(t *testing.T) {
	const graph = "MATCH ()-[r:T]->() WHERE r.p=k "
	const count = "COUNT { " + graph + "}"
	const exists = "EXISTS { " + graph + "}"
	const collect = "COLLECT { " + graph + "RETURN r.p }"
	for _, tc := range []struct {
		name, query string
		want        interface{}
	}{
		{"with", "WITH $x AS k " + graph + "RETURN r.p", "x"},
		{"distinct", "WITH DISTINCT $x AS k " + graph + "RETURN r.p", "x"},
		{"aggregate", "UNWIND [$x] AS v WITH head(collect(v)) AS k " + graph + "RETURN r.p", "x"},
		{"unwind", "UNWIND [$x] AS k " + graph + "RETURN r.p", "x"},
		{"optional", "WITH $x AS k OPTIONAL " + graph + "RETURN r.p", "x"},
		{"bound_start", "MATCH (a:x) WITH a,$x AS k MATCH (a)-[r:T {p:k}]->() RETURN r.p", "x"},
		{"bound_end", "MATCH (:x)-[:T]->(b) WITH b,$x AS k MATCH ()-[r:T {p:k}]->(b) RETURN r.p", "x"},
		{"call_output", "CALL { WITH $x AS k RETURN k } " + graph + "RETURN r.p", "x"},
		{"call_wildcard", "CALL { WITH $x AS k RETURN * } " + graph + "RETURN r.p", "x"},
		{"call_nested_star", "CALL { CALL { WITH $x AS k RETURN * } RETURN * } " + graph + "RETURN r.p", "x"},
		{"call_union", "CALL { WITH $x AS k RETURN k UNION WITH 'missing' AS k RETURN k } " + graph + "RETURN r.p", "x"},
		{"call_import", "WITH $x AS k CALL (k) { " + graph + "RETURN r.p AS v } RETURN v", "x"},
		{"call_star_import", "WITH $x AS k CALL (*) { " + graph + "RETURN r.p AS v } RETURN v", "x"},
		{"call_legacy", "WITH $x AS k CALL { WITH k " + graph + "RETURN r.p AS v } RETURN v", "x"},
		{"yield", "CALL db.labels() YIELD label AS k " + graph + "RETURN r.p", "x"},
		{"count", "WITH $x AS k RETURN " + count, int64(1)},
		{"exists", "WITH $x AS k RETURN " + exists, true},
		{"collect", "WITH $x AS k RETURN " + collect, []interface{}{"x"}},
		{"imported_count", "WITH $x AS k CALL (k) { RETURN " + count + " AS c } RETURN c", int64(1)},
		{"wildcard_exists", "CALL { WITH $x AS k RETURN * } RETURN " + exists, true},
		{"nested_graph_expression", "WITH $x AS k RETURN COUNT { WITH k RETURN " + count + " AS c }", int64(1)},
		{"map", "WITH $x AS k RETURN {value:" + count + "}", map[string]interface{}{"value": int64(1)}},
		{"function", "WITH $x AS k RETURN head([" + count + "])", int64(1)},
		{"case", "WITH $x AS k RETURN CASE WHEN " + exists + " THEN 'yes' ELSE 'no' END", "yes"},
		{"filter", "WITH $x AS k WHERE " + exists + " RETURN k", "x"},
		{"list_projection", "RETURN [k IN [$x] | " + count + "]", []interface{}{int64(1)}},
		{"list_predicate", "RETURN [k IN [$x] WHERE " + exists + " | k]", []interface{}{"x"}},
		{"list_source", "WITH $x AS k RETURN [v IN " + collect + " | v]", []interface{}{"x"}},
		{"list_shadow", "WITH 'missing' AS k RETURN [k IN [$x] | " + count + "]", []interface{}{int64(1)}},
		{"any", "RETURN any(k IN [$x] WHERE " + exists + ")", true},
		{"all", "RETURN all(k IN [$x] WHERE " + exists + ")", true},
		{"none", "RETURN none(k IN [$x] WHERE NOT " + exists + ")", true},
		{"single", "RETURN single(k IN [$x] WHERE " + exists + ")", true},
		{"reduce", "RETURN reduce(total=0, k IN [$x] | total+" + count + ")", int64(1)},
		{"all_reduce", "CYPHER 25 RETURN allReduce(total=0, k IN [$x] | total+" + count + ", total>0)", true},
		{"nested_list_reduce", "RETURN [v IN [$x] | reduce(total=0, k IN [v] | total+" + count + ")]", []interface{}{int64(1)}},
		{"pattern_comprehension", "WITH $x AS k RETURN [()-[r:T {p:k}]->() | r.p]", []interface{}{"x"}},
		{"pattern_path", "WITH $x AS k RETURN [p=()-[r:T {p:k}]->() | length(p)]", []interface{}{int64(1)}},
		{"pattern_where", "WITH $x AS k RETURN [()-[r:T]->() WHERE r.p=k | r.p]", []interface{}{"x"}},
		{"pattern_bound_node", "MATCH (a:x) WITH a,$x AS k RETURN [(a)-[r:T {p:k}]->() | r.p]", []interface{}{"x"}},
		{"pattern_local_relationship", "RETURN [()-[s:T]->() | COUNT { MATCH ()-[r:T]->() WHERE r.p=s.p }]", []interface{}{int64(1)}},
		{"pattern_projection", "WITH $x AS k RETURN [()-[r:T {p:k}]->() | " + count + "]", []interface{}{int64(1)}},
		{"create_expression", "WITH $x AS k CREATE (n:Output {v:" + count + "}) RETURN n.v", int64(1)},
		{"foreach_set", "CREATE (n:Output) WITH n, $x AS k FOREACH (v IN [1] | SET n.v=" + count + ") RETURN n.v", int64(1)},
		{"foreach_nested", "CREATE (n:Output) WITH n, $x AS k FOREACH (v IN [1] | FOREACH (w IN [v] | SET n.v=" + count + ")) RETURN n.v", int64(1)},
		{"foreach_source", "CREATE (n:Output) WITH n, $x AS k FOREACH (v IN " + collect + " | SET n.v=v) RETURN n.v", "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "construct_plan"))
			for _, query := range []string{"CREATE (:x)-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
				_, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
			}
			params := map[string]interface{}{"x": "x"}
			ctx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
			actual, err := exec.Execute(ctx, tc.query, params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, actual.Rows)
			executed := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).seeks["r\x00rel_p"]
			require.NotNil(t, executed, "normal execution must really seek")
			require.Positive(t, executed.lookups)
			for _, mode := range []string{"EXPLAIN", "PROFILE"} {
				// CYPHER options precede execution mode.
				query := mode + " " + tc.query
				if len(tc.query) > 10 && tc.query[:10] == "CYPHER 25 " {
					query = "CYPHER 25 " + mode + " " + tc.query[10:]
				}
				result, err := exec.Execute(context.Background(), query, params)
				require.NoError(t, err, mode)
				plan := result.Metadata["plan"].(*ExecutionPlan)
				seek := relationshipPlanOperator(plan.Root, "RelationshipIndexSeek")
				require.NotNil(t, seek, "%s differs from actual seek: %s", mode, planStringFromResult(t, result))
				require.Equal(t, "rel_p", seek.Arguments["index"])
				require.Equal(t, int64(10), seek.EstimatedRows)
				if mode == "PROFILE" {
					require.Equal(t, actual.Rows, result.Rows)
					require.Positive(t, seek.Arguments["indexLookups"])
					require.Equal(t, plan.TotalDBHits, plan.Root.DBHits)
				} else {
					require.Empty(t, result.Rows)
				}
			}
		})
	}
}
