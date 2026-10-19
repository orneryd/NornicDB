package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// Reviewer-derived inventory, including graph expressions in dynamic write tokens.
// Inventory from the clause enum, expression dispatch, subquery,
// pattern-expression and pipeline binding owners, rather than the fix's tests.
func TestRelationshipPropertySeekReviewerInventory(t *testing.T) {
	const g = "MATCH ()-[r:T]->() WHERE r.p=k "
	const c = "COUNT { " + g + "}"
	const x = "EXISTS { " + g + "}"
	const l = "COLLECT { " + g + "RETURN r.p }"
	cases := []struct{ name, q string }{
		{"dynamic_create_exists_label", "WITH $x AS k CREATE (n:$(CASE WHEN " + x + " THEN 'Made' ELSE 'Missing' END)) RETURN labels(n)"},
		{"dynamic_create_collect_label", "WITH $x AS k CREATE (n:$(head(" + l + "))) RETURN labels(n)"},
		{"dynamic_create_collect_type", "WITH $x AS k CREATE ()-[s:$(head(" + l + "))]->() RETURN type(s)"},
		{"parameter", "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p"},
		{"with_distinct", "WITH DISTINCT $x AS k " + g + "RETURN r.p"},
		{"with_aggregate", "UNWIND [$x,$x] AS v WITH head(collect(v)) AS k " + g + "RETURN r.p"},
		{"with_star_map_rename", "WITH {v:$x} AS m WITH *,m.v AS k " + g + "RETURN r.p"},
		{"unwind_parameter_duplicate", "UNWIND $keys AS k " + g + "RETURN r.p"},
		{"unwind_collect", "WITH $x AS k UNWIND " + l + " AS k2 MATCH ()-[r:T]->() WHERE r.p=k2 RETURN r.p"},
		{"unwind_list_local", "UNWIND [k IN [$x] | " + c + "] AS n RETURN n"},
		{"call_explicit_output", "CALL { RETURN $x AS k } " + g + "RETURN r.p"},
		{"call_wildcard_output", "CALL { WITH $x AS k RETURN * } " + g + "RETURN r.p"},
		{"call_import_explicit", "WITH $x AS k CALL (k) { " + g + "RETURN r.p AS v } RETURN v"},
		{"call_import_wildcard", "WITH $x AS k CALL (*) { " + g + "RETURN r.p AS v } RETURN v"},
		{"call_import_legacy", "WITH $x AS k CALL { WITH k " + g + "RETURN r.p AS v } RETURN v"},
		{"call_nested_export", "CALL { CALL { RETURN $x AS k } RETURN * } " + g + "RETURN r.p"},
		{"call_union_export", "CALL { RETURN $x AS k UNION ALL RETURN $x AS k } " + g + "RETURN r.p"},
		{"optional_call", "WITH $x AS k OPTIONAL CALL (k) { " + g + "RETURN r.p AS v } RETURN v"},
		{"yield", "CALL db.labels() YIELD label AS k " + g + "RETURN r.p"},
		{"yield_where_expression", "CALL db.labels() YIELD label AS k WHERE " + x + " RETURN k"},
		{"count", "WITH $x AS k RETURN " + c},
		{"exists", "WITH $x AS k RETURN " + x},
		{"collect", "WITH $x AS k RETURN " + l},
		{"count_shorthand", "WITH $x AS k RETURN COUNT { ()-[r:T {p:k}]->() }"},
		{"exists_shorthand", "WITH $x AS k RETURN EXISTS { ()-[r:T {p:k}]->() }"},
		{"count_import_nested", "WITH $x AS k CALL (k) { RETURN " + c + " AS n } RETURN n"},
		{"count_wildcard_nested", "CALL { WITH $x AS k RETURN * } RETURN " + c},
		{"collect_local_unwind", "RETURN COLLECT { UNWIND [$x] AS k " + g + "RETURN r.p }"},
		{"exists_local_call", "RETURN EXISTS { CALL { RETURN $x AS k } " + g + "RETURN r }"},
		{"nested_count_exists", "WITH $x AS k RETURN COUNT { WITH k WHERE " + x + " RETURN k }"},
		{"list_projection", "RETURN [k IN [$x] | " + c + "]"},
		{"list_predicate", "RETURN [k IN [$x] WHERE " + x + " | k]"},
		{"list_shadow", "WITH 'absent' AS k RETURN [k IN [$x] | " + c + "]"},
		{"list_nested", "RETURN [v IN [$x] | [k IN [v] | " + c + "]]"},
		{"any", "RETURN any(k IN [$x] WHERE " + x + ")"},
		{"all", "RETURN all(k IN [$x] WHERE " + x + ")"},
		{"none", "RETURN none(k IN [$x] WHERE NOT " + x + ")"},
		{"single", "RETURN single(k IN [$x] WHERE " + x + ")"},
		{"reduce_local", "RETURN reduce(n=0,k IN [$x] | n+" + c + ")"},
		{"reduce_shadow", "WITH 'absent' AS k RETURN reduce(n=0,k IN [$x] | n+" + c + ")"},
		{"pattern_inline", "WITH $x AS k RETURN [()-[r:T {p:k}]->() | r.p]"},
		{"pattern_where", "WITH $x AS k RETURN [()-[r:T]->() WHERE r.p=k | r.p]"},
		{"pattern_path", "WITH $x AS k RETURN [p=()-[r:T {p:k}]->() | length(p)]"},
		{"pattern_bound_node", "MATCH (a:Key) WITH a,$x AS k RETURN [(a)-[r:T {p:k}]->() | r.p]"},
		{"pattern_local_rel", "RETURN [()-[s:T]->() | COUNT { MATCH ()-[r:T]->() WHERE r.p=s.p }]"},
		{"pattern_nested_projection", "WITH $x AS k RETURN [()-[s:T {p:k}]->() | " + c + "]"},
		{"pattern_nested_predicate", "WITH $x AS k RETURN [()-[s:T]->() WHERE " + x + " | s.p]"},
		{"nested_pattern_list", "RETURN [k IN [$x] | [()-[r:T {p:k}]->() | r.p]]"},
		{"node_binding", "MATCH (n:Key) MATCH ()-[r:T]->() WHERE r.p=n.p RETURN r.p"},
		{"relationship_binding", "MATCH ()-[s:T]->() WITH s MATCH ()-[r:T]->() WHERE r.p=s.p RETURN r.p"},
		{"path_binding", "MATCH p=(:Key)-[:T]->() WITH p MATCH ()-[r:T]->() WHERE r.p=nodes(p)[0].p RETURN r.p"},
		{"optional", "WITH $x AS k OPTIONAL " + g + "RETURN r.p"},
		{"comma", "WITH $x AS k MATCH ()-[s:T {p:k}]->(), ()-[r:T {p:k}]->() RETURN r.p"},
		{"repeated_match", "WITH $x AS k " + g + "WITH k,r AS first " + g + "RETURN first.p,r.p"},
		{"repeated_expression", "WITH $x AS k RETURN " + c + " AS first," + c + " AS second"},
		{"union", "WITH $x AS k " + g + "RETURN r.p AS v UNION ALL WITH $x AS k " + g + "RETURN r.p AS v"},
		{"map_case", "WITH $x AS k RETURN {v:CASE WHEN " + x + " THEN " + c + " ELSE 0 END}"},
		{"function", "WITH $x AS k RETURN head([" + c + "])"},
		{"with_filter_expression", "WITH $x AS k WHERE " + x + " RETURN k"},
		{"order_expression", "WITH $x AS k RETURN k ORDER BY " + c},
		{"create_expression", "WITH $x AS k CREATE (n:Output {v:" + c + "}) RETURN n.v"},
		{"set_expression", "MATCH (n:Key) WITH n,$x AS k SET n.v=" + c + " RETURN n.v"},
		{"foreach_source", "MATCH (n:Key) WITH n,$x AS k FOREACH (v IN " + l + " | SET n.v=v) RETURN n.v"},
		{"foreach_local_source", "MATCH (n:Key) FOREACH (k IN [$x] | SET n.v=" + c + ") RETURN n.v"},
		{"foreach_nested", "MATCH (n:Key) WITH n,$x AS k FOREACH (v IN [1] | FOREACH (w IN [v] | SET n.v=" + c + ")) RETURN n.v"},
		{"call_unit_update", "MATCH (n:Key) WITH n,$x AS k CALL (n,k) { SET n.v=" + c + " } RETURN n.v"},
		{"let", "CYPHER 25 LET k=$x " + g + "RETURN r.p"},
		{"for", "CYPHER 25 FOR k IN [$x] " + g + "RETURN r.p"},
		{"filter", "CYPHER 25 WITH $x AS k FILTER " + x + " RETURN k"},
		{"let_expression", "CYPHER 25 WITH $x AS k LET n=" + c + " RETURN n"},
		{"for_shadow_list", "CYPHER 25 FOR m IN [{v:$x}] LET k=m.v " + g + "RETURN r.p"},
		{"allreduce", "CYPHER 25 RETURN allReduce(n=0,k IN [$x] | n+" + c + ", n>0)"},
		{"escaped_variable", "WITH $x AS `a b` MATCH ()-[`r r`:T]->() WHERE `r r`.p=`a b` RETURN `r r`.p"},
		{"commented", "WITH $x AS k /* scope */ " + g + "RETURN r.p"},
	}
	cases = append(cases, relationshipAccessGeneratedCases()...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r3_inventory"))
			for _, q := range []string{"CREATE (:Key {p:'x'})-[:T {p:'x',q:'y'}]->()", "CREATE RANGE INDEX sol_p FOR ()-[r:T]-() ON (r.p)"} {
				_, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err)
			}
			params := map[string]interface{}{"x": "x", "keys": []interface{}{"x", "x"}}
			ctx := withRelationshipSeekProfile(context.WithValue(context.Background(), profileExecutionKey{}, true))
			actual, err := e.Execute(ctx, tc.q, params)
			if strings.HasPrefix(tc.name, "generated_match_") {
				// Existing normalized-statement validation failure on both bases;
				// these controls must not be counted as successful seek cases.
				require.ErrorContains(t, err, "unknown function: "+dynamicLabelTestFunction)
				t.Logf("existing dynamic MATCH validation limit: %v", err)
				return
			}
			if tc.name == "foreach_local_source" || (tc.name == "yield_where_expression" && err != nil) {
				require.Error(t, err)
				message := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(err.Error(), "`", ""), " is ", " "))
				require.Contains(t, message, "variable k not defined")
				t.Logf("existing binding validation limit: %v", err)
				return
			}
			require.NoError(t, err, "query=%s", tc.q)
			if strings.HasPrefix(tc.name, "generated_") {
				require.Equal(t, relationshipAccessGeneratedRows(tc.name), actual.Rows)
			}
			switch tc.name {
			case "dynamic_create_exists_label":
				require.Equal(t, [][]interface{}{{[]interface{}{"Made"}}}, actual.Rows)
			case "dynamic_create_collect_label":
				require.Equal(t, [][]interface{}{{[]interface{}{"x"}}}, actual.Rows)
			case "dynamic_create_collect_type":
				require.Equal(t, [][]interface{}{{"x"}}, actual.Rows)
			}
			stats := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).seeks
			require.Empty(t, ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile).unregistered, "runtime access must originate in the shared program")
			require.NotEmpty(t, stats, "expected executed seek; rows=%v", actual.Rows)
			for _, mode := range []string{"EXPLAIN", "PROFILE"} {
				query := mode + " " + tc.q
				if strings.HasPrefix(tc.q, "CYPHER 25 ") {
					query = "CYPHER 25 " + mode + " " + tc.q[len("CYPHER 25 "):]
				}
				result, err := e.Execute(context.Background(), query, params)
				if mode == "PROFILE" && (tc.name == "let" || tc.name == "for" || tc.name == "filter" || tc.name == "let_expression" || tc.name == "for_shadow_list") && err != nil {
					require.ErrorContains(t, err, "LET, FILTER and FOR require CYPHER 25 with the ANTLR parser")
					t.Log("existing ANTLR PROFILE preamble limit; normal and EXPLAIN checked, PROFILE equality unavailable")
					continue
				}
				require.NoError(t, err, mode)
				plan := result.Metadata["plan"].(*ExecutionPlan)
				observed := map[string][3]int64{}
				var visit func(*PlanOperator)
				visit = func(op *PlanOperator) {
					if op.OperatorType == "RelationshipIndexSeek" {
						require.Equal(t, int64(10), op.EstimatedRows, "%s estimate", mode)
						key := op.Identifiers[0] + "\x00" + fmt.Sprint(op.Arguments["index"])
						if tc.name == "escaped_variable" {
							key = "quoted\x00" + fmt.Sprint(op.Arguments["index"])
						}
						a := observed[key]
						if mode == "PROFILE" {
							a[0] += op.Arguments["indexLookups"].(int64)
							a[1] += op.Arguments["relationshipCandidates"].(int64)
							a[2] += op.ActualRows
							require.Equal(t, op.Arguments["indexLookups"].(int64)+op.Arguments["relationshipCandidates"].(int64), op.DBHits)
						}
						observed[key] = a
					}
					for _, child := range op.Children {
						visit(child)
					}
				}
				visit(plan.Root)
				for key, s := range stats {
					if tc.name == "escaped_variable" {
						key = "quoted\x00" + strings.SplitN(key, "\x00", 2)[1]
					}
					got, present := observed[key]
					require.True(t, present, "%s missing executed %q: %s", mode, key, planStringFromResult(t, result))
					if mode == "PROFILE" {
						require.Equal(t, [3]int64{s.lookups, s.candidates, s.rows}, got, "%s", key)
					}
				}
				require.Equal(t, len(stats), len(observed), "plan contains an access execution did not select")
				if mode == "PROFILE" {
					require.Equal(t, actual.Rows, result.Rows)
					require.Equal(t, plan.Root.DBHits, plan.TotalDBHits)
				} else {
					require.Empty(t, result.Rows)
				}
			}
		})
	}
}
