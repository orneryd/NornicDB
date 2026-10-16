package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIssue1226MainRuntimeRegressions(t *testing.T) {
	tests := []struct {
		name, setup, query, code string
		rows                     [][]interface{}
	}{
		{name: "valid_comprehension_shadow", setup: "CREATE (:X)", query: "MATCH (k:X) RETURN [k IN [1, 2] | k * 2]", rows: [][]interface{}{{[]interface{}{int64(2), int64(4)}}}},
		{name: "valid_comprehension_filtered_mixed", setup: "CREATE (n:X)", query: "MATCH (n:X) WITH n, [n, 1] AS xs RETURN [k IN xs WHERE k = n | labels(k)]", rows: [][]interface{}{{[]interface{}{[]interface{}{"X"}}}}},
		{name: "valid_unknown_coalesce", setup: "CREATE (:X {a: 'hi'})", query: "MATCH (n:X) RETURN toUpper(coalesce(n.a, 'x'))", rows: [][]interface{}{{"HI"}}},
		{name: "valid_path_length", setup: "CREATE (:X)-[:R]->(:Y)", query: "MATCH p = (a)-->(b) RETURN length(p)", rows: [][]interface{}{{int64(1)}}},
		{name: "valid_map_keys_marker", query: "WITH {_pathResult: 1, a: 2} AS m RETURN keys(m)", rows: [][]interface{}{{[]interface{}{"_pathResult", "a"}}}},
		{name: "length_runtime_map", query: "UNWIND [{length: 42}] AS m RETURN length(CASE WHEN true THEN m ELSE null END)", code: "Neo.ClientError.Statement.TypeError"},
		{name: "valid_comprehension_case_shadow", setup: "CREATE (:X)", query: "MATCH (x:X) RETURN [x IN ['a'] | toUpper(CASE WHEN true THEN x ELSE x END)]", rows: [][]interface{}{{[]interface{}{"A"}}}},
		{name: "comprehension_case_runtime_error", query: "RETURN [x IN [1] | toUpper(CASE WHEN true THEN x ELSE 'a' END)] AS v", code: "Neo.ClientError.Statement.TypeError"},
		{name: "valid_case_mixed_argument", query: "RETURN toUpper(CASE WHEN false THEN 1 ELSE 'a' END) AS v", rows: [][]interface{}{{"A"}}},
		{name: "valid_comprehension_case_mixed", query: "RETURN [x IN [1] | toUpper(CASE WHEN false THEN x ELSE 'a' END)] AS v", rows: [][]interface{}{{[]interface{}{"A"}}}},
		{name: "case_mixed_runtime_error", query: "RETURN toUpper(CASE WHEN true THEN 1 ELSE 'a' END) AS v", code: "Neo.ClientError.Statement.TypeError"},
		{name: "valid_simple_case_mixed", query: "RETURN toUpper(CASE 0 WHEN 1 THEN 1 ELSE 'a' END) AS v", rows: [][]interface{}{{"A"}}},
		{name: "valid_nested_case_mixed", query: "RETURN toUpper(CASE WHEN true THEN CASE WHEN false THEN 1 ELSE 'a' END ELSE 'b' END) AS v", rows: [][]interface{}{{"A"}}},
		{name: "comprehension_case_absent_else_static_error", query: "RETURN [x IN [1] | toUpper(CASE WHEN true THEN x END)] AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_integer_nullable_runtime_error", query: "RETURN toUpper(CASE WHEN true THEN 1 ELSE null END) AS v", code: "Neo.ClientError.Statement.TypeError"},
		{name: "valid_case_null_branch", query: "RETURN toUpper(CASE WHEN false THEN 1 ELSE null END) AS v", rows: [][]interface{}{{nil}}},
		{name: "valid_case_string_nullable", query: "RETURN toUpper(CASE WHEN true THEN 'a' ELSE null END) AS v", rows: [][]interface{}{{"A"}}},
		{name: "case_mixed_empty_input", query: "UNWIND [] AS outer RETURN toUpper(CASE WHEN true THEN 1 ELSE 'a' END) AS v", rows: [][]interface{}{}},
		{name: "case_homogeneous_empty_input_static_error", query: "UNWIND [] AS outer RETURN toUpper(CASE WHEN true THEN 1 ELSE 2 END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_numeric_union_static_error", query: "RETURN toUpper(CASE WHEN true THEN 1 ELSE 2.0 END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_numeric_union_empty_static_error", query: "UNWIND [] AS outer RETURN toUpper(CASE WHEN true THEN 1 ELSE 2.0 END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_list_union_static_error", query: "RETURN toUpper(CASE WHEN true THEN [1] ELSE ['a'] END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_list_union_empty_static_error", query: "UNWIND [] AS outer RETURN toUpper(CASE WHEN true THEN [1] ELSE ['a'] END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_nested_list_union_static_error", query: "RETURN toUpper(CASE WHEN true THEN [[1]] ELSE [['a']] END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_simple_numeric_union_static_error", query: "RETURN toUpper(CASE 0 WHEN 0 THEN 1 ELSE 2.0 END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_nested_numeric_union_static_error", query: "RETURN toUpper(CASE WHEN true THEN CASE WHEN false THEN 1 ELSE 2.0 END ELSE 3 END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_comprehension_numeric_union_static_error", query: "RETURN [x IN [1] | toUpper(CASE WHEN true THEN x ELSE 2.0 END)] AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_comprehension_list_union_static_error", query: "RETURN [x IN [1] | toUpper(CASE WHEN true THEN [x] ELSE ['a'] END)] AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_projected_numeric_union_static_error", query: "WITH CASE WHEN true THEN 1 ELSE 2.0 END AS x RETURN toUpper(x) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_valid_list_union", query: "RETURN size(CASE WHEN true THEN [1] ELSE ['a'] END) AS v", rows: [][]interface{}{{int64(1)}}},
		{name: "case_valid_nested_list_union", query: "RETURN size(CASE WHEN false THEN [[1]] ELSE [['a']] END) AS v", rows: [][]interface{}{{int64(1)}}},
		{name: "case_valid_numeric_union", query: "RETURN abs(CASE WHEN true THEN 1 ELSE 2.0 END) AS v", rows: [][]interface{}{{int64(1)}}},
		{name: "case_valid_numeric_union_float", query: "RETURN abs(CASE WHEN false THEN 1 ELSE 2.0 END) AS v", rows: [][]interface{}{{float64(2)}}},
		{name: "case_numeric_union_nullable_runtime_error", query: "RETURN toUpper(CASE WHEN true THEN CASE WHEN true THEN 1 ELSE 2.0 END ELSE null END) AS v", code: "Neo.ClientError.Statement.TypeError"},
		{name: "case_numeric_union_nullable_empty", query: "UNWIND [] AS outer RETURN toUpper(CASE WHEN true THEN CASE WHEN true THEN 1 ELSE 2.0 END ELSE null END) AS v", rows: [][]interface{}{}},
		{name: "case_nested_list_union_element_static_error", query: "RETURN toUpper((CASE WHEN true THEN [[1]] ELSE [['a']] END)[0]) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_list_union_comprehension_source_static_error", query: "RETURN [x IN CASE WHEN true THEN [1] ELSE [2.0] END | toUpper(x)] AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_numeric_union_arithmetic", query: "RETURN (CASE WHEN true THEN 1 ELSE 2.0 END) + 1 AS v", rows: [][]interface{}{{int64(2)}}},
		{name: "case_valid_branch_after_invalid_nested_union", query: "RETURN toUpper(CASE WHEN false THEN CASE WHEN true THEN [1] ELSE [2.0] END ELSE 'a' END) AS v", rows: [][]interface{}{{"A"}}},
		{name: "case_three_invalid_types_static_error", query: "RETURN toUpper(CASE WHEN false THEN 1 WHEN false THEN 2.0 ELSE true END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_unknown_property_branch", setup: "CREATE (:X {a: 'a'})", query: "MATCH (n:X) RETURN toUpper(CASE WHEN true THEN n.a ELSE 1 END) AS v", rows: [][]interface{}{{"A"}}},
		{name: "case_unknown_property_branch_runtime_error", setup: "CREATE (:X {a: 1})", query: "MATCH (n:X) RETURN toUpper(CASE WHEN true THEN n.a ELSE 1 END) AS v", code: "Neo.ClientError.Statement.TypeError"},
		{name: "case_invalid_branch_expression_static_error", query: "RETURN toUpper(CASE WHEN false THEN 1 + true ELSE 'a' END) AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_all_null", query: "RETURN toUpper(CASE WHEN true THEN null ELSE null END) AS v", rows: [][]interface{}{{nil}}},
		{name: "case_projected_numeric_union_index", query: "WITH CASE WHEN true THEN 1 ELSE 2.0 END AS x RETURN [10,20][x] AS v", rows: [][]interface{}{{int64(20)}}},
		{name: "map_union_keeps_arithmetic_static_error", query: "UNWIND [{a:1}] AS m RETURN m + 1 AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "map_union_keeps_unary_static_error", query: "UNWIND [{a:1}] AS m RETURN -m AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "map_union_keeps_boolean_static_error", query: "UNWIND [{a:1}] AS m WITH m WHERE m RETURN 1 AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "case_map_node_union_valid_branch", setup: "CREATE (:X)", query: "MATCH (n:X) RETURN labels(CASE WHEN true THEN n ELSE {a:1} END) AS v", rows: [][]interface{}{{[]interface{}{"X"}}}},
		{name: "case_map_node_union_runtime_error", setup: "CREATE (:X)", query: "MATCH (n:X) RETURN labels(CASE WHEN false THEN n ELSE {a:1} END) AS v", code: "Neo.ClientError.Statement.TypeError"},
		{name: "comprehension_nodes_static_error", query: "UNWIND [1] AS x RETURN [y IN [x] | nodes(y)]", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "property_concat_predicate_filters", setup: "CREATE (:X {id: 'a'})", query: "MATCH (n:X) WHERE n.id + 'z' RETURN n.id", rows: [][]interface{}{}},
		{name: "property_concat_and_filters", setup: "CREATE (:X {id: 'a'})", query: "MATCH (n:X) WHERE n.id + 'z' AND true RETURN n.id", rows: [][]interface{}{}},
		{name: "property_concat_or_preserves_match", setup: "CREATE (:X {id: 'a'})", query: "MATCH (n:X) WHERE n.id + 'z' OR n.id = 'a' RETURN n.id", rows: [][]interface{}{{"a"}}},
		{name: "property_concat_not_runtime_error", setup: "CREATE (:X {id: 'a'})", query: "MATCH (n:X) WHERE NOT (n.id + 'z') RETURN n.id", code: "Neo.ClientError.Statement.TypeError"},
		{name: "mixed_number_arithmetic", query: "UNWIND [1, 2.5] AS v RETURN v * 10 AS a, toInteger(v * 10) AS b", rows: [][]interface{}{{int64(10), int64(10)}, {float64(25), int64(25)}}},
		{name: "mixed_number_with_order", query: "UNWIND [1, 2.5] AS v WITH v ORDER BY v RETURN toInteger(v * 10)", rows: [][]interface{}{{int64(10)}, {int64(25)}}},
		{name: "negative_round_precision", query: "RETURN round(-3.75, 1), round(3.75, 1), round(-2.5), round(-0.25, 1)", rows: [][]interface{}{{float64(-3.8), float64(3.8), float64(-2), float64(-0.3)}}},
		{name: "single_row_order_error", query: "UNWIND [1] AS x RETURN x ORDER BY 1 / (x - 1)", code: "Neo.ClientError.Statement.ArithmeticError"},
		{name: "delete_static_map", query: "WITH {a: 1} AS m DELETE m", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "delete_static_integer", query: "WITH 1 AS v DELETE v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "delete_static_unwind_integer", query: "UNWIND [1] AS m DELETE m", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "delete_runtime_map", query: "UNWIND [{a: 1}] AS m DELETE m", code: "Neo.ClientError.Statement.TypeError"},
		{name: "case_argument_static_error", query: "RETURN toUpper(CASE WHEN true THEN 1 ELSE 2 END)", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "subscript_argument_static_error", query: "RETURN toUpper([1, 2][0])", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "string_concat_subscript_static_error", query: "MATCH (n) RETURN n.arr[n.k + '']", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "coalesce_checks_later_argument", query: "MATCH (n:Missing) RETURN coalesce(n.x, 1 + true)", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "coalesce_argument_static_error", query: "UNWIND [1] AS x RETURN size(coalesce(x, 1))", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "labels_map_in_where", setup: "CREATE (:X)", query: "WITH {a: 1} AS m MATCH (n) WHERE labels([m][0]) IS NOT NULL RETURN count(n)", code: "Neo.ClientError.Statement.TypeError"},
		{name: "keys_path_nullable_case", setup: "CREATE (:X)-[:R]->(:Y)", query: "MATCH p = (a)-->(b) RETURN keys(CASE WHEN true THEN p ELSE null END)", code: "Neo.ClientError.Statement.TypeError"},
		{name: "length_string_nullable_case", query: "RETURN length(CASE WHEN true THEN 'ab' ELSE null END)", code: "Neo.ClientError.Statement.TypeError"},
		{name: "keys_integer_nullable_case", query: "RETURN keys(CASE WHEN true THEN 1 ELSE null END)", code: "Neo.ClientError.Statement.TypeError"},
		{name: "comprehension_element_static_error", query: "UNWIND [1] AS x RETURN [k IN [1] | labels(k)]", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "comprehension_list_static_error", query: "UNWIND [0] AS x RETURN [k IN 1 / x | k]", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "exists_pattern_where", setup: "CREATE (:X {id: 1})-[:R]->(:Y)", query: "MATCH (n:X {id: 1}) WHERE exists((n)-->()) RETURN n.id", rows: [][]interface{}{{int64(1)}}},
		{name: "exists_pattern_with_where", setup: "CREATE (:X {id: 1})-[:R]->(:Y)", query: "MATCH (n:X {id: 1}) WITH n WHERE exists((n)-->()) RETURN n.id", rows: [][]interface{}{{int64(1)}}},
		{name: "exists_pattern_where_comparison", setup: "CREATE (:X {id: 1})-[:R]->(:Y)", query: "MATCH (n:X {id: 1}) WHERE exists((n)-->()) = true RETURN n.id", rows: [][]interface{}{{int64(1)}}},
	}
	for _, mode := range []string{"auto", "tx"} {
		t.Run(mode, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "issue1226")
					exec := NewStorageExecutor(engine)
					ctx := context.Background()
					if mode == "tx" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					if tt.setup != "" {
						_, err := exec.Execute(ctx, tt.setup, nil)
						require.NoError(t, err)
					}
					result, err := exec.Execute(ctx, tt.query, nil)
					if tt.code != "" {
						require.Error(t, err)
						var classified interface{ BoltErrorCode() string }
						require.True(t, errors.As(err, &classified), "unclassified error: %v", err)
						require.Equal(t, tt.code, classified.BoltErrorCode(), "%v", err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, tt.rows, result.Rows)
					if mode == "tx" {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestIssue1226DeleteRejectsOrdinaryEntityMarkerMaps(t *testing.T) {
	for _, query := range []string{
		"UNWIND [{_nodeId: 'victim'}] AS m DELETE m",
		"UNWIND [{_edgeId: 'link'}] AS m DELETE m",
		"MATCH (n:Victim) UNWIND [{owner: n}] AS m DELETE m",
		"MATCH (n:Victim) UNWIND [{_pathResult: n}] AS m DELETE m",
	} {
		t.Run(query, func(t *testing.T) {
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_marker")
			for id, label := range map[string]string{"victim": "Victim", "left": "Left", "right": "Right"} {
				_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{label}})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "link", StartNode: "left", EndNode: "right", Type: "R"}))
			_, err := NewStorageExecutor(engine).Execute(context.Background(), query, nil)
			// Check survival even on the unfixed implementation: marker maps must
			// not authorize mutations of the node or edge they happen to name.
			if err == nil {
				t.Error("ordinary map was accepted as a DELETE target")
			} else {
				var classified interface{ BoltErrorCode() string }
				require.True(t, errors.As(err, &classified), "%v", err)
				require.Equal(t, "Neo.ClientError.Statement.TypeError", classified.BoltErrorCode(), "%v", err)
			}
			_, err = engine.GetNode("victim")
			require.NoError(t, err, "ordinary map deleted the node")
			_, err = engine.GetEdge("link")
			require.NoError(t, err, "ordinary map deleted the relationship")
		})
	}
}

func TestIssue1226DeletePreservesEntityPathAndStringTargets(t *testing.T) {
	for _, mode := range []string{"auto", "tx"} {
		t.Run(mode, func(t *testing.T) {
			for _, tc := range []struct {
				name, query                string
				nodeGone, edgeGone         bool
				endpointsGone              bool
				nodesDeleted, edgesDeleted int
			}{
				{"node", "MATCH (n:Victim) DELETE n", true, false, false, 1, 0},
				{"relationship", "MATCH ()-[r:R]->() DELETE r", false, true, false, 0, 1},
				{"path", "MATCH p = (:Left)-[:R]->(:Right) DELETE p", false, true, true, 2, 1},
				{"string_id_extension", "DELETE 'victim'", true, false, false, 1, 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_valid")
					for id, label := range map[string]string{"victim": "Victim", "left": "Left", "right": "Right"} {
						_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{label}})
						require.NoError(t, err)
					}
					require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "link", StartNode: "left", EndNode: "right", Type: "R"}))
					exec := NewStorageExecutor(engine)
					ctx := context.Background()
					if mode == "tx" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					result, err := exec.Execute(ctx, tc.query, nil)
					require.NoError(t, err)
					require.NotNil(t, result.Stats)
					require.Equal(t, tc.nodesDeleted, result.Stats.NodesDeleted)
					require.Equal(t, tc.edgesDeleted, result.Stats.RelationshipsDeleted)
					if mode == "tx" {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
					for id, gone := range map[storage.NodeID]bool{"victim": tc.nodeGone, "left": tc.endpointsGone, "right": tc.endpointsGone} {
						_, err = engine.GetNode(id)
						if gone {
							require.ErrorIs(t, err, storage.ErrNotFound, "node %s must be deleted", id)
						} else {
							require.NoError(t, err, "node %s must survive", id)
						}
					}
					_, err = engine.GetEdge("link")
					if tc.edgeGone {
						require.ErrorIs(t, err, storage.ErrNotFound)
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	}
}
