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
		{name: "comprehension_case_runtime_error", query: "RETURN [x IN [1] | toUpper(CASE WHEN true THEN x ELSE 'a' END)]", code: "Neo.ClientError.Statement.TypeError"},
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "issue1226")
			exec := NewStorageExecutor(engine)
			ctx := context.Background()
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
	for _, tc := range []struct {
		name, query        string
		nodeGone, edgeGone bool
	}{
		{"node", "MATCH (n:Victim) DELETE n", true, false},
		{"relationship", "MATCH ()-[r:R]->() DELETE r", false, true},
		{"path", "MATCH p = (:Left)-[:R]->(:Right) DELETE p", false, true},
		{"string_id_extension", "DELETE 'victim'", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_valid")
			for id, label := range map[string]string{"victim": "Victim", "left": "Left", "right": "Right"} {
				_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{label}})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "link", StartNode: "left", EndNode: "right", Type: "R"}))
			result, err := NewStorageExecutor(engine).Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			_, err = engine.GetNode("victim")
			require.Equal(t, tc.nodeGone, err != nil)
			_, err = engine.GetEdge("link")
			require.Equal(t, tc.edgeGone, err != nil)
			if tc.name == "path" {
				for _, id := range []storage.NodeID{"left", "right"} {
					_, err := engine.GetNode(id)
					require.Error(t, err, "path endpoint survived DELETE")
				}
				require.Equal(t, 2, result.Stats.NodesDeleted)
				require.Equal(t, 1, result.Stats.RelationshipsDeleted)
			}
		})
	}
}
