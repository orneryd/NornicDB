package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Aggregate input/fallback alternatives must preserve every shared type fact,
// even when an unrelated map member disagrees or a later filter removes rows.
func TestScopedCallAggregateCommonTypes(t *testing.T) {
	for _, tc := range []struct {
		name, original, rebound, tail string
		rows                          []interface{}
		want                          [][]interface{}
	}{
		{"map_kind", "{factor:11}", "{factor:'s'}", "RETURN x*3 AS a", nil, nil},
		{"different_keys", "{factor:11}", "{other:2}", "RETURN x*3 AS a", nil, nil},
		{"shared_member", "{factor:'a', other:11}", "{factor:'b', other:'s'}", "RETURN x.factor*3 AS a", nil, nil},
		{"nested_member", "{inner:{factor:'a', other:11}}", "{inner:{factor:'b', other:'s'}}", "RETURN x.inner.factor*3 AS a", nil, nil},
		{"nested_map_kind", "{inner:{factor:11}}", "{inner:{other:2}}", "RETURN x.inner*3 AS a", nil, nil},
		{"filtered_map", "{factor:'a', other:11}", "{factor:'b', other:'s'}", "WITH 1 AS ignored WHERE false RETURN x*3 AS a", nil, nil},
		{"filtered_member", "{factor:'a', other:11}", "{factor:'b', other:'s'}", "WITH 1 AS ignored WHERE false RETURN x.factor*3 AS a", nil, nil},
		{"match_map", "{factor:11}", "{other:2}", "MATCH (x) RETURN count(*) AS a", nil, nil},
		{"nonempty_member", "{factor:'a', other:11}", "{factor:'b', other:'s'}", "RETURN x.factor*3 AS a", []interface{}{int64(1)}, nil},
		{"shared_integer_empty", "{factor:11, other:11}", "{factor:2, other:'s'}", "RETURN x.factor*3 AS a", nil, [][]interface{}{{int64(33)}}},
		{"shared_integer_nonempty", "{factor:11, other:11}", "{factor:2, other:'s'}", "RETURN x.factor*3 AS a", []interface{}{int64(1)}, [][]interface{}{{int64(6)}}},
		{"different_member_empty", "{factor:11}", "{factor:'s'}", "RETURN CASE WHEN c=0 THEN x.factor*3 ELSE size(x.factor) END AS a", nil, [][]interface{}{{int64(33)}}},
		{"different_member_nonempty", "{factor:11}", "{factor:'s'}", "RETURN CASE WHEN c=0 THEN x.factor*3 ELSE size(x.factor) END AS a", []interface{}{int64(1)}, [][]interface{}{{int64(1)}}},
		{"missing_member", "{factor:'a'}", "{other:2}", "RETURN x.factor*3 AS a", []interface{}{int64(1)}, [][]interface{}{{nil}}},
	} {
		for _, imports := range []string{"*", "x"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/imports=%s/explicit=%t", tc.name, imports, explicit), func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_common_types"))
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					query := fmt.Sprintf("CYPHER 25 WITH %s AS x CALL (%s) { WITH %s AS x UNWIND $rows AS y WITH count(*) AS c %s } RETURN a", tc.original, imports, tc.rebound, tc.tail)
					result, err := exec.Execute(ctx, query, map[string]interface{}{"rows": tc.rows})
					if tc.want == nil {
						require.Error(t, err)
						code, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.want, result.Rows)
					if explicit {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

// A global aggregate with no input row restores original imports, while a
// nonempty group retains rebound values. This is the existing extension's
// runtime behavior, including when emptiness depends on parameters.
func TestScopedCallEmptyAggregateImportType(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		rows        []interface{}
		want        [][]interface{}
	}{
		{"map_to_integer", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x UNWIND [] AS y WITH count(*) AS c RETURN x.factor*3 AS a } RETURN a", nil, [][]interface{}{{int64(33)}}},
		{"integer_to_map", "WITH 11 AS x CALL (%s) { WITH {factor:2} AS x UNWIND [] AS y WITH count(*) AS c RETURN x*3 AS a } RETURN a", nil, [][]interface{}{{int64(33)}}},
		{"filtered", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x WHERE false WITH count(*) AS c RETURN x.factor*3 AS a } RETURN a", nil, [][]interface{}{{int64(33)}}},
		{"no_rebind", "WITH {factor:11} AS x CALL (%s) { UNWIND [] AS y WITH count(*) AS c RETURN x.factor*3 AS a } RETURN a", nil, [][]interface{}{{int64(33)}}},
		{"dynamic_empty", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x UNWIND $rows AS y WITH count(*) AS c RETURN (CASE WHEN c=0 THEN x.factor ELSE x END)*3 AS a } RETURN a", []interface{}{}, [][]interface{}{{int64(33)}}},
		{"dynamic_nonempty", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x UNWIND $rows AS y WITH count(*) AS c RETURN (CASE WHEN c=0 THEN x.factor ELSE x END)*3 AS a } RETURN a", []interface{}{int64(1)}, [][]interface{}{{int64(6)}}},
		{"aggregate_alias_wins", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x UNWIND [] AS y WITH count(*) AS x RETURN x*3 AS a } RETURN a", nil, [][]interface{}{{int64(0)}}},
		{"grouped_empty", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x UNWIND [] AS y WITH y, count(*) AS c RETURN x*3 AS a } RETURN a", nil, [][]interface{}{}},
	} {
		for _, imports := range []string{"*", "x"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/imports=%s/explicit=%t", tc.name, imports, explicit), func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_empty_aggregate"))
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					result, err := exec.Execute(ctx, fmt.Sprintf(tc.query, imports), map[string]interface{}{"rows": tc.rows})
					require.NoError(t, err)
					if len(tc.want) == 0 {
						require.Empty(t, result.Rows)
					} else {
						require.Equal(t, tc.want, result.Rows)
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

// Rebinding an imported name is a NornicDB extension. Its current value and
// type survive later WITH horizons; Neo4j rejects imported-name shadowing.
func TestScopedCallReboundImportType(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"map_to_integer", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x WITH 1 AS ignored RETURN x*3 AS a } RETURN a", [][]interface{}{{int64(6)}}},
		{"integer_to_map", "WITH 11 AS x CALL (%s) { WITH {factor:2} AS x WITH 1 AS ignored RETURN x.factor*3 AS a } RETURN x AS original, a", [][]interface{}{{int64(11), int64(6)}}},
		{"distinct", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x WITH DISTINCT 1 AS ignored RETURN x*3 AS a } RETURN a", [][]interface{}{{int64(6)}}},
		{"aggregation", "WITH {factor:11} AS x CALL (%s) { WITH 2 AS x UNWIND [1,2] AS y WITH count(*) AS c RETURN x*c AS a } RETURN a", [][]interface{}{{int64(4)}}},
		{"unwind", "WITH {factor:11} AS x CALL (%s) { UNWIND [2] AS x WITH 1 AS ignored RETURN x*3 AS a } RETURN a", [][]interface{}{{int64(6)}}},
	} {
		for _, imports := range []string{"*", "x"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/imports=%s/explicit=%t", tc.name, imports, explicit), func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_rebound"))
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					result, err := exec.Execute(ctx, fmt.Sprintf(tc.query, imports), nil)
					require.NoError(t, err)
					require.Equal(t, tc.want, result.Rows)
					if explicit {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

// Answers independently checked against Neo4j 5.26.30 in both transaction modes.
func TestCallStarScope(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"with", "WITH 11 AS x CALL (*) { WITH x WITH 2 AS y RETURN x*y AS a } RETURN a", [][]interface{}{{int64(22)}}},
		{"distinct", "WITH 11 AS x CALL (*) { WITH x UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a ORDER BY a", [][]interface{}{{int64(22)}, {int64(33)}}},
		{"aggregation", "WITH 11 AS x CALL (*) { WITH x UNWIND [2,2,3] AS y WITH sum(y) AS total RETURN x*total AS a } RETURN a", [][]interface{}{{int64(77)}}},
		{"nested", "WITH 11 AS x CALL (*) { WITH x WITH 1 AS ignored CALL (*) { WITH x UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a } RETURN a ORDER BY a", [][]interface{}{{int64(22)}, {int64(33)}}},
		{"writes", "WITH 11 AS x CALL (*) { WITH x WITH 1 AS ignored CREATE (n:CallStar {v:x}) RETURN n.v AS a } RETURN a", [][]interface{}{{int64(11)}}},
		{"union", "WITH 11 AS x CALL (*) { WITH x UNWIND [2,2] AS y WITH DISTINCT y RETURN x*y AS a UNION WITH x UNWIND [3,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a ORDER BY a", [][]interface{}{{int64(22)}, {int64(33)}}},
		{"unknown", "WITH 11 AS x CALL (*) { WITH x WITH DISTINCT x RETURN missing AS a } RETURN a", nil},
	} {
		for _, explicit := range []bool{false, true} {
			mode := "auto"
			if explicit {
				mode = "explicit"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_star"))
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
				result, err := exec.Execute(ctx, tc.query, nil)
				if tc.want == nil {
					require.Error(t, err)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					require.EqualError(t, err, "Variable `missing` not defined")
					return
				}
				require.NoError(t, err)
				require.Equal(t, []string{"a"}, result.Columns)
				require.Equal(t, tc.want, result.Rows)
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				if tc.name == "writes" {
					stored, err := exec.Execute(ctx, "MATCH (n:CallStar) RETURN n.v AS a", nil)
					require.NoError(t, err)
					require.Equal(t, tc.want, stored.Rows)
				}
			})
		}
	}
}

func TestCallScopeClauseSyntax(t *testing.T) {
	for _, tc := range []struct {
		query   string
		scoped  bool
		names   []string
		invalid bool
	}{
		{" \n cAlL ( * ) { RETURN 1 AS a } \t", true, nil, false},
		{"CALL () { RETURN 1 AS a }", true, []string{}, false},
		{"CALL (x, `y`) { RETURN x+y AS a }", true, []string{"x", "y"}, false},
		{"CALL (x+1) { RETURN 1 AS a }", true, []string{}, true},
		{"CALL { RETURN 1 AS a }", false, nil, false},
		{"CALL (x", false, nil, false},
		{"CALL db.labels()", false, nil, false},
		{"MATCH (n) CALL (n) { RETURN n }", false, nil, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			scope := readCallSubqueryScope(tc.query)
			require.Equal(t, tc.scoped, scope.scoped)
			require.Equal(t, tc.names, scope.names)
			require.Equal(t, tc.invalid, scope.invalid)
			if tc.scoped || tc.query == "CALL { RETURN 1 AS a }" {
				require.Equal(t, byte('{'), tc.query[scope.bodyOffset], "scope reader locates the body after whitespace and the import clause")
			}
		})
	}
}

func TestCallScopeClausePublicRejection(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, query := range []string{"CALL (x+1) { RETURN 1 AS a }", "CALL (x"} {
				t.Run(fmt.Sprintf("normalize=%t/explicit=%t/%s", normalize, explicit, query), func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_scope_syntax"))
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
					}
					_, err := exec.Execute(ctx, query, nil)
					require.Error(t, err)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
				})
			}
		}
	}
}
