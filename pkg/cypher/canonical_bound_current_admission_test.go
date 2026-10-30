package cypher

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

func TestCanonicalBoundWriteTransitionPopulation(t *testing.T) {
	raw, err := os.ReadFile("testdata/bound_write_transition_population.json")
	require.NoError(t, err)
	var population struct {
		KindClasses []struct {
			Symbol, Name                  string
			Kind                          int
			Updating, Boundary            bool
			RequiresWith5, RequiresWith25 bool
		}
	}
	require.NoError(t, json.Unmarshal(raw, &population))
	require.Len(t, population.KindClasses, len(boundClauseAdmissionClasses))
	seen := map[pipelineClauseKind]bool{}
	for _, row := range population.KindClasses {
		kind := pipelineClauseKind(row.Kind)
		require.False(t, seen[kind], row.Symbol)
		seen[kind] = true
		class, found := boundClauseAdmissionClasses[kind]
		require.True(t, found, row.Symbol)
		require.Equal(t, row.Name, class.name, row.Symbol)
		require.Equal(t, row.Updating, class.updating, row.Symbol)
		require.Equal(t, row.Boundary, class.boundary, row.Symbol)
		require.Equal(t, row.RequiresWith5, class.requiresWith5, row.Symbol)
		require.Equal(t, row.RequiresWith25, class.requiresWith25, row.Symbol)
	}
	require.Len(t, boundClauseAdmissionVariants, 1)
	require.Equal(t, "FOR", boundClauseAdmissionVariants[cypherantlr.CypherParserRULE_forSt].name)
}

func TestCanonicalBoundProcedureExtensions(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:RuleNode)", nil)
	require.NoError(t, err)
	for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
		t.Run(mode+"leading-discard", func(t *testing.T) {
			// Keep the release's leading-CALL output-discard extension. A
			// non-void call is not rejected merely because it omits YIELD.
			result, err := exec.Execute(ctx, mode+"CALL db.labels() RETURN 1 AS one", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"one"}, result.Columns)
			if mode == "EXPLAIN " {
				require.Empty(t, result.Rows)
			} else {
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			}
		})
		for _, input := range []string{"UNWIND [] AS input ", "UNWIND [0] AS input "} {
			t.Run(mode+input+"declared-terminal", func(t *testing.T) {
				result, err := exec.Execute(ctx, mode+input+"CALL db.labels() YIELD label WHERE true", nil)
				require.NoError(t, err)
				require.Equal(t, []string{"label"}, result.Columns)
				if mode == "EXPLAIN " || input == "UNWIND [] AS input " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, [][]interface{}{{"RuleNode"}}, result.Rows)
				}
			})
		}
	}
}

func TestCanonicalBoundOpaqueProcedureSchema(t *testing.T) {
	replaceProcedureRegistryForTest(t, NewProcedureRegistry())
	require.NoError(t, globalProcedureRegistry.RegisterUser(ProcedureSpec{Name: "custom.opaque", MaxArgs: 0},
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(7)}}}, nil
		}))
	exec, ctx := newUnitExecutor(t)
	// An absent user-declared schema is opaque, not a closed empty schema.
	// Keep the release extension admitting an explicit YIELD from that handler.
	for _, input := range []string{"", "UNWIND [] AS input ", "UNWIND [0] AS input "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+input, func(t *testing.T) {
				result, err := exec.Execute(ctx, mode+input+"CALL custom.opaque() YIELD value RETURN value", nil)
				require.NoError(t, err)
				require.Equal(t, []string{"value"}, result.Columns)
				if mode == "EXPLAIN " || input == "UNWIND [] AS input " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
				}
			})
		}
	}
	var failure error
	exec.StatementFacts("CALL custom.opaque() YIELD value RETURN value", resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) {}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { failure = reason.Err })
	require.NoError(t, failure)
}

func TestCanonicalBoundPersistedProcedureArity(t *testing.T) {
	replaceProcedureRegistryForTest(t, NewProcedureRegistry())
	exec, ctx := newUnitExecutor(t)
	spec, handler, _, err := exec.compilePersistedProcedure(ctx, persistedProcedureRecord{
		Name: "custom.ruleEcho", Mode: "READ", ArgNames: []string{"value"},
		Body: "RETURN $value AS value", Outputs: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
	})
	require.NoError(t, err)
	require.NoError(t, globalProcedureRegistry.RegisterUser(spec, handler))
	for _, input := range []string{"", "UNWIND [] AS input ", "UNWIND [0] AS input "} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			for _, args := range []string{"", "1, 2"} {
				t.Run(mode+input+"arity("+args+")", func(t *testing.T) {
					_, err := exec.Execute(ctx, mode+input+"CALL custom.ruleEcho("+args+") YIELD value RETURN value", nil)
					require.Error(t, err)
					code, detail, message := boundRuleError(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					require.Equal(t, "InvalidNumberOfArguments", detail)
					if args == "" {
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError: procedure custom.ruleEcho requires at least 1 arguments, got 0", message)
					} else {
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError: procedure custom.ruleEcho accepts at most 1 arguments, got 2", message)
					}
				})
			}
			t.Run(mode+input+"value", func(t *testing.T) {
				result, err := exec.Execute(ctx, mode+input+"CALL custom.ruleEcho(7) YIELD value RETURN value", nil)
				require.NoError(t, err)
				require.Equal(t, []string{"value"}, result.Columns)
				if mode == "EXPLAIN " || input == "UNWIND [] AS input " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
				}
			})
		}
	}
}
