package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Parsing may be cached, but a compiled subquery must use the same row-owned
// statement context and return the same error as its text adapter.
func TestRowSubqueryAdapterStatementContract(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_subquery_contract"))
	for _, expression := range []string{
		"EXISTS { RETURN $key AS value }",
		"COUNT { RETURN $key AS value }",
		"COLLECT { RETURN $key AS value }",
		"COLLECT { RETURN 1 / 0 AS invalid }",
	} {
		t.Run(expression, func(t *testing.T) {
			root, _ := (&rowValueCompiler{}).compile(t.Context(), expressionPart(expression))
			for _, bound := range []bool{false, true} {
				t.Run(fmt.Sprint(bound), func(t *testing.T) {
					for _, key := range []string{"first", "second", "first"} {
						statement := withExpressionFailureSlot(withParams(t.Context(), map[string]any{"key": key}))
						row := map[string]any{}
						if bound {
							row[rowStatementContextKey] = statement
						}
						operation := withParams(t.Context(), map[string]any{"key": "wrong operation binding"})
						want, wantOK, wantErr := e.evaluateRowValueText(operation, expression, row)
						got, gotOK, gotErr := root.eval(operation, e, row, nil)
						require.Equal(t, want, got)
						require.Equal(t, wantOK, gotOK)
						require.Equal(t, wantErr, gotErr)
						if bound && expression == "COLLECT { RETURN $key AS value }" {
							require.NoError(t, gotErr)
							require.Equal(t, []any{key}, got)
						}
						if bound {
							require.Same(t, statement, row[rowStatementContextKey])
						} else {
							require.NotContains(t, row, rowStatementContextKey)
						}
					}
				})
			}
		})
	}
}

type rowSubqueryRequestKey struct{}

// Both adapters preserve the request context and return a real body failure,
// rather than an adapter-generated "could not evaluate" error.
type rowSubqueryErrorEngine struct {
	storage.Engine
	failure error
	seen    context.Context
}

func (s *rowSubqueryErrorEngine) GetNodesByLabel(ctx context.Context, _ string) ([]*storage.Node, error) {
	s.seen = ctx
	return nil, s.failure
}

func TestRowSubqueryAdapterBodyErrorIdentity(t *testing.T) {
	failure := errors.New("subquery body scan failure")
	store := &rowSubqueryErrorEngine{
		Engine:  storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_subquery_error"),
		failure: failure,
	}
	e := NewStorageExecutor(store)
	expression := "COLLECT { MATCH (q:Q) RETURN q }"
	root, _ := (&rowValueCompiler{}).compile(t.Context(), expressionPart(expression))
	for _, compiled := range []bool{false, true} {
		statement := context.WithValue(t.Context(), rowSubqueryRequestKey{}, "request value")
		ctx, cancel := context.WithCancel(statement)
		t.Cleanup(cancel)
		row := map[string]any{rowStatementContextKey: ctx}
		store.seen = nil
		var value any
		var resolved bool
		var err error
		if compiled {
			value, resolved, err = root.eval(t.Context(), e, row, nil)
		} else {
			value, resolved, err = e.evaluateRowValueText(t.Context(), expression, row)
		}
		require.Nil(t, value)
		require.False(t, resolved)
		require.ErrorIs(t, err, failure)
		require.NotNil(t, store.seen)
		require.Equal(t, "request value", store.seen.Value(rowSubqueryRequestKey{}))
		cancel()
		require.ErrorIs(t, store.seen.Err(), context.Canceled)
		// A canceled statement must not be replaced by the live operation context.
		if compiled {
			_, _, err = root.eval(t.Context(), e, row, nil)
		} else {
			_, _, err = e.evaluateRowValueText(t.Context(), expression, row)
		}
		require.ErrorIs(t, err, context.Canceled)
	}
}

func TestRowSubqueryAdapterPublicParserModes(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, programs := range []bool{false, true} {
				t.Run(fmt.Sprintf("programs=%t", programs), func(t *testing.T) {
					previous := rowValueProgramsDisabled.Swap(!programs)
					t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
					e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_subquery_public"))
					_, err := e.Execute(t.Context(), "CREATE (:Q {id:1}), (:Q {id:2}), (:Q {id:3})", nil)
					require.NoError(t, err)
					const nestedExpression = "[COUNT { MATCH (q:Q) }, COLLECT { RETURN $key + i AS value }, EXISTS { MATCH (q:Q {id:$key}) }]"
					for _, key := range []int64{1, 2, 1} {
						result, err := e.Execute(t.Context(),
							"UNWIND [0,1] AS i RETURN COUNT { MATCH (q:Q) } AS total, COLLECT { RETURN $key + i AS value } AS items, EXISTS { MATCH (q:Q {id:$key}) } AS found",
							map[string]any{"key": key})
						require.NoError(t, err)
						require.Equal(t, [][]any{{int64(3), []any{key}, true}, {int64(3), []any{key + 1}, true}}, result.Rows)
						result, err = e.Execute(t.Context(), "UNWIND [0,1] AS i RETURN "+nestedExpression+" AS values", map[string]any{"key": key})
						require.NoError(t, err)
						require.Equal(t, [][]any{{[]any{int64(3), []any{key}, true}}, {[]any{int64(3), []any{key + 1}, true}}}, result.Rows)
					}
					if programs {
						program, cached := rowValuePrograms.get(nestedExpression)
						require.True(t, cached, "public execution must reach the row-program owner")
						require.NotNil(t, program)
						require.NotSame(t, rowValueSeen, program, "public execution must compile, not merely mark the expression seen")
					}
					_, directErr := e.Execute(t.Context(), "RETURN 1 / 0 AS invalid", nil)
					require.Error(t, directErr)
					for range 3 {
						for _, expression := range []string{"COLLECT { RETURN 1 / 0 AS invalid }", "[COLLECT { RETURN 1 / 0 AS invalid }]"} {
							_, err := e.Execute(t.Context(), "RETURN "+expression+" AS items", nil)
							require.Equal(t, directErr, err)
						}
					}
				})
			}
		})
	}
}
