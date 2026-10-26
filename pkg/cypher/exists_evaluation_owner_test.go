package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/subqueryeval"
	"github.com/stretchr/testify/require"
)

func TestExistsSubqueryEvaluationOwner(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subquery_owner"))
	ctx := context.Background()
	for _, query := range subqueryeval.Setup {
		_, err := e.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	for _, tc := range subqueryeval.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			for nonce := int64(1); nonce <= 3; nonce++ {
				result, err := e.Execute(ctx, tc.Query, map[string]any{"nonce": nonce})
				require.NoError(t, err)
				require.Equal(t, [][]any{{tc.Want}}, result.Rows)
			}
		})
	}
}

// The statement's parameters must survive both the text evaluator and a
// compiled predicate. Repeated calls change the bindings, never the query text.
func TestExistsSubqueryEvaluationOwnerPrograms(t *testing.T) {
	for _, programs := range []bool{false, true} {
		t.Run(fmt.Sprint(programs), func(t *testing.T) {
			oldValues := rowValueProgramsDisabled.Swap(!programs)
			oldPredicates := rowPredicateProgramsDisabled.Swap(!programs)
			defer rowValueProgramsDisabled.Store(oldValues)
			defer rowPredicateProgramsDisabled.Store(oldPredicates)
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "exists_programs"))
			_, err := e.Execute(context.Background(), "CREATE (:Source {id:'present1'})", nil)
			require.NoError(t, err)
			predicate := "NOT EXISTS { MATCH (s:Source {id:$prefix + toString(i)}) }"
			for _, prefix := range []string{"present", "missing", "present"} {
				ctx := withExpressionFailureSlot(withParams(context.Background(), map[string]any{"prefix": prefix}))
				held := e.evaluateRowPredicate(ctx, originalBraceView(predicate), map[string]any{"i": int64(1)})
				require.NoError(t, getExpressionFailure(ctx))
				require.Equal(t, prefix != "present", held)
			}
		})
	}
}

// Indexed correlation may cache syntax, never a row's values or a lookup's
// results. The storage spy proves that present, absent and null keys are all
// answered without a label/full-node scan, for each subquery expression kind.
func TestExistsSubqueryEvaluationOwnerIndex(t *testing.T) {
	spy := &labelStreamCountingEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "exists_index")}
	e := NewStorageExecutor(spy)
	ctx := withExpressionFailureSlot(context.Background())
	for _, query := range []string{
		"CREATE (:Source {id:'present',roles:[]}), (:Source {id:'other',roles:['other']})",
		"CREATE INDEX source_id FOR (s:Source) ON (s.id)",
	} {
		_, err := e.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	body := "MATCH (s:Source {id:n.source_id}) WHERE size(coalesce(s.roles,[]))=0"
	check := func(key any, want int64) {
		t.Helper()
		values := map[string]any{"n": &storage.Node{ID: "outer", Properties: map[string]any{"source_id": key}}}
		for _, kind := range []string{"EXISTS", "COUNT", "COLLECT"} {
			query := body
			var expected any = want
			if kind == "EXISTS" {
				expected = want > 0
			} else if kind == "COLLECT" {
				query += " RETURN s.id"
				items := []any{}
				if want > 0 {
					items = append(items, key)
				}
				expected = items
			}
			spy.nodesVisited = 0
			value, resolved, err := e.rowSubqueryValue(ctx, kind, originalBraceView(query), values)
			require.NoError(t, err, "%s key=%v", kind, key)
			require.NoError(t, getExpressionFailure(ctx))
			require.True(t, resolved)
			require.Equal(t, expected, value, "%s key=%v", kind, key)
			require.Zero(t, spy.nodesVisited, "%s key=%v must use Source(id)", kind, key)
		}
	}
	check("present", 1)
	check("missing", 0)
	check(nil, 0)
	check("other", 0)
	check("present", 1)
	_, err := e.Execute(ctx, "CREATE (:Source {id:'missing',roles:[]})", nil)
	require.NoError(t, err)
	check("missing", 1)
	_, err = e.Execute(ctx, "MATCH (s:Source {id:'present'}) SET s.roles=['other']", nil)
	require.NoError(t, err)
	check("present", 0)
}
