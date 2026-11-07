package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// LET binds its items in order. An item's expression reads
// the items the clause bound before it, as a variable and with the static type
// that item gave it; it never reads its own alias or a later item's. These run
// the public execution path on both parsers.
func TestLetSequentialItems(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, test := range []struct {
			query string
			rows  [][]interface{}
		}{
			{"CYPHER 25 LET a = 1, b = a + 1 RETURN b", [][]interface{}{{int64(2)}}},
			{"CYPHER 25 LET a = 1, b = a + 1, c = b * 2 RETURN a, b, c", [][]interface{}{{int64(1), int64(2), int64(4)}}},
			{"CYPHER 25 LET s = 'x', t = toUpper(s) RETURN t", [][]interface{}{{"X"}}},
			{"CYPHER 25 LET m = {v: 2}, b = m.v * 3 RETURN b", [][]interface{}{{int64(6)}}},
			{"CYPHER 25 LET a = [1, 2], b = [x IN a | x + 1] RETURN b", [][]interface{}{{[]interface{}{int64(2), int64(3)}}}},
			{"CYPHER 25 LET a = 1, b = COUNT { RETURN 1 AS x } RETURN b", [][]interface{}{{int64(1)}}},
			{"CYPHER 25 FOR x IN [1, 2] LET y = x * 2, z = y + 1 RETURN z", [][]interface{}{{int64(3)}, {int64(5)}}},
			// The review's reproducer: the second item reads the first,
			// in a scoped CALL subquery body.
			{"CYPHER 25 WITH 1 AS a CALL (a) { LET b = a, c = b + 1 RETURN c } RETURN c", [][]interface{}{{int64(2)}}},
		} {
			t.Run(test.query, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "let-order"))
				result, err := exec.Execute(context.Background(), test.query, nil)
				require.NoError(t, err)
				require.Equal(t, test.rows, result.Rows)
			})
		}
	})
}

// An item that reads an alias the clause has not bound yet — its own or a
// later item's — is still Neo4j's "Variable `x` not defined".
func TestLetItemsReadOnlyEarlierBindings(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, test := range []struct {
			query string
			want  string
		}{
			{"CYPHER 25 LET a = b + 1, b = 1 RETURN a", "Variable `b` not defined"},
			{"CYPHER 25 LET a = 1, b = a + c RETURN b", "Variable `c` not defined"},
			{"CYPHER 25 LET a = a + 1 RETURN a", "Variable `a` not defined"},
			{"CYPHER 25 LET a = b, b = a RETURN b", "Variable `b` not defined"},
		} {
			t.Run(test.query, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "let-order"))
				_, err := exec.Execute(context.Background(), test.query, nil)
				require.ErrorContains(t, err, test.want)
			})
		}
	})
}

// The type a LET item derived reaches the items after it: an illegal use of an
// earlier item's value is the same compile-time error the identical WITH
// binding gives, rather than an unrecognized variable.
func TestLetItemTypesReachLaterItems(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "let-order"))
	_, withErr := exec.Execute(context.Background(), "CYPHER 25 WITH true AS a RETURN a + 1", nil)
	require.Error(t, withErr)
	_, letErr := exec.Execute(context.Background(), "CYPHER 25 LET a = true, b = a + 1 RETURN b", nil)
	require.Error(t, letErr)
	require.EqualError(t, letErr, withErr.Error())
}

// A LET item's expression subquery reads the items before it, so a COUNT
// subquery in a later item sees an earlier item's binding. The LET owner
// accepts that clause; the statement path still runs the whole-clause
// validateClauseSubqueries check first, with the clause's incoming scope only
// (NOR-25: that call must yield LET to this owner, as FILTER's and the items'
// own checks already are). This pins the owner-level behavior only.
func TestValidateSharedClauseLetItemSubqueriesSeeEarlierItems(t *testing.T) {
	ctx := context.Background()
	clauses, ok := pipelineClausesFor(ctx, "CYPHER 25 LET a = 1, b = COUNT { RETURN a AS x UNION RETURN a AS x } RETURN b")
	require.True(t, ok)
	require.Equal(t, pipelineClauseLet, clauses[0].kind)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "let-order"))
	require.NoError(t, exec.validateSharedClause(ctx, matchSemanticScope{}, nil, clauses[0], true))
	// The item that reads a later item's alias is still refused.
	later, ok := pipelineClausesFor(ctx, "CYPHER 25 LET b = COUNT { RETURN a AS x } , a = 1 RETURN b")
	require.True(t, ok)
	require.ErrorContains(t, exec.validateSharedClause(ctx, matchSemanticScope{}, nil, later[0], true), "Variable `a` not defined")
}
