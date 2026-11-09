package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// testRetainedCall is a CALL clause read from its own retained product, the
// same node a statement split hands the executor.
func testRetainedCall(t *testing.T, source string) (pipelineClause, queryBody) {
	t.Helper()
	syntax, err := cypherantlr.ParseSyntax(t.Context(), source, cypherantlr.FragmentEntry, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	call, found := syntax.LeadingCall(0)
	require.True(t, found, source)
	clause := pipelineClause{kind: pipelineClauseCallSubquery, text: call.Text(), syntax: call}
	body, retained := callBodyFromSyntax(clause)
	require.True(t, retained, source)
	return clause, body
}

func TestCallReturnClauseSurvivesUnknownTypes(t *testing.T) {
	for _, tc := range []struct {
		name, call, want string
		unknown          bool
	}{
		{"USE no return", "CALL { USE g CREATE (n) }", "", true},
		{"USE opaque return", "CALL { USE g RETURN abs(-1) AS x }", "RETURN abs(-1) AS x", true},
		{"USE ordered wildcard", "CALL { USE g RETURN *, 1 AS k }", "RETURN *, 1 AS k", true},
		{"USE first UNION", "CALL { USE g RETURN 1 AS z UNION RETURN 2 AS z }", "RETURN 1 AS z", false},
		{"procedure wildcard", "CALL { CALL db.labels() YIELD * RETURN * }", "RETURN *", true},
		{"known ordered columns", "CALL { RETURN 1 AS z, 2 AS a }", "RETURN 1 AS z, 2 AS a", false},
		{"mixed wildcard", "CALL { UNWIND [7] AS v RETURN *, 1 AS k }", "RETURN *, 1 AS k", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(newTestMemoryEngine(t))
			call, body := testRetainedCall(t, tc.call)
			types, clause, err := e.callSubqueryReturnTypes(t.Context(), call, body, typeScope{}, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, clause)
			if tc.unknown {
				require.Nil(t, types)
			} else {
				require.NotNil(t, types)
			}
			if tc.name == "known ordered columns" {
				require.Equal(t, []string{"z", "a"}, pipelineReturnSourceColumns(t.Context(), clause))
			}
			if tc.name == "mixed wildcard" {
				require.Equal(t, []string{"*", "k"}, pipelineReturnSourceColumns(t.Context(), clause))
			}
		})
	}
}

// Unknown inferred types are permitted, but an undefined variable is a rejected
// statement. Retaining its RETURN text must not swallow that semantic error.
func TestCallReturnClauseRetainsUndefinedVariableError(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct{ query, firstReturn string }{
			{"CALL { RETURN missing AS k }", "RETURN missing AS k"},
			{"CALL { RETURN 1 AS k UNION RETURN missing AS k }", "RETURN 1 AS k"},
		} {
			t.Run(tc.query, func(t *testing.T) {
				e := NewStorageExecutor(newTestMemoryEngine(t))
				call, body := testRetainedCall(t, tc.query)
				types, clause, err := e.callSubqueryReturnTypes(t.Context(), call, body, typeScope{}, false)
				require.Error(t, err)
				require.Contains(t, err.Error(), "missing")
				require.Equal(t, tc.firstReturn, clause)
				require.Nil(t, types)
				_, err = e.Execute(t.Context(), tc.query+" RETURN k", nil)
				require.Error(t, err)
				require.Contains(t, err.Error(), "missing")
				var classified interface{ BoltErrorCode() string }
				require.ErrorAs(t, err, &classified)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode())
			})
		}
	})
}

func TestPipelineClauseProductCacheIsolation(t *testing.T) {
	ctx := t.Context()
	opaque := "USE g RETURN abs(-1) AS x"
	first := cachedPipelineClauses(ctx, opaque, true)
	require.False(t, first.ok)
	require.Nil(t, first.clauses)
	require.Equal(t, "RETURN abs(-1) AS x", first.firstReturn)
	first.firstReturn = "mutated"
	require.Equal(t, "RETURN abs(-1) AS x", cachedPipelineClauses(ctx, opaque, true).firstReturn)
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(ctx, opaque)
	require.False(t, ok)
	require.Nil(t, clauses)
	source := "RETURN 1 AS x"
	returned := cachedPipelineClauses(ctx, source, true)
	require.True(t, returned.ok)
	returned.clauses[0].text = "mutated"
	require.Equal(t, source, cachedPipelineClauses(ctx, source, true).clauses[0].text)
	procedure := "CALL db.labels() YIELD label RETURN label"
	allowed := cachedPipelineClauses(ctx, procedure, true)
	require.True(t, allowed.ok)
	require.True(t, allowed.topLevelCall)
	denied, ok := splitPipelineClauses(ctx, procedure)
	require.False(t, ok)
	require.Nil(t, denied)
	require.Equal(t, "RETURN label", cachedPipelineClauses(ctx, procedure, true).firstReturn)
}
