// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCallReturnClauseSurvivesUnknownTypes(t *testing.T) {
	for _, tc := range []struct {
		name, call, want string
		branches         []string
		unknown          bool
	}{
		{"USE no return", "CALL { USE g CREATE (n) }", "", []string{"USE g CREATE (n)"}, true},
		{"USE opaque return", "CALL { USE g RETURN abs(-1) AS x }", "RETURN abs(-1) AS x", []string{"USE g RETURN abs(-1) AS x"}, true},
		{"USE ordered wildcard", "CALL { USE g RETURN *, 1 AS k }", "RETURN *, 1 AS k", []string{"USE g RETURN *, 1 AS k"}, true},
		{"USE first UNION", "CALL { USE g RETURN 1 AS z UNION RETURN 2 AS z }", "RETURN 1 AS z", []string{"USE g RETURN 1 AS z", "RETURN 2 AS z"}, false},
		{"undefined implicit body", "CALL { RETURN missing AS k }", "RETURN missing AS k", []string{"RETURN missing AS k"}, true},
		{"unknown later UNION", "CALL { RETURN 1 AS k UNION RETURN missing AS k }", "RETURN 1 AS k", []string{"RETURN 1 AS k", "RETURN missing AS k"}, true},
		{"procedure wildcard", "CALL { CALL db.labels() YIELD * RETURN * }", "RETURN *", []string{"CALL db.labels() YIELD * RETURN *"}, true},
		{"known ordered columns", "CALL { RETURN 1 AS z, 2 AS a }", "RETURN 1 AS z, 2 AS a", []string{"RETURN 1 AS z, 2 AS a"}, false},
		{"mixed wildcard", "CALL { UNWIND [7] AS v RETURN *, 1 AS k }", "RETURN *, 1 AS k", []string{"UNWIND [7] AS v RETURN *, 1 AS k"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(newTestMemoryEngine(t))
			types, clause, err := e.callSubqueryReturnTypes(t.Context(), retainedClauseForTest(t.Context(), tc.call), queryBodiesForTest(t.Context(), tc.branches), typeScope{}, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, clause.text)
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

func TestPipelineClauseProductCacheIsolation(t *testing.T) {
	ctx := t.Context()
	opaque := "USE g RETURN abs(-1) AS x"
	first := cachedPipelineTextClauses(ctx, opaque, true)
	require.False(t, first.ok)
	require.Nil(t, first.clauses)
	require.Equal(t, "RETURN abs(-1) AS x", first.firstReturn.text)
	first.firstReturn.text = "mutated"
	require.Equal(t, "RETURN abs(-1) AS x", cachedPipelineTextClauses(ctx, opaque, true).firstReturn.text)
	clauses, ok := splitPipelineTextClausesAllowingProcedureCalls(ctx, opaque)
	require.False(t, ok)
	require.Nil(t, clauses)
	source := "RETURN 1 AS x"
	returned := cachedPipelineTextClauses(ctx, source, true)
	require.True(t, returned.ok)
	returned.clauses[0].text = "mutated"
	require.Equal(t, source, cachedPipelineTextClauses(ctx, source, true).clauses[0].text)
	procedure := "CALL db.labels() YIELD label RETURN label"
	allowed := cachedPipelineTextClauses(ctx, procedure, true)
	require.True(t, allowed.ok)
	require.True(t, allowed.topLevelCall)
	denied, ok := splitPipelineTextClauses(ctx, procedure)
	require.False(t, ok)
	require.Nil(t, denied)
	require.Equal(t, "RETURN label", cachedPipelineTextClauses(ctx, procedure, true).firstReturn.text)
}
