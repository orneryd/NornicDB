package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
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
			types, clause, err := e.callSubqueryReturnTypes(t.Context(), tc.call, tc.branches, typeScope{}, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, clause)
			// A missing metadata hint cannot turn an invalid program into
			// admitted Facts. This public boundary keeps the one owner's error.
			if tc.name == "undefined implicit body" || tc.name == "unknown later UNION" || tc.name == "procedure wildcard" {
				var admissionErr error
				e.StatementFacts(tc.call, resultmetadata.KnownScope(nil), false, RawSource).Visit(
					func(resultmetadata.Statement) { t.Fatal("invalid program admitted through a type hint") },
					func() { t.Fatal("admission declined instead of rejecting") },
					func(reason resultmetadata.Unresolved) {
						require.Error(t, reason.Err)
						admissionErr = reason.Err
					},
				)
				wantCode, wantDetail, wantMessage := boundRuleError(admissionErr)
				for _, prefix := range []string{"", "UNWIND [] AS ignored ", "UNWIND [0] AS ignored "} {
					for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
						result, err := e.Execute(t.Context(), mode+prefix+tc.call+" RETURN k", nil)
						require.Error(t, err, mode+prefix+tc.call)
						require.Nil(t, result)
						code, detail, message := boundRuleError(err)
						require.Equal(t, wantCode, code)
						require.Equal(t, wantDetail, detail)
						require.Equal(t, wantMessage, message)
					}
				}
			}
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

// callSubqueryReturnTypes is a test-only witness of the former partial type
// hint contract. Compiled callers are tests only; no product consumer may use
// this compatibility witness as an admission result.
// The shared clause product retains the first written RETURN even when a
// branch is outside this hint's supported clause set. Unknown types never
// authorize execution or publish Facts: the canonical constructor does that.
func (e *StorageExecutor) callSubqueryReturnTypes(ctx context.Context, callText string, branches []string, outer typeScope, cypher25 bool) (typeScope, string, error) {
	firstReturn := ""
	typedBranch := false
	for index, branch := range branches {
		product := cachedPipelineClauses(ctx, branch, true)
		if index == 0 {
			firstReturn = product.firstReturn
		}
		typedBranch = typedBranch || product.ok
	}
	if !typedBranch || firstReturn == "" {
		return nil, firstReturn, ctx.Err()
	}
	model, err := e.callQueryScope(ctx, callText, outer, subqueryScope{}, cypher25)
	if err != nil || model == nil {
		// A failed admitted program supplies no type hint. Its rejection is
		// still returned by Facts/Execute, never decided again in this view.
		return nil, firstReturn, ctx.Err()
	}
	return model.returned, firstReturn, nil
}
