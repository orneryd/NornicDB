package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Remove the program's site, while leaving the executable query unchanged.
// Both readers must lose the seek. A runtime planner that re-derives sites from
// its pattern, or a display scanner that rebuilds them, fails this guard.
func TestRelationshipAccessProgramIsSharedSource(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "site_owner"))
	for _, q := range []string{"CREATE ()-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := e.Execute(context.Background(), q, nil)
		require.NoError(t, err)
	}
	query := "WITH $x AS k RETURN COLLECT { MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p }"
	program := relationshipProgramFor(query)
	require.Len(t, program.sites, 1)
	for _, populated := range []bool{true, false} {
		t.Run(map[bool]string{true: "registered", false: "removed"}[populated], func(t *testing.T) {
			source := program
			if !populated {
				source = &relationshipAccessProgram{query: query, byID: map[string]*relationshipAccessTemplate{}}
			}
			ctx := context.WithValue(context.Background(), relationshipAccessProgramKey{}, source)
			ctx = withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
			params := map[string]interface{}{"x": "x"}
			actual, err := e.Execute(ctx, query, params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{[]interface{}{"x"}}}, actual.Rows)
			profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
			require.Equal(t, populated, len(profile.seeks) != 0)
			require.Equal(t, !populated, len(profile.unregistered) != 0)
			result, err := e.Execute(ctx, "EXPLAIN "+query, params)
			require.NoError(t, err)
			seek := relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek")
			require.Equal(t, populated, seek != nil)
		})
	}
}
