package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type relationshipAccessScanOnly struct{ storage.Engine }

func TestRelationshipAccessProgramKeepsSchemaAndRowsLive(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "live_site_owner"))
	_, err := e.Execute(context.Background(), "CREATE ()-[:T {p:'x'}]->()", nil)
	require.NoError(t, err)
	const query = "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p"
	for _, phase := range []struct {
		ddl  string
		seek bool
	}{
		{"", false},
		{"CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)", true},
		{"DROP INDEX rel_p", false},
	} {
		if phase.ddl != "" {
			_, err := e.Execute(context.Background(), phase.ddl, nil)
			require.NoError(t, err)
		}
		for _, value := range []string{"x", "absent"} {
			for _, mode := range []string{"EXPLAIN", "PROFILE"} {
				result, err := e.Execute(context.Background(), mode+" "+query, map[string]interface{}{"x": value})
				require.NoError(t, err)
				seek := relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek")
				require.Equal(t, phase.seek, seek != nil, "%s, %s, %s", phase.ddl, value, mode)
				if mode == "PROFILE" {
					if value == "x" {
						require.Equal(t, [][]interface{}{{"x"}}, result.Rows)
					} else {
						require.Empty(t, result.Rows)
					}
				}
			}
		}
	}
}

func TestRelationshipAccessProgramScanView(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "scan_site_owner")
	setup := NewStorageExecutor(store)
	for _, q := range []string{"CREATE (:Key)-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := setup.Execute(context.Background(), q, nil)
		require.NoError(t, err)
	}
	e := NewStorageExecutor(&relationshipAccessScanOnly{Engine: store})
	for _, mode := range []string{"EXPLAIN", "PROFILE"} {
		result, err := e.Execute(context.Background(), mode+" MATCH ()-[r:T]->() WHERE r.p='x' RETURN r.p", nil)
		require.NoError(t, err)
		root := result.Metadata["plan"].(*ExecutionPlan).Root
		require.Nil(t, relationshipPlanOperator(root, "RelationshipIndexSeek"))
		expand := relationshipPlanOperator(root, "Expand")
		require.NotNil(t, expand)
		require.Equal(t, []string{"r"}, expand.Identifiers)
		require.Equal(t, int64(100), expand.EstimatedRows)
	}
}

// Remove the program's site, while leaving the executable query unchanged.
// Both readers must lose the seek. A runtime planner that re-derives sites from
// its pattern, or a display scanner that rebuilds them, fails this guard.
func TestRelationshipAccessProgramIsSharedSource(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "site_owner"))
	for _, q := range []string{"CREATE (:Key)-[:T {p:'x'}]->()", "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)"} {
		_, err := e.Execute(context.Background(), q, nil)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		name, query string
		rows        [][]interface{}
	}{
		{"collect", "WITH $x AS k RETURN COLLECT { MATCH ()-[r:T]->() WHERE r.p=k RETURN r.p }", [][]interface{}{{[]interface{}{"x"}}}},
		{"anchored_count", "MATCH (a:Key) RETURN COUNT { MATCH (a)-[r:T]->() WHERE r.p=$x }", [][]interface{}{{int64(1)}}},
		{"anchored_exists", "MATCH (a:Key) RETURN EXISTS { (a)-[r:T {p:$x}]->() }", [][]interface{}{{true}}},
		{"call", "MATCH (a:Key) CALL (a) { MATCH (a)-[r:T]->() WHERE r.p=$x RETURN r.p AS value } RETURN value", [][]interface{}{{"x"}}},
		{"all_token", "CREATE (n:$all(head(COLLECT { MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p }))) RETURN labels(n)", [][]interface{}{{[]interface{}{"x"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := tc.query
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
					require.Equal(t, tc.rows, actual.Rows)
					profile := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
					require.Equal(t, populated, len(profile.seeks) != 0)
					require.Equal(t, !populated, len(profile.unregistered) != 0)
					result, err := e.Execute(ctx, "EXPLAIN "+query, params)
					require.NoError(t, err)
					seek := relationshipPlanOperator(result.Metadata["plan"].(*ExecutionPlan).Root, "RelationshipIndexSeek")
					require.Equal(t, populated, seek != nil)
				})
			}
		})
	}
}
