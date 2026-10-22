package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMergeSharedScannerQuotedKeywordsAndModifiers(t *testing.T) {
	query := "MERGE (n:Node {name:'MATCH in string'}) ON MATCH SET n.a = 1 WITH n OPTIONAL MATCH (m:Node) RETURN n"
	clauses, ok := splitPipelineClauses(query)
	require.True(t, ok)
	require.Len(t, clauses, 4)
	require.Equal(t, pipelineClauseMerge, clauses[0].kind)
	require.Equal(t, "MERGE (n:Node {name:'MATCH in string'}) ON MATCH SET n.a = 1", clauses[0].text)
	require.Equal(t, pipelineClauseWith, clauses[1].kind)
	require.Equal(t, pipelineClauseOptionalMatch, clauses[2].kind)
	require.Equal(t, pipelineClauseReturn, clauses[3].kind)
}

func TestMergeContextHelpers_MoreBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	node := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "alice"}}
	rel := &storage.Edge{ID: "e1", Type: "REL", StartNode: "n1", EndNode: "n1", Properties: map[string]interface{}{"w": int64(1)}}
	input := []pipelineRow{{"n": node, "r": rel, "s": int64(7)}}
	_, err := exec.Execute(ctx, "WITH RETURN 1", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
	fixtureAdmission1322Context, fixtureAdmission1322 := statementTestContext(ctx, "WITH *")
	_ = fixtureAdmission1322Context
	projected, ok := exec.pipelineApplyWith(fixtureAdmission1322, fixtureAdmission1322Context, input, "WITH *")
	require.True(t, ok)
	require.Equal(t, input, projected)
	fixtureAdmission1323Context, fixtureAdmission1323 := statementTestContext(ctx, "WITH n AS nn, r AS rr, s AS ss")
	_ = fixtureAdmission1323Context
	projected, ok = exec.pipelineApplyWith(fixtureAdmission1323, fixtureAdmission1323Context, input, "WITH n AS nn, r AS rr, s AS ss")
	require.True(t, ok)
	require.Equal(t, []pipelineRow{{"nn": node, "rr": rel, "ss": int64(7)}}, projected)
	_, err = exec.Execute(ctx, "WITH 7 AS s WITH s AS ss, ghost AS gg RETURN ss", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")

}

// A WHERE whose value isn't a boolean is Neo4j 5.26.30's TypeError, before a
// MATCH, a WITH, or a MERGE that follows (#1041: on the executor path).
func TestWhereRejectsNonBooleanValue(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where_non_boolean"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:WB {name: 'alice'})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:WB) WHERE n.name RETURN n.name AS v",
		"MATCH (n:WB) WITH n WHERE n.name RETURN n.name AS v",
		"MATCH (n:WB) WHERE n.name MERGE (m:WM {k: 1}) RETURN m.k AS v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")
	}
}
