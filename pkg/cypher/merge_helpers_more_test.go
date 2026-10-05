package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEvaluateSimpleWhereClauseForNodeMap_MoreBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	nodeMap := map[string]*storage.Node{
		"n": {ID: "n1", Properties: map[string]interface{}{"k": "v", "x": int64(2), "y": int64(3)}},
		"m": {ID: "m1", Properties: map[string]interface{}{"k": "v2", "x": int64(2)}},
	}

	ok, pass := exec.evaluateSimpleWhereClauseForNodeMap(ctx, nodeMap, "")
	require.True(t, ok)
	require.True(t, pass)

	ok, pass = exec.evaluateSimpleWhereClauseForNodeMap(ctx, nodeMap, "n.k IN $vals")
	require.True(t, ok)
	require.False(t, pass) // non-list rhs in this context

	ok, pass = exec.evaluateSimpleWhereClauseForNodeMap(ctx, nodeMap, "n.k IN ['x','v']")
	require.True(t, ok)
	require.True(t, pass)

	ok, pass = exec.evaluateSimpleWhereClauseForNodeMap(ctx, nodeMap, "n.x = m.x")
	require.True(t, ok)
	require.True(t, pass)

	ok, pass = exec.evaluateSimpleWhereClauseForNodeMap(ctx, nodeMap, "n.y = 3")
	require.True(t, ok)
	require.True(t, pass)

	ok, pass = exec.evaluateSimpleWhereClauseForNodeMap(ctx, nodeMap, "3 = m.x")
	require.True(t, ok)
	require.False(t, pass)

	ok, pass = exec.evaluateSimpleWhereClauseForNodeMap(ctx, nodeMap, "missing.prop = 1")
	require.False(t, ok)
	require.False(t, pass)
}

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
	nodeCtx := map[string]*storage.Node{"n": node}
	relCtx := map[string]*storage.Edge{"r": rel}
	input := []pipelineRow{{"n": node, "r": rel, "s": int64(7)}}
	_, err := exec.Execute(ctx, "WITH RETURN 1", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
	projected, ok := exec.pipelineApplyWith(ctx, input, "WITH *")
	require.True(t, ok)
	require.Equal(t, input, projected)
	projected, ok = exec.pipelineApplyWith(ctx, input, "WITH n AS nn, r AS rr, s AS ss")
	require.True(t, ok)
	require.Equal(t, []pipelineRow{{"nn": node, "rr": rel, "ss": int64(7)}}, projected)
	_, err = exec.Execute(ctx, "WITH 7 AS s WITH s AS ss, ghost AS gg RETURN ss", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")

	require.True(t, exec.evaluateWhereForMergeContext(ctx, "true", nodeCtx, relCtx))
	require.True(t, exec.evaluateWhereForMergeContext(ctx, "n.name = 'alice'", nodeCtx, relCtx))
	require.False(t, exec.evaluateWhereForMergeContext(ctx, "n.name = 'bob'", nodeCtx, relCtx))

}

func TestMergeWhereRejectsNonBooleanProperty(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := withExpressionFailureSlot(context.Background())
	nodeCtx := map[string]*storage.Node{
		"n": {Properties: map[string]interface{}{"name": "alice"}},
	}

	require.False(t, exec.evaluateWhereForMergeContext(ctx, "n.name", nodeCtx, nil))
	require.ErrorContains(t, getExpressionFailure(ctx), "Type mismatch: expected Boolean but was String")
}
