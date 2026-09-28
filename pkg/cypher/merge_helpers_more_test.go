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

func TestCollectTopLevelMergeClauseBoundaries_MoreBranches(t *testing.T) {
	q := "MERGE (n:Node {name:'MATCH in string'}) ON MATCH SET n.a = 1 OPTIONAL MATCH (m:Node) RETURN n"
	b := collectTopLevelMergeClauseBoundaries(q, []string{"MATCH", "OPTIONAL MATCH", "MERGE", "RETURN"})
	require.NotEmpty(t, b)

	foundMerge := false
	foundOptionalMatch := false
	foundReturn := false
	foundOnMatchAsClause := false
	for _, x := range b {
		switch x.kw {
		case "MERGE":
			foundMerge = true
		case "OPTIONAL MATCH":
			foundOptionalMatch = true
		case "RETURN":
			foundReturn = true
		case "MATCH":
			// Must not capture ON MATCH modifier as a clause boundary.
			if isOnMatchModifier(q, x.pos) {
				foundOnMatchAsClause = true
			}
		}
	}
	require.True(t, foundMerge)
	require.True(t, foundOptionalMatch)
	require.True(t, foundReturn)
	require.False(t, foundOnMatchAsClause)
}

func TestMergeContextHelpers_MoreBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	node := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "alice"}}
	rel := &storage.Edge{ID: "e1", Type: "REL", StartNode: "n1", EndNode: "n1", Properties: map[string]interface{}{"w": int64(1)}}
	nodeCtx := map[string]*storage.Node{"n": node}
	relCtx := map[string]*storage.Edge{"r": rel}
	scalarCtx := map[string]interface{}{"s": int64(7)}

	outN, outR, outS := exec.projectWithContext(ctx, "", nodeCtx, relCtx, scalarCtx)
	require.Equal(t, nodeCtx, outN)
	require.Equal(t, relCtx, outR)
	require.Equal(t, scalarCtx, outS)

	outN, outR, outS = exec.projectWithContext(ctx, "*", nodeCtx, relCtx, scalarCtx)
	require.Equal(t, nodeCtx, outN)
	require.Equal(t, relCtx, outR)
	require.Equal(t, scalarCtx, outS)

	outN, outR, outS = exec.projectWithContext(ctx, "n AS nn, r AS rr, s AS ss, ghost AS gg", nodeCtx, relCtx, scalarCtx)
	require.Contains(t, outN, "nn")
	require.Contains(t, outR, "rr")
	require.Contains(t, outS, "ss")
	require.NotContains(t, outS, "gg")

	require.True(t, exec.evaluateWhereForMergeContext(ctx, "true", nodeCtx, relCtx))
	require.True(t, exec.evaluateWhereForMergeContext(ctx, "n.name = 'alice'", nodeCtx, relCtx))
	require.False(t, exec.evaluateWhereForMergeContext(ctx, "n.name = 'bob'", nodeCtx, relCtx))

	require.True(t, isOnMatchModifier("MERGE (n) ON MATCH SET n.x = 1", 12))
	require.True(t, isOptionalMatchModifier("OPTIONAL MATCH (n) RETURN n", 9))
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
