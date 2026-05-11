package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCallWithCreateTail_DoesNotHitPipelineExecutor probes which router path
// the CALL YIELD CREATE test hits. It asserts the pipeline executor is NOT
// invoked for this shape (CALL starts the query, not MATCH).
func TestCallWithCreateTail_DoesNotHitPipelineExecutor(t *testing.T) {
	q := `
CALL db.index.vector.queryNodes('idx_tail', 2, [1.0, 0.0, 0.0, 0.0])
YIELD node, score
CREATE (m:TailProbe {name: node.originalText})
RETURN m.name AS probeName`

	clauses, ok := canExecuteAsPipeline(q)
	t.Logf("canExecuteAsPipeline returned ok=%v, clauses=%d", ok, len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] kind=%d text=%q", i, c.kind, c.text[:min(80, len(c.text))])
	}
	// CALL is not a supported clause kind in our splitter; it should reject.
	require.False(t, ok, "pipeline must not claim to handle CALL queries")
}

// TestPipelineBailsOnCallInMiddle ensures the splitter's disallow-list
// catches CALL in the middle of a query too.
func TestPipelineBailsOnCallInMiddle(t *testing.T) {
	q := `MATCH (n) WITH n CALL foo.bar() YIELD x CREATE (:Y {v: x})`
	_, ok := canExecuteAsPipeline(q)
	require.False(t, ok, "pipeline must reject queries with CALL in the middle")
}

// For debugging regression: check the bare Execute path for the CALL test's
// query succeeds (i.e. returns the expected count of side-effects).
func TestCallYieldCreate_PreservesTailSideEffects(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Minimal setup to emulate the real test — create a vector index with 2 items.
	_, err := exec.Execute(ctx, `CALL db.index.vector.createNodeIndex('idx_probe', 'Item', 'embedding', 4, 'cosine')`, nil)
	if err != nil {
		t.Skipf("vector index setup unavailable in this build: %v", err)
	}
	_, err = exec.Execute(ctx,
		`CREATE (:Item {originalText: 'a', embedding: [1.0, 0.0, 0.0, 0.0]}),`+
			`(:Item {originalText: 'b', embedding: [0.0, 1.0, 0.0, 0.0]})`, nil)
	require.NoError(t, err)

	q := `CALL db.index.vector.queryNodes('idx_probe', 2, [1.0, 0.0, 0.0, 0.0])
YIELD node, score
CREATE (m:TailProbe {name: node.originalText})
RETURN m.name AS probeName`

	result, err := exec.Execute(ctx, q, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	check, err := exec.Execute(ctx, `MATCH (m:TailProbe) RETURN count(m) AS c`, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, check.Rows[0][0].(int64), int64(1),
		"CALL YIELD CREATE must produce at least one TailProbe node")

	_ = storage.NodeID("unused")
}
