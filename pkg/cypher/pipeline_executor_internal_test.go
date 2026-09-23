package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCanExecuteAsPipeline_SimpleSeederShape(t *testing.T) {
	q := `
		MATCH (c:Customer {customerID: 1})
		CREATE (o:Order {orderID: 9001})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, {}
		UNWIND [{productID: 1, quantity: 3}] AS prodRef
		MATCH (p:Product {productID: prodRef.productID})
		CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)`

	clauses, ok := canExecuteAsPipeline(q)
	require.True(t, ok, "pipeline splitter must accept composite MATCH+CREATE+WITH+UNWIND+MATCH+CREATE")
	t.Logf("got %d clauses", len(clauses))
	for i, c := range clauses {
		head := strings.SplitN(strings.TrimSpace(c.text), "\n", 2)[0]
		if len(head) > 80 {
			head = head[:80] + "..."
		}
		t.Logf("  [%d] kind=%d text=%q", i, c.kind, head)
	}

	require.GreaterOrEqual(t, len(clauses), 7, "expected at least 7 clauses")
	require.Equal(t, pipelineClauseMatch, clauses[0].kind)
	require.Equal(t, pipelineClauseCreate, clauses[1].kind)
	require.Equal(t, pipelineClauseCreate, clauses[2].kind)
	require.Equal(t, pipelineClauseWith, clauses[3].kind)
	require.Equal(t, pipelineClauseUnwind, clauses[4].kind)
	require.Equal(t, pipelineClauseMatch, clauses[5].kind)
	require.Equal(t, pipelineClauseCreate, clauses[6].kind)
}

func TestPipelineSimpleNodeReadPlan_HandlesBoundedLabelStream(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = store.Close() })
	for index := 0; index < 20; index++ {
		_, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID("nornic:" + string(rune('a'+index))),
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": index},
		})
		require.NoError(t, err)
	}
	exec := NewStorageExecutor(store)
	query := "MATCH (n:Person) RETURN n.name LIMIT 3"
	clauses, ok := canExecuteAsPipeline(query)
	require.True(t, ok)

	result, handled, err := exec.tryExecutePipelineSimpleNodeReadPlan(context.Background(), clauses, nil)
	require.NoError(t, err)
	require.True(t, handled, "bounded single-node reads must use the pipeline's fused physical operator")
	require.Len(t, result.Rows, 3)
}
