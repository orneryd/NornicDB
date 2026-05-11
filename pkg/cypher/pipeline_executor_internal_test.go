package cypher

import (
	"strings"
	"testing"

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
