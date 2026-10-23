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

func TestCanExecuteAsPipeline_StandaloneMerge(t *testing.T) {
	for _, query := range []string{
		"MERGE (n:Standalone {id:$id})",
		"MERGE (n:Standalone {id:$id}) ON CREATE SET n.created=true ON MATCH SET n.seen=true",
		"MERGE (a:Standalone {id:1})-[:R]->(b:Standalone {id:2})",
	} {
		t.Run(query, func(t *testing.T) {
			clauses, ok := canExecuteAsPipeline(query)
			require.True(t, ok)
			require.Len(t, clauses, 1)
			require.Equal(t, pipelineClauseMerge, clauses[0].kind)
		})
	}
}

func TestUnwindRewrittenOperatorsPreserveExecutionErrors(t *testing.T) {
	for _, fixture := range []struct {
		name      string
		remainder string
		create    bool
	}{
		{name: "read", remainder: "MATCH (n:UnwindError) WHERE n.id = row AND 1 / 0 = 0 RETURN count(n)"},
		{name: "create", remainder: "MATCH (n:UnwindError) WHERE n.id = row CREATE (m:UnwindErrorCopy {value: 1 / 0}) RETURN count(m)", create: true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:UnwindError {id: 1})", nil)
			require.NoError(t, err)
			plan := topLevelUnwindPlan{variable: "row", items: []interface{}{int64(1)}, remainder: fixture.remainder}
			rewritten, ok := rewriteUnwindCorrelationToIn(plan.remainder, plan.variable, "__unwind_items")
			require.True(t, ok)
			_, expectedErr := exec.Execute(ctx, rewritten, map[string]interface{}{"__unwind_items": plan.items})
			require.Error(t, expectedErr)
			outcome := exec.executePipeline(withQueryParams(ctx, map[string]interface{}{"__unwind_items": plan.items}), rewritten)
			require.Error(t, outcome.err)
			require.Equal(t, expectedErr.Error(), outcome.err.Error())
			require.True(t, outcome.terminal())
			require.Nil(t, outcome.result)
			var result *ExecuteResult
			var handled bool
			if fixture.create {
				result, handled, err = exec.executeSetBasedUnwindCreateOperator(ctx, plan)
			} else {
				result, handled, err = exec.executeUnwindBatchOperator(ctx, plan)
			}
			require.Error(t, err)
			require.Equal(t, expectedErr.Error(), err.Error())
			require.True(t, handled)
			require.Nil(t, result)
			persisted, err := exec.Execute(ctx, "MATCH (m:UnwindErrorCopy) RETURN count(m)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, persisted.Rows)
		})
	}
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
