package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCanExecuteAsPipeline_QueryShapes is the shared recognition contract for
// seeder, CALL-tail, CREATE-window and standalone MERGE statement shapes.
func TestCanExecuteAsPipeline_QueryShapes(t *testing.T) {
	seederKinds := []pipelineClauseKind{pipelineClauseMatch, pipelineClauseCreate, pipelineClauseCreate, pipelineClauseWith, pipelineClauseUnwind, pipelineClauseMatch, pipelineClauseCreate}
	for _, tc := range []struct {
		name       string
		query      string
		prefix     []pipelineClauseKind
		exactCount int
	}{
		{name: "seeder with relationship properties", query: `
		MATCH (c:Customer {customerID: 1})
		CREATE (o:Order {orderID: 9001})
		CREATE (c)-[:PURCHASED]->(o)
		WITH o, {}
		UNWIND [{productID: 1, quantity: 3}] AS prodRef
		MATCH (p:Product {productID: prodRef.productID})
		CREATE (o)-[:ORDERS {quantity: prodRef.quantity}]->(p)`, prefix: seederKinds},
		{name: "seeder", query: `MATCH (c:Customer {customerID: 1}) CREATE (o:Order {orderID: 9001}) CREATE (c)-[:PURCHASED]->(o) WITH o, {} UNWIND [{productID: 1}] AS prodRef MATCH (p:Product {productID: prodRef.productID}) CREATE (o)-[:ORDERS]->(p)`},
		{name: "comma MATCH then CREATE", query: `MATCH (o:OriginalText {id:'o1'}), (t:TranslatedText {id:'t1'}) CREATE (o)-[:TRANSLATES_TO]->(t)`},
		{name: "standalone CREATE RETURN", query: `CREATE (node:Item {num: 1}) RETURN node`},
		{name: "CREATE RETURN window", query: `CREATE (node:Item {num: 1}) RETURN node LIMIT 0`},
		{name: "CALL tail binding", query: `MATCH (node) WHERE id(node) = $seed_id_node WITH node CREATE (m:TailProbe {name: node.originalText}) RETURN m.name AS probeName`},
		{name: "CALL tail scalar projection", query: `MATCH (node) WHERE id(node) = $seed_id_node WITH node, $seed_score AS score CREATE (m:TailProbe {name: node.originalText, score: score}) RETURN m.name AS probeName`},
		{name: "standalone node MERGE", query: "MERGE (n:Standalone {id:$id})", prefix: []pipelineClauseKind{pipelineClauseMerge}, exactCount: 1},
		{name: "standalone MERGE actions", query: "MERGE (n:Standalone {id:$id}) ON CREATE SET n.created=true ON MATCH SET n.seen=true", prefix: []pipelineClauseKind{pipelineClauseMerge}, exactCount: 1},
		{name: "standalone relationship MERGE", query: "MERGE (a:Standalone {id:1})-[:R]->(b:Standalone {id:2})", prefix: []pipelineClauseKind{pipelineClauseMerge}, exactCount: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clauses, ok := canExecuteAsPipeline(t.Context(), tc.query)
			require.True(t, ok, "pipeline splitter must accept %s", tc.query)
			if tc.exactCount > 0 {
				require.Len(t, clauses, tc.exactCount)
			}
			require.GreaterOrEqual(t, len(clauses), len(tc.prefix))
			for i, want := range tc.prefix {
				require.Equal(t, want, clauses[i].kind, "clause %d", i)
			}
			t.Logf("got %d clauses", len(clauses))
			for i, clause := range clauses {
				head := strings.SplitN(strings.TrimSpace(clause.text), "\n", 2)[0]
				if len(head) > 80 {
					head = head[:80] + "..."
				}
				t.Logf("  [%d] kind=%d text=%q", i, clause.kind, head)
			}
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
			plan := topLevelUnwindPlan{variable: "row", items: []interface{}{int64(1)}, remainder: originalBraceView(fixture.remainder)}
			rewritten, ok := rewriteUnwindCorrelationToIn(plan.remainder, plan.variable, "__unwind_items")
			require.True(t, ok)
			_, expectedErr := exec.Execute(ctx, rewritten.text(), map[string]interface{}{"__unwind_items": plan.items})
			require.Error(t, expectedErr)
			outcome := exec.executePipeline(withQueryParams(ctx, map[string]interface{}{"__unwind_items": plan.items}), rewritten.text())
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
	store := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
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
	clauses, ok := canExecuteAsPipeline(t.Context(), query)
	require.True(t, ok)

	result, handled, err := exec.tryExecutePipelineSimpleNodeReadPlan(context.Background(), clauses, nil)
	require.NoError(t, err)
	require.True(t, handled, "bounded single-node reads must use the pipeline's fused physical operator")
	require.Len(t, result.Rows, 3)
}
