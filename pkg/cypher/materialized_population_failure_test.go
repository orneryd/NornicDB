package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Each consumer of materializedBatches returns its failure when the owner
// yields the cancellation: an error, or its documented incomplete value that
// the statement's final decision then fails. The public cancellation sweep
// reaches most of them through Execute; these cover the remaining ones
// directly, with a canceled context and a population of two.
func TestMaterializedPopulationConsumersReturnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "failure"), 0, 0)
	node := &storage.Node{ID: "failure:a", Labels: []string{"P"}}
	rows := []pipelineRow{{"n": node}, {"n": node}}
	result := func() *ExecuteResult {
		return &ExecuteResult{Columns: []string{"b", "a"}, Rows: [][]interface{}{{int64(1), int64(2)}, {int64(3), int64(4)}}}
	}

	_, err := reorderUnionRows(ctx, result().Rows, []int{1, 0})
	require.ErrorIs(t, err, context.Canceled)

	deleted := &deletedEntities{}
	deleted.add([]storage.NodeID{node.ID}, nil)
	require.ErrorIs(t, validateDeletedEntityReads(ctx, rows, "RETURN n.value", deleted), context.Canceled)
	executor.replaceDeletedEntityViews(ctx, rows, deleted)
	require.Same(t, node, rows[0]["n"], "a canceled pass replaces nothing")

	_, _, err = collectDeleteMutationTargets(ctx, result())
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, executor.normalizeSetMatchRowsToNodes(ctx, result(), executor.storage), context.Canceled)
	require.ErrorIs(t, executor.normalizeSetMatchRowsToEdges(ctx, result(), executor.storage), context.Canceled)
	require.ErrorIs(t, validateBoundPatternNodes(ctx, rows, "(n)-->(m)"), context.Canceled)

	handled := true
	_, err = pipelineApplyOptionally(ctx, rows, func([]pipelineRow) ([]pipelineRow, []string, bool, error) {
		t.Fatal("no row runs after cancellation")
		return nil, nil, true, nil
	}, &handled)
	require.ErrorIs(t, err, context.Canceled)

	_, err = combineCallTailRowResults(ctx, []callTailRowResult{{res: result()}, {res: result()}}, []string{"b", "a"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = materializedRowsWithCancellation(ctx, nil, result().Rows)
	require.ErrorIs(t, err, context.Canceled)
	_, err = executor.extractSeedNodesFromResult(ctx, &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{node}, {node}}}, "n")
	require.ErrorIs(t, err, context.Canceled)
	_, err = executor.processCallSubqueryReturn(ctx, result(), "RETURN a")
	require.ErrorIs(t, err, context.Canceled)

	sorted := result()
	sortColumnsByName(ctx, sorted)
	require.Equal(t, []string{"b", "a"}, sorted.Columns, "a canceled restoration leaves the result as it was")
	generated := &ExecuteResult{Columns: []string{"a", generatedVariablePrefix + "0"}, Rows: [][]interface{}{{1, 2}, {3, 4}}}
	require.Same(t, generated, withoutGeneratedColumns(ctx, generated))
	require.Empty(t, appendCallReturnRows(ctx, nil, pipelineRow{}, []string{"a"}, [][]interface{}{{1}, {2}}))
	_, accepted := executor.evaluatePipelinePercentile(ctx, []pipelineRow{{"x": int64(1)}, {"x": int64(2)}}, "percentilecont", "x, 0.5", false)
	require.False(t, accepted)
	executor.refreshRowEntities(ctx, rows)

	// One row: no batch boundary, so its key's own checkpoint stops a large
	// nested value (rowKeyCancellation), recovered as the cancellation.
	single := [][]interface{}{{[]interface{}{int64(1), []interface{}{int64(2)}}}}
	var seen cypherValueIndex
	retention := newMemoryRetention(context.Background())
	defer retention.close()
	require.ErrorIs(t, appendDistinctUnionRows(ctx, &seen, &retention, &ExecuteResult{}, single), context.Canceled)
}
