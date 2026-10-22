package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// entityFailureFixture is a small graph whose candidate populations have at
// least two members: four :P nodes, each related to a :Q node by an :R
// relationship carrying a weight.
func entityFailureFixture(t *testing.T) (*StorageExecutor, storage.Engine, []*storage.Node, []*storage.Edge) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "entity_failure")
	var nodes []*storage.Node
	var edges []*storage.Edge
	for i := int64(1); i <= 4; i++ {
		p := &storage.Node{ID: storage.NodeID("p" + string(rune('0'+i))), Labels: []string{"P"}, Properties: map[string]interface{}{"id": i, "k": i % 2, "name": "p"}}
		q := &storage.Node{ID: storage.NodeID("q" + string(rune('0'+i))), Labels: []string{"Q"}, Properties: map[string]interface{}{"id": i, "k": i % 2, "name": "q"}}
		for _, node := range []*storage.Node{p, q} {
			_, err := store.CreateNode(node)
			require.NoError(t, err)
		}
		edge := &storage.Edge{ID: storage.EdgeID("r" + string(rune('0'+i))), Type: "R", StartNode: p.ID, EndNode: q.ID, Properties: map[string]interface{}{"w": float64(i)}}
		require.NoError(t, store.CreateEdge(edge))
		nodes = append(nodes, p, q)
		edges = append(edges, edge)
	}
	return NewStorageExecutorWithQueryCachePolicy(store, 0, 0), store, nodes, edges
}

// The entity populations' consumers (candidate, join, traversal, MERGE,
// DELETE, fast-path and filter passes) return their failure once the owner
// yields the cancellation; each also completes its pass when nothing
// cancels it.
func TestMaterializedEntityPopulationConsumersReturnCancellation(t *testing.T) {
	executor, store, nodes, edges := entityFailureFixture(t)
	background := context.Background()
	parent, cancel := context.WithCancel(background)
	cancel()
	canceled := withExpressionFailureSlot(parent)
	ps := []*storage.Node{nodes[0], nodes[2], nodes[4], nodes[6]}
	ids := []storage.NodeID{nodes[0].ID, nodes[2].ID}
	edgeIDs := []storage.EdgeID{edges[0].ID, edges[1].ID}

	matches := []struct {
		variable string
		nodes    []*storage.Node
	}{{variable: "a", nodes: ps}, {variable: "b", nodes: ps}}
	_, _, err := executor.buildCombinationsUsingWhereJoin(canceled, matches, "a.k = b.k")
	assert.ErrorIs(t, err, context.Canceled, "check 1")
	joined, ok, err := executor.buildCombinationsUsingWhereJoin(background, matches, "a.k = b.k")
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, joined, 8)

	items := []returnItem{{expr: "n", alias: "n"}, {expr: "n.id", alias: "id"}}
	_, err = projectPipelineSimpleNodeRead(canceled, ps, items, "n")
	assert.ErrorIs(t, err, context.Canceled, "check 2")
	_, err = executor.projectVisiblePipelineNodeRead(canceled, ps, items, "n")
	assert.ErrorIs(t, err, context.Canceled, "check 3")

	_, _, err = countUndirectedRelationshipMatches(canceled, store, nil)
	assert.ErrorIs(t, err, context.Canceled, "check 4")
	_, _, err = countUndirectedRelationshipMatches(canceled, store, []string{"R"})
	assert.ErrorIs(t, err, context.Canceled, "check 5")

	assert.ErrorIs(t, validateNoResidualRelationships(canceled, store, ids, nil), context.Canceled, "check 6")
	_, err = connectedDeleteTargets(canceled, store, ids, nil)
	assert.ErrorIs(t, err, context.Canceled, "check 7")
	err = addDeletingEdges(canceled, map[storage.EdgeID]struct{}{}, edgeIDs)
	assert.ErrorIs(t, err, context.Canceled, "check 8")

	_, _, err = executor.selectTopKNodesByOrder(canceled, ps, "n", "n.id DESC", 2)
	assert.ErrorIs(t, err, context.Canceled, "check 9")
	top, ok, err := executor.selectTopKNodesByOrder(background, ps, "n", "n.id DESC", 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(4), top[0].Properties["id"])

	_, err = executor.loadMergeCandidateNodes(canceled, store, ids)
	assert.ErrorIs(t, err, context.Canceled, "check 10")
	_, err = executor.findMergeNode(canceled, store, []string{"P"}, nil)
	assert.ErrorIs(t, err, context.Canceled, "check 11")
	_, err = executor.findMergeNode(canceled, store, nil, nil)
	assert.ErrorIs(t, err, context.Canceled, "check 12")
	_, err = executor.findMergeNode(canceled, store, []string{"P"}, map[string]interface{}{"id": int64(9)})
	assert.ErrorIs(t, err, context.Canceled, "check 13")
	_, _, err = executor.findMergeNodesScanned(canceled, store, []string{"P"}, map[string]interface{}{"id": int64(9)})
	assert.ErrorIs(t, err, context.Canceled, "check 14")

	for _, run := range []func(context.Context) (*ExecuteResult, error){
		func(ctx context.Context) (*ExecuteResult, error) {
			return executor.executeIncomingCountOptimized(ctx, "MATCH (a)-[:R]->(b) RETURN b.name, count(a)", PatternInfo{RelType: "R"})
		},
		func(ctx context.Context) (*ExecuteResult, error) {
			return executor.executeOutgoingCountOptimized(ctx, "MATCH (a)-[:R]->(b) RETURN a.name, count(b)", PatternInfo{RelType: "R"})
		},
		func(ctx context.Context) (*ExecuteResult, error) {
			return executor.executeEdgePropertyAggOptimized(ctx, "MATCH (a)-[r:R]->(b) RETURN b.name, avg(r.w)", PatternInfo{RelType: "R", AggProperty: "w", AggFunctions: []string{"avg"}})
		},
		func(ctx context.Context) (*ExecuteResult, error) {
			return executor.executeMutualRelationshipOptimized(ctx, "MATCH (a)-[:R]->(b)-[:R]->(a) RETURN a.name, b.name", PatternInfo{RelType: "R"})
		},
	} {
		_, err = run(canceled)
		assert.ErrorIs(t, err, context.Canceled, "check 15")
		_, err = run(background)
		require.NoError(t, err)
	}

	keep := func(*storage.Node) bool { return true }
	_, err = sequentialFilterNodes(canceled, ps, keep)
	assert.ErrorIs(t, err, context.Canceled, "check 16")
	previousParallel := GetParallelConfig()
	t.Cleanup(func() { SetParallelConfig(previousParallel) })
	SetParallelConfig(ParallelConfig{Enabled: true, MaxWorkers: 2, MinBatchSize: 1})
	_, err = parallelFilterNodes(canceled, ps, keep)
	assert.ErrorIs(t, err, context.Canceled, "check 17")
	filtered, err := parallelFilterNodes(background, ps, keep)
	require.NoError(t, err)
	require.Len(t, filtered, 4)
	assert.ErrorIs(t, visitNodeList(canceled, ps, func(*storage.Node) error { return nil }), context.Canceled, "check 18")
	_, err = filterNodesByTemporalViewport(canceled, ps, TemporalViewport{Mode: TemporalViewportAsOf, AsOf: time.Now()}, everyNodeCurrent{})
	assert.ErrorIs(t, err, context.Canceled, "check 19")
	_, _, err = buildNodeBatchLabelMatchIndex(canceled, store, nodeBatchMatchKey{label: "P", prop: "id"}, map[string]interface{}{"1": int64(1)})
	assert.ErrorIs(t, err, context.Canceled, "check 20")

	traversal := executor.parseTraversalPattern(background, "(p:P)-[r:R]->(q:Q)")
	require.NotNil(t, traversal)
	paths := executor.traverseGraph(background, traversal)
	require.Len(t, paths, 4)
	require.Nil(t, executor.traverseGraph(canceled, traversal))
	require.Nil(t, executor.traverseChainedGraph(canceled, traversal, ps))
	require.Nil(t, executor.filterPathsByWhere(canceled, paths, traversal, "q.id > 0", nil))
	require.Len(t, executor.filterPathsByWhere(background, paths, traversal, "q.id > 0", nil), 4)
	require.Nil(t, executor.aggregatePathSum(canceled, paths, traversal, "r.w"))
	require.Nil(t, executor.aggregatePathAvg(canceled, paths, traversal, "r.w"))
	require.Nil(t, executor.aggregatePathMinMax(canceled, paths, traversal, "r.w", true))
	require.Zero(t, executor.aggregatePathCount(canceled, paths, traversal, "count(q)"))
	require.Nil(t, executor.aggregatePathCollect(canceled, paths, traversal, "collect(q.id)", false))
	require.Nil(t, executor.aggregatePathCollect(canceled, paths, traversal, "collect(DISTINCT q.id)", true))
	for _, aggregate := range []interface{}{
		executor.aggregatePathSum(background, paths, traversal, "r.w"),
		executor.aggregatePathAvg(background, paths, traversal, "r.w"),
		executor.aggregatePathMinMax(background, paths, traversal, "r.w", false),
		executor.aggregatePathCollect(background, paths, traversal, "collect(DISTINCT q.id)", true),
	} {
		require.NotNil(t, aggregate)
	}
	result := &ExecuteResult{}
	executor.appendTraversalRows(canceled, result, paths, traversal, []returnItem{{expr: "q.id"}}, -1)
	require.Empty(t, result.Rows)
	reverse, seeded := executor.traverseFromEndSeeds(canceled, traversal, nil, []*storage.Node{nodes[1], nodes[3]}, "")
	require.Nil(t, reverse)
	require.True(t, seeded)
	_, _, err = executor.tryCollectNodesFromStartPropertyScan(canceled, nodePatternInfo{variable: "p", labels: []string{"P"}}, "p.id > 0")
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled, "check 21")
	}

	aggregation := executor.parseTraversalPattern(background, "(q:Q)<-[:R]-(p:P)")
	require.NotNil(t, aggregation)
	aggregationItems := []returnItem{{expr: "q.name"}, {expr: "count(p)", alias: "c"}, {expr: "sum(p.id)", alias: "s"}, {expr: "avg(p.id)", alias: "a"}, {expr: "collect(p.id)", alias: "ids"}}
	_, handledAgg, err := executor.tryFastRelationshipAggregations(canceled, aggregation, aggregationItems)
	if handledAgg {
		assert.ErrorIs(t, err, context.Canceled, "check 22")
	}
	_, _, err = executor.tryFastRelationshipAggregations(background, aggregation, aggregationItems)
	require.NoError(t, err)

	_, err = executor.Execute(background, "CREATE INDEX entity_failure_id FOR (n:P) ON (n.id)", nil)
	require.NoError(t, err)
	inList := map[string]interface{}{"ids": []interface{}{int64(1), int64(2), int64(3)}}
	_, used, err := executor.tryCollectNodesFromPropertyIndexInAnyOf(canceled, nodePatternInfo{variable: "n", labels: []string{"P"}}, "n.id IN $ids", inList, nil)
	if used {
		assert.ErrorIs(t, err, context.Canceled, "check 23")
	}
	seedNodes, used, err := executor.tryCollectNodesFromPropertyIndexInAnyOf(background, nodePatternInfo{variable: "n", labels: []string{"P"}}, "n.id IN $ids", inList, nil)
	require.NoError(t, err)
	if used {
		require.Len(t, seedNodes, 3)
	}

	bindings := []binding{{"p": nodes[0]}, {"p": nodes[2]}}
	newBindings, _ := executor.executeChainedMatch(canceled, "(p)-[:R]->(q)", bindings, nil)
	require.Nil(t, newBindings)
	newBindings, _ = executor.executeChainedMatch(background, "(p)-[:R]->(q)", bindings, nil)
	require.Len(t, newBindings, 2)
	newBindings, _ = executor.executeChainedMatch(canceled, "(q:Q)", bindings, nil)
	require.Nil(t, newBindings)
	newBindings, _ = executor.executeChainedMatch(background, "(q:Q)", bindings, nil)
	require.Len(t, newBindings, 8)
	rows := []pipelineRow{{"p": nodes[0]}, {"p": nodes[2]}}
	out, handled := executor.pipelineApplyChainedMatch(canceled, rows, "MATCH (p)-[:R]->(q)")
	if handled {
		require.Nil(t, out)
	}
	out, handled = executor.pipelineApplyChainedMatch(background, rows, "MATCH (p)-[:R]->(q)")
	if handled {
		require.Len(t, out, 2)
	}
}

// everyNodeCurrent is a temporal checker that finds every node current.
type everyNodeCurrent struct{}

func (everyNodeCurrent) IsCurrentTemporalNode(*storage.Node, time.Time) (bool, error) {
	return true, nil
}
