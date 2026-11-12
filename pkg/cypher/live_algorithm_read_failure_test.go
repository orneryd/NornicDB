package cypher

// The live adjacency reads of the whole-graph algorithms. Each route below
// reads the population's adjacency and answers with what the read returned, so
// a read that fails is the statement's error: a swallowed failure would answer
// with a graph that has fewer relationships than the one storage holds. The
// control fails every outgoing adjacency read and performs every other read,
// and each route runs through the public entry point on a graph the same
// statement answers without the failure.
//
// The prior paths through this control are the link prediction stream (the
// candidate graph and the source's degree are adjacency reads) and, in
// TestGraphAdjacencyReadFailureFailsThePathStatements, the path expander and
// dijkstra.

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// liveAlgorithmReadFailureEngine fails every outgoing adjacency read and
// performs every other read.
type liveAlgorithmReadFailureEngine struct {
	storage.Engine
	failure error
}

func (e *liveAlgorithmReadFailureEngine) GetInnerEngine() storage.Engine { return e.Engine }

func (e *liveAlgorithmReadFailureEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if request.Direction != storage.AdjacentOutgoing {
		return e.Engine.ReadAdjacentEdges(ctx, request)
	}
	return nil, e.failure
}

// newLiveAlgorithmReadFixture is the graph every route below reads: two :P
// nodes an :X relationship connects, with test:a the stored ID the link
// prediction config names as its source.
func newLiveAlgorithmReadFixture(t *testing.T) storage.Engine {
	t.Helper()
	ctx := context.Background()
	store := newTestMemoryEngine(t)
	for _, node := range []*storage.Node{
		{ID: "test:a", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "a"}},
		{ID: "test:b", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "b"}},
	} {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: "test:e", Type: "X", StartNode: "test:a", EndNode: "test:b"}))
	return store
}

// TestLiveAlgorithmRoutesPropagateAdjacencyReadFailure: every route below reads
// adjacency before it ranks or partitions anything, so a failed read is the
// whole statement's failure, with no result. healthy is the same statement's
// ordinary answer over the same graph; the link prediction route's rows are the
// candidates its source has, so its control is that the same statement answers
// without the failure.
func TestLiveAlgorithmRoutesPropagateAdjacencyReadFailure(t *testing.T) {
	failure := errors.New("live relationships unavailable")
	for _, test := range []struct {
		name    string
		query   string
		healthy [][]interface{}
	}{
		{
			name:  "gds.linkPrediction.commonNeighbors.stream",
			query: `CALL gds.linkPrediction.commonNeighbors.stream({sourceNode:'test:a'}) YIELD node1,node2,score RETURN node1,node2,score`,
		},
		{
			name:    "apoc.algo.pageRank",
			query:   `CALL apoc.algo.pageRank(['P']) YIELD node, score RETURN count(node) AS nodes`,
			healthy: [][]interface{}{{int64(2)}},
		},
		{
			name:    "apoc.algo.betweenness",
			query:   `CALL apoc.algo.betweenness(['P']) YIELD node, score RETURN count(node) AS nodes`,
			healthy: [][]interface{}{{int64(2)}},
		},
		{
			name:    "apoc.algo.louvain",
			query:   `CALL apoc.algo.louvain(['P']) YIELD node, community RETURN count(node) AS nodes`,
			healthy: [][]interface{}{{int64(2)}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			forEachParser(t, func(t *testing.T) {
				ctx := context.Background()
				store := newLiveAlgorithmReadFixture(t)

				// The same statement over the same graph without the failure: the
				// control that the query, its arguments and this graph answer.
				healthy, err := NewStorageExecutor(store).Execute(ctx, test.query, nil)
				require.NoError(t, err)
				require.NotNil(t, healthy)
				if test.healthy != nil {
					require.Equal(t, test.healthy, healthy.Rows)
				}

				exec := NewStorageExecutor(&liveAlgorithmReadFailureEngine{Engine: store, failure: failure})
				result, err := exec.Execute(ctx, test.query, nil)
				require.ErrorIs(t, err, failure)
				require.Nil(t, result)
			})
		})
	}
}
