package cypher

// The apoc.path.subgraphAll final pass. After the expander has walked the graph,
// the call answers with the nodes it returned and the relationships between
// them, reading each of those nodes' outgoing adjacency once more. That read is
// the call's own, not the expander's, so it owns its failure: a read that fails
// is the statement's error, not an empty relationship list. The control below
// fails exactly the read only that final pass performs, after the expansion's
// own reads succeeded, and runs the call through the public entry point.

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// subgraphAllFinalPassReadFailureEngine fails the outgoing adjacency read of one
// node and performs every other read.
type subgraphAllFinalPassReadFailureEngine struct {
	storage.Engine
	node    storage.NodeID
	failure error
}

func (e *subgraphAllFinalPassReadFailureEngine) GetInnerEngine() storage.Engine { return e.Engine }

func (e *subgraphAllFinalPassReadFailureEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if request.Direction == storage.AdjacentOutgoing && request.Node == e.node {
		return nil, e.failure
	}
	return e.Engine.ReadAdjacentEdges(ctx, request)
}

// TestGraphSubgraphAllFinalPassAdjacencyReadFailureFailsTheStatement: the
// second step c sits at the call's maxLevel, so the expander includes it but
// does not expand it: the only outgoing read of c's adjacency is the final
// pass's, and it happens after both expansion reads succeeded. Before that read
// reported its failure these statements returned one row - the two nodes and the
// relationships read so far - and no error.
func TestGraphSubgraphAllFinalPassAdjacencyReadFailureFailsTheStatement(t *testing.T) {
	const query = "MATCH (n:Start) CALL apoc.path.subgraphAll(n, {relationshipFilter: 'X>', minLevel: 1, maxLevel: 2}) YIELD nodes, relationships RETURN size(nodes) AS nodes, size(relationships) AS relationships"
	failure := errors.New("live relationships unavailable")
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "subgraph_read_error")
		seed := NewStorageExecutor(store)
		_, err := seed.Execute(context.Background(), `CREATE (a:Start {name: 'a'})-[:X]->(b:Step {name: 'b'}), (b)-[:X]->(c:End {name: 'c'})`, nil)
		require.NoError(t, err)
		end, err := store.GetFirstNodeByLabel(context.Background(), "End")
		require.NoError(t, err)
		require.NotNil(t, end)

		// The same graph without the failure is the call's ordinary answer: the
		// expander reaches b and c, and the relationships between them are the
		// one it reads in the final pass.
		plain, err := seed.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2), int64(1)}}, plain.Rows)

		exec := NewStorageExecutor(&subgraphAllFinalPassReadFailureEngine{Engine: store, node: end.ID, failure: failure})
		result, err := exec.Execute(context.Background(), query, nil)
		require.ErrorIs(t, err, failure)
		require.Nil(t, result)
	})
}
