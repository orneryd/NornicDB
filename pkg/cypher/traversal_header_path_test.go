package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// edgePathCountingEngine counts whether a traversal reads edge headers (no
// bodies) or full edge records.
type edgePathCountingEngine struct {
	*storage.NamespacedEngine
	headerReads, fullReads int
}

func (e *edgePathCountingEngine) OutgoingEdgeHeaders(id storage.NodeID) ([]*storage.Edge, bool, error) {
	e.headerReads++
	return e.NamespacedEngine.OutgoingEdgeHeaders(id)
}

func (e *edgePathCountingEngine) IncomingEdgeHeaders(id storage.NodeID) ([]*storage.Edge, bool, error) {
	e.headerReads++
	return e.NamespacedEngine.IncomingEdgeHeaders(id)
}

func (e *edgePathCountingEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	e.fullReads++
	return e.NamespacedEngine.GetOutgoingEdges(id)
}

func (e *edgePathCountingEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	e.fullReads++
	return e.NamespacedEngine.GetIncomingEdges(id)
}

// A typed chained traversal lists adjacency headers and filters by type before
// any edge body read or endpoint load (#1110 step 3).
func TestChainedTypedTraversalUsesEdgeHeaders(t *testing.T) {
	ctx := context.Background()
	store := &edgePathCountingEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")}
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, `
		CREATE (r:Root {id: 1}),
		       (c1:Child {id: 1}),
		       (c2:Child {id: 2})
		CREATE (c1)-[:A]->(r), (c2)-[:A]->(r)
		CREATE (l1:Leaf {id: 1, text: 'x'}), (l2:Leaf {id: 2, text: 'y'}), (l3:Leaf {id: 3, text: 'z'})
		CREATE (l1)-[:B]->(c1), (l2)-[:B]->(c1), (l3)-[:B]->(c2)
		CREATE (l1)-[:OTHER]->(c1)
	`, nil)
	require.NoError(t, err)

	store.headerReads, store.fullReads = 0, 0
	result, err := exec.Execute(ctx, "MATCH (r:Root {id: 1})<-[:A]-(c:Child)<-[:B]-(l:Leaf) WHERE r.id >= 1 RETURN count(l) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
	require.Greater(t, store.headerReads, 0, "typed chained traversal must list edge headers")
	require.Zero(t, store.fullReads, "no full edge-body read for a typed anonymous-relationship traversal")
}
