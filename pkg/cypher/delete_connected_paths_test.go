package cypher

import (
	"context"
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// FOREACH's non-DETACH DELETE of a connected node fails at once on a store
// without transactions; in a transaction the node is deleted for now and
// checked at COMMIT, like the MATCH route (#907).
func TestForeachDeleteOfConnectedNode(t *testing.T) {
	exec, engine := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:F {k: 1})-[:LINK]->(:T)", nil)
	require.NoError(t, err)
	run := func(query string) error {
		_, err := exec.Execute(ctx, query, nil)
		return err
	}
	const deleteBound = "MATCH (f:F) WITH collect(f) AS items FOREACH (x IN items | DELETE x)"

	code, _ := nornicerrors.Neo4jStatus(run(deleteBound))
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", code)

	// Its relationship deleted before COMMIT, the delete stands.
	for _, query := range []string{"BEGIN", deleteBound, "MATCH (:T)<-[r:LINK]-() DELETE r", "COMMIT"} {
		require.NoError(t, run(query), query)
	}
	left, err := engine.GetNodesByLabel("F")
	require.NoError(t, err)
	require.Empty(t, left)
}

var errAdjacencyRead = errors.New("adjacency read failed")

// adjacencyFailingEngine is a store whose relationship lookups fail (all, or
// only the incoming ones) and that defers connected-node deletes.
type adjacencyFailingEngine struct {
	storage.Engine
	incomingOnly bool
}

func (f adjacencyFailingEngine) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	if f.incomingOnly {
		return nil, nil
	}
	return nil, errAdjacencyRead
}

func (f adjacencyFailingEngine) GetIncomingEdges(storage.NodeID) ([]*storage.Edge, error) {
	return nil, errAdjacencyRead
}

func (f adjacencyFailingEngine) DeleteConnectedNode(storage.NodeID) error { return nil }

// A failed relationship lookup fails the connected-node check and the DELETE.
func TestConnectedDeleteTargetsLookupErrors(t *testing.T) {
	base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for _, incomingOnly := range []bool{false, true} {
		store := adjacencyFailingEngine{Engine: base, incomingOnly: incomingOnly}
		_, err := connectedDeleteTargets(store, []storage.NodeID{"n"}, nil)
		require.ErrorIs(t, err, errAdjacencyRead)
	}

	exec := NewStorageExecutor(adjacencyFailingEngine{Engine: base})
	node := &storage.Node{ID: "n", Labels: []string{"D"}}
	_, _, err := exec.pipelineApplyDelete(context.Background(), []pipelineRow{{"n": node}}, map[string]struct{}{"n": {}}, "DELETE n")
	require.ErrorIs(t, err, errAdjacencyRead)
}
