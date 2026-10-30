package cypher

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type liveAlgorithmReadFailureEngine struct {
	storage.Engine
	failure error
}

func (e *liveAlgorithmReadFailureEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return nil, e.failure
}
func TestLiveLinkPredictionPropagatesGraphReadFailure(t *testing.T) {
	store := newTestMemoryEngine(t)
	_, err := store.CreateNode(&storage.Node{ID: "test:a"})
	require.NoError(t, err)
	failure := errors.New("live relationships unavailable")
	exec := NewStorageExecutor(&liveAlgorithmReadFailureEngine{Engine: store, failure: failure})
	result, err := exec.Execute(context.Background(), `CALL gds.linkPrediction.commonNeighbors.stream({sourceNode:'test:a'}) YIELD node1,node2,score RETURN node1,node2,score`, nil)
	require.ErrorIs(t, err, failure)
	require.Nil(t, result)
}
