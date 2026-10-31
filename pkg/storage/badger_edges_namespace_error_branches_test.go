package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_EdgeOperations_UnprefixedIDNamespaceErrorBranches(t *testing.T) {
	engine := newTestEngine(t)
	seedRawRecord(t, engine, &Node{ID: "rawA", Labels: []string{"N"}})
	seedRawRecord(t, engine, &Node{ID: "rawB", Labels: []string{"N"}})

	t.Run("CreateEdge", func(t *testing.T) {
		err := engine.CreateEdge(&Edge{ID: "raw-edge", StartNode: "rawA", EndNode: "rawB", Type: "REL"})
		require.Error(t, err)
	})

	seedRawRecord(t, engine, &Edge{ID: "raw-edge", StartNode: "rawA", EndNode: "rawB", Type: "REL"})

	t.Run("UpdateEdge", func(t *testing.T) {
		err := engine.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "raw-edge", StartNode: "rawA", EndNode: "rawB", Type: "REL2"}})
		require.Error(t, err)
	})

	t.Run("DeleteEdge", func(t *testing.T) {
		err := engine.DeleteEdge("raw-edge")
		require.Error(t, err)
	})
}

func TestBadgerEngine_BulkDelete_MixedNamespaceErrorBranches(t *testing.T) {
	engine := newTestEngine(t)

	err := engine.BulkDeleteNodes([]NodeID{"a:n1", "b:n2"})
	require.Error(t, err)

	err = engine.BulkDeleteEdges([]EdgeID{"a:e1", "b:e2"})
	require.Error(t, err)
}
