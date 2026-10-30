package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func seedRawNodeForEdgeNamespaceTests(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		data, _, err := engine.encodeNodeInTxn(txn, "", &Node{ID: id, Labels: []string{"N"}})
		if err != nil {
			return err
		}
		return txn.Set(nodeKey(id), data)
	}))
}

func seedRawEdgeForNamespaceTests(t *testing.T, engine *BadgerEngine, edge *Edge) {
	t.Helper()
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		data, err := engine.encodeEdgeInTxn(txn, "", edge)
		if err != nil {
			return err
		}
		return txn.Set(edgeKey(edge.ID), data)
	}))
}

func TestBadgerEngine_EdgeOperations_UnprefixedIDNamespaceErrorBranches(t *testing.T) {
	engine := newTestEngine(t)
	seedRawNodeForEdgeNamespaceTests(t, engine, "rawA")
	seedRawNodeForEdgeNamespaceTests(t, engine, "rawB")

	t.Run("CreateEdge", func(t *testing.T) {
		err := engine.CreateEdge(&Edge{ID: "raw-edge", StartNode: "rawA", EndNode: "rawB", Type: "REL"})
		require.Error(t, err)
	})

	seedRawEdgeForNamespaceTests(t, engine, &Edge{ID: "raw-edge", StartNode: "rawA", EndNode: "rawB", Type: "REL"})

	t.Run("UpdateEdge", func(t *testing.T) {
		err := engine.UpdateEdge(&Edge{ID: "raw-edge", StartNode: "rawA", EndNode: "rawB", Type: "REL2"})
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
