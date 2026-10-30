package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestMVCC_WriteHelpers_InvalidIDBranches(t *testing.T) {
	engine := newTestEngine(t)
	v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 9}

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		err := engine.writeNodeMVCCVersionInTxn(txn, &Node{ID: "bad", Labels: []string{"L"}}, v)
		require.NoError(t, err)
		err = engine.writeNodeMVCCTombstoneInTxn(txn, "bad", v)
		require.NoError(t, err)
		err = engine.writeEdgeMVCCVersionInTxn(txn, &Edge{ID: "bad", StartNode: "x", EndNode: "y", Type: "R"}, v)
		require.NoError(t, err)
		err = engine.writeEdgeMVCCTombstoneInTxn(txn, "bad", v)
		require.NoError(t, err)
		err = engine.writeNodeMVCCHeadWithFloorInTxn(txn, "bad", v, false, v)
		require.NoError(t, err)
		err = engine.writeEdgeMVCCHeadWithFloorInTxn(txn, "bad", v, false, v)
		require.NoError(t, err)
		return nil
	}))
}

func TestMVCC_MaterializeCommit_NilOpsAndHeadDecodeErrors(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	version := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(time.Second), CommitSequence: 1010}

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"p": "v"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "R", Properties: map[string]any{"p": "v"}}))

	// Hit nil-op continue branches and create branches.
	err = engine.withUpdate(func(txn *badger.Txn) error {
		ops := []Operation{
			{Type: OpCreateNode, Node: nil},
			{Type: OpUpdateNode, Node: nil},
			{Type: OpCreateEdge, Edge: nil},
			{Type: OpUpdateEdge, Edge: nil},
			{Type: OpCreateNode, Node: &Node{ID: "test:n-new", Labels: []string{"L"}}, FreshID: false},
			{Type: OpCreateEdge, Edge: &Edge{ID: "test:e-new", StartNode: "test:n1", EndNode: "test:n2", Type: "R"}, FreshID: false},
		}
		return engine.materializeMVCCCommit(singleBatchWriter(txn), version, ops)
	})
	require.NoError(t, err)

	// Corrupt head records to force load*MVCCHead decode-error branches in materialize update/delete paths.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		nh := engine.mvccNodeHeadKeyStringLookup("test:n1")
		eh := engine.mvccEdgeHeadKeyStringLookup("test:e1")
		if nh == nil || eh == nil {
			return ErrNotFound
		}
		if err := txn.Set(nh, []byte("bad-head")); err != nil {
			return err
		}
		return txn.Set(eh, []byte("bad-head"))
	}))

	err = engine.withUpdate(func(txn *badger.Txn) error {
		ops := []Operation{
			{Type: OpUpdateNode, Node: &Node{ID: "test:n1", Labels: []string{"L"}}, OldNode: &Node{ID: "test:n1", Labels: []string{"L"}}},
		}
		return engine.materializeMVCCCommit(singleBatchWriter(txn), version, ops)
	})
	require.Error(t, err)

	err = engine.withUpdate(func(txn *badger.Txn) error {
		ops := []Operation{
			{Type: OpUpdateEdge, Edge: &Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "R"}, OldEdge: &Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "R"}},
		}
		return engine.materializeMVCCCommit(singleBatchWriter(txn), version, ops)
	})
	require.Error(t, err)
}

func TestMVCC_RebuildAndBootstrap_ContextAndSkipBranches(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:b1", Labels: []string{"L"}})
	require.NoError(t, err)

	// Existing head should trigger skip branch in applyNodeBootstrapBatch.
	require.NoError(t, engine.applyNodeBootstrapBatch([]*Node{
		{ID: "test:b1", Labels: []string{"L"}},
		nil,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = engine.bootstrapMVCCHeadsFromCurrentState(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}
