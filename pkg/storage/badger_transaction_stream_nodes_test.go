package storage

// BadgerTransaction.StreamNodesWithOptions streams the transaction's view of
// every node, the snapshot overlaid with its pending writes, with projection,
// PropertyFilter and prefix, without materialising the population (#824).

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func streamTxIDs(t *testing.T, tx *BadgerTransaction, opts StreamNodesOptions) (ids []string, props map[string]map[string]interface{}) {
	t.Helper()
	props = map[string]map[string]interface{}{}
	require.NoError(t, tx.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
		ids = append(ids, string(node.ID))
		props[string(node.ID)] = node.Properties
		return nil
	}))
	sort.Strings(ids)
	return ids, props
}

func TestBadgerTransactionStreamNodesWithOptions(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, node := range []*Node{
		{ID: "a:1", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "x", "body": "one"}},
		{ID: "a:2", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "y", "body": "two"}},
		{ID: "a:3", Labels: []string{"Q"}, Properties: map[string]interface{}{"id": "x", "body": "three"}},
		{ID: "b:1", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "x"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	ids, props := streamTxIDs(t, tx, StreamNodesOptions{})
	require.Equal(t, []string{"a:1", "a:2", "a:3", "b:1"}, ids)
	require.Equal(t, "one", props["a:1"]["body"])

	// Pending writes: a new node, an update that changes the filtered
	// property, an update that keeps it, and a delete.
	_, err = tx.CreateNode(&Node{ID: "a:4", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "x", "body": "four"}})
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{ID: "a:2", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "x", "body": "two-updated"}}))
	require.NoError(t, tx.UpdateNode(&Node{ID: "a:1", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "z", "body": "one-updated"}}))
	require.NoError(t, tx.DeleteNode("a:3"))

	ids, props = streamTxIDs(t, tx, StreamNodesOptions{})
	require.Equal(t, []string{"a:1", "a:2", "a:4", "b:1"}, ids)
	require.Equal(t, "two-updated", props["a:2"]["body"])
	require.Equal(t, "z", props["a:1"]["id"])

	wantX := func(p map[string]interface{}) bool { return p["id"] == "x" }
	ids, props = streamTxIDs(t, tx, StreamNodesOptions{Prefix: "a:", Projection: []string{"id"}, PropertyFilter: wantX})
	// Committed a:1 (now z) is filtered; pending versions are emitted as they
	// are (the caller tests them): a:1 (z), a:2 (x, pending update of a node
	// whose committed id was y), a:4 (new). a:3 is deleted, b:1 is outside the prefix.
	require.Equal(t, []string{"a:1", "a:2", "a:4"}, ids)
	require.Equal(t, map[string]interface{}{"id": "x"}, props["a:2"], "projected")
	require.NotContains(t, props["a:4"], "body", "pending nodes are projected too")

	// An early stop ends the stream, pending overlay included.
	visited := 0
	require.NoError(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, func(*Node) error {
		visited++
		return ErrIterationStopped
	}))
	require.Equal(t, 1, visited)

	failure := errors.New("visit failed")
	require.ErrorIs(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, func(*Node) error { return failure }), failure)
	require.ErrorIs(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, nil), ErrInvalidData)

	// A node created after the transaction began is not in its snapshot.
	_, err = engine.CreateNode(&Node{ID: "a:9", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "x"}})
	require.NoError(t, err)
	ids, _ = streamTxIDs(t, tx, StreamNodesOptions{Prefix: "a:"})
	require.NotContains(t, ids, "a:9")
}

func TestBadgerTransactionStreamNodesAfterCommitFails(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Error(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, func(*Node) error { return nil }))
}

// Transactions built without a pinned physical snapshot (the legacy shape)
// stream the engine's current view when they have no read version, and the
// MVCC view at their read version otherwise.
func TestBadgerTransactionStreamNodesWithoutPinnedSnapshot(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, node := range []*Node{
		{ID: "a:1", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "x", "body": "one"}},
		{ID: "a:2", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "y", "body": "two"}},
		{ID: "b:1", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "x"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	legacy := func(readTS MVCCVersion) *BadgerTransaction {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		tx.snapshotTx.Discard()
		tx.snapshotTx = nil
		tx.readTS = readTS
		return tx
	}
	atBegin := legacy(engine.currentMVCCReadVersion("a"))
	current := legacy(MVCCVersion{})
	_, err = engine.CreateNode(&Node{ID: "a:3", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "x"}})
	require.NoError(t, err)

	ids, _ := streamTxIDs(t, current, StreamNodesOptions{Prefix: "a:"})
	require.Equal(t, []string{"a:1", "a:2", "a:3"}, ids, "no read version: the engine's current view")
	ids, props := streamTxIDs(t, atBegin, StreamNodesOptions{Prefix: "a:", Projection: []string{"id"}, PropertyFilter: func(p map[string]interface{}) bool { return p["id"] == "x" }})
	require.Equal(t, []string{"a:1"}, ids, "the MVCC view at the read version, filtered")
	require.Equal(t, map[string]interface{}{"id": "x"}, props["a:1"], "projected")
}

// Errors and stops from the visitor end the pending-only part of the stream.
func TestBadgerTransactionStreamNodesPendingVisitErrors(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for _, id := range []NodeID{"a:1", "a:2"} {
		_, err = tx.CreateNode(&Node{ID: id, Labels: []string{"P"}})
		require.NoError(t, err)
	}
	failure := errors.New("visit failed")
	require.ErrorIs(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, func(*Node) error { return failure }), failure)
	visited := 0
	require.NoError(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, func(*Node) error {
		visited++
		return ErrIterationStopped
	}))
	require.Equal(t, 1, visited)

	// The engine's prefix scan returns a visitor's error as well.
	_, err = engine.CreateNode(&Node{ID: "a:9", Labels: []string{"P"}})
	require.NoError(t, err)
	require.ErrorIs(t, engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{Prefix: "a:"}, func(*Node) error { return failure }), failure)
	require.NoError(t, engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{Prefix: "a:"}, func(*Node) error { return ErrIterationStopped }))
	// So does a legacy transaction's committed scan, and its read error.
	tx.snapshotTx.Discard()
	tx.snapshotTx = nil
	tx.readTS = MVCCVersion{}
	var first []NodeID
	require.ErrorIs(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, func(node *Node) error {
		first = append(first, node.ID)
		return failure
	}), failure)
	require.Equal(t, []NodeID{"a:9"}, first, "the committed node is visited first")
	require.NoError(t, engine.Close())
	require.Error(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{}, func(*Node) error { return nil }))
}
