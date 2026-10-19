package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sol1406R2Acknowledge(t *testing.T, b *BadgerEngine, write func(*BadgerTransaction) error) {
	t.Helper()
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, write(tx))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the prerequisite write must be acknowledged through write-behind")
}

func TestSol1406R2BufferedEdgeThenDirectNodeDelete(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	sol1406R2Acknowledge(t, b, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(&Edge{ID: "test:buffered-edge", StartNode: "test:a", EndNode: "test:b", Type: "R"})
	})

	require.NoError(t, b.DeleteNode("test:a"))
	require.NoError(t, b.FlushWriteBehind())

	_, err := b.GetNode("test:a")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = b.GetEdge("test:buffered-edge")
	require.ErrorIs(t, err, ErrNotFound, "the later endpoint deletion must cascade to the earlier acknowledged edge")
}

func TestSol1406R2UpdateEdgeJoinsBufferedEndpoint(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}
	require.NoError(t, b.CreateEdge(edge))
	sol1406R2Acknowledge(t, b, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "test:c", Labels: []string{"N"}})
		return err
	})

	updated := &Edge{ID: edge.ID, StartNode: edge.StartNode, EndNode: "test:c", Type: edge.Type}
	require.NoError(t, b.UpdateEdge(updated), "a later synchronous edge update must see an earlier acknowledged endpoint")
	require.NoError(t, b.FlushWriteBehind())
	got, err := b.GetEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, NodeID("test:c"), got.EndNode)
}

func TestSol1406R2NestedFlushPreservesSubscriberOrder(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	id := NodeID("test:listener-order")
	nested := make(chan error, 1)
	b.OnNodeCreated(func(node *Node) {
		if node.ID != id {
			return
		}
		tx, err := b.BeginTransaction()
		if err == nil {
			err = tx.SetImplicit(true)
		}
		if err == nil {
			err = tx.SetNamespace("test")
		}
		if err == nil {
			err = tx.DeleteNode(id)
		}
		if err == nil {
			err = tx.Commit()
		}
		if err == nil {
			err = b.FlushWriteBehind()
		}
		nested <- err
	})

	var mu sync.Mutex
	var events []string
	b.OnNodeCreated(func(node *Node) {
		if node.ID == id {
			mu.Lock()
			events = append(events, "create")
			mu.Unlock()
		}
	})
	b.OnNodeDeleted(func(nodeID NodeID) {
		if nodeID == id {
			mu.Lock()
			events = append(events, "delete")
			mu.Unlock()
		}
	})

	sol1406R2Acknowledge(t, b, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: id})
		return err
	})
	require.NoError(t, b.FlushWriteBehind())
	select {
	case err := <-nested:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("nested listener Flush did not return")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"create", "delete"}, events, "every subscriber must observe replay commits in acknowledgement order")
}
