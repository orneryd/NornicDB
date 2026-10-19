package storage

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// A write-behind acknowledgement comes first in the logical order, but its
// replay commits later, at a newer timestamp. A synchronous write of an
// entity the buffer still holds is therefore ordered after the replay
// (orderBehindBufferedWrites, commitWriter.admit): the replay never lands the
// older acknowledged write over the newer one.

// commitBuffered acknowledges write through the write-behind buffer.
func commitBuffered(t *testing.T, b *BadgerEngine, write func(tx *BadgerTransaction) error) {
	t.Helper()
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, write(tx))
	require.NoError(t, tx.Commit())
	require.NotZero(t, b.writeBehind.PendingOps(), "the commit is acknowledged through the buffer")
}

func storedKey(t *testing.T, b *BadgerEngine, key []byte) bool {
	t.Helper()
	found := false
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		found = err == nil
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}))
	return found
}

// storedNode is id's committed body in Badger, nil when there is none.
func storedNode(t *testing.T, b *BadgerEngine, id NodeID) *Node {
	t.Helper()
	var node *Node
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			node, err = b.decodeNodeWithEmbeddings(txn, val, id)
			return err
		})
	}))
	return node
}

// storedEdge is id's committed body in Badger, nil when there is none.
func storedEdge(t *testing.T, b *BadgerEngine, id EdgeID) *Edge {
	t.Helper()
	var edge *Edge
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(edgeKey(id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			edge, err = b.decodeEdgeBodyByID(val, id)
			return err
		})
	}))
	return edge
}

func orderNode(value string) *Node {
	return &Node{ID: "test:x", Labels: []string{"L"}, Properties: map[string]any{"v": value}}
}

func orderEdge(value string) *Edge {
	return &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": value}}
}

func seedOrderNode(t *testing.T, b *BadgerEngine) {
	t.Helper()
	_, err := b.CreateNode(orderNode("initial"))
	require.NoError(t, err)
}

func seedOrderEdge(t *testing.T, b *BadgerEngine) {
	t.Helper()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(orderEdge("initial")))
}

// Every synchronous writer, after an acknowledged write of the same entity
// and before its replay: what the synchronous writer returned is what holds
// once the buffer is flushed.
func TestWriteBehindSynchronousWritersFollowBufferedWrites(t *testing.T) {
	for _, c := range []struct {
		name     string
		seed     func(*testing.T, *BadgerEngine)
		buffered []func(tx *BadgerTransaction) error
		write    func(*BadgerEngine) error
		check    func(*testing.T, *BadgerEngine)
	}{
		{
			name:     "update node",
			seed:     seedOrderNode,
			buffered: []func(*BadgerTransaction) error{func(tx *BadgerTransaction) error { return tx.UpdateNode(orderNode("buffered")) }},
			write:    func(b *BadgerEngine) error { return b.UpdateNode(orderNode("newest")) },
			check: func(t *testing.T, b *BadgerEngine) {
				require.Equal(t, "newest", storedNode(t, b, "test:x").Properties["v"])
				got, err := b.GetNode("test:x")
				require.NoError(t, err)
				require.Equal(t, "newest", got.Properties["v"])
			},
		},
		{
			name:     "delete node",
			seed:     seedOrderNode,
			buffered: []func(*BadgerTransaction) error{func(tx *BadgerTransaction) error { return tx.UpdateNode(orderNode("buffered")) }},
			write:    func(b *BadgerEngine) error { return b.DeleteNode("test:x") },
			check: func(t *testing.T, b *BadgerEngine) {
				require.Nil(t, storedNode(t, b, "test:x"))
				_, err := b.GetNode("test:x")
				require.ErrorIs(t, err, ErrNotFound)
			},
		},
		{
			name:     "create node",
			seed:     seedOrderNode,
			buffered: []func(*BadgerTransaction) error{func(tx *BadgerTransaction) error { return tx.DeleteNode("test:x") }},
			write:    func(b *BadgerEngine) error { _, err := b.CreateNode(orderNode("newest")); return err },
			check: func(t *testing.T, b *BadgerEngine) {
				require.Equal(t, "newest", storedNode(t, b, "test:x").Properties["v"])
			},
		},
		{
			name: "suppress node",
			seed: seedOrderNode,
			buffered: []func(*BadgerTransaction) error{
				func(tx *BadgerTransaction) error { return tx.DeleteNode("test:x") },
				func(tx *BadgerTransaction) error { _, err := tx.CreateNode(orderNode("recreated")); return err },
			},
			write: func(b *BadgerEngine) error { return b.SetNodeManuallySuppressed("test:x", true) },
			check: func(t *testing.T, b *BadgerEngine) {
				require.True(t, storedKey(t, b, manualSuppressionKey(manualSuppressionNode, "test:x")), "the later suppression survives the delete's replay")
				require.True(t, b.manuallySuppressed(manualSuppressionNode, "test:x"))
			},
		},
		{
			name:     "update edge",
			seed:     seedOrderEdge,
			buffered: []func(*BadgerTransaction) error{func(tx *BadgerTransaction) error { return tx.UpdateEdge(orderEdge("buffered")) }},
			write:    func(b *BadgerEngine) error { return b.UpdateEdge(orderEdge("newest")) },
			check: func(t *testing.T, b *BadgerEngine) {
				require.Equal(t, "newest", storedEdge(t, b, "test:r").Properties["v"])
			},
		},
		{
			name:     "delete edge",
			seed:     seedOrderEdge,
			buffered: []func(*BadgerTransaction) error{func(tx *BadgerTransaction) error { return tx.UpdateEdge(orderEdge("buffered")) }},
			write:    func(b *BadgerEngine) error { return b.DeleteEdge("test:r") },
			check: func(t *testing.T, b *BadgerEngine) {
				require.Nil(t, storedEdge(t, b, "test:r"))
			},
		},
		{
			name:     "create edge",
			seed:     seedOrderEdge,
			buffered: []func(*BadgerTransaction) error{func(tx *BadgerTransaction) error { return tx.DeleteEdge("test:r") }},
			write:    func(b *BadgerEngine) error { return b.CreateEdge(orderEdge("newest")) },
			check: func(t *testing.T, b *BadgerEngine) {
				require.Equal(t, "newest", storedEdge(t, b, "test:r").Properties["v"])
			},
		},
		{
			name: "create edge between buffered nodes",
			buffered: []func(*BadgerTransaction) error{func(tx *BadgerTransaction) error {
				for _, id := range []NodeID{"test:a", "test:b"} {
					if _, err := tx.CreateNode(&Node{ID: id, Labels: []string{"N"}}); err != nil {
						return err
					}
				}
				return nil
			}},
			write: func(b *BadgerEngine) error { return b.CreateEdge(orderEdge("newest")) },
			check: func(t *testing.T, b *BadgerEngine) {
				require.Equal(t, "newest", storedEdge(t, b, "test:r").Properties["v"])
			},
		},
		{
			name: "suppress edge",
			seed: seedOrderEdge,
			buffered: []func(*BadgerTransaction) error{
				func(tx *BadgerTransaction) error { return tx.DeleteEdge("test:r") },
				func(tx *BadgerTransaction) error { return tx.CreateEdge(orderEdge("recreated")) },
			},
			write: func(b *BadgerEngine) error { return b.SetEdgeManuallySuppressed("test:r", true) },
			check: func(t *testing.T, b *BadgerEngine) {
				require.True(t, storedKey(t, b, manualSuppressionKey(manualSuppressionEdge, "test:r")), "the later suppression survives the delete's replay")
				require.True(t, b.manuallySuppressed(manualSuppressionEdge, "test:r"))
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			if c.seed != nil {
				c.seed(t, b)
			}
			for _, write := range c.buffered {
				commitBuffered(t, b, write)
			}
			require.NoError(t, c.write(b))
			require.NoError(t, b.FlushWriteBehind())
			c.check(t, b)
		})
	}
}

// A transaction that commits to Badger and writes an entity the buffer holds,
// begun after the acknowledgement or before it: its commit either holds once
// the buffer is flushed, or fails with ErrConflict and leaves the
// acknowledged write. It never succeeds only to be overwritten by the older
// acknowledged write. (A transaction begun before the acknowledgement is a
// concurrent writer of the entity; one begun after it read it through the
// overlay, but Badger tracks its reads from its begin timestamp, which
// precedes the replay. Explicit transactions in Cypher drain the buffer before
// they begin, so their snapshot already holds every acknowledged write.)
func TestWriteBehindTransactionAfterBufferedWriteIsNeverOverwritten(t *testing.T) {
	for _, beganBefore := range []bool{false, true} {
		t.Run(fmt.Sprintf("began-before-ack=%v", beganBefore), func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			seedOrderNode(t, b)
			ack := func() {
				commitBuffered(t, b, func(tx *BadgerTransaction) error { return tx.UpdateNode(orderNode("buffered")) })
			}
			if !beganBefore {
				ack()
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			if beganBefore {
				_, err := tx.GetNode("test:x")
				require.NoError(t, err)
				ack()
			}
			require.NoError(t, tx.UpdateNode(orderNode("newest")))
			commitErr := tx.Commit()
			require.NoError(t, b.FlushWriteBehind())
			stored := storedNode(t, b, "test:x").Properties["v"]
			if commitErr == nil {
				require.Equal(t, "newest", stored, "a successful commit holds after the replay")
				return
			}
			require.ErrorIs(t, commitErr, ErrConflict)
			require.Equal(t, "buffered", stored)
		})
	}
}

// The commit gate is where the order is enforced: a synchronous commit of an
// entity the buffer holds is refused, writing nothing, and the engine's
// writer drains and runs it again.
func TestWriteBehindRefusesSynchronousCommitOfHeldEntity(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	seedOrderNode(t, b)
	commitBuffered(t, b, func(tx *BadgerTransaction) error { return tx.UpdateNode(orderNode("buffered")) })

	err := b.setNodeManuallySuppressed("test:x", true)
	require.True(t, errors.Is(err, errBufferedWritePending), "got %v", err)
	require.False(t, storedKey(t, b, manualSuppressionKey(manualSuppressionNode, "test:x")), "a refused commit writes nothing")
	require.False(t, b.manuallySuppressed(manualSuppressionNode, "test:x"))

	require.NoError(t, b.SetNodeManuallySuppressed("test:x", true))
	require.True(t, storedKey(t, b, manualSuppressionKey(manualSuppressionNode, "test:x")))
	require.Zero(t, b.writeBehind.PendingOps(), "the writer drained the buffer first")
}

// Decay's suppression rewrite, a synchronous writer too, evaluates and
// rewrites the acknowledged body, not the one it replaces.
func TestWriteBehindDecayRewriteFollowsBufferedWrite(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	old := time.Now().Add(-72 * time.Hour)
	initial := orderNode("initial")
	initial.CreatedAt, initial.UpdatedAt = old, old
	_, err := b.CreateNode(initial)
	require.NoError(t, err)
	sm := b.GetSchemaForNamespace("test")
	require.NoError(t, sm.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "l_decay", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 1, VisibilityThreshold: 0.10, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, sm.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "l_binding", ProfileRef: "l_decay", TargetLabels: []string{"L"}}))
	b.SetDecayEnabled(true)
	commitBuffered(t, b, func(tx *BadgerTransaction) error {
		node := orderNode("buffered")
		node.CreatedAt = old
		return tx.UpdateNode(node)
	})

	_, err = b.EnqueueDeindexIfSuppressed("test:x", false)
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind())
	stored := storedNode(t, b, "test:x")
	require.Equal(t, "buffered", stored.Properties["v"])
	require.True(t, stored.VisibilitySuppressed, "the suppression rewrite survives the replay")
}

// A replay listener's synchronous write of the replayed node reads back as
// written: the replay retired its overlay before the listeners run.
func TestWriteBehindReplayListenerReadsItsOwnSynchronousWrite(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	observed := make(chan error, 1)
	b.OnNodeCreated(func(node *Node) {
		if node.ID != "test:own" {
			return
		}
		next := CopyNode(node)
		next.Properties = map[string]any{"v": "new"}
		if err := b.UpdateNode(next); err != nil {
			observed <- err
			return
		}
		got, err := b.GetNode(node.ID)
		if err == nil && got.Properties["v"] != "new" {
			err = fmt.Errorf("the listener read %v after its update returned", got.Properties["v"])
		}
		observed <- err
	})
	bufferedCreate(t, b, &Node{ID: "test:own", Labels: []string{"L"}, Properties: map[string]any{"v": "old"}})
	require.NoError(t, b.FlushWriteBehind())
	select {
	case err := <-observed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the listener did not finish")
	}
}

// A replay listener that acknowledges a write and flushes finds it in Badger
// when the flush returns: the flush is a barrier from a listener too.
func TestWriteBehindFlushFromReplayListenerIsDurable(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	observed := make(chan error, 1)
	b.OnNodeCreated(func(node *Node) {
		if node.ID != "test:first" {
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
			_, err = tx.CreateNode(&Node{ID: "test:second", Labels: []string{"L"}})
		}
		if err == nil {
			err = tx.Commit()
		}
		if err == nil {
			err = b.FlushWriteBehind()
		}
		if err == nil && !storedKey(t, b, nodeKey("test:second")) {
			err = fmt.Errorf("the listener's flush returned with its acknowledged node absent from Badger")
		}
		observed <- err
	})
	bufferedCreate(t, b, &Node{ID: "test:first", Labels: []string{"L"}})
	require.NoError(t, b.FlushWriteBehind())
	select {
	case err := <-observed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the listener did not finish")
	}
}
