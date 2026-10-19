package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A dependency may have been absent from the physical snapshot, and its
// generation may already have replayed by the time the writer commits.
func TestWriteBehindTransactionDependencyBarrier(t *testing.T) {
	for _, flushBeforeCommit := range []bool{false, true} {
		for _, beginBeforeAck := range []bool{false, true} {
			t.Run(fmt.Sprintf("begin-before-ack=%v/flush-before-commit=%v", beginBeforeAck, flushBeforeCommit), func(t *testing.T) {
				b := newWriteBehindTestEngine(t, time.Hour)
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id, Labels: []string{"N"}})
					require.NoError(t, err)
				}
				ack := func() {
					commitBuffered(t, b, func(tx *BadgerTransaction) error {
						return tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"})
					})
				}
				if !beginBeforeAck {
					ack()
				}
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.DeleteNode("test:a"))
				if beginBeforeAck {
					ack()
				}
				if flushBeforeCommit {
					require.NoError(t, b.FlushWriteBehind())
				}
				commitErr := tx.Commit()
				require.NoError(t, b.FlushWriteBehind())
				if commitErr != nil {
					require.ErrorIs(t, commitErr, ErrConflict)
					_, err = b.GetNode("test:a")
					require.NoError(t, err)
					_, err = b.GetEdge("test:r")
					require.NoError(t, err)
				} else {
					_, err = b.GetNode("test:a")
					require.ErrorIs(t, err, ErrNotFound)
					_, err = b.GetEdge("test:r")
					require.ErrorIs(t, err, ErrNotFound, "successful endpoint deletion must include every earlier acknowledged relationship")
				}
			})
		}
	}
}

// Bypass the pre-drain to model an ACK that won the gate after a writer
// began. Admission must refuse unrelated writes too, for every generation.
func TestWriteBehindAdmissionIsBufferWide(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		t.Run(fmt.Sprintf("rotated=%v", rotate), func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			seedOrderNode(t, b)
			var held *CommitBuffer
			if rotate {
				// Keep the real flusher from retiring this generation between
				// rotation and admission; the schedule must remain deterministic.
				b.writeBehind.mu.RLock()
				held = b.writeBehind.active
				claimed := held.claimDrain()
				b.writeBehind.mu.RUnlock()
				require.True(t, claimed)
				defer func() {
					if held != nil {
						held.releaseDrain()
					}
				}()
			}
			commitBuffered(t, b, func(tx *BadgerTransaction) error {
				_, err := tx.CreateNode(&Node{ID: "test:unrelated"})
				return err
			})
			if rotate {
				b.writeBehind.rotate()
			}
			err := b.setNodeManuallySuppressed("test:x", true)
			require.ErrorIs(t, err, errBufferedWritePending)
			require.False(t, storedKey(t, b, manualSuppressionKey(manualSuppressionNode, "test:x")))
			if held != nil {
				held.releaseDrain()
				held = nil
			}
			require.NoError(t, b.SetNodeManuallySuppressed("test:x", true))
			require.True(t, storedKey(t, b, manualSuppressionKey(manualSuppressionNode, "test:x")))
		})
	}
}

func TestWriteBehindAdmissionCatchesAcknowledgementDuringWrite(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	seedOrderNode(t, b)
	attempts := 0
	err := b.orderBehindBufferedWrites(func() error {
		attempts++
		return b.withPublishingUpdate(func(txn *badger.Txn) error {
			if attempts == 1 {
				commitBuffered(t, b, func(tx *BadgerTransaction) error {
					_, err := tx.CreateNode(&Node{ID: "test:unrelated"})
					return err
				})
			}
			return txn.Set(manualSuppressionKey(manualSuppressionNode, "test:x"), []byte{1})
		}, nil, func() []any { return []any{NodeID("test:x")} })
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Zero(t, b.writeBehind.PendingOps())
	require.True(t, storedKey(t, b, manualSuppressionKey(manualSuppressionNode, "test:x")))
}

// Every direct data writer uses the same buffer-wide fence, independent of
// which entity the earlier acknowledgement wrote.
func TestWriteBehindAllSynchronousWritersDrainAcknowledgedWork(t *testing.T) {
	for _, c := range []struct {
		name  string
		write func(*BadgerEngine) error
	}{
		{"create node", func(b *BadgerEngine) error { _, err := b.CreateNode(orderNode("new")); return err }},
		{"update node", func(b *BadgerEngine) error { return b.UpdateNode(orderNode("new")) }},
		{"delete node", func(b *BadgerEngine) error { return b.DeleteNode("test:x") }},
		{"create edge", func(b *BadgerEngine) error {
			return b.CreateEdge(&Edge{ID: "test:new-edge", StartNode: "test:a", EndNode: "test:b", Type: "R"})
		}},
		{"update edge", func(b *BadgerEngine) error { return b.UpdateEdge(orderEdge("new")) }},
		{"delete edge", func(b *BadgerEngine) error { return b.DeleteEdge("test:r") }},
		{"suppress node", func(b *BadgerEngine) error { return b.SetNodeManuallySuppressed("test:x", true) }},
		{"unsuppress node", func(b *BadgerEngine) error { return b.SetNodeManuallySuppressed("test:x", false) }},
		{"suppress edge", func(b *BadgerEngine) error { return b.SetEdgeManuallySuppressed("test:r", true) }},
		{"unsuppress edge", func(b *BadgerEngine) error { return b.SetEdgeManuallySuppressed("test:r", false) }},
		{"decay node", func(b *BadgerEngine) error {
			b.SetDecayEnabled(true)
			_, err := b.EnqueueDeindexIfSuppressed("test:x", false)
			return err
		}},
		{"decay edge", func(b *BadgerEngine) error {
			b.SetDecayEnabled(true)
			_, err := b.EnqueueDeindexIfSuppressed("test:r", true)
			return err
		}},
		{"embedding sidecar", func(b *BadgerEngine) error {
			node, err := b.GetNode("test:x")
			if err != nil {
				return err
			}
			node.ChunkEmbeddings = [][]float32{{1, 2, 3}}
			return b.UpdateNodeEmbeddingSidecar(node)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			seedOrderEdge(t, b)
			if c.name != "create node" {
				seedOrderNode(t, b)
			}
			commitBuffered(t, b, func(tx *BadgerTransaction) error {
				_, err := tx.CreateNode(&Node{ID: "test:unrelated"})
				return err
			})
			require.NoError(t, c.write(b))
			require.Zero(t, b.writeBehind.PendingOps(), "the barrier covers the complete dependency population")
			require.NotNil(t, storedNode(t, b, "test:unrelated"))
		})
	}
}

func TestWriteBehindBulkDependencyOrdering(t *testing.T) {
	for _, capture := range []bool{false, true} {
		t.Run(fmt.Sprintf("capture=%v", capture), func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{"N"}})
				require.NoError(t, err)
			}
			commitBuffered(t, b, func(tx *BadgerTransaction) error {
				return tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"})
			})
			if capture {
				b.SetChangeCapture("test", ChangeCaptureFull)
			}
			err := b.BulkDeleteNodes([]NodeID{"test:a"})
			if err != nil {
				// A synchronous transaction's physical snapshot can predate
				// the drain. Its permitted result is an atomic conflict, then
				// success when the caller starts a fresh transaction.
				require.ErrorIs(t, err, ErrConflict)
				_, readErr := b.GetNode("test:a")
				require.NoError(t, readErr)
				_, readErr = b.GetEdge("test:r")
				require.NoError(t, readErr)
				require.NoError(t, b.BulkDeleteNodes([]NodeID{"test:a"}))
			}
			require.NoError(t, b.FlushWriteBehind())
			_, err = b.GetNode("test:a")
			require.ErrorIs(t, err, ErrNotFound)
			_, err = b.GetEdge("test:r")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestWriteBehindRelationshipCountsFollowBufferedEndpointLabels(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	seedOrderEdge(t, b)
	commitBuffered(t, b, func(tx *BadgerTransaction) error {
		return tx.UpdateNode(&Node{ID: "test:a", Labels: []string{"New"}})
	})
	require.NoError(t, b.DeleteEdge("test:r"))
	require.NoError(t, b.FlushWriteBehind())
	for _, label := range []string{"N", "New"} {
		count, err := b.EdgeCountByStartLabelInNamespace("test", label, "LINK")
		require.NoError(t, err)
		require.Zero(t, count, "deleted relationship cannot remain in counts for %s", label)
	}
}
