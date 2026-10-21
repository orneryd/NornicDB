package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Every public writer must reserve its callback position before ordered cache
// publication can retire. A dispatch-time reservation permits a later commit
// to overtake it even when both cache deltas run in the right order.
func TestCommitNotifications_AllWriterReservations(t *testing.T) {
	cases := []struct {
		name  string
		write func(*BadgerEngine) error
	}{
		{"create-node", func(b *BadgerEngine) error { _, err := b.CreateNode(&Node{ID: "test:new"}); return err }},
		{"update-node", func(b *BadgerEngine) error { return b.UpdateNode(&Node{ID: "test:target"}) }},
		{"upsert-create-node", func(b *BadgerEngine) error { return b.UpdateNode(&Node{ID: "test:upsert"}) }},
		{"delete-node-cascade", func(b *BadgerEngine) error { return b.DeleteNode("test:target") }},
		{"create-edge", func(b *BadgerEngine) error {
			return b.CreateEdge(&Edge{ID: "test:new-edge", StartNode: "test:target", EndNode: "test:peer", Type: "R"})
		}},
		{"update-edge", func(b *BadgerEngine) error {
			return b.UpdateEdge(&Edge{ID: "test:edge", StartNode: "test:target", EndNode: "test:peer", Type: "S"})
		}},
		{"delete-edge", func(b *BadgerEngine) error { return b.DeleteEdge("test:edge") }},
		{"embedding-sidecar", func(b *BadgerEngine) error {
			n, err := b.GetNode("test:target")
			if err != nil {
				return err
			}
			n.ChunkEmbeddings = [][]float32{{1, 2}}
			return b.UpdateNodeEmbeddingSidecar(n)
		}},
		{"manual-suppression", func(b *BadgerEngine) error { return b.SetNodeManuallySuppressed("test:target", false) }},
		{"transaction", func(b *BadgerEngine) error {
			tx, err := b.BeginTransaction()
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if _, err := tx.CreateNode(&Node{ID: "test:transaction"}); err != nil {
				return err
			}
			return tx.Commit()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestEngine(t)
			_, err := b.CreateNode(&Node{ID: "test:target"})
			require.NoError(t, err)
			_, err = b.CreateNode(&Node{ID: "test:peer"})
			require.NoError(t, err)
			require.NoError(t, b.CreateEdge(&Edge{ID: "test:edge", StartNode: "test:target", EndNode: "test:peer", Type: "R"}))
			if tc.name == "manual-suppression" {
				require.NoError(t, b.SetNodeManuallySuppressed("test:target", true))
			}
			var events int
			b.OnNodeCreated(func(*Node) { events++ })
			b.OnNodeUpdated(func(*Node) { events++ })
			b.OnNodeDeleted(func(NodeID) { events++ })
			b.OnEdgeCreated(func(*Edge) { events++ })
			b.OnEdgeUpdated(func(*Edge) { events++ })
			b.OnEdgeDeleted(func(EdgeID) { events++ })
			publications, reserved := 0, true
			restore := setPublicationStageHook(func(uint64) {
				publications++
				b.notificationMu.Lock()
				reserved = reserved && b.notificationHead != nil
				b.notificationMu.Unlock()
			})
			defer restore()
			require.NoError(t, tc.write(b))
			b.notifyWG.Wait()
			require.Positive(t, publications, "the public writer must exercise ordered publication")
			require.True(t, reserved, "a notification slot must already exist before publication")
			require.Positive(t, events, "the successful writer must deliver its callback")
		})
	}
}
