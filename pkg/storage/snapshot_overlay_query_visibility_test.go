package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Keep release's query-node visibility decision when the captured point
// reader replaces the independent write-behind lookup. Transaction-local
// bodies and completed scan caches must make the same hide/reveal decision.
func TestSnapshotOverlayQueryNodeVisibility(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, source := range []string{"engine-overlay", "engine-retired", "transaction-pending", "transaction-connected", "transaction-scan"} {
			t.Run(fmt.Sprintf("buffered=%t/%s", buffered, source), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{
					DataDir:             t.TempDir(),
					WriteBehind:         buffered,
					WriteBehindInterval: time.Hour,
				})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				if buffered {
					b.writeBehind.Close()
				}
				id := NodeID("test:visibility")
				_, err = b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"title": "before"}})
				require.NoError(t, err)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				read := b.GetQueryNode
				readStored := b.GetStoredNode
				switch source {
				case "engine-overlay", "engine-retired":
					require.NoError(t, tx.SetImplicit(true))
					require.NoError(t, tx.SetNamespace("test"))
					require.NoError(t, tx.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"title": "after"}}))
					require.NoError(t, tx.Commit())
					if buffered {
						require.Positive(t, b.writeBehind.PendingOps())
						if source == "engine-retired" {
							require.NoError(t, b.FlushWriteBehind())
							require.Zero(t, b.writeBehind.PendingOps())
						}
					}
				case "transaction-pending":
					require.NoError(t, tx.UpdateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"title": "after"}}))
					read = tx.GetQueryNode
					readStored = tx.GetStoredNode
				case "transaction-connected":
					require.NoError(t, tx.DeleteConnectedNode(id))
					read = tx.GetQueryNode
					readStored = tx.GetStoredNode
				case "transaction-scan":
					nodes, err := tx.AllNodes()
					require.NoError(t, err)
					require.Len(t, nodes, 1)
					read = tx.GetQueryNode
					readStored = tx.GetStoredNode
				}
				require.NoError(t, b.SetNodeManuallySuppressed(id, true))
				for _, reveal := range []bool{false, true, false} {
					b.SetRevealAll(reveal)
					stored, storedErr := readStored(id)
					require.NoError(t, storedErr, "integrity reads retain the physical body regardless of reveal")
					require.Equal(t, id, stored.ID)
					node, err := read(id)
					if reveal {
						require.NoError(t, err)
						require.Equal(t, id, node.ID)
					} else {
						require.ErrorIs(t, err, ErrNotFound)
						require.Nil(t, node)
					}
				}
			})
		}
	}
}
