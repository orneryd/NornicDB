package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Reopening must preserve deleted-head sequences and the original snapshot
// boundary, even before a namespace's in-memory state has hydrated.
func TestDispatcherColdNamespaceTombstonesAndConflict(t *testing.T) {
	for _, edge := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "edge"}[edge], func(t *testing.T) {
			options := BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour}
			b, err := NewBadgerEngineWithOptions(options)
			require.NoError(t, err)
			for _, id := range []NodeID{"test:guard", "test:end"} {
				_, err = b.CreateNode(&Node{ID: id, Properties: map[string]any{"cnt": int64(0)}})
				require.NoError(t, err)
			}
			relationship := &Edge{ID: "test:gone", StartNode: "test:guard", EndNode: "test:end", Type: "R"}
			if edge {
				require.NoError(t, b.CreateEdge(relationship))
				require.NoError(t, b.DeleteEdge(relationship.ID))
			} else {
				_, err = b.CreateNode(&Node{ID: "test:gone"})
				require.NoError(t, err)
				require.NoError(t, b.DeleteNode("test:gone"))
			}
			require.NoError(t, b.Close())
			b, err = NewBadgerEngineWithOptions(options)
			require.NoError(t, err)
			defer b.Close()
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			node, err := tx.GetNode("test:guard")
			require.NoError(t, err)
			require.NoError(t, b.UpdateNode(&Node{ID: node.ID, Properties: map[string]any{"cnt": int64(1)}}))
			node.Properties["cnt"] = int64(1)
			require.NoError(t, tx.UpdateNode(node))
			require.ErrorIs(t, tx.Commit(), ErrConflict, "cold hydration must not adopt the later peer's sequence")
			tx, err = b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			if edge {
				require.NoError(t, tx.CreateEdge(relationship))
			} else {
				_, err = tx.CreateNode(&Node{ID: "test:gone"})
				require.NoError(t, err)
			}
			require.NoError(t, tx.Commit(), "sequential recreation must accept the already-visible tombstone")
			require.NoError(t, b.FlushWriteBehind())
		})
	}
}
