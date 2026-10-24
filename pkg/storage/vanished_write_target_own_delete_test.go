package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A node this transaction created and deleted is not a peer's concurrent
// change: an observed update of it stays not found, so a retry cannot loop.
func TestVanishedWriteTargetOwnDeleteStaysNotFound(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: async, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer b.Close()
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			node := &Node{ID: "test:own", Labels: []string{"Doc"}, Properties: map[string]any{"id": "own"}}
			_, err = tx.CreateNode(node)
			require.NoError(t, err)
			before, err := tx.GetNode(node.ID)
			require.NoError(t, err)
			require.NoError(t, tx.DeleteNode(node.ID))
			after := copyNode(before)
			after.Properties["note"] = "changed"
			err = tx.UpdateNodeObserved(before, after)
			require.ErrorIs(t, err, ErrNotFound)
			require.NotErrorIs(t, err, ErrConflict)
		})
	}
}
