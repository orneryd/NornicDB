package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A coarse or backward clock must not hide commits that preceded BeginTransaction.
// Before namespace pinning, the physical snapshot also supports cross-database scans.
func TestTransactionUnpinnedReadsIncludeLogicalHighWater(t *testing.T) {
	engine := newTestEngine(t)
	for attempt := 0; attempt < 128; attempt++ {
		if !t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			for _, ns := range []string{"a", "b"} {
				_, err := engine.CreateNode(&Node{ID: NodeID(ns + ":before"), Labels: []string{"Before"}})
				require.NoError(t, err)
			}
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			_, err = engine.CreateNode(&Node{ID: "a:after", Labels: []string{"After"}})
			require.NoError(t, err)
			nodes, err := tx.GetNodesByLabel("Before")
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"a:before", "b:before"}, nodeIDs(nodes))
			nodes, err = tx.AllNodes()
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"a:before", "b:before"}, nodeIDs(nodes))
			require.NoError(t, tx.SetNamespace("a"))
			_, err = tx.GetNode("a:before")
			require.NoError(t, err)
			_, err = tx.GetNode("a:after")
			require.ErrorIs(t, err, ErrNotFound)
			require.NoError(t, tx.Rollback())
			require.NoError(t, engine.DeleteNode("a:before"))
			require.NoError(t, engine.DeleteNode("b:before"))
			require.NoError(t, engine.DeleteNode("a:after"))
		}) {
			return
		}
	}
}
