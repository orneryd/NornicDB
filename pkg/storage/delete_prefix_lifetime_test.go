package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeleteByPrefixRejectsEarlierTransactions(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, stage := range []string{"before", "after", "unbound", "read-only"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, stage), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				_, err = b.CreateNode(&Node{ID: "a:seed"})
				require.NoError(t, err)
				old, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = old.Rollback() })
				require.NoError(t, old.SetImplicit(buffered))
				if stage != "unbound" {
					require.NoError(t, old.SetNamespace("a"))
				}
				if stage == "before" {
					_, err = old.CreateNode(&Node{ID: "a:stale"})
					require.NoError(t, err)
				}
				peer, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, peer.SetNamespace("b"))
				_, err = peer.CreateNode(&Node{ID: "b:live"})
				require.NoError(t, err)
				_, _, err = b.DeleteByPrefixContext(context.Background(), "a:")
				require.NoError(t, err)
				// Recreate A before committing the old transaction. Its new lifetime
				// must remain usable, including after another DROP/recreate cycle.
				_, err = b.CreateNode(&Node{ID: "a:recreated"})
				require.NoError(t, err)
				if stage == "unbound" {
					require.NoError(t, old.SetNamespace("a"))
				}
				if stage == "after" || stage == "unbound" {
					_, err = old.CreateNode(&Node{ID: "a:stale"})
					require.NoError(t, err)
				}
				require.ErrorIs(t, old.Commit(), ErrConflict, "DROP must permanently retire earlier transactions")
				require.Equal(t, TxStatusRolledBack, old.Status)
				require.Error(t, old.Commit(), "a rejected transaction cannot later commit")
				require.NoError(t, peer.Commit(), "DROP A must not retire transactions in B")
				fresh, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, fresh.SetNamespace("a"))
				_, err = fresh.CreateNode(&Node{ID: "a:fresh"})
				require.NoError(t, err)
				require.NoError(t, fresh.Commit())
				require.NoError(t, b.FlushWriteBehind())
				count, err := NewNamespacedEngine(b, "a").NodeCount()
				require.NoError(t, err)
				require.Equal(t, int64(2), count)
				_, err = b.GetNode("a:stale")
				require.ErrorIs(t, err, ErrNotFound)
			})
		}
	}
}
