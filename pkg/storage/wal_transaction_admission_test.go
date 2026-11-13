package storage

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestJournalTransactionRejectsBeforePublication(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, scenario := range []string{"foreign_transaction", "canceled_context", "closed_journal"} {
		t.Run(scenario, func(t *testing.T) {
			base := newTestEngine(t)
			wal, err := NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			owner := NewWALEngine(base, wal)
			staging := base
			if scenario == "foreign_transaction" {
				staging = newTestEngine(t)
			}
			tx, err := staging.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("test"))
			_, err = tx.CreateNode(t.Context(), &Node{ID: "test:unpublished", Labels: []string{"Probe"}})
			require.NoError(t, err)
			ctx := t.Context()
			want := ErrInvalidData
			switch scenario {
			case "canceled_context":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx, want = canceled, context.Canceled
			case "closed_journal":
				require.NoError(t, wal.Close())
				want = ErrWALClosed
			}
			require.ErrorIs(t, owner.CommitTransaction(ctx, tx), want)
			require.Zero(t, wal.Sequence(), "rejected transactions must not receive journal records")
			_, err = staging.GetNode(t.Context(), "test:unpublished")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}
