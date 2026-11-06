package storage

import (
	"context"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestSnapshotCompactionAdmissionDistinguishesCloseFromCancellation(t *testing.T) {
	for _, phase := range []string{"request_canceled", "owner_closing", "owner_closing_live_context", "owner_closed", "journal_closed"} {
		t.Run(phase, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			wal, err := NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			owner := NewWALEngine(newTestMemoryEngine(t), wal)
			owner.snapshotDir = t.TempDir()
			t.Cleanup(func() { require.NoError(t, owner.Close()) })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := error(context.Canceled)
			switch phase {
			case "owner_closing", "owner_closing_live_context":
				// A second closer may cancel work before the first finishes
				// sealing the WAL. Admission is already terminal in this gap.
				require.True(t, owner.work.CloseAdmission())
				if phase == "owner_closing" {
					cancel()
				}
				want = ErrWALClosed
			case "owner_closed":
				require.NoError(t, owner.Close())
				ctx = owner.work.Context()
				want = ErrWALClosed
			case "journal_closed":
				require.NoError(t, wal.Close())
				cancel()
				want = ErrWALClosed
			default:
				cancel()
			}
			sequence := wal.Sequence()
			err = owner.createSnapshotAndCompact(ctx)
			require.ErrorIs(t, err, want)
			require.Equal(t, sequence, wal.Sequence(), "rejected admission must not append a checkpoint")
			files, err := os.ReadDir(owner.snapshotDir)
			require.NoError(t, err)
			require.Empty(t, files, "rejected admission must not publish a snapshot")
			require.True(t, owner.mutationMu.TryLock(), "rejected snapshot must not retain its barrier")
			owner.mutationMu.Unlock()
		})
	}
}
