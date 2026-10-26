package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestResidualWALAcceptanceAdmission(t *testing.T) {
	for _, route := range []string{"snapshot", "registration"} {
		t.Run(route, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			b, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			owner := NewWALEngine(b, wal)
			t.Cleanup(func() { _ = owner.Close() })
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("test"))
			_, err = tx.CreateNode(&Node{ID: "test:wal-wait"})
			require.NoError(t, err)
			var decisions atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = WithPublicationDecision(ctx, func(_ context.Context, _ PublicationIdentity, action func() error) error {
				decisions.Add(1)
				return action()
			})
			prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "wal-wait"})
			require.NoError(t, err)
			t.Cleanup(prepared.Abort)
			var target sync.Locker = &owner.mutationMu
			if route == "registration" {
				target = &wal.mu
			}
			target.Lock()
			var unlockOnce sync.Once
			unlock := func() { unlockOnce.Do(target.Unlock) }
			t.Cleanup(unlock)
			waited := make(chan context.Context, 1)
			t.Cleanup(setAdmissionWaitObserver(func(actual any, waitCtx context.Context) {
				if actual == target {
					select {
					case waited <- waitCtx:
					default:
					}
				}
			}))
			done := make(chan error, 1)
			go func() {
				_, err := owner.SubmitEntityCommit(ctx, prepared, nil)
				done <- err
			}()
			var admittedContext context.Context
			select {
			case waitCtx := <-waited:
				admittedContext = waitCtx
			case <-time.After(2 * time.Second):
				cancel()
				unlock()
				<-done
				t.Fatal("WAL submit did not reach its held acceptance admission")
			}
			cancel()
			var submitErr error
			returnedBeforeUnlock := false
			select {
			case submitErr = <-done:
				returnedBeforeUnlock = true
			case <-time.After(2 * time.Second):
			}
			unlock()
			if !returnedBeforeUnlock {
				submitErr = <-done
			}
			require.True(t, returnedBeforeUnlock)
			require.ErrorIs(t, submitErr, context.Canceled)
			require.Equal(t, ctx, admittedContext)
			require.Zero(t, decisions.Load())
			prepared.Abort()
			_, err = b.GetStoredNode("test:wal-wait")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}
