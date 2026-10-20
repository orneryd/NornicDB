package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cancellablePrefixEngine struct {
	storage.Engine
	entered chan struct{}
}

func (e *cancellablePrefixEngine) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	close(e.entered)
	<-ctx.Done()
	return 0, 0, ctx.Err()
}

func TestReplicatedDropContextLocalModes(t *testing.T) {
	for _, mode := range []string{"standalone", "ha"} {
		t.Run(mode, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			owner := &cancellablePrefixEngine{Engine: base, entered: make(chan struct{})}
			adapter, err := NewStorageAdapterWithWAL(owner, t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, adapter.Close()) })
			cfg := DefaultConfig()
			var replicator Replicator
			if mode == "standalone" {
				r := NewStandaloneReplicator(cfg, adapter)
				require.NoError(t, r.Start(context.Background()))
				replicator = r
			} else {
				// Exercise HA's real local-apply path without starting peer networking.
				r := &HAStandbyReplicator{config: cfg, storage: adapter}
				r.started.Store(true)
				r.isPrimary.Store(true)
				replicator = r
			}
			engine := NewReplicatedEngine(owner, replicator, 30*time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, _, err := engine.DeleteByPrefixContext(ctx, "a:"); done <- err }()
			select {
			case <-owner.entered:
			case <-time.After(time.Second):
				t.Fatal("replication did not forward to the context-aware owner")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("replicated DROP did not cancel")
			}
			require.NoError(t, adapter.FlushWAL())
			entries, err := adapter.GetWALEntries(0, 100)
			require.NoError(t, err)
			require.Empty(t, entries, "a cancelled DROP must not be journaled for a standby to execute")
		})
	}
}

func TestReplicatedDropContextConsensusAdmission(t *testing.T) {
	for _, multiRegion := range []bool{false, true} {
		t.Run(map[bool]string{false: "raft", true: "multi-region"}[multiRegion], func(t *testing.T) {
			r := &RaftReplicator{
				config: DefaultConfig(), state: StateLeader,
				applyCh: make(chan *applyFuture, 1),
				log:     []*RaftLogEntry{{Index: 0}},
			}
			r.started.Store(true)
			var replicator ContextReplicator = r
			if multiRegion {
				mr := &MultiRegionReplicator{localRaft: r}
				mr.started.Store(true)
				replicator = mr
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- replicator.ApplyContext(ctx, &Command{Type: CmdDeleteByPrefix}, time.Minute) }()
			var queued *applyFuture
			select {
			case queued = <-r.applyCh:
			case <-time.After(time.Second):
				t.Fatal("command was not queued")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("consensus wait ignored cancellation")
			}
			r.processApply(context.Background(), queued)
			require.ErrorIs(t, <-queued.errCh, context.Canceled)
			require.Len(t, r.log, 1, "a cancelled queued DROP must never enter the committed log")
			// A full admission queue must also obey the caller's deadline.
			r.applyCh <- &applyFuture{}
			deadline, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer stop()
			require.ErrorIs(t, replicator.ApplyContext(deadline, &Command{Type: CmdDeleteByPrefix}, time.Minute), context.DeadlineExceeded)
		})
	}
}

func TestReplicatedDropContextHAAcknowledgement(t *testing.T) {
	r := &HAStandbyReplicator{config: DefaultConfig(), walStreamer: &WALStreamer{}}
	r.config.HAStandby.SyncMode = SyncQuorum
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.waitForReplicationAckContext(ctx, 1, time.Minute), context.DeadlineExceeded)
}
