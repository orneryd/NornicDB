package replication

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStorageAdapterLeaderBackpressureReplaysExactNativeState(t *testing.T) {
	f := setupTestAdapter(t, adapterFixtureOptions{standalone: true})
	a, e := f.adapter, f.engine
	_, err := e.CreateNode(t.Context(), &storage.Node{ID: "test:ordered-leader", Properties: map[string]any{"v": int64(0)}})
	require.NoError(t, err)
	require.NoError(t, a.FlushWAL())
	update := func(i int) error {
		return e.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &storage.Node{ID: "test:ordered-leader", Properties: map[string]any{"v": int64(i)}}})
	}
	a.walMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(a.walMu.Unlock) }
	t.Cleanup(unlock)
	require.NoError(t, update(1))
	require.Eventually(t, func() bool {
		a.walBatchMu.Lock()
		defer a.walBatchMu.Unlock()
		return len(a.walQueue) == 0 && len(a.walBatch) == 0
	}, 5*time.Second, time.Millisecond)
	for i := 2; i <= 1001; i++ {
		require.NoError(t, update(i))
	}
	require.Equal(t, 1000, len(a.walQueue))
	applied := make(chan error, 1)
	go func() { applied <- update(1002) }()
	select {
	case err := <-applied:
		t.Fatalf("leader overflow bypassed held physical WAL authority: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unlock()
	require.NoError(t, <-applied)
	require.NoError(t, a.FlushWAL())
	current, err := a.engine.GetNode(t.Context(), "test:ordered-leader")
	require.NoError(t, err)
	require.Equal(t, "1002", fmt.Sprint(current.Properties["v"]))
	entries, err := a.GetWALEntries(0, 2000)
	require.NoError(t, err)
	require.Len(t, entries, 1003)
	for _, entry := range entries {
		require.Equal(t, CmdCommitTransaction, entry.Command.Type, "real leader must normalize every leaf before state-machine application")
	}
	follower := setupTestAdapter(t).adapter
	for _, entry := range entries {
		require.NoError(t, applyQueued(t, follower, entry.Command))
	}
	copied, err := follower.engine.GetNode(t.Context(), "test:ordered-leader")
	require.NoError(t, err)
	t.Logf("supported leader native=%v normalized replay=%v envelopes=%d", current.Properties["v"], copied.Properties["v"], len(entries))
	require.Equal(t, fmt.Sprint(current.Properties["v"]), fmt.Sprint(copied.Properties["v"]))
}

func TestStorageAdapterLeaderRejectDoesNotEnterReplicationLog(t *testing.T) {
	f := setupTestAdapter(t, adapterFixtureOptions{standalone: true})
	a, e := f.adapter, f.engine
	require.NoError(t, a.engine.GetSchema().AddUniqueConstraint("unique-key", "Unique", "key"))
	_, err := e.CreateNode(t.Context(), &storage.Node{ID: "test:accepted-leader", Labels: []string{"Unique"}, Properties: map[string]any{"key": "same"}})
	require.NoError(t, err)
	require.NoError(t, a.FlushWAL())
	_, rejected := e.CreateNode(t.Context(), &storage.Node{ID: "test:rejected-leader", Labels: []string{"Unique"}, Properties: map[string]any{"key": "same"}})
	require.Error(t, rejected)
	require.NoError(t, a.FlushWAL())
	entries, err := a.GetWALEntries(0, 10)
	require.NoError(t, err)
	t.Logf("real leader native rejection=%v normalized log records=%d", rejected, len(entries))
	require.Len(t, entries, 1)
}

func TestStorageAdapterConcurrentLeaderReplayMatchesNative(t *testing.T) {
	for _, shared := range []bool{true, false} {
		t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
			f := setupTestAdapter(t, adapterFixtureOptions{standalone: true})
			const workers = 4
			for i := 0; i < workers; i++ {
				_, err := f.engine.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:concurrent-%d", i)), Properties: map[string]any{"v": int64(0)}})
				require.NoError(t, err)
			}
			type result struct {
				successful int
				err        error
			}
			results := make(chan result, workers)
			for worker := 0; worker < workers; worker++ {
				go func(w int) {
					key := w
					if shared {
						key = 0
					}
					count := 0
					for i := 1; i <= 20; i++ {
						if err := f.engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:concurrent-%d", key)), Properties: map[string]any{"v": int64(w*100 + i)}}}); err != nil {
							results <- result{count, err}
							return
						}
						count++
					}
					results <- result{count, nil}
				}(worker)
			}
			successful := 0
			for range workers {
				select {
				case result := <-results:
					successful += result.successful
					if result.err != nil {
						require.True(t, shared, "disjoint updates must not conflict")
						require.ErrorIs(t, result.err, storage.ErrConflict)
						var accepted *storage.AcceptedCommitError
						require.False(t, errors.As(result.err, &accepted), "uncertain accepted work cannot be treated as a rejected client snapshot")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("concurrent real leader updates did not join")
				}
			}
			require.NoError(t, f.adapter.FlushWAL())
			entries, err := f.adapter.GetWALEntries(0, 1000)
			require.NoError(t, err)
			require.Positive(t, successful)
			require.Len(t, entries, workers+successful, "definitely rejected client snapshots must not enter accepted log")
			follower := setupTestAdapter(t).adapter
			for _, entry := range entries {
				require.Equal(t, CmdCommitTransaction, entry.Command.Type)
				require.NoError(t, applyQueued(t, follower, entry.Command))
			}
			for i := 0; i < workers; i++ {
				id := storage.NodeID(fmt.Sprintf("test:concurrent-%d", i))
				native, err := f.adapter.engine.GetNode(t.Context(), id)
				require.NoError(t, err)
				replayed, err := follower.engine.GetNode(t.Context(), id)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprint(native.Properties["v"]), fmt.Sprint(replayed.Properties["v"]))
			}
		})
	}
}
