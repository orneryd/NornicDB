package nornicdb

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// After a restart the lifecycle prune walks whatever history accumulated
// before it. Writes made meanwhile must not wait for that walk. Before this
// fix the first cycles after start held the lock every write takes to
// register its snapshot for the whole walk, so writes froze for seconds a few
// cycles after start. Regression of 7b14b9133 (fix for GitHub #732); no NOR
// issue, recorded on NOR-405.

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRestartWithPruneBacklogDoesNotHoldBackWrites(t *testing.T) {
	dir := t.TempDir()
	config := func(interval time.Duration) *Config {
		c := DefaultConfig()
		c.Memory.EmbeddingEnabled = false
		c.Memory.AutoLinksEnabled = false
		c.Memory.DecayEnabled = false
		c.Database.AsyncWritesEnabled = false
		c.Database.MVCCRetentionMaxVersions = 2
		c.Database.MVCCLifecycleEnabled = true
		c.Database.MVCCLifecycleCycleInterval = interval
		return c
	}
	db, err := Open(dir, config(0))
	require.NoError(t, err)
	engine := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "nornic")
	const total, hot = 100000, 2000
	ids := make([]storage.NodeID, total)
	for offset := 0; offset < total; offset += 5000 {
		nodes := make([]*storage.Node, 0, 5000)
		for i := offset; i < offset+5000; i++ {
			ids[i] = storage.NodeID(fmt.Sprintf("backlog-%d", i))
			nodes = append(nodes, &storage.Node{ID: ids[i], Labels: []string{"Backlog"}, Properties: map[string]any{"v": 0}})
		}
		require.NoError(t, engine.BulkCreateNodes(context.Background(), nodes))
	}
	var wg sync.WaitGroup
	seedErr := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < total; i += 8 {
				rounds := 1
				if i < hot {
					rounds = 4
				}
				for v := 1; v <= rounds; v++ {
					if err := engine.UpdateNode(context.Background(), storage.NodeUpdateRequest{After: &storage.Node{ID: ids[i], Labels: []string{"Backlog"}, Properties: map[string]any{"v": v}}}); err != nil {
						seedErr <- err
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(seedErr)
	for err := range seedErr {
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	// Restart with the lifecycle loop running at once.
	db, err = Open(dir, config(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() {
		// A write stuck behind the prune would also hold Close: bound it.
		closed := make(chan struct{})
		go func() { _ = db.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
		}
	})
	// write fails the test instead of hanging when a write does not return
	// within limit.
	write := func(i int, limit time.Duration) (time.Duration, bool) {
		start := time.Now()
		done := make(chan error, 1)
		go func() {
			_, err := db.ExecuteCypher(context.Background(), "CREATE (:RestartWrite {i: $i})", map[string]interface{}{"i": i})
			done <- err
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
			return time.Since(start), true
		case <-time.After(limit):
			_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 2)
			t.Errorf("write %d did not return within %s while the prune ran", i, limit)
			return time.Since(start), false
		}
	}
	// The first write after a restart recovers the database's MVCC sequence
	// by reading its heads once (seconds over 100,000 nodes, far longer under
	// the race detector); that is not the prune, so it gets its own allowance
	// and is not measured.
	warmup, ok := write(-1, 5*time.Minute)
	if !ok {
		return
	}
	var worst time.Duration
	writes := 0
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); writes++ {
		took, ok := write(writes, 15*time.Second)
		if !ok {
			return
		}
		if took > worst {
			worst = took
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, err := db.LifecycleStatus()
	require.NoError(t, err)
	t.Logf("warmup write %s; %d writes during the first prune cycles, slowest %s; last cycle %v", warmup.Round(time.Millisecond), writes, worst.Round(time.Millisecond), status["last_run"])
	require.Less(t, worst, time.Second, "a write waited for the prune of the backlog")
}
