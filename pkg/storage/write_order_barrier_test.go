package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOrderedDrainProceedsPastQueuedExclusiveWriter: an ordered write drains
// an acknowledged generation while it holds the write barrier shared. An
// exclusive barrier writer (Close, a schema transaction, a restore) that
// queued meanwhile waits for that write, so the replay the drain runs must
// not wait for the barrier in turn.
func TestOrderedDrainProceedsPastQueuedExclusiveWriter(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	// No background flusher: only the ordered write's drain applies the
	// acknowledged generation.
	engine.writeBehind.Close()
	// A deadlocked engine cannot close: the failure below leaves it.
	deadlocked := false
	t.Cleanup(func() {
		if !deadlocked {
			_ = engine.Close()
		}
	})
	_, err = engine.CreateNode(&Node{ID: "test:n", Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind())
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:n", Properties: map[string]any{"v": int64(2)}}))
	require.NoError(t, tx.Commit())

	// Park the direct update on its write-order stripe, after it took the
	// barrier shared and before its drain.
	var stripes orderStripeSet
	stripes.entities(writtenEntities{nodes: []NodeID{"test:n"}})
	engine.writeBehind.order.lock(&stripes, true)
	updated := make(chan error, 1)
	go func() {
		updated <- engine.UpdateNode(&Node{ID: "test:n", Properties: map[string]any{"v": int64(3)}})
	}()
	require.Eventually(t, func() bool {
		if engine.writeBarrier.TryLock() {
			engine.writeBarrier.Unlock()
			return false
		}
		return true
	}, 10*time.Second, time.Millisecond, "the update holds the barrier shared")
	exclusive := make(chan struct{})
	go func() {
		engine.writeBarrier.Lock()
		engine.writeBarrier.Unlock()
		close(exclusive)
	}()
	require.Eventually(t, func() bool {
		if engine.writeBarrier.TryRLock() {
			engine.writeBarrier.RUnlock()
			return false
		}
		return true
	}, 10*time.Second, time.Millisecond, "the exclusive writer is queued")
	engine.writeBehind.order.unlock(&stripes, true)

	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		deadlocked = true
		t.Fatal("the ordered write's drain waits for the barrier behind the queued exclusive writer, which waits for the ordered write")
	}
	select {
	case <-exclusive:
	case <-time.After(30 * time.Second):
		deadlocked = true
		t.Fatal("the exclusive writer never acquired the barrier")
	}
	node, err := engine.GetNode("test:n")
	require.NoError(t, err)
	require.Equal(t, int64(3), node.Properties["v"])
}
