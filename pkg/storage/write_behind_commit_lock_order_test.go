package storage

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// Pin a real synchronous Commit after its pre-drain and before its count
// lock, then let an acknowledged generation become a large replay.
func TestReview1406R3LargeReplayCountLockOrder(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	// Do not Close on a demonstrated deadlock: the Go timeout emits both stacks.
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err = b.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("review_k", "Item", "k"))
	syncTx, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = syncTx.CreateNode(&Node{ID: "test:sync", Labels: []string{"Item"}, Properties: map[string]any{"k": "sync"}})
	require.NoError(t, err)
	require.NoError(t, syncTx.CreateEdge(&Edge{ID: "test:sync-edge", StartNode: "test:a", EndNode: "test:b", Type: "SYNC"}))
	ack, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, ack.SetImplicit(true))
	pad := strings.Repeat("x", 700)
	for i := 0; i < 3000; i++ {
		_, err = ack.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:review-big-%d", i)), Labels: []string{"Big"}, Properties: map[string]any{"pad": pad}})
		require.NoError(t, err)
	}
	require.NoError(t, ack.CreateEdge(&Edge{ID: "test:buffer-edge", StartNode: "test:a", EndNode: "test:b", Type: "BUFFER"}))
	validating := make(chan struct{})
	resumeSync := make(chan struct{})
	restoreScan := setUniqueConstraintScanHook(func() { close(validating); <-resumeSync })
	defer restoreScan()
	syncDone := make(chan error, 1)
	go func() { syncDone <- syncTx.Commit() }()
	select {
	case <-validating:
	case <-time.After(5 * time.Second):
		t.Fatal("sync validation seam not reached")
	}
	require.NoError(t, ack.Commit())
	require.True(t, ack.AcknowledgedBuffered())
	replayLarge := make(chan struct{})
	resumeReplay := make(chan struct{})
	restoreBatch := setLargeCommitBatchHook(func(batch int) error {
		if batch == 1 {
			close(replayLarge)
			<-resumeReplay
		}
		return nil
	})
	defer restoreBatch()
	flushDone := make(chan error, 1)
	go func() { flushDone <- b.FlushWriteBehind() }()
	select {
	case <-replayLarge:
	case <-time.After(5 * time.Second):
		t.Fatal("replay did not become large")
	}
	close(resumeSync)
	require.Eventually(t, func() bool {
		if b.edgeTypeCountWriteMu.TryLock() {
			b.edgeTypeCountWriteMu.Unlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond, "sync transaction must reach its positional count lock")
	close(resumeReplay)
	select {
	case err := <-syncDone:
		t.Logf("sync result: %v", err)
	case <-time.After(3 * time.Second):
		t.Log("sync writer blocked with count lock while replay holds gate")
	}
	select {
	case err := <-flushDone:
		require.NoError(t, err)
		require.NoError(t, b.Close())
	case <-time.After(3 * time.Second):
		var stacks bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
		t.Log(stacks.String())
		t.Fatal("large replay and synchronous commit deadlocked")
	}
}
