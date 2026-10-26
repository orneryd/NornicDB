package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Another notification drainer can reach the replay's ready event before the
// original drain returns from apply. A listener's public flush must exclude
// its own already-published generation, while original waiters still await
// the drain's settlement after dispatch.
func TestReplayNotificationRetiresGenerationBeforeConcurrentListenerFlush(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	applied := make(chan *CommitBuffer, 1)
	resume := make(chan struct{})
	var resumeOnce sync.Once
	resumeApply := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(resumeApply)
	drained := make(chan struct{})
	originalFlushDone := make(chan error, 1)
	listenerEntered := make(chan struct{})
	listenerDone := make(chan error, 1)
	dispatcherDone := make(chan struct{})
	apply := b.writeBehind.apply
	b.writeBehind.apply = func(buf *CommitBuffer) error {
		err := apply(buf)
		applied <- buf
		<-resume
		return err
	}
	b.OnNodeCreated(func(n *Node) {
		if n.ID != "test:notification-retirement" {
			return
		}
		close(listenerEntered)
		listenerDone <- b.FlushWriteBehind()
	})
	bufferedCreate(t, b, &Node{ID: "test:notification-retirement"})
	b.writeBehind.rotate()
	go func() { defer close(drained); originalFlushDone <- b.FlushWriteBehind() }()
	var generation *CommitBuffer
	select {
	case generation = <-applied:
	case <-time.After(5 * time.Second):
		resumeApply()
		t.Fatal("replay did not reach its publication boundary")
	}
	go func() { defer close(dispatcherDone); b.drainCommitNotifications() }()
	select {
	case <-listenerEntered:
	case <-time.After(5 * time.Second):
		resumeApply()
		<-drained
		t.Fatal("concurrent dispatcher did not reach replay listener")
	}
	completedBeforeApplyReturned := false
	select {
	case err := <-listenerDone:
		require.NoError(t, err)
		completedBeforeApplyReturned = true
	case <-time.After(5 * time.Second):
	}
	select {
	case <-generation.Settled():
		t.Error("generation settled before its original drain returned")
	default:
	}
	originalFlushReturned := false
	select {
	case err := <-originalFlushDone:
		originalFlushReturned = true
		t.Errorf("original captured flush returned before its generation settled: %v", err)
	default:
	}
	resumeApply()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("original drain did not finish")
	}
	if !originalFlushReturned {
		require.NoError(t, <-originalFlushDone)
	}
	if !completedBeforeApplyReturned {
		select {
		case err := <-listenerDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("listener flush did not finish after original drain resumed")
		}
	}
	select {
	case <-dispatcherDone:
	case <-time.After(5 * time.Second):
		t.Fatal("notification dispatcher did not finish")
	}
	require.True(t, completedBeforeApplyReturned, "listener flush waited on its own generation's drain settlement")
}

func TestReplayFailedApplyKeepsGenerationForRetry(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	notified := 0
	b.OnNodeCreated(func(*Node) { notified++ })
	bufferedCreate(t, b, &Node{ID: "test:retry-retirement"})
	b.writeBehind.rotate()
	b.writeBehind.mu.RLock()
	buf := b.writeBehind.draining[0]
	b.writeBehind.mu.RUnlock()
	commits := buf.Commits()
	// Badger rejects an empty physical key before publication. The failed
	// replay must preserve the generation and its overlay for a later retry.
	commits[0].writes[""] = []byte("invalid")
	err := b.replayWriteBehind(buf)
	require.ErrorIs(t, err, badger.ErrEmptyKey)
	require.False(t, buf.IsApplied())
	b.writeBehind.mu.RLock()
	retained := len(b.writeBehind.draining) == 1 && b.writeBehind.draining[0] == buf
	b.writeBehind.mu.RUnlock()
	require.True(t, retained)
	b.drainCommitNotifications()
	require.Zero(t, notified)
	delete(commits[0].writes, "")
	b.writeBehind.drainOne(true, nil)
	require.NoError(t, b.writeBehind.LastErr())
	require.True(t, buf.IsApplied())
	b.writeBehind.mu.RLock()
	remaining := len(b.writeBehind.draining)
	b.writeBehind.mu.RUnlock()
	require.Zero(t, remaining)
	require.Equal(t, 1, notified)
}
