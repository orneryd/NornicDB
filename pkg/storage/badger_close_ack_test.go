package storage

import (
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"log/slog"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An in-flight implicit writer may ACK after Close's initial drain, since
// Close does not take the publication barrier until the flusher has stopped.
func TestCommitNotifications_CloseWithLateACK(t *testing.T) {
	if os.Getenv("NORNIC_R4_CLOSE_CHILD") == "1" {
		barrier := &writeConflictBarrier{reached: make(chan struct{}), resume: make(chan struct{})}
		b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, Logger: slog.New(barrier)}))
		if b != nil {
			fixtureowner.Own(t, b)
		}
		require.NoError(t, err)
		entered, resume := make(chan struct{}), make(chan struct{})
		b.OnNodeCreated(func(n *Node) {
			if n.ID == "test:dispatch" {
				close(entered)
				<-resume
			}
		})
		dispatch := make(chan error, 1)
		go func() { _, e := b.CreateNode(&Node{ID: "test:dispatch"}); dispatch <- e }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("callback did not start")
		}
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		_, err = tx.CreateNode(&Node{ID: "test:late-ack"})
		require.NoError(t, err)
		require.NoError(t, tx.SetMetadata(map[string]any{"review": true}))
		barrier.target = tx.ID
		committed := make(chan error, 1)
		go func() { committed <- tx.Commit() }()
		select {
		case <-barrier.reached:
		case <-time.After(3 * time.Second):
			t.Fatal("commit did not pause")
		}
		rejected, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, rejected.SetImplicit(true))
		_, err = rejected.CreateNode(&Node{ID: "test:rejected-ack"})
		require.NoError(t, err)
		closed := make(chan error, 1)
		go func() { closed <- b.Close() }()
		deadline := time.Now().Add(3 * time.Second)
		for {
			b.mu.RLock()
			stopped := b.closing
			b.mu.RUnlock()
			if stopped {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("Close did not close write admission")
			}
			time.Sleep(time.Millisecond)
		}
		b.writeBehind.closeMu.Lock()
		workerStopped := b.writeBehind.closed
		b.writeBehind.closeMu.Unlock()
		require.False(t, workerStopped, "replay worker must remain available until admitted ACKs finish")
		rejectedCommit := make(chan error, 1)
		go func() { rejectedCommit <- rejected.Commit() }()
		close(barrier.resume)
		select {
		case e := <-committed:
			require.NoError(t, e)
		case <-time.After(3 * time.Second):
			t.Fatal("late ACK did not finish")
		}
		require.True(t, tx.AcknowledgedBuffered())
		select {
		case e := <-rejectedCommit:
			require.ErrorIs(t, e, ErrStorageClosed)
		case <-time.After(3 * time.Second):
			t.Fatal("new commit was not rejected")
		}
		require.False(t, rejected.AcknowledgedBuffered())
		close(resume)
		select {
		case e := <-dispatch:
			require.NoError(t, e)
		case <-time.After(3 * time.Second):
			t.Fatal("dispatch did not finish")
		}
		t.Logf("in-flight commit ACKed after Close started; pending ops=%d", b.writeBehind.PendingOps())
		select {
		case e := <-closed:
			require.NoError(t, e)
		case <-time.After(2 * time.Second):
			b.notificationMu.Lock()
			head := b.notificationHead
			pending := head != nil
			ready := pending && head.ready
			b.notificationMu.Unlock()
			t.Fatalf("Close stuck waiting for notification: head=%v ready=%v; replay worker already stopped", pending, ready)
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCommitNotifications_CloseWithLateACK$", "-test.timeout=12s", "-test.v")
	cmd.Env = append(os.Environ(), "NORNIC_R4_CLOSE_CHILD=1")
	out, err := cmd.CombinedOutput()
	t.Logf("bounded child output:\n%s", out)
	require.NoError(t, err)
}

func TestCommitNotifications_CloseRetriesFailedReplay(t *testing.T) {
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	defer b.Close()
	original := b.writeBehind.apply
	injected := errors.New("replay failure")
	var fail atomic.Bool
	fail.Store(true)
	b.writeBehind.apply = func(buf *CommitBuffer) error {
		if fail.Load() {
			return injected
		}
		return original(buf)
	}
	r3Implicit(t, b, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(&Node{ID: "test:close-retry"}); return err })
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		require.ErrorIs(t, err, injected)
	case <-time.After(5 * time.Second):
		t.Fatal("Close must return its drain failure")
	}
	require.Greater(t, b.writeBehind.PendingOps(), 0)
	fail.Store(false)
	require.NoError(t, b.Close())
	require.Zero(t, b.writeBehind.PendingOps())
}
