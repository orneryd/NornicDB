package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func dropRetentionEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	for _, id := range []NodeID{"a:1", "b:1", "keep:1"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Target"}})
		require.NoError(t, err)
	}
	return b
}

// dropRetentionDrainTimeout keeps a regression short: a drop that waits for
// older readers fails with this deadline instead of the production 30 s.
func dropRetentionDrainTimeout(t *testing.T) {
	t.Helper()
	previous := restoreReadDrainTimeout
	restoreReadDrainTimeout = 2 * time.Second
	t.Cleanup(func() { restoreReadDrainTimeout = previous })
}

func requireDropDoesNotWait(t *testing.T, b *BadgerEngine, prefix string) {
	t.Helper()
	start := time.Now()
	_, _, err := b.DeleteByPrefix(prefix)
	elapsed := time.Since(start)
	require.NoError(t, err, "DROP DATABASE %s must not wait for an older open reader", prefix)
	require.Less(t, elapsed, restoreReadDrainTimeout, "DROP DATABASE %s blocked on an older open reader", prefix)
}

func dropRetentionLabelIDs(t *testing.T, b *BadgerEngine) []string {
	t.Helper()
	nodes, err := b.GetNodesByLabelInScope("", "Target")
	require.NoError(t, err)
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, string(node.ID))
	}
	sort.Strings(ids)
	return ids
}

// An explicit transaction that is merely open must not make a later,
// unrelated DROP DATABASE wait and then fail.
func TestDeleteByPrefixWithOlderOpenTransaction(t *testing.T) {
	dropRetentionDrainTimeout(t)
	b := dropRetentionEngine(t)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	requireDropDoesNotWait(t, b, "a:")
	requireDropDoesNotWait(t, b, "b:")
	require.Equal(t, []string{"keep:1"}, dropRetentionLabelIDs(t, b))
}

// pausedBackupWriter holds the backup stream, and so its registered read,
// open until released.
type pausedBackupWriter struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
	buf     bytes.Buffer
}

func (w *pausedBackupWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return w.buf.Write(p)
}

// A Backup that stays open across two drops must not block them, and still
// writes the snapshot it started with.
func TestDeleteByPrefixWithBackupOpenAcrossTwoDrops(t *testing.T) {
	dropRetentionDrainTimeout(t)
	b := dropRetentionEngine(t)
	w := &pausedBackupWriter{started: make(chan struct{}), release: make(chan struct{})}
	backupDone := make(chan error, 1)
	go func() {
		_, err := b.db.Backup(w, 0)
		backupDone <- err
	}()
	released := false
	defer func() {
		if !released {
			close(w.release)
			<-backupDone
		}
	}()
	select {
	case <-w.started:
	case <-time.After(time.Minute):
		t.Fatal("timed out waiting for the backup stream")
	}

	requireDropDoesNotWait(t, b, "a:")
	requireDropDoesNotWait(t, b, "b:")
	require.Equal(t, []string{"keep:1"}, dropRetentionLabelIDs(t, b))

	close(w.release)
	released = true
	require.NoError(t, <-backupDone)
	path := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, os.WriteFile(path, w.buf.Bytes(), 0o600))
	restored, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	defer restored.Close()
	require.NoError(t, restored.Restore(path))
	require.Equal(t, []string{"a:1", "b:1", "keep:1"}, dropRetentionLabelIDs(t, restored),
		"the backup keeps the snapshot it started with")
}

// Only a full rebuild retires a generation. An empty-store mark, a namespace
// drop and an aborted drop leave the retire point where the last rebuild put
// it.
func TestLabelIndexOnlyFullRebuildMovesRetirePoint(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	defer b.Close()
	require.Zero(t, b.labelIndex.retireBeforeTs.Load(), "marking an empty store ready retires nothing")
	for _, id := range []NodeID{"a:1", "b:1", "keep:1"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Target"}})
		require.NoError(t, err)
	}

	_, err = b.rebuildLabelIndex(context.Background())
	require.NoError(t, err)
	retireBefore := b.labelIndex.retireBeforeTs.Load()
	require.NotZero(t, retireBefore, "a full rebuild sets the retire point")

	_, _, err = b.DeleteByPrefix("a:")
	require.NoError(t, err)
	require.Equal(t, retireBefore, b.labelIndex.retireBeforeTs.Load(), "DROP DATABASE retires no generation")

	// An aborted drop schedules a rebuild; hold that rebuild at its retention
	// wait to see that the drop itself moved nothing.
	reached, release := make(chan struct{}), make(chan struct{})
	b.labelIndex.beforeRetireWait = func() {
		close(reached)
		<-release
	}
	reset := b.beginLabelIndexReset()
	require.NoError(t, reset.markBuilding())
	dropFailed := errors.New("drop failed")
	require.ErrorIs(t, reset.finish(dropFailed), dropFailed)
	select {
	case <-reached:
	case <-time.After(time.Minute):
		t.Fatal("timed out waiting for the scheduled rebuild")
	}
	require.Equal(t, retireBefore, b.labelIndex.retireBeforeTs.Load(), "an aborted drop retires no generation")
	ready, err := b.labelIndexReady()
	require.NoError(t, err)
	require.False(t, ready, "the aborted drop's generation is not published")
	close(release)
	require.Eventually(t, func() bool {
		ready, err := b.labelIndexReady()
		return err == nil && ready
	}, time.Minute, 5*time.Millisecond, "the scheduled rebuild completes")
	b.labelIndex.beforeRetireWait = nil
	require.Greater(t, b.labelIndex.retireBeforeTs.Load(), retireBefore, "the scheduled rebuild moves the retire point")
	require.Equal(t, []string{"b:1", "keep:1"}, dropRetentionLabelIDs(t, b))
}

// pausedLabelStream is a public label stream held open after its second ID.
type pausedLabelStream struct {
	paused, resume, done chan struct{}
	once                 sync.Once
	got                  []NodeID
	err                  error
}

func startPausedLabelStream(t *testing.T, b *BadgerEngine) *pausedLabelStream {
	t.Helper()
	s := &pausedLabelStream{paused: make(chan struct{}), resume: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		s.err = b.ForEachNodeIDByLabelInScope("", "Target", func(id NodeID) bool {
			s.got = append(s.got, id)
			if len(s.got) == 2 {
				close(s.paused)
				<-s.resume
			}
			return true
		})
	}()
	t.Cleanup(func() { s.release(); <-s.done })
	waitDropRetention(t, s.paused, "admitted stream pause")
	return s
}

func (s *pausedLabelStream) release() { s.once.Do(func() { close(s.resume) }) }

func (s *pausedLabelStream) finish(t *testing.T) []string {
	t.Helper()
	s.release()
	waitDropRetention(t, s.done, "stream end")
	require.NoError(t, s.err)
	ids := make([]string, 0, len(s.got))
	for _, id := range s.got {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	return ids
}

func waitDropRetention(t *testing.T, c <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(time.Minute):
		t.Fatalf("timed out waiting for %s", name)
	}
}

// dropPausedAtFirstCommit runs DROP DATABASE and holds its first commit, the
// building marker, before the lifecycle reads it back and publishes it. Other
// commits pass through. The returned function lets the drop finish.
func dropPausedAtFirstCommit(t *testing.T, b *BadgerEngine, prefix string) func() {
	t.Helper()
	paused, resume := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	restoreHook := setCommitTailHook(func() {
		if held.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	})
	dropDone := make(chan error, 1)
	go func() {
		_, _, err := b.DeleteByPrefix(prefix)
		dropDone <- err
	}()
	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			close(resume)
			require.NoError(t, <-dropDone)
			restoreHook()
		})
	}
	t.Cleanup(finish)
	waitDropRetention(t, paused, "DROP DATABASE "+prefix+" building marker commit")
	return finish
}

func createTargetNodes(t *testing.T, b *BadgerEngine, ids ...string) []string {
	t.Helper()
	for _, id := range ids {
		_, err := b.CreateNode(&Node{ID: NodeID(id), Labels: []string{"Target"}})
		require.NoError(t, err)
	}
	want := append([]string(nil), ids...)
	sort.Strings(want)
	return want
}

func newDropRetentionEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// A stream admitted before one completed drop keeps every ID of its snapshot
// while a second drop publishes its building marker.
func TestLabelIndexStreamAcrossTwoDropMarkers(t *testing.T) {
	b := newDropRetentionEngine(t)
	want := createTargetNodes(t, b, "keep:first", "keep:second", "a:1", "a:2", "b:1", "keep:last")
	stream := startPausedLabelStream(t, b)
	_, _, err := b.DeleteByPrefix("a:")
	require.NoError(t, err)
	dropPausedAtFirstCommit(t, b, "b:")
	require.Equal(t, want, stream.finish(t), "an admitted stream keeps its original generation")
}

// The same holds across two completed drops and a third in flight.
func TestLabelIndexStreamAcrossThirdDropMarker(t *testing.T) {
	b := newDropRetentionEngine(t)
	want := createTargetNodes(t, b, "keep:first", "keep:second", "a:1", "a:2", "b:1", "c:1", "c:2", "keep:last")
	stream := startPausedLabelStream(t, b)
	for _, prefix := range []string{"a:", "b:"} {
		_, _, err := b.DeleteByPrefix(prefix)
		require.NoError(t, err)
	}
	dropPausedAtFirstCommit(t, b, "c:")
	require.Equal(t, want, stream.finish(t), "an admitted stream keeps its original generation")
}

// A Backup opened between the drops neither blocks them nor loses its own
// snapshot, and the older stream stays complete.
func TestLabelIndexStreamAndBackupInterleavedWithDrops(t *testing.T) {
	b := newDropRetentionEngine(t)
	want := createTargetNodes(t, b, "keep:first", "keep:second", "a:1", "a:2", "b:1", "keep:last")
	stream := startPausedLabelStream(t, b)
	_, _, err := b.DeleteByPrefix("a:")
	require.NoError(t, err)

	w := &pausedBackupWriter{started: make(chan struct{}), release: make(chan struct{})}
	backupDone := make(chan struct{})
	var backupErr error
	go func() {
		defer close(backupDone)
		_, backupErr = b.db.Backup(w, 0)
	}()
	var releaseBackup sync.Once
	t.Cleanup(func() { releaseBackup.Do(func() { close(w.release) }); <-backupDone })
	waitDropRetention(t, w.started, "backup stream")

	finishDrop := dropPausedAtFirstCommit(t, b, "b:")
	require.Equal(t, want, stream.finish(t), "an admitted stream keeps its original generation")
	finishDrop()

	releaseBackup.Do(func() { close(w.release) })
	<-backupDone
	require.NoError(t, backupErr)
	path := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, os.WriteFile(path, w.buf.Bytes(), 0o600))
	restored := newDropRetentionEngine(t)
	require.NoError(t, restored.Restore(path))
	require.Equal(t, []string{"b:1", "keep:first", "keep:last", "keep:second"}, dropRetentionLabelIDs(t, restored),
		"the backup keeps the snapshot it started with, after the first drop")
}

// pauseAfterLabelAdmission holds the first label-index reader admitted after
// it is armed, right after its snapshot was taken.
func pauseAfterLabelAdmission(t *testing.T, b *BadgerEngine) (paused <-chan struct{}, release func()) {
	t.Helper()
	return pauseAtLabelHook(t, &b.labelIndex.afterAdmission)
}

// pauseAtLabelHook holds the first caller of a lifecycle test hook.
func pauseAtLabelHook(t *testing.T, slot *atomic.Pointer[func()]) (paused <-chan struct{}, release func()) {
	t.Helper()
	pausedCh, resume := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	var releaseOnce sync.Once
	hook := func() {
		if held.CompareAndSwap(false, true) {
			close(pausedCh)
			<-resume
		}
	}
	slot.Store(&hook)
	release = func() {
		releaseOnce.Do(func() {
			slot.Store(nil)
			close(resume)
		})
	}
	t.Cleanup(release)
	return pausedCh, release
}

// Membership paused right before it resolves an ID, while that ID is dropped
// and recreated, keeps the ID's numeric identity from its own snapshot.
func TestHasLabelBatchAcrossDropAndRecreate(t *testing.T) {
	b := newDropRetentionEngine(t)
	createTargetNodes(t, b, "a:1", "keep:1")
	before, ok := b.idDict.lookupNodeNumID("a:1")
	require.True(t, ok)
	paused, release := pauseAtLabelHook(t, &b.labelIndex.beforeForwardLookup)
	var got map[NodeID]bool
	var readErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		got, readErr = b.HasLabelBatch([]NodeID{"a:1", "keep:1"}, "Target")
	}()
	waitDropRetention(t, paused, "membership forward lookup")
	_, _, err := b.DeleteByPrefix("a:")
	require.NoError(t, err)
	createTargetNodes(t, b, "a:1")
	after, ok := b.idDict.lookupNodeNumID("a:1")
	require.True(t, ok)
	require.NotEqual(t, before, after, "recreation assigns a new numeric identity")
	release()
	waitDropRetention(t, done, "membership")
	require.NoError(t, readErr)
	require.Equal(t, map[NodeID]bool{"a:1": true, "keep:1": true}, got)
}

// Entries a drop retires stay resolvable while an older reader is open and
// are released once it ends.
func TestLabelIndexRetiredEntriesReleasedAfterOlderReaders(t *testing.T) {
	b := newDropRetentionEngine(t)
	createTargetNodes(t, b, "a:1", "keep:1")
	num, ok := b.idDict.lookupNodeNumID("a:1")
	require.True(t, ok)
	txn, readTs, err := b.db.beginTxn(false)
	require.NoError(t, err)
	_, _, err = b.DeleteByPrefix("a:")
	require.NoError(t, err)
	_, live := b.idDict.lookupNodeIDByNum(num)
	require.False(t, live, "the drop removes the live entry")
	id, ok := b.labelIndexNodeID(num)
	require.True(t, ok, "an older reader still resolves the dropped number")
	require.Equal(t, NodeID("a:1"), id)

	txn.Discard()
	b.db.endRead(readTs)
	require.Eventually(t, func() bool {
		_, ok := b.labelIndex.retired.lookup(num)
		return !ok
	}, time.Minute, 5*time.Millisecond, "the retired entry is released after the last older reader ends")
	_, ok = b.labelIndexNodeID(num)
	require.False(t, ok)
}
