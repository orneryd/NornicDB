package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func r4Wait(t *testing.T, c <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(time.Minute):
		t.Fatalf("timeout: %s", name)
	}
}

// A public ready stream switches from its index iterator to dictionary
// candidates at the first foreign namespace. Start a rebuild only after that
// switch, so later candidates require actual point lookups in the old txn.
func TestReview1667R4RebuildStartsDuringPublicRead(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer b.Close()
	_, err = b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "other:foreign", Labels: []string{"Target"}})
	require.NoError(t, err)
	for _, id := range []NodeID{"test:b", "test:c", "test:d"} {
		_, err = b.CreateNode(&Node{ID: id, Labels: []string{"Target"}})
		require.NoError(t, err)
	}
	paused, resume, readDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	cleared, rebuildResume, rebuildDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseReadOnce, releaseRebuildOnce sync.Once
	releaseRead := func() { releaseReadOnce.Do(func() { close(resume) }) }
	releaseRebuild := func() { releaseRebuildOnce.Do(func() { close(rebuildResume) }) }
	b.labelIndexAfterClearHook = func() {
		close(cleared)
		<-rebuildResume
	}
	var got []NodeID
	var readErr, rebuildErr error
	go func() {
		readErr = b.ForEachNodeIDByLabelInScope("test:", "Target", func(id NodeID) bool {
			got = append(got, id)
			if len(got) == 2 {
				close(paused)
				<-resume
			}
			return true
		})
		close(readDone)
	}()
	rebuildStarted := false
	defer func() {
		releaseRead()
		releaseRebuild()
		<-readDone
		if rebuildStarted {
			<-rebuildDone
		}
		b.labelIndexAfterClearHook = nil
	}()
	r4Wait(t, paused, "public stream second row")
	rebuildStarted = true
	go func() {
		_, rebuildErr = b.rebuildLabelIndex(context.Background())
		close(rebuildDone)
	}()
	r4Wait(t, cleared, "rebuild index clear")
	current, err := b.GetNodesByLabelInScope("test:", "Target")
	require.NoError(t, err)
	require.Len(t, current, 4)
	releaseRead()
	r4Wait(t, readDone, "existing public stream")
	require.NoError(t, readErr)
	t.Logf("IN_FLIGHT_PUBLIC_STREAM ids=%v want 4", got)
	require.ElementsMatch(t, []NodeID{"test:a", "test:b", "test:c", "test:d"}, got)
	releaseRebuild()
	r4Wait(t, rebuildDone, "rebuild completion")
	require.NoError(t, rebuildErr)
}

// A real NOR-142 failure begins with a populated READY cache; unlike the old
// fixture, this probe does not manually remove the marker before rebuilding.
func TestReview1667R4ReadyProofThenLowMemoryFailure(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:   t.TempDir(),
		LowMemory: true,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer b.Close()
	const n = 15000
	for start := 0; start < n; start += 1000 {
		var nodes []*Node
		for i := start; i < start+1000; i++ {
			nodes = append(nodes, &Node{ID: NodeID(fmt.Sprintf("test:%06d", i)), Labels: []string{"Target"}})
		}
		require.NoError(t, b.BulkCreateNodes(nodes))
	}
	ready, err := b.labelIndexReady()
	require.NoError(t, err)
	require.True(t, ready)
	generation := b.labelIndex.epoch.Load()
	processed, err := b.rebuildLabelIndex(context.Background())
	require.Error(t, err)
	require.True(t, IsTransactionTooBig(err), "%v", err)
	t.Logf("NOR142_FROM_READY processed=%d error=%v", processed, err)
	require.Greater(t, b.labelIndex.epoch.Load(), generation)
	ready, err = b.labelIndexReady()
	require.NoError(t, err)
	require.False(t, ready)
	nodes, err := b.GetNodesByLabelInScope("test:", "Target")
	require.NoError(t, err)
	require.Len(t, nodes, n)
	count, err := b.NodeCountByLabelInNamespace("test", "Target")
	require.NoError(t, err)
	require.EqualValues(t, n, count)
	count, err = b.NodeCountByLabel("Target")
	require.NoError(t, err)
	require.EqualValues(t, n, count)
}

func TestReview1667R4DropRecreateResetAndReopen(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: dir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer func() {
		if b != nil {
			_ = b.Close()
		}
	}()
	for _, ns := range []string{"test", "other"} {
		require.NoError(t, b.BulkCreateNodes([]*Node{
			{ID: NodeID(ns + ":a"), Labels: []string{"Target"}},
			{ID: NodeID(ns + ":b"), Labels: []string{"Target"}},
		}))
	}
	// Exercise the production index drop/recreate owner twice, starting READY.
	for cycle := 0; cycle < 2; cycle++ {
		cleared, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(resume) }) }
		b.labelIndexAfterClearHook = func() {
			close(cleared)
			<-resume
		}
		var rebuildErr error
		go func() {
			_, rebuildErr = b.rebuildLabelIndex(context.Background())
			close(done)
		}()
		func() {
			defer func() {
				release()
				<-done
				b.labelIndexAfterClearHook = nil
			}()
			r4Wait(t, cleared, "drop/recreate clear")
			nodes, err := b.GetNodesByLabelInScope("test:", "Target")
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			release()
			r4Wait(t, done, "drop/recreate complete")
			require.NoError(t, rebuildErr)
		}()
	}
	generation := b.labelIndex.epoch.Load()
	nd, ed, err := b.DeleteByPrefix("test:")
	require.NoError(t, err)
	require.EqualValues(t, 2, nd)
	require.Zero(t, ed)
	require.Greater(t, b.labelIndex.epoch.Load(), generation)
	_, err = b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
	require.NoError(t, err)
	check := func(want int) {
		t.Helper()
		nodes, err := b.GetNodesByLabel("Target")
		require.NoError(t, err)
		require.Len(t, nodes, want)
		count, err := b.NodeCountByLabel("Target")
		require.NoError(t, err)
		require.EqualValues(t, want, count)
	}
	check(3)
	// Exercise the production post-migration reopen boundary with no readers or
	// background workers active; the constructor normally owns this boundary.
	b.stopCountDeltaFoldWorker()
	b.stopDeleteCleanup()
	b.stopIndexBackfill(startupLabelIndex)
	b.stopIndexBackfill(startupEdgeBetweenIndex)
	generation = b.labelIndex.epoch.Load()
	_, err = b.reopenForPostMigrationCompaction(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.Greater(t, b.labelIndex.epoch.Load(), generation)
	check(3)
	ready, err := b.labelIndexReady()
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, b.Close())
	b = nil
	b, err = NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: dir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	check(3)
}

type r4PausedLabelStream struct {
	got        []NodeID
	err        error
	paused     chan struct{}
	resume     chan struct{}
	done       chan struct{}
	releaseOne sync.Once
}

func r4StartPausedLabelStream(t *testing.T, b *BadgerEngine) *r4PausedLabelStream {
	t.Helper()
	p := &r4PausedLabelStream{
		paused: make(chan struct{}),
		resume: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go func() {
		p.err = b.ForEachNodeIDByLabelInScope("test:", "Target", func(id NodeID) bool {
			p.got = append(p.got, id)
			if len(p.got) == 2 {
				close(p.paused)
				<-p.resume
			}
			return true
		})
		close(p.done)
	}()
	t.Cleanup(func() {
		p.release()
		<-p.done
	})
	r4Wait(t, p.paused, "public label stream pause")
	return p
}

func (p *r4PausedLabelStream) release() {
	p.releaseOne.Do(func() { close(p.resume) })
}

func (p *r4PausedLabelStream) finish(t *testing.T) []NodeID {
	t.Helper()
	p.release()
	r4Wait(t, p.done, "public label stream completion")
	require.NoError(t, p.err)
	return append([]NodeID(nil), p.got...)
}

func r4LabelIDs(t *testing.T, b *BadgerEngine) []NodeID {
	t.Helper()
	nodes, err := b.GetNodesByLabelInScope("test:", "Target")
	require.NoError(t, err)
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func r4EnsureLabelIndexReady(t *testing.T, b *BadgerEngine) {
	t.Helper()
	b.stopIndexBackfill(startupLabelIndex)
	ready, err := b.labelIndexReady()
	require.NoError(t, err)
	if !ready {
		_, err = b.rebuildLabelIndex(context.Background())
		require.NoError(t, err)
	}
	ready, err = b.labelIndexReady()
	require.NoError(t, err)
	require.True(t, ready)
}

// Generated event orders keep a public read paused while the index owner
// succeeds, fails, resets a namespace, or restores a prior snapshot. Writes
// land during both successful and failed rebuilds. Every paused reader must
// return the exact IDs from its admission snapshot.
func TestReview1667R4GeneratedLifecycleInterleavings(t *testing.T) {
	for seed := int64(1); seed <= 2; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d", "test:e", "test:f", "test:g", "test:h", "other:foreign"} {
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Target"}})
				require.NoError(t, err)
			}
			r4EnsureLabelIndexReady(t, b)
			backup := filepath.Join(t.TempDir(), "generation.backup")
			require.NoError(t, b.Backup(backup))
			backupIDs := r4LabelIDs(t, b)

			for _, action := range rand.New(rand.NewSource(seed)).Perm(4) {
				func(action int) {
					r4EnsureLabelIndexReady(t, b)
					beforeNodes, err := b.GetNodesByLabelInScope("test:", "Target")
					require.NoError(t, err)
					require.GreaterOrEqual(t, len(beforeNodes), 4)
					want := make([]NodeID, 0, len(beforeNodes))
					for _, node := range beforeNodes {
						want = append(want, node.ID)
					}
					p := r4StartPausedLabelStream(t, b)

					switch action {
					case 0, 1: // rebuild success/failure
						cleared := make(chan struct{})
						rebuildResume := make(chan struct{})
						var releaseRebuild sync.Once
						b.labelIndexAfterClearHook = func() {
							close(cleared)
							<-rebuildResume
						}
						ctx, cancel := context.WithCancel(context.Background())
						var rebuildErr error
						rebuildDone := make(chan struct{})
						go func() {
							_, rebuildErr = b.rebuildLabelIndex(ctx)
							close(rebuildDone)
						}()
						defer func() {
							cancel()
							releaseRebuild.Do(func() { close(rebuildResume) })
							<-rebuildDone
							b.labelIndexAfterClearHook = nil
						}()
						r4Wait(t, cleared, "generated rebuild clear")
						if action == 0 {
							node, getErr := b.GetNode("test:b")
							require.NoError(t, getErr)
							if nodeHasLabel(node, "Target") {
								node.Labels = []string{"Changed"}
							} else {
								node.Labels = []string{"Target"}
							}
							require.NoError(t, b.UpdateNode(node))
						} else {
							_, err = b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:failed-%d", seed)), Labels: []string{"Target"}})
							require.NoError(t, err)
							cancel()
						}
						pGot := p.finish(t)
						require.ElementsMatch(t, want, pGot)
						releaseRebuild.Do(func() { close(rebuildResume) })
						r4Wait(t, rebuildDone, "generated rebuild completion")
						if action == 0 {
							require.NoError(t, rebuildErr)
						} else {
							require.ErrorIs(t, rebuildErr, context.Canceled)
							ready, readyErr := b.labelIndexReady()
							require.NoError(t, readyErr)
							require.False(t, ready)
							require.Contains(t, r4LabelIDs(t, b), NodeID(fmt.Sprintf("test:failed-%d", seed)))
						}
						cancel()
					case 2: // namespace drop and recreate while the old snapshot is paused
						recreate := make([]*Node, 0, len(beforeNodes))
						for _, node := range beforeNodes {
							recreate = append(recreate, copyNode(node))
						}
						deleted, _, err := b.DeleteByPrefix("test:")
						require.NoError(t, err)
						require.GreaterOrEqual(t, deleted, int64(len(beforeNodes)))
						for _, node := range recreate {
							_, err := b.CreateNode(node)
							require.NoError(t, err)
						}
						require.ElementsMatch(t, want, p.finish(t))
						require.ElementsMatch(t, want, r4LabelIDs(t, b))
					case 3: // Restore waits for the paused snapshot and then replaces the store
						var restoreErr error
						restored := make(chan struct{})
						go func() {
							restoreErr = b.Restore(backup)
							close(restored)
						}()
						defer func() {
							p.release()
							<-restored
						}()
						require.Eventually(t, func() bool {
							b.db.oracle.mu.Lock()
							held := b.db.oracle.readsHeld
							b.db.oracle.mu.Unlock()
							return held
						}, time.Minute, time.Millisecond, "Restore must wait on the open snapshot")
						require.ElementsMatch(t, want, p.finish(t))
						r4Wait(t, restored, "generated Restore completion")
						require.NoError(t, restoreErr)
						require.ElementsMatch(t, backupIDs, r4LabelIDs(t, b))
					}
				}(action)
			}
		})
	}
}

func TestReview1667R4OldGenerationRetentionIsBounded(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	defer b.Close()
	const nodeCount = 128
	for start := 0; start < nodeCount; start += 32 {
		nodes := make([]*Node, 0, 32)
		for i := start; i < start+32; i++ {
			nodes = append(nodes, &Node{ID: NodeID(fmt.Sprintf("test:%04d", i)), Labels: []string{"Target"}})
		}
		require.NoError(t, b.BulkCreateNodes(nodes))
	}
	_, err = b.CreateNode(&Node{ID: "other:0001", Labels: []string{"Target"}})
	require.NoError(t, err)
	r4EnsureLabelIndexReady(t, b)
	oldReaderTxn, oldReadTs, err := b.db.beginTxn(false)
	require.NoError(t, err)
	oldReader := b.newLabelIndexReader(context.Background(), oldReaderTxn, "test:", "Target", "", storedRead, false)
	readerOpen := true
	defer func() {
		if readerOpen {
			oldReader.Close()
			oldReaderTxn.Discard()
			b.db.endRead(oldReadTs)
		}
	}()

	_, err = b.rebuildLabelIndex(context.Background())
	require.NoError(t, err)
	retireBefore := b.labelIndex.retireBeforeTs.Load()
	require.Greater(t, retireBefore, oldReadTs)
	var oldIDs []NodeID
	for oldReader.Next() {
		oldIDs = append(oldIDs, oldReader.NodeID())
	}
	require.NoError(t, oldReader.Err())
	require.Len(t, oldIDs, nodeCount, "the admitted reader still owns the full old generation")

	b.db.oracle.mu.Lock()
	discardWhilePinned := b.db.oracle.discardTs
	b.db.oracle.mu.Unlock()
	require.LessOrEqual(t, discardWhilePinned, oldReadTs,
		"Badger's discard floor cannot pass the oldest reader's generation")
	_, _, dropErr := b.DeleteByPrefix("other:")
	require.NoError(t, dropErr, "a namespace reset retires no generation, so an old reader cannot block it")
	require.Equal(t, retireBefore, b.labelIndex.retireBeforeTs.Load(),
		"only a full rebuild moves the retire point")
	require.Len(t, r4LabelIDs(t, b), nodeCount,
		"the reset keeps the other namespace's ready generation")

	waitStarted := make(chan struct{})
	b.labelIndex.beforeRetireWait = func() { close(waitStarted) }
	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	secondGeneration := b.labelIndex.markerGeneration.Load()
	go func() {
		_, rebuildErr := b.rebuildLabelIndex(ctx)
		secondDone <- rebuildErr
	}()
	r4Wait(t, waitStarted, "retired-generation bound")
	cancel()
	require.ErrorIs(t, <-secondDone, context.Canceled)
	b.labelIndex.beforeRetireWait = nil
	require.Equal(t, secondGeneration, b.labelIndex.markerGeneration.Load(),
		"a second rebuild cannot retire another generation while the first is pinned")

	oldReader.Close()
	oldReaderTxn.Discard()
	b.db.endRead(oldReadTs)
	readerOpen = false
	b.db.advanceDiscardTs()
	b.db.oracle.mu.Lock()
	discardAfterRelease := b.db.oracle.discardTs
	b.db.oracle.mu.Unlock()
	require.GreaterOrEqual(t, discardAfterRelease, retireBefore,
		"the old generation becomes discardable as soon as its final reader closes")
	_, err = b.rebuildLabelIndex(context.Background())
	require.NoError(t, err)
	require.Greater(t, b.labelIndex.markerGeneration.Load(), secondGeneration)
}
