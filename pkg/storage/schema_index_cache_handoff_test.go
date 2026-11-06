package storage

// Focused regression coverage for the sorted-key cache handoff in
// rLockWithFreshKeys (#1693). A reader that rebuilt the cache under the index
// write lock must keep the cache it published through the read hold it returns,
// so the caller never observes a cache another writer dirtied in between.

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testlatch"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rebuildGateContext blocks inside the index cache rebuild at a chosen Err
// poll. sortedKeysViewLocked polls the context once per index key while it
// holds the index write lock, so the poll is a fixed control point inside the
// locked rebuild instead of a timing window. Err delegates to the wrapped
// context after the block, so cancellation still reaches the rebuild.
type rebuildGateContext struct {
	context.Context
	blockAt int32
	polls   atomic.Int32
	reached chan struct{}
	release *testlatch.Latch
}

func newRebuildGateContext(parent context.Context, blockAt int32) *rebuildGateContext {
	return &rebuildGateContext{
		Context: parent,
		blockAt: blockAt,
		reached: make(chan struct{}),
		release: testlatch.New(),
	}
}

func (c *rebuildGateContext) Err() error {
	if c.polls.Add(1) == c.blockAt {
		close(c.reached)
		c.release.Wait()
	}
	return c.Context.Err()
}

// rebuildGatePoll is the Err poll that sortedKeysViewLocked makes before it
// starts collecting keys: the first RLockContext poll, the write LockContext
// poll, then the rebuild's own admission poll.
const rebuildGatePoll = 3

// handoffJoinTimeout bounds every teardown join in this file. A joined
// goroutine is the only proof that teardown left no work behind, but an
// unbounded join turns one failed assertion into a deadlocked package run, so
// teardown reports the stranded goroutine and carries on instead.
const handoffJoinTimeout = 10 * time.Second

// joinedWithin reports whether done closed within handoffJoinTimeout. It never
// blocks past that bound, so teardown always reaches its remaining steps.
func joinedWithin(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-time.After(handoffJoinTimeout):
		return false
	}
}

func handoffTestIndex(keys int) *PropertyIndex {
	idx := &PropertyIndex{values: make(map[interface{}][]NodeID, keys)}
	for i := 0; i < keys; i++ {
		idx.values[int64(i)] = []NodeID{NodeID(fmt.Sprintf("nornic:n%d", i))}
	}
	idx.keysDirty = true
	return idx
}

func awaitedSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal(what)
	}
}

// observeIndexAdmission reports goroutines admitted or waiting on the index
// lock, so a test can wait for a writer to queue behind a held index lock.
func observeIndexAdmission(t *testing.T, idx *PropertyIndex) <-chan struct{} {
	t.Helper()
	queued := make(chan struct{}, 16)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner != &idx.mu {
			return
		}
		select {
		case queued <- struct{}{}:
		default:
		}
	}))
	return queued
}

// gatedHandoff runs one rLockWithFreshKeys call that blocks inside the cache
// rebuild, and can queue one writer behind the index lock that call takes.
//
// Its teardown is registered before the call starts and releases everything in
// the only order that cannot deadlock: the rebuild gate and the writer release
// first, then the reader joins, then the read hold a successful call returned
// is released - that release is what lets the queued writer run - and finally
// the writer joins. Every join is bounded, so a failed assertion reports a
// stranded goroutine instead of hanging the run, and the gate, the writer
// release and the read hold are each given back at most once, whichever of
// teardown and the test body gets there first.
type gatedHandoff struct {
	idx  *PropertyIndex
	gate *rebuildGateContext

	readerJoined   chan struct{} // closed when the gated call returns
	readerResult   chan error    // buffered; the call's error, read once
	readerOwnsHold atomic.Bool   // the successful call returned a read hold

	writerAcquired chan struct{} // closed when the queued writer holds the lock
	writerRelease  *testlatch.Latch
	writerJoined   chan struct{}
	writerStarted  bool

	holdOnce sync.Once
}

// newGatedHandoff prepares a handoff over idx and registers its teardown before
// the gated call starts: no assertion failure inside the test can strand the
// gate, the read hold, the queued writer or either goroutine.
func newGatedHandoff(t *testing.T, idx *PropertyIndex, gate *rebuildGateContext) *gatedHandoff {
	t.Helper()
	h := &gatedHandoff{
		idx:            idx,
		gate:           gate,
		readerJoined:   make(chan struct{}),
		readerResult:   make(chan error, 1),
		writerAcquired: make(chan struct{}),
		writerRelease:  testlatch.New(),
		writerJoined:   make(chan struct{}),
	}
	t.Cleanup(func() { h.teardown(t) })
	return h
}

// startReader launches the gated call. The result is buffered and published
// before readerJoined closes, so a test that only waits for the join never
// consumes the error twice.
func (h *gatedHandoff) startReader() {
	go func() {
		defer close(h.readerJoined)
		err := h.idx.rLockWithFreshKeys(h.gate)
		if err == nil {
			// A successful call returns the read hold it expects its caller to
			// release; teardown has to release that hold exactly once.
			h.readerOwnsHold.Store(true)
		}
		h.readerResult <- err
	}()
}

// queueWriter starts the writer that dirties the cache once it acquires the
// index lock and then waits for releaseWriter.
func (h *gatedHandoff) queueWriter() {
	h.writerStarted = true
	go func() {
		defer close(h.writerJoined)
		h.idx.mu.Lock()
		h.idx.keysDirty = true
		close(h.writerAcquired)
		h.writerRelease.Wait()
		h.idx.mu.Unlock()
	}()
}

func (h *gatedHandoff) releaseGate() {
	h.gate.release.Release()
}

func (h *gatedHandoff) releaseWriter() {
	h.writerRelease.Release()
}

// releaseHold gives back the read hold a successful call returned, at most
// once. A call that returned an error owns no hold, so there is nothing to
// release.
func (h *gatedHandoff) releaseHold() {
	h.holdOnce.Do(func() {
		if h.readerOwnsHold.Load() {
			h.idx.mu.RUnlock()
		}
	})
}

// teardown releases the handoff in deadlock-free order and reports, rather than
// hangs on, a goroutine that does not return.
func (h *gatedHandoff) teardown(t *testing.T) {
	t.Helper()
	h.releaseGate()
	h.releaseWriter()
	if !joinedWithin(h.readerJoined) {
		t.Error("the gated reader did not return after its rebuild gate was released")
	}
	h.releaseHold()
	if h.writerStarted && !joinedWithin(h.writerJoined) {
		t.Error("the queued writer did not finish after the read hold was released")
	}
}

// TestPropertyIndexCacheRebuildHandsOffReadHold queues a writer behind the
// reader's cache rebuild. The reader must return holding the read lock on the
// cache it rebuilt; releasing that write lock after the rebuild lets the
// queued writer dirty the cache first and starves the reader under continuous
// writes.
func TestPropertyIndexCacheRebuildHandsOffReadHold(t *testing.T) {
	idx := handoffTestIndex(4)
	queued := observeIndexAdmission(t, idx)

	gate := newRebuildGateContext(t.Context(), rebuildGatePoll)
	handoff := newGatedHandoff(t, idx, gate)
	handoff.startReader()
	awaitedSignal(t, gate.reached, "the reader never reached the cache rebuild")
	acquired := idx.mu.TryLock()
	if acquired {
		idx.mu.Unlock()
	}
	require.False(t, acquired, "the cache rebuild must hold the index write lock")

	handoff.queueWriter()
	awaitedSignal(t, queued, "the writer never queued behind the cache rebuild")
	// Let the rebuild finish while the writer stays queued behind it.
	handoff.releaseGate()

	select {
	case <-handoff.readerJoined:
		require.NoError(t, <-handoff.readerResult)
	case <-handoff.writerAcquired:
		t.Fatal("the reader released the rebuilt cache: the queued writer dirtied it before the reader returned")
	case <-time.After(10 * time.Second):
		t.Fatal("neither the reader nor the queued writer made progress")
	}
	// The reader still holds the read lock on the cache it published: the
	// queued writer cannot dirty it, and the caller sees every rebuilt key.
	require.False(t, idx.keysDirty, "the returned read hold must expose a fresh cache")
	require.Equal(t, []interface{}{int64(0), int64(1), int64(2), int64(3)}, idx.sortedNonNilKeys)
	select {
	case <-handoff.writerAcquired:
		t.Fatal("a queued writer acquired the index while the reader held the rebuilt cache")
	default:
	}
	handoff.releaseHold()
	awaitedSignal(t, handoff.writerAcquired, "the queued writer did not acquire the index after the reader released it")
	handoff.releaseWriter()
	awaitedSignal(t, handoff.writerJoined, "the queued writer did not finish after it was released")
}

// TestPropertyIndexCacheRebuildCancellationReleasesWriteHold keeps the rebuild
// error path: a canceled rebuild must publish no cache and release the index
// write lock instead of handing a hold to its caller.
func TestPropertyIndexCacheRebuildCancellationReleasesWriteHold(t *testing.T) {
	idx := handoffTestIndex(4)
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	gate := newRebuildGateContext(parent, rebuildGatePoll)
	handoff := newGatedHandoff(t, idx, gate)
	handoff.startReader()
	awaitedSignal(t, gate.reached, "the reader never reached the cache rebuild")
	cancel()
	handoff.releaseGate()

	var err error
	select {
	case <-handoff.readerJoined:
		err = <-handoff.readerResult
	case <-time.After(10 * time.Second):
		t.Fatal("a canceled cache rebuild did not return")
	}
	if err == nil {
		// A rebuild that returned a hold instead of its error is the failure
		// under test: give that hold back before the failing assertion so
		// teardown cannot release a hold this test already released.
		handoff.releaseHold()
	}
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, idx.mu.TryLock(), "a canceled cache rebuild must release the index write lock")
	idx.mu.Unlock()
	require.True(t, idx.keysDirty, "a canceled cache rebuild must not publish a partial cache")
	require.Nil(t, idx.sortedNonNilKeys)
}

// The kernel holds the downgrade test can own: the write lock it takes first,
// the read hold the downgrade leaves it, and no hold once it gives that back.
const (
	kernelHoldNone = iota
	kernelHoldWrite
	kernelHoldRead
)

// TestContextRWMutexDowngradeKeepsQueuedWriterWaiting pins the kernel
// transition the cache handoff relies on: one atomic write-to-read step that
// keeps the caller's hold, keeps the queued writer queued and keeps new
// readers out until the downgraded hold is released.
func TestContextRWMutexDowngradeKeepsQueuedWriterWaiting(t *testing.T) {
	m := &contextRWMutex{}
	queued := make(chan struct{}, 16)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner != m {
			return
		}
		select {
		case queued <- struct{}{}:
		default:
		}
	}))
	// The test is the single owner of one kernel hold and gives it back at most
	// once, from the body or from teardown, so a failed assertion cannot leave
	// the queued writer waiting on a hold nobody releases. Both run on the test
	// goroutine, so the mode needs no further synchronization.
	held := kernelHoldNone
	releaseHold := func() {
		switch held {
		case kernelHoldWrite:
			m.Unlock()
		case kernelHoldRead:
			m.RUnlock()
		}
		held = kernelHoldNone
	}
	writerAcquired, writerDone := make(chan struct{}), make(chan struct{})
	writerStarted := false
	t.Cleanup(func() {
		releaseHold()
		if writerStarted && !joinedWithin(writerDone) {
			t.Error("the queued writer did not finish after the kernel hold was released")
		}
	})
	m.Lock()
	held = kernelHoldWrite
	writerStarted = true
	go func() {
		defer close(writerDone)
		m.Lock()
		close(writerAcquired)
		m.Unlock()
	}()
	awaitedSignal(t, queued, "the writer never queued behind the write hold")
	m.downgrade()
	held = kernelHoldRead
	// Read the kernel state under its own mutex and leave that mutex before the
	// assertions: a failing assertion must not Goexit while holding the
	// internal mutex the queued writer needs to make progress.
	m.mu.Lock()
	writer, readers, waitingWriters := m.writer, m.readers, m.waitingWriters
	m.mu.Unlock()
	require.False(t, writer, "the downgraded hold must not keep the write lock")
	require.Equal(t, 1, readers, "the downgraded hold must hold exactly one reader")
	require.Equal(t, 1, waitingWriters, "the queued writer must stay queued")
	writeAcquired := m.TryLock()
	if writeAcquired {
		m.Unlock()
	}
	require.False(t, writeAcquired, "a downgraded hold must keep the write lock out")
	readAcquired := m.TryRLock()
	if readAcquired {
		m.RUnlock()
	}
	require.False(t, readAcquired, "a queued writer must keep new readers out")
	select {
	case <-writerAcquired:
		t.Fatal("a queued writer acquired the mutex during the downgraded read hold")
	default:
	}
	releaseHold()
	awaitedSignal(t, writerAcquired, "the queued writer did not acquire the mutex after the downgraded hold was released")
	awaitedSignal(t, writerDone, "the queued writer did not finish after the downgraded hold was released")
}

// BenchmarkPropertyIndexSortedRead measures the public ordered read a range or
// top-k scan takes through the sorted-key cache: one that finds the cache
// current, and one that must rebuild it after a write.
func BenchmarkPropertyIndexSortedRead(b *testing.B) {
	const keys = 10000
	sm := NewSchemaManager()
	require.NoError(b, sm.AddPropertyIndex("bench_item_t", "Item", []string{"t"}))
	for i := 0; i < keys; i++ {
		sm.PropertyIndexInsert("Item", "t", NodeID(fmt.Sprintf("nornic:n%d", i)), int64(i))
	}
	idx, ok := sm.GetCompositeIndex("bench_item_t")
	require.True(b, ok)
	ctx := b.Context()
	if _, err := sm.PropertyIndexTopK(ctx, "Item", "t", 5, true); err != nil {
		b.Fatal(err)
	}
	b.Run("fresh", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := sm.PropertyIndexTopK(ctx, "Item", "t", 5, true); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("rebuild", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// The write a read follows: the write lock dirties the cache.
			idx.mu.Lock()
			idx.keysDirty = true
			idx.mu.Unlock()
			if _, err := sm.PropertyIndexTopK(ctx, "Item", "t", 5, true); err != nil {
				b.Fatal(err)
			}
		}
	})
}
