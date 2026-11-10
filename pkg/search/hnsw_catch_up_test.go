package search

// Author: hnsw-stall implementation lane (Claude Opus 5.5), 9 Nov 2026.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// catchUpPrompt is how long a write or search may take while a graph
	// catches up. The catch-up itself is held for as long as the test needs.
	catchUpPrompt = 2 * time.Second
	// catchUpEvent is how long a test waits for a build to reach the point it
	// is held at, or to end once released.
	catchUpEvent = 30 * time.Second
)

// awaitEvent receives from ch within catchUpEvent.
func awaitEvent[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(catchUpEvent):
		t.Fatalf("%s did not happen within %s", what, catchUpEvent)
		panic("unreachable")
	}
}

// awaitHeld waits until a build is held at point. The build ending first (its
// result arrives on ended, which may be nil) fails the test.
func awaitHeld(t *testing.T, point string, held <-chan struct{}, ended <-chan error) {
	t.Helper()
	select {
	case <-held:
	case err := <-ended:
		t.Fatalf("the build ended before %s: %v", point, err)
	case <-time.After(catchUpEvent):
		t.Fatalf("the build did not reach %s within %s", point, catchUpEvent)
	}
}

// catchUpCase records changes while a graph is constructed, then changes some
// of the same keys again while the graph catches up with them:
//   - "updated": recorded update; "added": recorded add; "removed": recorded remove;
//   - "readded": recorded remove, then re-added during the catch-up;
//   - "dropped": recorded add, then removed during the catch-up;
//   - "late": added during the catch-up only.
//
// Everything a case holds or starts belongs to its own service and test: the
// holds are set on that service, and the goroutines it starts (run) are
// joined before the test ends, whatever its outcome.
type catchUpCase struct {
	ctx    context.Context // the test's; cancelled when it ends
	svc    *Service
	engine storage.Engine
	// vectors is what the index must hold. Only the test goroutine uses it.
	vectors map[string][]float32
	ops     sync.WaitGroup
	// steps counts the catch-up steps the service's constructions have begun
	// since holdCatchUpAt was called.
	steps atomic.Int32
}

func newCatchUpCase(t *testing.T) *catchUpCase {
	t.Helper()
	servingTestEnv(t)
	c := &catchUpCase{ctx: t.Context(), engine: newNamespacedEngine(t), vectors: map[string][]float32{}}
	c.svc = NewService(c.engine, 3, nil)
	// Cleanups run last-registered first. The holds a test sets later are
	// released first; then the service closes, which cancels a construction
	// still running; then the goroutines the test started are joined; the
	// engine they use (newNamespacedEngine's cleanup) closes last.
	t.Cleanup(func() {
		joined := make(chan struct{})
		go func() { c.ops.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(catchUpEvent):
			t.Errorf("a goroutine the test started is still running %s after its service closed", catchUpEvent)
		}
	})
	t.Cleanup(func() { _ = c.svc.Close() })
	for i := 0; i < 40; i++ {
		c.put(t, fmt.Sprintf("seed-%02d", i), catchUpSeed(i))
	}
	for _, id := range []string{"updated", "removed", "readded"} {
		c.put(t, id, []float32{-1, 0.5, float32(len(id))})
	}
	return c
}

// run starts fn on a goroutine the case joins, and returns its result.
func (c *catchUpCase) run(fn func() error) <-chan error {
	done := make(chan error, 1)
	c.ops.Go(func() { done <- fn() })
	return done
}

// requirePrompt runs fn, which must return within catchUpPrompt.
func (c *catchUpCase) requirePrompt(t *testing.T, what string, fn func() error) {
	t.Helper()
	select {
	case err := <-c.run(fn):
		require.NoError(t, err, what)
	case <-time.After(catchUpPrompt):
		t.Fatalf("%s waited for the HNSW catch-up", what)
	}
}

// store writes id's vector to the engine and the index. It asserts nothing
// and touches none of the case's own state, so it may run off the test
// goroutine.
func (c *catchUpCase) store(id string, vec []float32) error {
	node := &storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{vec}}
	if _, err := c.engine.GetNode(c.ctx, node.ID); err == nil {
		if err := c.engine.UpdateNode(c.ctx, storage.NodeUpdateRequest{After: node}); err != nil {
			return err
		}
	} else if _, err := c.engine.CreateNode(c.ctx, node); err != nil {
		return err
	}
	return c.svc.IndexNode(node)
}

// drop is store for a removal.
func (c *catchUpCase) drop(id string) error {
	if err := c.engine.DeleteNode(c.ctx, storage.NodeID(id)); err != nil {
		return err
	}
	return c.svc.RemoveNode(storage.NodeID(id))
}

func (c *catchUpCase) put(t *testing.T, id string, vec []float32) {
	t.Helper()
	require.NoError(t, c.store(id, vec))
	c.vectors[id] = vec
}

func (c *catchUpCase) remove(t *testing.T, id string) {
	t.Helper()
	require.NoError(t, c.drop(id))
	delete(c.vectors, id)
}

// holdConstruction holds the service's next construction once it has taken its
// vector snapshot (of an in-memory index), from where every change is
// recorded for the catch-up, until release is called. held is closed when the
// construction is there.
func (c *catchUpCase) holdConstruction(t *testing.T) (held <-chan struct{}, release func()) {
	t.Helper()
	hold := &maintenanceTransitionLog{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release = func() { once.Do(func() { close(hold.release) }) }
	t.Cleanup(release)
	c.svc.SetLogger(slog.New(fixtureowner.RawLogHandler(hold.Handle)))
	return hold.entered, release
}

// holdCatchUpAt holds the step-th catch-up step (1-based) of the service's
// constructions until release is called or the construction is cancelled:
// step 1 comes before any change is applied, step 2 after the first. held is
// closed when the catch-up is there. Call it before the construction starts.
func (c *catchUpCase) holdCatchUpAt(t *testing.T, step int) (held <-chan struct{}, release func()) {
	t.Helper()
	begun, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hook := func(ctx context.Context) {
		if int(c.steps.Add(1)) != step {
			return
		}
		close(begun)
		select {
		case <-released:
		case <-ctx.Done():
		}
	}
	c.svc.hnswCatchUp.beforeStep.Store(&hook)
	release = func() { once.Do(func() { close(released) }) }
	t.Cleanup(release)
	return begun, release
}

// eachCatchUpHoldPoint runs test with the catch-up held before it applied any
// change (step 1) and after it applied the first (step 2), so writes made
// meanwhile supersede both a pending change and one already applied.
func eachCatchUpHoldPoint(t *testing.T, test func(t *testing.T, step int)) {
	t.Run("before the first change", func(t *testing.T) { test(t, 1) })
	t.Run("after the first change", func(t *testing.T) { test(t, 2) })
}

// recordDuringConstruction makes the changes a construction records.
func (c *catchUpCase) recordDuringConstruction(t *testing.T) {
	t.Helper()
	// Opposite to the vector the snapshot holds for it, so a graph that kept
	// the old one is far from the new one.
	c.put(t, "updated", []float32{1, -0.5, -7})
	c.put(t, "added", []float32{0, 1, 0})
	c.put(t, "dropped", []float32{0.2, 0.9, 0.1})
	c.remove(t, "removed")
	c.remove(t, "readded")
}

// changeDuringCatchUp makes the changes that reach the graph during its
// catch-up; each must return without waiting for it.
func (c *catchUpCase) changeDuringCatchUp(t *testing.T) {
	t.Helper()
	readded, late := []float32{1, 0, 0}, []float32{0.5, 0.5, 0.5}
	c.requirePrompt(t, "re-adding a vector removed during construction", func() error { return c.store("readded", readded) })
	c.vectors["readded"] = readded
	c.requirePrompt(t, "removing a vector added during construction", func() error { return c.drop("dropped") })
	delete(c.vectors, "dropped")
	c.requirePrompt(t, "adding a new vector", func() error { return c.store("late", late) })
	c.vectors["late"] = late
	minimum := -1.0
	c.requirePrompt(t, "a vector search", func() error {
		_, err := c.svc.Search(c.ctx, "", []float32{1, 0, 0}, &SearchOptions{Limit: 3, MinSimilarity: &minimum})
		return err
	})
}

// serving is the graph the service serves, or nil.
func (c *catchUpCase) serving() *HNSWIndex {
	c.svc.hnswMu.RLock()
	defer c.svc.hnswMu.RUnlock()
	return c.svc.servingHNSW()
}

// catchingUp reports whether a constructed graph is being caught up.
func (c *catchUpCase) catchingUp() bool {
	c.svc.hnswCatchUp.mu.Lock()
	defer c.svc.hnswCatchUp.mu.Unlock()
	return c.svc.hnswCatchUp.target != nil
}

// requireGraphHoldsEveryChange requires graph to hold exactly the keys the
// case's changes left, each with its own current vector (hnswGraphMismatch).
func (c *catchUpCase) requireGraphHoldsEveryChange(t *testing.T, graph *HNSWIndex) {
	t.Helper()
	require.NoError(t, hnswGraphMismatch(graph, c.vectors))
}

// A graph's first construction catches up with the changes recorded while it
// was built outside the mutation locks: writes and searches made meanwhile do
// not wait, and the newest change of each key wins over the recorded one,
// whether that one is still pending or already applied. The graph is
// published only when it holds every change.
func TestHNSWInitialBuildCatchUpDoesNotHoldWriters(t *testing.T) {
	eachCatchUpHoldPoint(t, testHNSWInitialBuildCatchUp)
}

func testHNSWInitialBuildCatchUp(t *testing.T, step int) {
	c := newCatchUpCase(t)
	constructing, construct := c.holdConstruction(t)
	catching, catchUp := c.holdCatchUpAt(t, step)
	built := c.run(func() error {
		_, err := c.svc.getOrCreateHNSWIndex(c.ctx, 3)
		return err
	})
	awaitHeld(t, "its construction", constructing, built)
	c.recordDuringConstruction(t)
	construct()
	awaitHeld(t, "its catch-up", catching, built)
	require.True(t, c.catchingUp(), "no catch-up is running")
	c.changeDuringCatchUp(t)
	require.Nil(t, c.serving(), "a graph was published before it caught up")
	catchUp()
	require.NoError(t, awaitEvent(t, "the construction's end", built))
	require.False(t, c.svc.hnswRebuildInFlight.Load())
	graph := c.serving()
	c.requireGraphHoldsEveryChange(t, graph)
	require.False(t, c.catchingUp(), "the published graph is still being caught up")
	// After publication, changes reach the published graph directly.
	c.put(t, "after", []float32{0.3, -1, 0.2})
	c.requireGraphHoldsEveryChange(t, graph)
}

// A tombstone rebuild catches up the same way. The old graph keeps serving
// and receives every change until the rebuilt graph replaces it.
func TestHNSWRebuildCatchUpDoesNotHoldWriters(t *testing.T) {
	eachCatchUpHoldPoint(t, testHNSWRebuildCatchUp)
}

func testHNSWRebuildCatchUp(t *testing.T, step int) {
	c := newCatchUpCase(t)
	old, err := c.svc.getOrCreateHNSWIndex(c.ctx, 3)
	require.NoError(t, err)
	c.remove(t, "seed-00") // a tombstone to rebuild
	constructing, construct := c.holdConstruction(t)
	catching, catchUp := c.holdCatchUpAt(t, step)
	rebuilt := c.run(func() error { return c.svc.maybeRebuildHNSW(c.ctx, 0, 1, time.Nanosecond) })
	awaitHeld(t, "its construction", constructing, rebuilt)
	c.recordDuringConstruction(t)
	construct()
	awaitHeld(t, "its catch-up", catching, rebuilt)
	require.True(t, c.catchingUp(), "no catch-up is running")
	c.changeDuringCatchUp(t)
	require.Same(t, old, c.serving(), "the old graph must serve until the rebuilt one caught up")
	// The serving graph received every change made during the construction
	// and the catch-up.
	c.requireGraphHoldsEveryChange(t, old)
	catchUp()
	require.NoError(t, awaitEvent(t, "the rebuild's end", rebuilt))
	graph := c.serving()
	require.NotSame(t, old, graph)
	c.requireGraphHoldsEveryChange(t, graph)
	require.False(t, c.svc.hnswRebuildInFlight.Load())
}

// Publication waits for a writer's insert in flight before it takes the
// service's two locks, not while it holds them. A recorded change is taken as
// a paying writer takes it, and kept in flight. While the publication waits
// for it, both locks can be taken and a search returns; nothing is published
// until the insert completes.
func TestHNSWPublicationWaitsForInFlightInsertOutsideServiceLocks(t *testing.T) {
	c := newCatchUpCase(t)
	constructing, construct := c.holdConstruction(t)
	catching, catchUp := c.holdCatchUpAt(t, 1)
	built := c.run(func() error {
		_, err := c.svc.getOrCreateHNSWIndex(c.ctx, 3)
		return err
	})
	awaitHeld(t, "its construction", constructing, built)
	c.recordDuringConstruction(t)
	construct()
	awaitHeld(t, "its catch-up", catching, built)
	pending := &c.svc.hnswCatchUp
	pending.mu.Lock()
	target := pending.target
	id, value, ok := pending.popLocked()
	pending.mu.Unlock()
	require.True(t, ok, "the catch-up has no recorded change to take")
	require.NotNil(t, target)
	var once sync.Once
	complete := func() {
		once.Do(func() {
			err := applyCatchUpChange(target, id, value)
			pending.mu.Lock()
			pending.completeLocked(id, err)
			pending.mu.Unlock()
		})
	}
	// On a failed assertion the insert still completes, so a publication
	// waiting for it goes on and the service can close.
	t.Cleanup(complete)
	catchUp()
	// The construction applies the other changes and starts to publish: it
	// stops new inserts and waits for the one in flight.
	require.Eventually(t, func() bool {
		pending.mu.Lock()
		defer pending.mu.Unlock()
		return pending.publishing && len(pending.inFlight) == 1
	}, catchUpEvent, time.Millisecond, "the publication did not start to wait for the insert in flight")
	// Both locks are taken, in the service's order, only to show they are free.
	locked := c.run(func() error {
		c.svc.pipelineMu.Lock()
		c.svc.indexMu.Lock()
		c.svc.indexMu.Unlock()
		c.svc.pipelineMu.Unlock()
		return nil
	})
	select {
	case <-locked:
	case <-time.After(catchUpPrompt):
		t.Fatal("the publication holds the service's locks while it waits for an insert in flight")
	}
	minimum := -1.0
	c.requirePrompt(t, "a vector search during the publication's wait", func() error {
		_, err := c.svc.Search(c.ctx, "", []float32{1, 0, 0}, &SearchOptions{Limit: 3, MinSimilarity: &minimum})
		return err
	})
	select {
	case err := <-built:
		t.Fatalf("the construction ended with an insert still in flight: %v", err)
	default:
	}
	require.Nil(t, c.serving(), "a graph was published with an insert still in flight")
	complete()
	require.NoError(t, awaitEvent(t, "the construction's end", built))
	c.requireGraphHoldsEveryChange(t, c.serving())
}

// Closing the service, or clearing its vectors, while a graph catches up stops
// the catch-up instead of waiting for it. Nothing is published, and the next
// construction starts from the vector store, so no change is lost.
func TestHNSWCatchUpStopsWhenItsOwnerStops(t *testing.T) {
	for _, owner := range []string{"close", "clear"} {
		t.Run(owner, func(t *testing.T) {
			c := newCatchUpCase(t)
			constructing, construct := c.holdConstruction(t)
			catching, _ := c.holdCatchUpAt(t, 1)
			_, job, err := c.svc.startHNSWBuild(3)
			require.NoError(t, err)
			require.NotNil(t, job)
			awaitHeld(t, "its construction", constructing, nil)
			c.recordDuringConstruction(t)
			construct()
			awaitHeld(t, "its catch-up", catching, nil)
			c.requirePrompt(t, "stopping the owner", func() error {
				if owner == "close" {
					return c.svc.Close()
				}
				return c.svc.ClearVectorIndex()
			})
			awaitEvent(t, "the construction's end", job.built.Done())
			require.ErrorIs(t, job.built.Err(), context.Canceled)
			// The catch-up was held at its first step, with every recorded
			// change still to apply. It began no other step once its owner
			// stopped, so it ended within the insert it was in.
			require.EqualValues(t, 1, c.steps.Load(), "the catch-up went on after its owner stopped")
			require.Nil(t, c.serving())
			require.False(t, c.svc.hnswRebuildInFlight.Load())
			require.False(t, c.catchingUp())
			if owner == "clear" {
				// The catch-up hold acts on its own step only, so the next
				// construction runs through.
				c.vectors = map[string][]float32{}
				c.put(t, "after", []float32{0, 1, 1})
				graph, err := c.svc.getOrCreateHNSWIndex(c.ctx, 3)
				require.NoError(t, err)
				c.requireGraphHoldsEveryChange(t, graph)
			}
		})
	}
}

func catchUpStoredVector(t *testing.T, graph *HNSWIndex, id string) []float32 {
	t.Helper()
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	internalID, ok := graph.idToInternal[id]
	require.True(t, ok, "the graph does not hold %s", id)
	return append([]float32(nil), graph.vectorAtLocked(internalID)...)
}

func drainCatchUp(t *testing.T, c *hnswCatchUp) {
	t.Helper()
	for {
		more, err := c.applyNext()
		require.NoError(t, err)
		if !more {
			return
		}
	}
}

// A live change of a key whose recorded change was already applied queues the
// key again, so the graph ends with the newest value; a live removal wins the
// same way. Recording never touches the graph; paying does. The graph here
// stores its vectors, so the value it holds is observable.
func TestHNSWCatchUpNewestChangeWins(t *testing.T) {
	graph := NewHNSWIndex(2, DefaultHNSWConfig())
	for id, vec := range map[string][]float32{"a": {1, 0}, "gone": {1, 1}, "kept": {0, 1}} {
		require.NoError(t, graph.Add(id, vec))
	}
	var c hnswCatchUp
	require.Equal(t, 2, c.begin(graph, map[string][]float32{"a": {0, 1}}, map[string]struct{}{"gone": {}}))
	for i := 0; i < 2; i++ { // the recorded removal, then the recorded update
		more, err := c.applyNext()
		require.NoError(t, err)
		require.True(t, more)
	}
	require.InDeltaSlice(t, []float32{0, 1}, catchUpStoredVector(t, graph, "a"), 1e-6)
	mark := c.owedMark()
	newest := vector.Normalize([]float32{1, -1})
	require.True(t, c.liveUpdate("a", newest))
	require.True(t, c.liveRemove("kept"))
	added := vector.Normalize([]float32{-1, 0})
	require.True(t, c.liveUpdate("new", added))
	owed := c.owedMark() - mark
	require.EqualValues(t, 3, owed)
	// Recorded only: the graph is unchanged until the writer pays.
	require.InDeltaSlice(t, []float32{0, 1}, catchUpStoredVector(t, graph, "a"), 1e-6)
	require.True(t, hnswHoldsLive(graph, "kept"))
	require.False(t, hnswHoldsLive(graph, "new"))
	c.pay(owed)
	require.Equal(t, 0, c.pending(), "the writer paid for every change it recorded")
	require.InDeltaSlice(t, newest, catchUpStoredVector(t, graph, "a"), 1e-6)
	require.False(t, hnswHoldsLive(graph, "kept"))
	require.False(t, hnswHoldsLive(graph, "gone"))
	require.True(t, hnswHoldsLive(graph, "new"))
	require.InDeltaSlice(t, added, catchUpStoredVector(t, graph, "new"), 1e-6, "the payment applied another vector than the one recorded")
	finished, err := c.finishLocked()
	require.NoError(t, err)
	require.True(t, finished)
	require.False(t, c.liveUpdate("late", vector.Normalize([]float32{0, -1})), "a finished catch-up takes no changes")
}

// A newer change of a key whose insert is in flight is not taken by anyone
// until that insert is done; it is queued again then and applied after it, so
// the newer value wins even when two inserts of the key would race.
func TestHNSWCatchUpKeyInFlightIsAppliedInOrder(t *testing.T) {
	graph := NewHNSWIndex(2, DefaultHNSWConfig())
	var c hnswCatchUp
	older := vector.Normalize([]float32{1, 0})
	c.begin(graph, map[string][]float32{"x": older}, nil)
	// Take the recorded change as an insert would, without applying it yet.
	c.mu.Lock()
	id, value, ok := c.popLocked()
	c.mu.Unlock()
	require.True(t, ok)
	require.Equal(t, "x", id)
	newer := vector.Normalize([]float32{0, 1})
	require.True(t, c.liveUpdate("x", newer))
	more, err := c.step()
	require.NoError(t, err)
	require.False(t, more, "the newer change was taken while its key was in flight")
	require.NoError(t, applyCatchUpChange(graph, id, value))
	c.mu.Lock()
	c.completeLocked(id, nil)
	c.mu.Unlock()
	c.pay(1)
	require.InDeltaSlice(t, newer, catchUpStoredVector(t, graph, "x"), 1e-6)
	finished, err := c.finishLocked()
	require.NoError(t, err)
	require.True(t, finished)
}

// Publication applies at most the publication tail under the write locks:
// with more pending (changes recorded by writers that have not paid yet, or a
// backlog the job has not reached), it refuses and applies nothing. With no
// more than the tail pending, it applies them and finishes.
func TestHNSWCatchUpPublishesOnlyABoundedRemainder(t *testing.T) {
	graph := NewHNSWIndex(2, DefaultHNSWConfig())
	var c hnswCatchUp
	recorded := map[string][]float32{}
	for i := 0; i < hnswCatchUpPublishTail+8; i++ {
		recorded[fmt.Sprintf("r-%d", i)] = vector.Normalize([]float32{1, float32(i)})
	}
	recordedCount := c.begin(graph, recorded, nil) // the catch-up owns the map now
	applied, added := 0, 0
	applyOne := func() {
		t.Helper()
		more, err := c.applyNext()
		require.NoError(t, err)
		require.True(t, more)
		applied++
	}
	requireRefused := func(why string) {
		t.Helper()
		finished, err := c.finishLocked()
		require.NoError(t, err)
		require.False(t, finished, why)
		require.Equal(t, applied, graph.Size(), "a refused publication applied changes")
	}
	// Writers record two changes for each one the job applies, so the pending
	// set stays above the tail.
	for round := 0; round < 20; round++ {
		for i := 0; i < 2; i++ {
			require.True(t, c.liveUpdate(fmt.Sprintf("live-%d", added), vector.Normalize([]float32{float32(added + 1), 1})))
			added++
		}
		require.Equal(t, applied, graph.Size(), "recording reached the graph")
		applyOne()
		requireRefused("publication took a backlog larger than its tail")
	}
	// The job drains the set to one change more than the tail: still refused.
	for c.pending() > hnswCatchUpPublishTail+1 {
		applyOne()
	}
	requireRefused("publication took one change more than its tail")
	applyOne()
	require.Equal(t, hnswCatchUpPublishTail, c.pending())
	finished, err := c.finishLocked()
	require.NoError(t, err)
	require.True(t, finished, "publication refused a backlog no larger than its tail")
	require.Equal(t, recordedCount+added, graph.Size(), "publication did not apply its tail")
	require.Equal(t, 0, c.pending())
}
