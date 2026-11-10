package search

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// hnswCatchUp brings a constructed, not yet published HNSW graph up to date
// with the vector changes made while it was built.
//
// It keeps the changes still to apply, one per key (the newest vector, or a
// removal). A live write made meanwhile (hnswUpdateLive, hnswRemoveLive) only
// replaces its key's pending change under indexMu, a map update under mu, and
// counts it as owed. Its writer pays once it holds neither indexMu nor the
// node's mutation lane, so nobody waiting for either waits for the payment:
// it applies as many pending changes as it recorded (pay), outside every
// Service lock: one graph update or removal per recorded change, about the
// work the same write does on a published graph (a single-vector rewrite
// records up to three: its entries' removals and the new vector). The
// construction job drains the rest, also outside every lock. A key being
// inserted is marked in flight, and nobody else takes it until that insert
// is done, so the newest change of each key is the last the graph receives.
//
// A writer thus never waits for the backlog: it waits under indexMu for at
// most a map update, and outside it for its own inserts, behind at most the
// inserts already in progress on the graph (the job's and other writers'), as
// it would behind other writers on a published graph. Writes cannot grow the
// pending set, since each pays for what it records, so the job's backlog only
// shrinks and the catch-up always finishes.
//
// Publication never holds pipelineMu or indexMu while graph work it does not
// bound runs. The job first drains what it can, then quiesces outside every
// Service lock (quiesce): new inserts stop (payers wait, holding no lock) and
// it waits for the inserts in flight. Only then does it take pipelineMu and
// indexMu, so nothing can be recorded, and finishLocked, which waits for
// nothing, applies the pending changes when at most hnswCatchUpPublishTail
// remain, or applies nothing; the job then releases the locks, resumes the
// payers and continues. Under the global locks publication therefore performs
// at most hnswCatchUpPublishTail inserts and waits for none. Cancellation
// (close, drop, reset) stops the job within one insert; the unpublished graph
// is discarded.
// Lock order: indexMu -> mu -> the graph's own lock; pay and the job take mu
// only between inserts.
type hnswCatchUp struct {
	mu         sync.Mutex
	idle       *sync.Cond // signalled when no insert is in flight
	target     *HNSWIndex
	vectors    map[string][]float32
	tombstones map[string]struct{}
	order      []vectorEntryID
	next       int
	inFlight   map[string]struct{}
	// publishing stops new inserts while the job publishes (quiesce).
	publishing bool
	// failed is an insert error from a writer's payment; the build then
	// fails instead of publishing a graph that lacks the change.
	failed error
	// recorded counts the live changes recorded, so a writer can tell, under
	// indexMu, how many it owes.
	recorded atomic.Uint64
	// beforeStep, set by tests only, runs before each step of the construction
	// job's catch-up, outside every Service lock.
	beforeStep atomic.Pointer[func(context.Context)]
}

// hnswCatchUpPublishTail is the most pending changes publication applies
// under the write locks; hnswCatchUpProgressInterval is how often a running
// catch-up logs what is left.
const (
	hnswCatchUpPublishTail      = 16
	hnswCatchUpProgressInterval = 5 * time.Second
)

// begin hands the recorded changes to the catch-up for target. Caller holds
// indexMu, so every change made before it is recorded and every change made
// after it goes through the catch-up.
func (c *hnswCatchUp) begin(target *HNSWIndex, vectors map[string][]float32, tombstones map[string]struct{}) int {
	if vectors == nil {
		vectors = make(map[string][]float32)
	}
	if tombstones == nil {
		tombstones = make(map[string]struct{})
	}
	c.mu.Lock()
	if c.idle == nil {
		c.idle = sync.NewCond(&c.mu)
	}
	c.target, c.failed, c.publishing = target, nil, false
	c.vectors, c.tombstones, c.next = vectors, tombstones, 0
	// Removals first, as the recorded log was applied before.
	c.order = make([]vectorEntryID, 0, len(c.tombstones)+len(c.vectors))
	for id := range c.tombstones {
		c.order = append(c.order, vectorEntryIdentity(id))
	}
	for id := range c.vectors {
		c.order = append(c.order, vectorEntryIdentity(id))
	}
	c.inFlight = make(map[string]struct{})
	queued := len(c.order)
	c.mu.Unlock()
	return queued
}

// end detaches the catch-up from its graph, applying nothing. Caller holds
// indexMu. Used when the graph will not be published; an insert still in
// flight lands in the discarded graph.
func (c *hnswCatchUp) end() {
	c.mu.Lock()
	c.detachLocked()
	c.mu.Unlock()
}

func (c *hnswCatchUp) detachLocked() {
	c.target, c.failed, c.publishing = nil, nil, false
	c.vectors, c.tombstones, c.order, c.next, c.inFlight = nil, nil, nil, 0, nil
	if c.idle != nil {
		c.idle.Broadcast()
	}
}

func (c *hnswCatchUp) pendingLocked() int { return len(c.vectors) + len(c.tombstones) }

// pending is the number of changes still to apply.
func (c *hnswCatchUp) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pendingLocked()
}

func (c *hnswCatchUp) isPendingLocked(id string) bool {
	_, vector := c.vectors[id]
	_, removed := c.tombstones[id]
	return vector || removed
}

// queueLocked records that id has a pending change. A key already pending
// keeps its place; any other is queued (again).
func (c *hnswCatchUp) queueLocked(id string) {
	if !c.isPendingLocked(id) {
		c.order = append(c.order, vectorEntryIdentity(id))
	}
}

// popLocked takes the next pending change whose key is not being inserted,
// marks the key in flight, and returns it: its key, its vector (nil for a
// removal), and whether one was found. A key skipped because it is in flight
// is queued again when that insert completes.
func (c *hnswCatchUp) popLocked() (string, []float32, bool) {
	for c.next < len(c.order) {
		id := c.order[c.next].entryKeyString()
		c.order[c.next] = ""
		c.next++
		if _, busy := c.inFlight[id]; busy {
			continue
		}
		if _, removed := c.tombstones[id]; removed {
			delete(c.tombstones, id)
			c.inFlight[id] = struct{}{}
			return id, nil, true
		}
		if value, ok := c.vectors[id]; ok {
			delete(c.vectors, id)
			c.inFlight[id] = struct{}{}
			return id, value, true
		}
	}
	// Everything queued has been taken; reuse the queue's storage.
	c.order, c.next = c.order[:0], 0
	return "", nil, false
}

// completeLocked ends id's insert: a change recorded for it meanwhile is
// queued, and publication is woken when nothing is in flight.
func (c *hnswCatchUp) completeLocked(id string, err error) {
	if c.inFlight == nil { // detached meanwhile
		return
	}
	delete(c.inFlight, id)
	if err != nil && c.failed == nil {
		c.failed = err
	}
	if c.isPendingLocked(id) {
		c.order = append(c.order, vectorEntryIdentity(id))
	}
	if len(c.inFlight) == 0 {
		c.idle.Broadcast()
	}
}

func applyCatchUpChange(target *HNSWIndex, id string, value []float32) error {
	if value == nil {
		target.Remove(id)
		return nil
	}
	return target.Update(id, value)
}

// step takes one pending change and applies it outside mu. It reports false
// when there is nothing it may take. While the job publishes it takes
// nothing: it waits for publication to finish or resume, holding no lock but
// mu's condition.
func (c *hnswCatchUp) step() (bool, error) {
	c.mu.Lock()
	for c.target != nil && c.publishing {
		c.idle.Wait()
	}
	if c.target == nil {
		c.mu.Unlock()
		return false, nil
	}
	if c.failed != nil {
		err := c.failed
		c.mu.Unlock()
		return false, err
	}
	id, value, ok := c.popLocked()
	target := c.target
	c.mu.Unlock()
	if !ok {
		return false, nil
	}
	err := applyCatchUpChange(target, id, value)
	c.mu.Lock()
	c.completeLocked(id, err)
	c.mu.Unlock()
	return true, err
}

// applyNext is the job's step.
func (c *hnswCatchUp) applyNext() (bool, error) { return c.step() }

// owedMark is read under indexMu before a write's index changes; the
// difference after them is what that write recorded and owes.
func (c *hnswCatchUp) owedMark() uint64 { return c.recorded.Load() }

// pay applies up to n pending changes on behalf of a writer that recorded n,
// after it released indexMu and its node's mutation lane. With nothing it may take (the rest is in flight,
// or the catch-up ended) it returns.
func (c *hnswCatchUp) pay(n uint64) {
	for ; n > 0; n-- {
		if more, err := c.step(); !more || err != nil {
			return
		}
	}
}

// quiesce, called by the job outside every Service lock, stops new inserts
// and waits for the ones in flight. It reports false when the catch-up has
// ended meanwhile.
func (c *hnswCatchUp) quiesce() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.publishing = true
	for c.target != nil && len(c.inFlight) > 0 {
		c.idle.Wait()
	}
	return c.target != nil
}

// resume lets inserts continue after an attempt to publish applied nothing.
func (c *hnswCatchUp) resume() {
	c.mu.Lock()
	c.publishing = false
	if c.idle != nil {
		c.idle.Broadcast()
	}
	c.mu.Unlock()
}

// finishLocked completes the catch-up for publication. Caller holds
// pipelineMu and indexMu, so nothing can be recorded meanwhile, after
// quiesce. It waits for nothing: with no insert in flight and at most
// hnswCatchUpPublishTail changes pending, it applies them, detaches and
// reports true; otherwise it applies nothing and reports false, and the
// caller releases the locks, resumes and lets the job continue.
func (c *hnswCatchUp) finishLocked() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.target == nil {
		return true, nil
	}
	if c.failed != nil {
		return false, c.failed
	}
	if len(c.inFlight) > 0 || c.pendingLocked() > hnswCatchUpPublishTail {
		return false, nil
	}
	for {
		id, value, ok := c.popLocked()
		if !ok {
			break
		}
		err := applyCatchUpChange(c.target, id, value)
		delete(c.inFlight, id)
		if err != nil {
			return false, err
		}
	}
	c.detachLocked()
	return true, nil
}

// liveUpdate records a vector change made during the catch-up as its key's
// pending change; record is the canonical normalized vector. It reports false
// when no catch-up runs. Caller holds indexMu and pays after releasing it.
func (c *hnswCatchUp) liveUpdate(id string, record []float32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.target == nil {
		return false
	}
	c.queueLocked(id)
	delete(c.tombstones, id)
	c.vectors[id] = record
	c.recorded.Add(1)
	return true
}

// liveRemove is liveUpdate for a removed vector.
func (c *hnswCatchUp) liveRemove(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.target == nil {
		return false
	}
	c.queueLocked(id)
	delete(c.vectors, id)
	c.tombstones[id] = struct{}{}
	c.recorded.Add(1)
	return true
}

// beginHNSWCatchUp hands the changes recorded while built was constructed to
// the catch-up; from here on live writes are recorded there.
func (s *Service) beginHNSWCatchUp(built *HNSWIndex) {
	s.indexMu.Lock()
	vectors, tombstones := s.hnswRebuildOverlay.take()
	recorded := s.hnswCatchUp.begin(built, vectors, tombstones)
	s.indexMu.Unlock()
	if recorded > 0 {
		s.logPrintf("[HNSW] Applying %d vector changes recorded during construction", recorded)
	}
}

// catchUpHNSWBuild applies pending changes outside every lock until none is
// left that it may take. It returns early when ctx is cancelled; the
// unpublished graph is then discarded and nothing is lost, since the vector
// store holds every change and the next build starts from it.
func (s *Service) catchUpHNSWBuild(ctx context.Context) error {
	logged := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if hook := s.hnswCatchUp.beforeStep.Load(); hook != nil {
			(*hook)(ctx)
		}
		if now := time.Now(); now.Sub(logged) >= hnswCatchUpProgressInterval {
			logged = now
			s.logPrintf("[HNSW] Catching up: %d vector changes pending", s.hnswCatchUp.pending())
		}
		more, err := s.hnswCatchUp.applyNext()
		if err != nil || !more {
			return err
		}
	}
}

// lockForHNSWPublication catches built up and returns holding pipelineMu and
// indexMu with the catch-up finished, so the caller publishes a graph that
// holds every change. It quiesces the catch-up before taking the locks, so
// under them it waits for no insert and applies at most the publication
// tail; with more pending it releases the locks at once, resumes and
// continues. On error no lock is held and the catch-up is resumed.
func (s *Service) lockForHNSWPublication(ctx context.Context) error {
	for {
		if err := s.catchUpHNSWBuild(ctx); err != nil {
			return err
		}
		if !s.hnswCatchUp.quiesce() {
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.Canceled
		}
		s.pipelineMu.Lock()
		s.indexMu.Lock()
		finished, err := false, ctx.Err()
		if err == nil {
			finished, err = s.hnswCatchUp.finishLocked()
		}
		if finished && err == nil {
			return nil
		}
		s.indexMu.Unlock()
		s.pipelineMu.Unlock()
		s.hnswCatchUp.resume()
		if err != nil {
			return err
		}
	}
}
