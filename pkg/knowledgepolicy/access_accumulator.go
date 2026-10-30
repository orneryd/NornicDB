package knowledgepolicy

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// counter is a buffered count. Counters from different shards, and a
// buffered counter on top of the persisted one, combine by addition.
type counter int64

func (c counter) merge(other counter) counter { return c + other }

// timestamp is a buffered point in time (Unix nanoseconds). Timestamps from
// different shards, and a buffered timestamp on top of the persisted one,
// combine by keeping the latest; they are never added.
type timestamp int64

func (t timestamp) merge(other timestamp) timestamp {
	if other > t {
		return other
	}
	return t
}

// entityDelta is one entity's buffered access metadata. Every slot holds
// either a counter or a timestamp, and merge applies each kind's own rule,
// so a value can't be combined under the other kind's rule.
type entityDelta struct {
	accessCount     counter
	traversalCount  counter
	lastAccessedAt  timestamp
	lastTraversedAt timestamp
	counters        map[string]counter   // IncrementCustom keys
	timestamps      map[string]timestamp // SetTimestamp keys
}

func (d *entityDelta) addCounter(key string, delta counter) {
	if d.counters == nil {
		d.counters = make(map[string]counter)
	}
	d.counters[key] = d.counters[key].merge(delta)
}

func (d *entityDelta) observeTimestamp(key string, ts timestamp) {
	if ts <= d.timestamps[key] {
		return
	}
	if d.timestamps == nil {
		d.timestamps = make(map[string]timestamp)
	}
	d.timestamps[key] = ts
}

// merge folds other into d: counters add, timestamps keep the latest.
func (d *entityDelta) merge(other *entityDelta) {
	d.accessCount = d.accessCount.merge(other.accessCount)
	d.traversalCount = d.traversalCount.merge(other.traversalCount)
	d.lastAccessedAt = d.lastAccessedAt.merge(other.lastAccessedAt)
	d.lastTraversedAt = d.lastTraversedAt.merge(other.lastTraversedAt)
	for key, delta := range other.counters {
		d.addCounter(key, delta)
	}
	for key, ts := range other.timestamps {
		d.observeTimestamp(key, ts)
	}
}

type pLocalShard struct {
	mu     sync.Mutex
	deltas map[string]*entityDelta
	_pad   [56]byte // cache-line padding
}

// delta returns the shard's delta for entityID, creating it when absent.
// The caller holds s.mu.
func (s *pLocalShard) delta(entityID string) (d *entityDelta, isNew bool) {
	d = s.deltas[entityID]
	if d == nil {
		d = &entityDelta{}
		s.deltas[entityID] = d
		isNew = true
	}
	return d, isNew
}

func (s *pLocalShard) recordAccess(entityID string, now timestamp) (isNew bool) {
	s.mu.Lock()
	d, isNew := s.delta(entityID)
	d.accessCount = d.accessCount.merge(1)
	d.lastAccessedAt = d.lastAccessedAt.merge(now)
	s.mu.Unlock()
	return isNew
}

func (s *pLocalShard) recordTraversal(entityID string, now timestamp) (isNew bool) {
	s.mu.Lock()
	d, isNew := s.delta(entityID)
	d.traversalCount = d.traversalCount.merge(1)
	d.lastTraversedAt = d.lastTraversedAt.merge(now)
	s.mu.Unlock()
	return isNew
}

func (s *pLocalShard) incrementCustom(entityID string, key string, delta counter) {
	s.mu.Lock()
	d, _ := s.delta(entityID)
	d.addCounter(key, delta)
	s.mu.Unlock()
}

func (s *pLocalShard) setTimestamp(entityID string, key string, ts timestamp) {
	s.mu.Lock()
	d, _ := s.delta(entityID)
	d.observeTimestamp(key, ts)
	s.mu.Unlock()
}

// AccessAccumulator is a per-P sharded counter ring for hot-path access
// metadata accumulation. Goroutines write to the shard of their current P
// via sync.Pool affinity, eliminating cross-core contention.
type AccessAccumulator struct {
	shards        []pLocalShard
	pool          sync.Pool
	counter       atomic.Int64
	entityCount   atomic.Int64
	enabled       bool
	maxBufferSize int64
	onBufferFull  func()

	// Custom key kinds, fixed by first use (claimCustomKind).
	customKinds          sync.Map // key -> customKind
	loggedCustomRefusals sync.Map // key -> struct{}
	rejectedCustomWrites atomic.Int64
}

// NewAccessAccumulator creates an accumulator with one shard per GOMAXPROCS.
// maxBufferSize limits distinct entity count before triggering an auto-flush.
// Pass 0 for unlimited (flush only on timer).
func NewAccessAccumulator(enabled bool, maxBufferSize int) *AccessAccumulator {
	return newAccessAccumulator(enabled, maxBufferSize, runtime.GOMAXPROCS(0))
}

func newAccessAccumulator(enabled bool, maxBufferSize int, shardCount int) *AccessAccumulator {
	a := &AccessAccumulator{
		shards:        make([]pLocalShard, shardCount),
		enabled:       enabled,
		maxBufferSize: int64(maxBufferSize),
	}
	for i := range a.shards {
		a.shards[i].deltas = make(map[string]*entityDelta)
	}
	a.pool.New = func() interface{} {
		idx := int(a.counter.Add(1)-1) % len(a.shards)
		return idx
	}
	return a
}

// SetOnBufferFull sets the callback invoked (in a goroutine) when the buffer
// reaches maxBufferSize. The flusher wires this to trigger an immediate flush.
func (a *AccessAccumulator) SetOnBufferFull(fn func()) {
	a.onBufferFull = fn
}

func (a *AccessAccumulator) currentShard() *pLocalShard {
	idx := a.pool.Get().(int)
	a.pool.Put(idx)
	return &a.shards[idx]
}

func (a *AccessAccumulator) IncrementAccess(entityID string) {
	if !a.enabled {
		return
	}
	now := timestamp(time.Now().UnixNano())
	if a.currentShard().recordAccess(entityID, now) {
		a.checkBufferFull()
	}
}

func (a *AccessAccumulator) IncrementTraversal(entityID string) {
	if !a.enabled {
		return
	}
	now := timestamp(time.Now().UnixNano())
	if a.currentShard().recordTraversal(entityID, now) {
		a.checkBufferFull()
	}
}

// IncrementCustom adds delta to the custom counter key. Custom counters are
// summed across shards and onto the persisted value. A key first used with
// SetTimestamp is refused (see claimCustomKind).
func (a *AccessAccumulator) IncrementCustom(entityID string, key string, delta int64) {
	if !a.enabled || !a.claimCustomKind(key, customCounter) {
		return
	}
	a.currentShard().incrementCustom(entityID, key, counter(delta))
}

// SetTimestamp records ts for the custom timestamp key. Custom timestamps keep
// the latest value across shards and against the persisted value. A key first
// used with IncrementCustom is refused (see claimCustomKind).
func (a *AccessAccumulator) SetTimestamp(entityID string, key string, ts int64) {
	if !a.enabled || !a.claimCustomKind(key, customTimestamp) {
		return
	}
	a.currentShard().setTimestamp(entityID, key, timestamp(ts))
}

// ReadThrough returns the persisted value combined with the buffered deltas
// across all P-local shards: persisted + sum for counters, the latest of the
// persisted and buffered values for timestamps. The fixed key names read the
// fixed fields; any other key reads by the kind of its first use, and a key
// never used returns persisted.
// This is eventually-consistent and NOT bound by MVCC snapshot isolation.
func (a *AccessAccumulator) ReadThrough(entityID string, key string, persisted int64) int64 {
	if !a.enabled {
		return persisted
	}
	switch key {
	case "accessCount":
		return int64(a.sumCounters(entityID, counter(persisted), func(d *entityDelta) counter { return d.accessCount }))
	case "traversalCount":
		return int64(a.sumCounters(entityID, counter(persisted), func(d *entityDelta) counter { return d.traversalCount }))
	case "lastAccessedAt":
		return int64(a.latestTimestamp(entityID, timestamp(persisted), func(d *entityDelta) timestamp { return d.lastAccessedAt }))
	case "lastTraversedAt":
		return int64(a.latestTimestamp(entityID, timestamp(persisted), func(d *entityDelta) timestamp { return d.lastTraversedAt }))
	}
	switch a.customKindOf(key) {
	case customCounter:
		return int64(a.sumCounters(entityID, counter(persisted), func(d *entityDelta) counter { return d.counters[key] }))
	case customTimestamp:
		return int64(a.latestTimestamp(entityID, timestamp(persisted), func(d *entityDelta) timestamp { return d.timestamps[key] }))
	}
	return persisted
}

// customKind is the kind a custom key was first used as.
type customKind uint8

const (
	customCounter customKind = iota + 1
	customTimestamp
)

func (k customKind) String() string {
	if k == customTimestamp {
		return "timestamp"
	}
	return "counter"
}

// claimCustomKind fixes key's kind on its first use and reports whether key
// may be used as kind. A key keeps its kind for the accumulator's lifetime, so
// the buffers, ReadThrough and the flusher never hold one key as both a
// counter and a timestamp. A use as the other kind is refused: every refusal
// is counted (RejectedCustomWrites) and the first one per key is logged.
func (a *AccessAccumulator) claimCustomKind(key string, kind customKind) bool {
	current, _ := a.customKinds.LoadOrStore(key, kind)
	if current.(customKind) == kind {
		return true
	}
	a.rejectedCustomWrites.Add(1)
	if _, logged := a.loggedCustomRefusals.LoadOrStore(key, struct{}{}); !logged {
		log.Printf("knowledgepolicy: custom access key %q is a %s; refused its use as a %s", key, current, kind)
	}
	return false
}

// customKindOf returns key's kind, or 0 when key has not been used.
func (a *AccessAccumulator) customKindOf(key string) customKind {
	kind, _ := a.customKinds.Load(key)
	k, _ := kind.(customKind)
	return k
}

// RejectedCustomWrites returns how many IncrementCustom and SetTimestamp calls
// were refused because the key was first used as the other kind.
func (a *AccessAccumulator) RejectedCustomWrites() int64 {
	return a.rejectedCustomWrites.Load()
}

// sumCounters merges one counter slot of entityID's deltas across all shards
// onto total.
func (a *AccessAccumulator) sumCounters(entityID string, total counter, slot func(*entityDelta) counter) counter {
	for i := range a.shards {
		a.shards[i].mu.Lock()
		if d := a.shards[i].deltas[entityID]; d != nil {
			total = total.merge(slot(d))
		}
		a.shards[i].mu.Unlock()
	}
	return total
}

// latestTimestamp merges one timestamp slot of entityID's deltas across all
// shards onto latest.
func (a *AccessAccumulator) latestTimestamp(entityID string, latest timestamp, slot func(*entityDelta) timestamp) timestamp {
	for i := range a.shards {
		a.shards[i].mu.Lock()
		if d := a.shards[i].deltas[entityID]; d != nil {
			latest = latest.merge(slot(d))
		}
		a.shards[i].mu.Unlock()
	}
	return latest
}

func (a *AccessAccumulator) checkBufferFull() {
	if a.maxBufferSize <= 0 || a.onBufferFull == nil {
		return
	}
	count := a.entityCount.Add(1)
	if count >= a.maxBufferSize {
		go a.onBufferFull()
	}
}

// BufferFullness returns the current entity count as a fraction of the
// configured maxBufferSize (in [0, 1]). Returns 0 when the accumulator is
// unbounded (maxBufferSize <= 0). Intended for passive-scrape observability:
// sustained values near 1.0 indicate the flush interval can't keep up.
func (a *AccessAccumulator) BufferFullness() float64 {
	if a == nil || a.maxBufferSize <= 0 {
		return 0
	}
	count := a.entityCount.Load()
	if count <= 0 {
		return 0
	}
	f := float64(count) / float64(a.maxBufferSize)
	if f > 1.0 {
		f = 1.0
	}
	return f
}

// DrainAll atomically swaps out all shard deltas and returns a merged map.
// Used by the flush goroutine.
func (a *AccessAccumulator) DrainAll() map[string]*entityDelta {
	a.entityCount.Store(0)
	merged := make(map[string]*entityDelta)
	for i := range a.shards {
		a.shards[i].mu.Lock()
		local := a.shards[i].deltas
		a.shards[i].deltas = make(map[string]*entityDelta)
		a.shards[i].mu.Unlock()

		for id, d := range local {
			if m := merged[id]; m != nil {
				m.merge(d)
				continue
			}
			merged[id] = d
		}
	}
	return merged
}

// ClearEntity removes any buffered deltas for the given entity from all shards.
func (a *AccessAccumulator) ClearEntity(entityID string) {
	for i := range a.shards {
		a.shards[i].mu.Lock()
		delete(a.shards[i].deltas, entityID)
		a.shards[i].mu.Unlock()
	}
}
