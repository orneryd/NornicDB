package knowledgepolicy

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type entityDelta struct {
	accessCount     int64
	traversalCount  int64
	lastAccessedAt  int64
	lastTraversedAt int64
	overflow        map[string]int64
}

type pLocalShard struct {
	mu     sync.Mutex
	deltas map[string]*entityDelta
	_pad   [56]byte // cache-line padding
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
}

// NewAccessAccumulator creates an accumulator with one shard per GOMAXPROCS.
// maxBufferSize limits distinct entity count before triggering an auto-flush.
// Pass 0 for unlimited (flush only on timer).
func NewAccessAccumulator(enabled bool, maxBufferSize int) *AccessAccumulator {
	n := runtime.GOMAXPROCS(0)
	a := &AccessAccumulator{
		shards:        make([]pLocalShard, n),
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
	now := time.Now().UnixNano()
	shard := a.currentShard()
	isNew := false
	shard.mu.Lock()
	d := shard.deltas[entityID]
	if d == nil {
		d = &entityDelta{}
		shard.deltas[entityID] = d
		isNew = true
	}
	d.accessCount++
	if now > d.lastAccessedAt {
		d.lastAccessedAt = now
	}
	shard.mu.Unlock()
	if isNew {
		a.checkBufferFull()
	}
}

func (a *AccessAccumulator) IncrementTraversal(entityID string) {
	if !a.enabled {
		return
	}
	now := time.Now().UnixNano()
	shard := a.currentShard()
	isNew := false
	shard.mu.Lock()
	d := shard.deltas[entityID]
	if d == nil {
		d = &entityDelta{}
		shard.deltas[entityID] = d
		isNew = true
	}
	d.traversalCount++
	if now > d.lastTraversedAt {
		d.lastTraversedAt = now
	}
	shard.mu.Unlock()
	if isNew {
		a.checkBufferFull()
	}
}

func (a *AccessAccumulator) IncrementCustom(entityID string, key string, delta int64) {
	if !a.enabled {
		return
	}
	shard := a.currentShard()
	shard.mu.Lock()
	d := shard.deltas[entityID]
	if d == nil {
		d = &entityDelta{}
		shard.deltas[entityID] = d
	}
	if d.overflow == nil {
		d.overflow = make(map[string]int64)
	}
	d.overflow[key] += delta
	shard.mu.Unlock()
}

func (a *AccessAccumulator) SetTimestamp(entityID string, key string, ts int64) {
	if !a.enabled {
		return
	}
	shard := a.currentShard()
	shard.mu.Lock()
	d := shard.deltas[entityID]
	if d == nil {
		d = &entityDelta{}
		shard.deltas[entityID] = d
	}
	if d.overflow == nil {
		d.overflow = make(map[string]int64)
	}
	if ts > d.overflow[key] {
		d.overflow[key] = ts
	}
	shard.mu.Unlock()
}

// ReadThrough returns persisted + sum(buffered deltas) across all P-local shards.
// This is eventually-consistent and NOT bound by MVCC snapshot isolation.
func (a *AccessAccumulator) ReadThrough(entityID string, key string, persisted int64) int64 {
	if !a.enabled {
		return persisted
	}
	total := persisted
	for i := range a.shards {
		a.shards[i].mu.Lock()
		d := a.shards[i].deltas[entityID]
		if d != nil {
			switch key {
			case "accessCount":
				total += d.accessCount
			case "traversalCount":
				total += d.traversalCount
			case "lastAccessedAt":
				if d.lastAccessedAt > total {
					total = d.lastAccessedAt
				}
			case "lastTraversedAt":
				if d.lastTraversedAt > total {
					total = d.lastTraversedAt
				}
			default:
				if d.overflow != nil {
					total += d.overflow[key]
				}
			}
		}
		a.shards[i].mu.Unlock()
	}
	return total
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
			m := merged[id]
			if m == nil {
				merged[id] = d
				continue
			}
			m.accessCount += d.accessCount
			m.traversalCount += d.traversalCount
			if d.lastAccessedAt > m.lastAccessedAt {
				m.lastAccessedAt = d.lastAccessedAt
			}
			if d.lastTraversedAt > m.lastTraversedAt {
				m.lastTraversedAt = d.lastTraversedAt
			}
			if d.overflow != nil {
				if m.overflow == nil {
					m.overflow = make(map[string]int64)
				}
				for k, v := range d.overflow {
					m.overflow[k] += v
				}
			}
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
