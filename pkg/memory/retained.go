// Author: #1683 implementation lane (Codex), 2026-11-01.
package memory

import (
	"math"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"
	"weak"
)

// Budget describes an existing memory boundary, not a separately configured
// cache limit. Runtime usage includes Go's heap, stacks and runtime metadata.
// Application usage retains Policy's physical sample/ancestor semantics.
type Budget struct {
	Source    string
	Used      int64
	Limit     int64
	Headroom  int64
	Available int64
	Fresh     bool
}

// RuntimeBudget observes the limit already installed by configuration (including
// NORNICDB_MEMORY_LIMIT and GOMEMLIMIT). No limit is installed or changed here.
func RuntimeBudget() Budget {
	limit := debug.SetMemoryLimit(-1)
	samples := []metrics.Sample{{Name: "/memory/classes/total:bytes"}, {Name: "/memory/classes/heap/released:bytes"}}
	metrics.Read(samples)
	used := int64(samples[0].Value.Uint64() - samples[1].Value.Uint64())
	if limit == math.MaxInt64 {
		return Budget{Source: "runtime", Used: used, Limit: limit, Fresh: true, Available: math.MaxInt64}
	}
	// An optional cache is released before the runtime reaches its configured
	// boundary. Refill requires this same margin twice, preventing oscillation.
	return Budget{Source: "runtime", Used: used, Limit: limit, Available: limit - used, Headroom: limit / 20, Fresh: true}
}

// RuntimeLiveHeapHeadroom preserves the search build queue's existing
// post-collection live-heap reservation contract. Both consumers observe the
// same applied runtime limit; this projection is not a pressure measurement.
func RuntimeLiveHeapHeadroom() int64 {
	limit := debug.SetMemoryLimit(-1)
	if limit == math.MaxInt64 {
		return -1
	}
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return -1
	}
	return max(int64(0), limit-int64(sample[0].Value.Uint64()))
}

func (p *Policy) retainedBudget() Budget {
	if p == nil || p.limit <= 0 {
		return Budget{Fresh: true, Available: math.MaxInt64}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	capacity, err := p.reservationCapacityLocked()
	available := int64(0)
	if err == nil {
		// The canonical admission arithmetic already checked freshness,
		// ancestor headroom, forecasts and overflow. Reconstruct its remaining
		// unreserved margin for the common retention/hysteresis predicate.
		available = capacity + p.headroom
	}
	return Budget{Source: "application", Used: max(p.sample.Used, p.baseline), Limit: p.limit,
		Headroom: p.headroom, Available: available, Fresh: err != ErrStaleSample}
}

func (b Budget) permits(growth int64, refill bool) bool {
	if !b.Fresh || b.Available <= b.Headroom || growth < 0 {
		return false
	}
	available := b.Available - b.Headroom
	if refill {
		if available <= b.Headroom {
			return false
		}
		available -= b.Headroom
	}
	return growth <= available
}

// RetainedCache is a lifecycle registration for optional, reconstructible data.
// Its release callback detaches installed ownership; active request data may
// finish its admitted work but cannot be republished by that generation.
type RetainedCache struct {
	id      uint64
	policy  *Policy
	release func(Budget)
	closed  atomic.Bool
}

var retainedCaches = struct {
	sync.Mutex
	entries map[uint64]weak.Pointer[RetainedCache]
	next    uint64
	running bool
	count   atomic.Int64
}{entries: make(map[uint64]weak.Pointer[RetainedCache])}

// RegisterRetainedCache binds an optional copy to the existing admission owner.
// The process observer exists only while live registrations exist. Weak entries
// cannot extend a database/service lifetime, including callers that omit Close.
func RegisterRetainedCache(policy *Policy, release func(Budget)) *RetainedCache {
	c := &RetainedCache{policy: policy, release: release}
	retainedCaches.Lock()
	retainedCaches.next++
	c.id = retainedCaches.next
	retainedCaches.entries[retainedCaches.next] = weak.Make(c)
	retainedCaches.count.Add(1)
	if !retainedCaches.running {
		retainedCaches.running = true
		go observeRetainedMemory()
	}
	retainedCaches.Unlock()
	return c
}

// Close unregisters this cache. Its owner releases its own data at shutdown.
func (c *RetainedCache) Close() {
	if c == nil {
		return
	}
	c.closed.Store(true)
	retainedCaches.Lock()
	if _, exists := retainedCaches.entries[c.id]; exists {
		delete(retainedCaches.entries, c.id)
		retainedCaches.count.Add(-1)
	}
	retainedCaches.Unlock()
}

// Admission returns the strictest existing budget for this cache. growth is a
// forecast for the optional data only; it never reserves or denies query work.
// The caller selects refill hysteresis when recovering from an earlier release.
func (c *RetainedCache) Admission(growth int64, refill bool) (Budget, bool) {
	if c == nil || c.closed.Load() {
		return Budget{}, false
	}
	runtimeBudget, application := RuntimeBudget(), c.policy.retainedBudget()
	for _, b := range []Budget{runtimeBudget, application} {
		if !b.permits(growth, refill) {
			ReleaseRetainedCaches(c.policy, growth)
			return b, false
		}
	}
	if application.Limit > 0 && application.Available-application.Headroom < runtimeBudget.Available-runtimeBudget.Headroom {
		return application, true
	}
	return runtimeBudget, true
}

// ReleaseRetainedCaches runs before the existing application admission decision
// and from the runtime observer. It never collects, waits for a query lease, or
// changes a policy's sample/reservation arithmetic.
func ReleaseRetainedCaches(policy *Policy, forecast int64) {
	if retainedCaches.count.Load() == 0 {
		return
	}
	runtimeBudget, application := RuntimeBudget(), policy.retainedBudget()
	runtimePressure, applicationPressure := !runtimeBudget.permits(forecast, false), !application.permits(forecast, false)
	if !runtimePressure && !applicationPressure {
		return
	}
	for _, c := range registeredRetainedCaches() {
		if runtimePressure {
			c.release(runtimeBudget)
		} else if applicationPressure && c.policy == policy {
			c.release(application)
		}
	}
}

func registeredRetainedCaches() []*RetainedCache {
	retainedCaches.Lock()
	defer retainedCaches.Unlock()
	owners := make([]*RetainedCache, 0, len(retainedCaches.entries))
	for id, ref := range retainedCaches.entries {
		owner := ref.Value()
		if owner == nil || owner.closed.Load() {
			delete(retainedCaches.entries, id)
			retainedCaches.count.Add(-1)
			continue
		}
		owners = append(owners, owner)
	}
	return owners
}

func observeRetainedMemory() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		// Application sampling remains with Policy.Run/Refresh. This one
		// process observer covers the runtime limit even with admission off.
		_ = registeredRetainedCaches()
		ReleaseRetainedCaches(nil, 0)
		retainedCaches.Lock()
		if len(retainedCaches.entries) == 0 {
			retainedCaches.running = false
			retainedCaches.Unlock()
			return
		}
		retainedCaches.Unlock()
	}
}
