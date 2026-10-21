package memory

import (
	"runtime/debug"
	"time"
)

const pressureRecoveryInterval = 5 * time.Second

// pressureRecovery belongs to Policy.mu. Physical admission can otherwise
// deadlock after rejected work: GC has collected its objects, but Go's
// idle, unscavenged heap still keeps the cgroup above the admission boundary.
type pressureRecovery struct {
	pressure  bool
	running   bool
	attempted bool
	last      time.Time
	released  uint64
	attempt   uint64
	reclaim   func()
}

func releaseHeapPages() { debug.FreeOSMemory() }

// availableLocked keeps the operator threshold and every measured ancestor's
// ceiling in the same decision for both admission and pressure reclamation.
func (p *Policy) availableLocked() int64 {
	observed := max(p.sample.Used, p.baseline)
	available := p.limit - observed
	if p.sample.HasHeadroom && p.sample.Headroom < available {
		available = p.sample.Headroom
	} else if !p.sample.HasHeadroom && p.sample.Ceiling > 0 && p.sample.Ceiling-observed < available {
		available = p.sample.Ceiling - observed
	}
	return available
}

// reclaimPressure runs only from the sampler, never in a request or under the
// admission mutex. At most one attempt runs at once, no more than once in five
// seconds. After the first attempt, completed admitted work must release a
// lease before another is eligible. Rejected retries alone cannot cause an
// endless GC loop under sustained live or external cgroup pressure.
func (p *Policy) reclaimPressure() bool {
	if p == nil || p.limit <= 0 {
		return false
	}
	p.mu.Lock()
	r := &p.recovery
	if !r.pressure || r.running || p.sampledAt.IsZero() || p.availableLocked() > p.headroom ||
		(r.attempted && (r.released == r.attempt || p.now().Sub(r.last) < pressureRecoveryInterval)) {
		p.mu.Unlock()
		return false
	}
	r.running = true
	r.attempted = true
	r.attempt = r.released
	r.last = p.now()
	p.mu.Unlock()

	r.reclaim()

	p.mu.Lock()
	r.running = false
	p.mu.Unlock()
	return true
}
