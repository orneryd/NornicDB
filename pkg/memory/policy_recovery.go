package memory

import (
	"runtime/debug"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

const (
	pressureRecoveryInterval      = time.Duration(config.DefaultMemoryRecoveryIntervalMS) * time.Millisecond
	pressureRecoveryRetryInterval = time.Duration(config.DefaultMemoryRecoveryRearmMS) * time.Millisecond
)

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
	interval  time.Duration
	rearm     time.Duration
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
// admission mutex. At most one attempt runs at once, spaced by the configured
// interval when admitted work releases a lease. Without a release, a later
// attempt requires the configured rearm duration since the previous start, so unleased
// allocations cannot permanently exhaust recovery. Rejected requests never
// collect or shorten either cooldown under sustained physical pressure.
func (p *Policy) reclaimPressure() bool {
	if p == nil || p.limit <= 0 {
		return false
	}
	p.mu.Lock()
	r := &p.recovery
	now := p.now()
	elapsed := now.Sub(r.last)
	if !r.pressure || r.running || p.sampledAt.IsZero() || p.availableLocked() > p.headroom ||
		(r.attempted && (elapsed < r.interval ||
			(r.released == r.attempt && elapsed < r.rearm))) {
		p.mu.Unlock()
		return false
	}
	r.running = true
	r.attempted = true
	r.attempt = r.released
	r.last = now
	p.mu.Unlock()

	r.reclaim()

	p.mu.Lock()
	r.running = false
	p.mu.Unlock()
	return true
}
