// Package memory owns opt-in application memory admission. It consumes cached
// physical usage samples; callers reserve forecast growth before allocating
// and release it when the owned work ends. This is a soft limit, not an
// allocator or kernel OOM boundary.
package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

var (
	// ErrStaleSample means an enabled policy has no fresh physical reading.
	ErrStaleSample = errors.New("memory usage reading unavailable or stale")
	// ErrLimitExceeded means the requested reservation exceeds soft capacity.
	ErrLimitExceeded = errors.New("application memory soft limit exceeded")
)

// Sample is one physical observation. Used includes all members and file
// cache of a shared cgroup; Ceiling is its effective hard limit, if known.
type Sample struct {
	Used    int64
	Ceiling int64
}

// Policy is the single application admission owner. It does not poll from
// execution loops; an external periodic worker calls Refresh.
type Policy struct {
	limit      int64
	headroom   int64
	freshness  time.Duration
	sampleFunc func() (Sample, error)
	now        func() time.Time

	mu        sync.Mutex
	sample    Sample
	sampledAt time.Time
	baseline  int64
	reserved  int64
}

// NewPolicy constructs an opt-in application policy. A nonpositive limit
// disables sampling and admission. Headroom is held below both the operator
// limit and a known container ceiling.
func NewPolicy(limit, headroom int64, freshness time.Duration, sampler func() (Sample, error), now func() time.Time) *Policy {
	if headroom < 0 {
		headroom = 0
	}
	if now == nil {
		now = time.Now
	}
	return &Policy{limit: limit, headroom: headroom, freshness: freshness, sampleFunc: sampler, now: now}
}

// CgroupSample obtains one Linux cgroup observation. A missing usage file is
// an unavailable reading rather than a permissive zero. The effective hard
// limit is included when the cgroup exposes one.
func CgroupSample() (Sample, error) {
	used, ok := config.DetectCgroupMemoryUsage()
	if !ok {
		return Sample{}, ErrStaleSample
	}
	ceiling, _ := config.DetectCgroupMemoryLimit()
	return Sample{Used: used, Ceiling: ceiling}, nil
}

// Run refreshes immediately and after every tick until shutdown. The caller
// owns the ticker; admission still checks freshness if sampling stalls.
func (p *Policy) Run(ctx context.Context, ticks <-chan time.Time) {
	if p == nil || p.limit <= 0 {
		return
	}
	_ = p.Refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok {
				return
			}
			_ = p.Refresh()
		}
	}
}

// Refresh publishes a reading after the sampler succeeds. Quiescent readings
// update the persistent baseline; active reservations are never learned into
// it as though they were persistent resources.
func (p *Policy) Refresh() error {
	if p == nil || p.limit <= 0 {
		return nil
	}
	if p.sampleFunc == nil {
		return ErrStaleSample
	}
	sample, err := p.sampleFunc()
	if err != nil || sample.Used <= 0 {
		return ErrStaleSample
	}
	p.mu.Lock()
	p.sample = sample
	p.sampledAt = p.now()
	if p.reserved == 0 {
		p.baseline = sample.Used
	}
	p.mu.Unlock()
	return nil
}

// Lease owns one atomic forecast reservation. Release is idempotent because
// failure, cancellation and normal cleanup can converge at a session owner.
type Lease struct {
	policy *Policy
	bytes  int64
	once   sync.Once
}

// disabledLease is immutable: Release returns before touching once when policy
// is nil. Reusing it avoids an allocation when application limits are off.
var disabledLease = &Lease{}

// Reserve atomically checks cached physical pressure and all outstanding
// forecasts. Adding both is conservative while an allocation may already
// appear in the physical sample, but prevents unrelated growth during active
// work from silently spending the same headroom twice.
func (p *Policy) Reserve(bytes int64) (*Lease, error) {
	if p == nil || p.limit <= 0 || bytes <= 0 {
		return disabledLease, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sampledAt.IsZero() || p.freshness <= 0 || p.now().Sub(p.sampledAt) > p.freshness {
		return nil, ErrStaleSample
	}
	ceiling := p.limit
	if p.sample.Ceiling > 0 && p.sample.Ceiling < ceiling {
		ceiling = p.sample.Ceiling
	}
	capacity := ceiling - p.headroom
	if capacity < 0 {
		capacity = 0
	}
	observed := p.sample.Used
	if p.baseline > observed {
		observed = p.baseline
	}
	if observed > capacity || p.reserved > capacity-observed || bytes > capacity-observed-p.reserved {
		return nil, ErrLimitExceeded
	}
	p.reserved += bytes
	return &Lease{policy: p, bytes: bytes}, nil
}

// Release returns a forecast when its owner has completed cleanup.
func (l *Lease) Release() {
	if l == nil || l.policy == nil {
		return
	}
	l.once.Do(func() {
		l.policy.mu.Lock()
		l.policy.reserved -= l.bytes
		l.policy.mu.Unlock()
	})
}

// Reserved reports outstanding forecast bytes for diagnostics and tests.
func (p *Policy) Reserved() int64 {
	if p == nil || p.limit <= 0 {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reserved
}
