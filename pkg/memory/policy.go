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
	// Headroom is the tightest limit-minus-usage across the cgroup hierarchy.
	Headroom    int64
	HasHeadroom bool
}

// Policy is the single application admission owner. It does not poll from
// execution loops; an external periodic worker calls Refresh.
type Policy struct {
	limit      int64
	headroom   int64
	freshness  time.Duration
	sampleFunc func() (Sample, error)
	now        func() time.Time

	mu         sync.Mutex
	sample     Sample
	sampledAt  time.Time
	baseline   int64
	reserved   int64
	nextSample uint64
	lastSample uint64

	recovery pressureRecovery
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
	return &Policy{limit: limit, headroom: headroom, freshness: freshness, sampleFunc: sampler, now: now,
		recovery: pressureRecovery{reclaim: releaseHeapPages}}
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
	headroom, paired := config.DetectCgroupMemoryHeadroom()
	if ceiling > 0 && !paired {
		return Sample{}, ErrStaleSample
	}
	return Sample{Used: used, Ceiling: ceiling, Headroom: headroom, HasHeadroom: paired}, nil
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
// it as though they were persistent resources. After physical pressure rejects
// work, the sampler may reclaim idle heap pages and remeasure, subject to the
// recovery cooldown and completed-work allowance.
func (p *Policy) Refresh() error {
	if err := p.refreshSample(); err != nil {
		return err
	}
	if p.reclaimPressure() {
		// A collection is not evidence of capacity. Only a new physical
		// observation may reopen admission, including ancestor pressure.
		return p.refreshSample()
	}
	return nil
}

func (p *Policy) refreshSample() error {
	if p == nil || p.limit <= 0 {
		return nil
	}
	if p.sampleFunc == nil {
		return ErrStaleSample
	}
	p.mu.Lock()
	p.nextSample++
	generation := p.nextSample
	p.mu.Unlock()
	sample, err := p.sampleFunc()
	if err != nil || sample.Used <= 0 {
		// A failed observation invalidates the cached decision immediately;
		// otherwise new work can enter for an entire freshness interval.
		p.mu.Lock()
		if generation > p.lastSample {
			p.lastSample = generation
			p.sampledAt = time.Time{}
		}
		p.mu.Unlock()
		return ErrStaleSample
	}
	p.mu.Lock()
	if generation <= p.lastSample {
		p.mu.Unlock()
		return nil
	}
	p.lastSample = generation
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
	policy   *Policy
	bytes    int64
	once     sync.Once
	released bool // guarded by policy.mu
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
	if err := p.reserveLocked(bytes); err != nil {
		return nil, err
	}
	return &Lease{policy: p, bytes: bytes}, nil
}

// reservationCapacityLocked is the one admission arithmetic owner for new
// requests and incremental wire growth. All callers hold p.mu.
func (p *Policy) reservationCapacityLocked() (int64, error) {
	if p.sampledAt.IsZero() || p.freshness <= 0 || p.now().Sub(p.sampledAt) > p.freshness {
		return 0, ErrStaleSample
	}
	available := p.availableLocked()
	// Headroom is nonnegative. Reject before subtraction so a very large
	// configured reserve cannot wrap negative capacity back into admission.
	if available <= p.headroom {
		p.recovery.pressure = true
		return 0, ErrLimitExceeded
	}
	available -= p.headroom
	if p.reserved > available {
		return 0, ErrLimitExceeded
	}
	return available - p.reserved, nil
}

func (p *Policy) reserveLocked(bytes int64) error {
	available, err := p.reservationCapacityLocked()
	if err != nil {
		return err
	}
	if bytes < 0 || bytes > available {
		return ErrLimitExceeded
	}
	p.reserved += bytes
	return nil
}

// ReserveWireRequest forecasts decode and parser expansion for an incoming
// message. Unknown-size streams grow this same reservation before accumulating
// bytes. A transport may also impose its own size bound. This forecast is not
// a precise bound on allocations made by a decoder.
func (p *Policy) ReserveWireRequest(wireBytes, wireLimit int64) (*Lease, error) {
	if p == nil || p.limit <= 0 {
		return disabledLease, nil
	}
	return p.Reserve(wireRequestForecast(wireBytes, wireLimit))
}

func wireRequestForecast(wireBytes, wireLimit int64) int64 {
	if wireBytes < 0 {
		wireBytes = max(int64(0), wireLimit)
	}
	// Saturate before multiplication. A malicious length must never wrap into
	// a small forecast and bypass admission.
	if wireBytes > (1<<63-1)/4 {
		return 1<<63 - 1
	}
	return max(int64(64<<10), wireBytes*4)
}

// GrowWireRequest extends a ReserveWireRequest lease to cover the accumulated
// wire size, before a transport grows its input buffer. It charges only the
// increase and rechecks fresh physical capacity even within the minimum
// forecast. The caller keeps this lease through decode/setup and releases it
// on success, rejection, disconnect or cancellation.
func (l *Lease) GrowWireRequest(wireBytes, wireLimit int64) error {
	if l == nil || l.policy == nil {
		return nil
	}
	p := l.policy
	p.mu.Lock()
	defer p.mu.Unlock()
	if l.released {
		return ErrLimitExceeded
	}
	forecast := max(l.bytes, wireRequestForecast(wireBytes, wireLimit))
	if err := p.reserveLocked(forecast - l.bytes); err != nil {
		return err
	}
	l.bytes = forecast
	return nil
}

// WireLimit returns the largest accumulated wire size this lease can currently
// cover, including its existing charge. Streaming transports use this as a
// read-limit snapshot and still grow/recheck the lease before accumulating
// bytes. Zero means the application policy is disabled.
func (l *Lease) WireLimit() (int64, error) {
	if l == nil || l.policy == nil {
		return 0, nil
	}
	p := l.policy
	p.mu.Lock()
	defer p.mu.Unlock()
	if l.released {
		return 0, ErrLimitExceeded
	}
	available, err := p.reservationCapacityLocked()
	if err != nil {
		return 0, err
	}
	return (available + l.bytes) / 4, nil
}

// Release returns a forecast when its owner has completed cleanup.
func (l *Lease) Release() {
	if l == nil || l.policy == nil {
		return
	}
	l.once.Do(func() {
		l.policy.mu.Lock()
		l.policy.reserved -= l.bytes
		l.released = true
		l.policy.recovery.released++
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
