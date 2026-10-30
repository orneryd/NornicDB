package lifecycle

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type backoffEntry struct {
	retryCount    int
	lastAttempt   time.Time
	nextEligible  time.Time
	cooldownUntil time.Time
	mismatchTimes []time.Time
}

type backoffQueue struct {
	mu      sync.Mutex
	entries map[string]*backoffEntry
	config  LifecycleConfig
}

func newBackoffQueue(config LifecycleConfig) *backoffQueue {
	return &backoffQueue{entries: make(map[string]*backoffEntry), config: config}
}

func (b *backoffQueue) isEligible(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entries[key]
	if entry == nil {
		return true
	}
	now := time.Now()
	if now.Before(entry.cooldownUntil) || now.Before(entry.nextEligible) {
		return false
	}
	return true
}

func (b *backoffQueue) recordMismatch(key string) (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entries[key]
	if entry == nil {
		entry = &backoffEntry{}
		b.entries[key] = entry
	}
	now := time.Now()
	cutoff := now.Add(-10 * time.Minute)
	filtered := entry.mismatchTimes[:0]
	for _, ts := range entry.mismatchTimes {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}
	entry.mismatchTimes = append(filtered, now)
	entry.retryCount++
	entry.lastAttempt = now
	delay := b.config.FenceRetryInitialDelay << max(0, entry.retryCount-1)
	if delay > b.config.FenceRetryMaxDelay {
		delay = b.config.FenceRetryMaxDelay
	}
	if delay > 0 {
		jitter := time.Duration(rand.Int63n(int64(delay/4 + 1)))
		entry.nextEligible = entry.lastAttempt.Add(delay + jitter)
	}
	return entry.retryCount, b.config.FenceRetryCrossRunLimit > 0 && len(entry.mismatchTimes) >= b.config.FenceRetryCrossRunLimit
}

func (b *backoffQueue) markHotContention(key string, cooldown time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entries[key]
	if entry == nil {
		entry = &backoffEntry{}
		b.entries[key] = entry
	}
	entry.cooldownUntil = time.Now().Add(cooldown)
	entry.retryCount = 0
	entry.nextEligible = entry.cooldownUntil
	entry.mismatchTimes = nil
}

func (b *backoffQueue) clear(key string) {
	b.mu.Lock()
	delete(b.entries, key)
	b.mu.Unlock()
}

// ApplyResult summarizes one apply pass.
type ApplyResult struct {
	KeysProcessed       int
	VersionsDeleted     int64
	BytesFreed          int64
	FenceMismatches     int
	HotContentionKeys   int
	NamespaceBytesFreed map[string]int64
}

// PruneApplier executes a prune plan with fence validation.
type PruneApplier struct {
	config  LifecycleConfig
	backoff *backoffQueue
	metrics *LifecycleMetrics
}

// NewPruneApplier creates an applier.
func NewPruneApplier(config LifecycleConfig, metrics *LifecycleMetrics) *PruneApplier {
	return &PruneApplier{config: config, backoff: newBackoffQueue(config), metrics: metrics}
}

// Apply executes a prune plan.
func (p *PruneApplier) Apply(ctx context.Context, engine LifecycleStorageEngine, plan *PrunePlan) ApplyResult {
	result := ApplyResult{}
	if plan == nil {
		return result
	}
	result.NamespaceBytesFreed = make(map[string]int64)
	cycleStart := time.Now()
	for _, entry := range plan.Entries {
		if p.config.MaxRuntimePerCycle > 0 && result.KeysProcessed > 0 && time.Since(cycleStart) >= p.config.MaxRuntimePerCycle {
			return result
		}
		if p.config.MaxIOBudgetBytesPerInterval > 0 && result.KeysProcessed > 0 && result.BytesFreed+entry.DebtBytes > p.config.MaxIOBudgetBytesPerInterval {
			return result
		}
		select {
		case <-ctx.Done():
			return result
		default:
		}
		workStart := time.Now()
		key := string(entry.LogicalKey)
		if !p.backoff.isEligible(key) {
			continue
		}
		head, err := engine.ReadMVCCHead(ctx, entry.LogicalKey)
		if err != nil {
			continue
		}
		if head.Version.Compare(entry.HeadVersion) != 0 {
			result.FenceMismatches++
			retries, crossRunExhausted := p.backoff.recordMismatch(key)
			if retries >= p.config.FenceRetryPerKeyLimit || crossRunExhausted {
				result.HotContentionKeys++
				p.backoff.markHotContention(key, p.config.FenceRetryCooldown)
			}
			continue
		}
		deleteFailed := false
		for _, version := range entry.VersionsToDelete {
			if err := engine.DeleteMVCCVersion(ctx, entry.LogicalKey, version); err != nil {
				deleteFailed = true
				continue
			}
			result.VersionsDeleted++
		}
		if deleteFailed {
			continue
		}
		head.FloorVersion = entry.NewFloorVersion
		if err := engine.WriteMVCCHead(ctx, entry.LogicalKey, head); err == nil {
			result.KeysProcessed++
			result.BytesFreed += entry.DebtBytes
			result.NamespaceBytesFreed[namespaceFromLogicalKey(entry.LogicalKey)] += entry.DebtBytes
			p.backoff.clear(key)
		}
		if p.throttleCPUShare(ctx, time.Since(workStart)) {
			return result
		}
	}
	return result
}

func (p *PruneApplier) throttleCPUShare(ctx context.Context, workDuration time.Duration) bool {
	if p.config.MaxCPUShare <= 0 || p.config.MaxCPUShare >= 1 || workDuration <= 0 {
		return false
	}
	sleepFor := time.Duration(float64(workDuration) * ((1 / p.config.MaxCPUShare) - 1))
	if sleepFor <= 0 {
		return false
	}
	timer := time.NewTimer(sleepFor)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
