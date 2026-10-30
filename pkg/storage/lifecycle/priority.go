package lifecycle

import (
	"sort"
	"strings"
	"time"
)

// CostEstimate holds coarse compaction cost proxies.
type CostEstimate struct {
	IteratorSeeks  int
	ValueLogReads  int
	BytesRewritten int64
}

// PriorityScheduler ranks prune plan entries and enforces namespace budgets.
type PriorityScheduler struct {
	skipCounts            map[string]int
	maxSkipCount          int
	reservedSliceFraction float64
	namespaceBudgets      map[string]NamespaceBudget
}

// NewPriorityScheduler creates a scheduler.
func NewPriorityScheduler(config LifecycleConfig) *PriorityScheduler {
	budgets := config.NamespaceBudgets
	if budgets == nil {
		budgets = map[string]NamespaceBudget{}
	}
	return &PriorityScheduler{
		skipCounts:            make(map[string]int),
		maxSkipCount:          50,
		reservedSliceFraction: 0.10,
		namespaceBudgets:      budgets,
	}
}

// PriorityScore computes a debt-first score.
func PriorityScore(debtBytes int64, tombstoneDepth int, keyHotness float64, keyAge time.Duration) float64 {
	return float64(debtBytes) + float64(tombstoneDepth)*1024 + keyHotness*2048 + keyAge.Seconds()
}

// EstimateCost returns a rough cost for applying an entry.
func EstimateCost(entry PrunePlanEntry) CostEstimate {
	return CostEstimate{
		IteratorSeeks:  len(entry.VersionsToDelete) + 1,
		ValueLogReads:  len(entry.VersionsToDelete),
		BytesRewritten: entry.DebtBytes,
	}
}

// Schedule orders entries by debt-reduction-per-cost and applies namespace budgets.
func (s *PriorityScheduler) Schedule(entries []PrunePlanEntry) []PrunePlanEntry {
	type scoredEntry struct {
		entry PrunePlanEntry
		score float64
		cost  float64
	}
	scored := make([]scoredEntry, 0, len(entries))
	for _, entry := range entries {
		cost := EstimateCost(entry)
		costValue := float64(cost.IteratorSeeks+cost.ValueLogReads) + float64(cost.BytesRewritten)/1024.0
		if costValue < 1 {
			costValue = 1
		}
		boost := float64(s.skipCounts[string(entry.LogicalKey)]) * 4096
		scored = append(scored, scoredEntry{
			entry: entry,
			score: PriorityScore(entry.DebtBytes, entry.TombstoneDepth, 0, time.Since(entry.CreatedAt)) + boost,
			cost:  costValue,
		})
	}
	sort.SliceStable(scored, func(i, j int) bool {
		left := scored[i].score / scored[i].cost
		right := scored[j].score / scored[j].cost
		if left == right {
			return string(scored[i].entry.LogicalKey) < string(scored[j].entry.LogicalKey)
		}
		return left > right
	})
	ordered := make([]PrunePlanEntry, 0, len(entries))
	consumed := make(map[string]int64)
	reserved := int(float64(len(scored))*s.reservedSliceFraction + 0.5)
	reservedUsed := 0
	for _, item := range scored {
		namespace := namespaceFromLogicalKey(item.entry.LogicalKey)
		budget, hasBudget := s.namespaceBudgets[namespace]
		if hasBudget && budget.MaxBytesPerCycle > 0 && consumed[namespace]+item.entry.DebtBytes > budget.MaxBytesPerCycle {
			if reservedUsed >= reserved {
				s.RecordSkipped(string(item.entry.LogicalKey))
				continue
			}
			reservedUsed++
		}
		consumed[namespace] += item.entry.DebtBytes
		ordered = append(ordered, item.entry)
	}
	return ordered
}

// ScheduleEmergency prioritizes the highest debt-yield keys first during emergency mode.
func (s *PriorityScheduler) ScheduleEmergency(entries []PrunePlanEntry) []PrunePlanEntry {
	ordered := append([]PrunePlanEntry(nil), entries...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].DebtBytes == ordered[j].DebtBytes {
			if ordered[i].TombstoneDepth == ordered[j].TombstoneDepth {
				return string(ordered[i].LogicalKey) < string(ordered[j].LogicalKey)
			}
			return ordered[i].TombstoneDepth > ordered[j].TombstoneDepth
		}
		return ordered[i].DebtBytes > ordered[j].DebtBytes
	})
	return ordered
}

// RecordProcessed resets skip accounting for a key.
func (s *PriorityScheduler) RecordProcessed(key string) {
	delete(s.skipCounts, key)
}

// RecordSkipped increments skip accounting for a key.
func (s *PriorityScheduler) RecordSkipped(key string) {
	if s.skipCounts[key] < s.maxSkipCount {
		s.skipCounts[key]++
	}
}

func namespaceFromLogicalKey(logicalKey []byte) string {
	if len(logicalKey) <= 1 {
		return ""
	}
	parts := strings.SplitN(string(logicalKey[1:]), ":", 2)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}
