package lifecycle

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// NamespaceMetrics contains per-namespace lifecycle counters.
type NamespaceMetrics struct {
	CompactionDebtBytes atomic.Int64
	CompactionDebtKeys  atomic.Int64
	PrunableBytesTotal  atomic.Int64
	PrunedBytesTotal    atomic.Int64
}

type NamespaceDebtSummary struct {
	DebtBytes     int64
	DebtKeys      int64
	PrunableBytes int64
}

type pruneRunSample struct {
	Timestamp       time.Time
	KeysProcessed   int
	VersionsDeleted int64
	BytesFreed      int64
	FenceMismatches int
}

type debtMetricSample struct {
	Timestamp time.Time
	DebtBytes int64
	DebtKeys  int64
}

// LifecycleMetrics stores lifecycle counters and the last-run summary.
type LifecycleMetrics struct {
	BytesPinnedByOldestReader atomic.Int64
	CompactionDebtBytes       atomic.Int64
	CompactionDebtKeys        atomic.Int64
	GracefulReaderExpires     atomic.Int64
	HardReaderExpires         atomic.Int64
	PrunableBytesTotal        atomic.Int64
	PrunedBytesTotal          atomic.Int64
	TombstoneChainMaxDepth    atomic.Int64
	FloorLagVersions          atomic.Int64
	PruneRunKeysScannedTotal  atomic.Int64
	PruneStalePlanSkipsTotal  atomic.Int64
	lastRunDuration           atomic.Int64

	mu               sync.RWMutex
	lastRunResult    map[string]interface{}
	namespaceMetrics map[string]*NamespaceMetrics
	topDebtKeys      []storage.MVCCLifecycleDebtKey
	pruneHistory     []pruneRunSample
	debtHistory      []debtMetricSample
}

// NewLifecycleMetrics allocates lifecycle counters.
func NewLifecycleMetrics() *LifecycleMetrics {
	return &LifecycleMetrics{namespaceMetrics: make(map[string]*NamespaceMetrics)}
}

// RecordPruneRun stores the last run summary and increments counters.
func (m *LifecycleMetrics) RecordPruneRun(result ApplyResult, duration time.Duration) {
	now := time.Now()
	m.lastRunDuration.Store(duration.Nanoseconds())
	m.PrunedBytesTotal.Add(result.BytesFreed)
	m.PruneStalePlanSkipsTotal.Add(int64(result.FenceMismatches))
	m.mu.Lock()
	m.pruneHistory = append(m.pruneHistory, pruneRunSample{
		Timestamp:       now,
		KeysProcessed:   result.KeysProcessed,
		VersionsDeleted: result.VersionsDeleted,
		BytesFreed:      result.BytesFreed,
		FenceMismatches: result.FenceMismatches,
	})
	m.pruneHistory = trimPruneHistory(m.pruneHistory, now)
	m.lastRunResult = map[string]interface{}{
		"keys_processed":      result.KeysProcessed,
		"versions_deleted":    result.VersionsDeleted,
		"bytes_freed":         result.BytesFreed,
		"fence_mismatches":    result.FenceMismatches,
		"hot_contention_keys": result.HotContentionKeys,
	}
	m.mu.Unlock()
}

// ToMap renders status-ready lifecycle metrics.
func (m *LifecycleMetrics) ToMap(registry *ReaderRegistry) map[string]interface{} {
	m.mu.RLock()
	lastRun := m.lastRunResult
	nsMetrics := make(map[string]map[string]int64, len(m.namespaceMetrics))
	topDebtKeys := append([]storage.MVCCLifecycleDebtKey(nil), m.topDebtKeys...)
	rollups := lifecycleRollupsFromHistory(time.Now(), m.pruneHistory, m.debtHistory)
	for namespace, metric := range m.namespaceMetrics {
		nsMetrics[namespace] = map[string]int64{
			"compaction_debt_bytes": metric.CompactionDebtBytes.Load(),
			"compaction_debt_keys":  metric.CompactionDebtKeys.Load(),
			"prunable_bytes_total":  metric.PrunableBytesTotal.Load(),
			"pruned_bytes_total":    metric.PrunedBytesTotal.Load(),
		}
	}
	m.mu.RUnlock()
	status := map[string]interface{}{
		"mvcc_active_snapshot_readers":             int64(0),
		"mvcc_oldest_reader_age_seconds":           0.0,
		"mvcc_bytes_pinned_by_oldest_reader":       m.BytesPinnedByOldestReader.Load(),
		"mvcc_compaction_debt_bytes":               m.CompactionDebtBytes.Load(),
		"mvcc_compaction_debt_keys":                m.CompactionDebtKeys.Load(),
		"mvcc_snapshot_graceful_expirations_total": m.GracefulReaderExpires.Load(),
		"mvcc_snapshot_hard_expirations_total":     m.HardReaderExpires.Load(),
		"mvcc_prunable_bytes_total":                m.PrunableBytesTotal.Load(),
		"mvcc_pruned_bytes_total":                  m.PrunedBytesTotal.Load(),
		"mvcc_tombstone_chain_max_depth":           m.TombstoneChainMaxDepth.Load(),
		"mvcc_floor_lag_versions":                  m.FloorLagVersions.Load(),
		"mvcc_prune_run_duration_seconds":          float64(m.lastRunDuration.Load()) / float64(time.Second),
		"mvcc_prune_run_keys_scanned_total":        m.PruneRunKeysScannedTotal.Load(),
		"mvcc_prune_stale_plan_skips_total":        m.PruneStalePlanSkipsTotal.Load(),
		"last_run":                                 lastRun,
		"top_debt_keys":                            topDebtKeys,
		"per_namespace":                            nsMetrics,
		"rollups":                                  rollups,
	}
	if registry != nil {
		status["mvcc_active_snapshot_readers"] = registry.ActiveCount()
		status["mvcc_oldest_reader_age_seconds"] = registry.OldestReaderAge().Seconds()
		status["readers"] = registry.Snapshot()
	}
	return status
}

// RecordReaderExpiration increments expiration counters for forced reader shutdowns.
func (m *LifecycleMetrics) RecordReaderExpiration(graceful bool, hard bool) {
	if hard {
		m.HardReaderExpires.Add(1)
		return
	}
	if graceful {
		m.GracefulReaderExpires.Add(1)
	}
}

// UpdateDebt replaces current debt counters.
func (m *LifecycleMetrics) UpdateDebt(debtBytes int64, debtKeys int64) {
	m.CompactionDebtBytes.Store(debtBytes)
	m.CompactionDebtKeys.Store(debtKeys)
	now := time.Now()
	m.mu.Lock()
	m.debtHistory = append(m.debtHistory, debtMetricSample{Timestamp: now, DebtBytes: debtBytes, DebtKeys: debtKeys})
	m.debtHistory = trimDebtHistory(m.debtHistory, now)
	m.mu.Unlock()
}

// UpdatePinnedBytes updates the pinned-byte gauge.
func (m *LifecycleMetrics) UpdatePinnedBytes(bytes int64) {
	m.BytesPinnedByOldestReader.Store(bytes)
}

// ReplaceNamespaceDebt replaces namespace debt gauges for the latest plan snapshot.
func (m *LifecycleMetrics) ReplaceNamespaceDebt(summaries map[string]NamespaceDebtSummary) {
	m.mu.Lock()
	if summaries == nil {
		summaries = map[string]NamespaceDebtSummary{}
	}
	for namespace := range m.namespaceMetrics {
		if _, ok := summaries[namespace]; !ok {
			delete(m.namespaceMetrics, namespace)
		}
	}
	for namespace, summary := range summaries {
		metric := m.namespaceMetrics[namespace]
		if metric == nil {
			metric = &NamespaceMetrics{}
			m.namespaceMetrics[namespace] = metric
		}
		metric.CompactionDebtBytes.Store(summary.DebtBytes)
		metric.CompactionDebtKeys.Store(summary.DebtKeys)
		metric.PrunableBytesTotal.Store(summary.PrunableBytes)
	}
	m.mu.Unlock()
}

// AddNamespacePrunedBytes increments pruned-byte counters for one namespace.
func (m *LifecycleMetrics) AddNamespacePrunedBytes(namespace string, bytesFreed int64) {
	if namespace == "" || bytesFreed == 0 {
		return
	}
	m.mu.Lock()
	metric := m.namespaceMetrics[namespace]
	if metric == nil {
		metric = &NamespaceMetrics{}
		m.namespaceMetrics[namespace] = metric
	}
	m.mu.Unlock()
	metric.PrunedBytesTotal.Add(bytesFreed)
}

// UpdatePlanInsights refreshes derived gauges from the latest plan.
func (m *LifecycleMetrics) UpdatePlanInsights(plan *PrunePlan) {
	if plan == nil {
		m.TombstoneChainMaxDepth.Store(0)
		m.FloorLagVersions.Store(0)
		return
	}
	var maxTombstoneDepth int64
	var maxFloorLag int64
	topDebtKeys := topDebtKeySummaries(plan.Entries, 16)
	for _, entry := range plan.Entries {
		if int64(entry.TombstoneDepth) > maxTombstoneDepth {
			maxTombstoneDepth = int64(entry.TombstoneDepth)
		}
		if int64(len(entry.VersionsToDelete)) > maxFloorLag {
			maxFloorLag = int64(len(entry.VersionsToDelete))
		}
	}
	m.TombstoneChainMaxDepth.Store(maxTombstoneDepth)
	m.FloorLagVersions.Store(maxFloorLag)
	m.PruneRunKeysScannedTotal.Add(int64(plan.KeysScanned))
	m.mu.Lock()
	m.topDebtKeys = topDebtKeys
	m.mu.Unlock()
}

// TopDebtKeys returns the highest-debt logical keys from the latest evaluated plan.
func (m *LifecycleMetrics) TopDebtKeys(limit int) []storage.MVCCLifecycleDebtKey {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if limit <= 0 || limit >= len(m.topDebtKeys) {
		return append([]storage.MVCCLifecycleDebtKey(nil), m.topDebtKeys...)
	}
	return append([]storage.MVCCLifecycleDebtKey(nil), m.topDebtKeys[:limit]...)
}

func topDebtKeySummaries(entries []PrunePlanEntry, limit int) []storage.MVCCLifecycleDebtKey {
	if len(entries) == 0 {
		return nil
	}
	summaries := make([]storage.MVCCLifecycleDebtKey, 0, len(entries))
	for _, entry := range entries {
		summaries = append(summaries, storage.MVCCLifecycleDebtKey{
			LogicalKey:       string(entry.LogicalKey),
			Namespace:        namespaceFromLogicalKey(entry.LogicalKey),
			DebtBytes:        entry.DebtBytes,
			TombstoneDepth:   entry.TombstoneDepth,
			FloorLagVersions: len(entry.VersionsToDelete),
			VersionsToDelete: len(entry.VersionsToDelete),
		})
	}
	sort.SliceStable(summaries, func(i, j int) bool {
		if summaries[i].DebtBytes == summaries[j].DebtBytes {
			return summaries[i].LogicalKey < summaries[j].LogicalKey
		}
		return summaries[i].DebtBytes > summaries[j].DebtBytes
	})
	if limit > 0 && len(summaries) > limit {
		summaries = summaries[:limit]
	}
	return summaries
}

func namespaceForInfo(info storage.SnapshotReaderInfo) string {
	return info.Namespace
}

func trimPruneHistory(samples []pruneRunSample, now time.Time) []pruneRunSample {
	cutoff := now.Add(-60 * time.Second)
	trimmed := samples[:0]
	for _, sample := range samples {
		if !sample.Timestamp.Before(cutoff) {
			trimmed = append(trimmed, sample)
		}
	}
	return trimmed
}

func trimDebtHistory(samples []debtMetricSample, now time.Time) []debtMetricSample {
	cutoff := now.Add(-60 * time.Second)
	trimmed := samples[:0]
	for _, sample := range samples {
		if !sample.Timestamp.Before(cutoff) {
			trimmed = append(trimmed, sample)
		}
	}
	return trimmed
}

func lifecycleRollupsFromHistory(now time.Time, pruneHistory []pruneRunSample, debtHistory []debtMetricSample) map[string]map[string]int64 {
	return map[string]map[string]int64{
		"10s": summarizeLifecycleWindow(now.Add(-10*time.Second), pruneHistory, debtHistory),
		"60s": summarizeLifecycleWindow(now.Add(-60*time.Second), pruneHistory, debtHistory),
	}
}

func summarizeLifecycleWindow(cutoff time.Time, pruneHistory []pruneRunSample, debtHistory []debtMetricSample) map[string]int64 {
	summary := map[string]int64{
		"prune_runs":                0,
		"keys_processed":            0,
		"versions_deleted":          0,
		"bytes_freed":               0,
		"fence_mismatches":          0,
		"compaction_debt_bytes_max": 0,
		"compaction_debt_keys_max":  0,
	}
	for _, sample := range pruneHistory {
		if sample.Timestamp.Before(cutoff) {
			continue
		}
		summary["prune_runs"]++
		summary["keys_processed"] += int64(sample.KeysProcessed)
		summary["versions_deleted"] += sample.VersionsDeleted
		summary["bytes_freed"] += sample.BytesFreed
		summary["fence_mismatches"] += int64(sample.FenceMismatches)
	}
	for _, sample := range debtHistory {
		if sample.Timestamp.Before(cutoff) {
			continue
		}
		if sample.DebtBytes > summary["compaction_debt_bytes_max"] {
			summary["compaction_debt_bytes_max"] = sample.DebtBytes
		}
		if sample.DebtKeys > summary["compaction_debt_keys_max"] {
			summary["compaction_debt_keys_max"] = sample.DebtKeys
		}
	}
	return summary
}
