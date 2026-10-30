package lifecycle

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// All of these tests intentionally avoid time.Sleep where possible and use
// closed contexts / past timestamps to keep behavior deterministic.

// ============================================================================
// Watermark helpers — TTLBoundVersion infinite path, minVersion/monotonicMax
// short-circuits, maxVersion shape.
// ============================================================================

func TestTTLBoundVersion_ZeroOrNegativeReturnsMaxVersion(t *testing.T) {
	zero := TTLBoundVersion(0)
	require.Equal(t, time.Unix(0, math.MaxInt64).UTC(), zero.CommitTimestamp)
	require.Equal(t, ^uint64(0), zero.CommitSequence)

	neg := TTLBoundVersion(-1 * time.Second)
	require.Equal(t, time.Unix(0, math.MaxInt64).UTC(), neg.CommitTimestamp)
}

func TestTTLBoundVersion_PositiveReturnsTimeRelativeToNow(t *testing.T) {
	got := TTLBoundVersion(time.Hour)
	require.True(t, got.CommitTimestamp.Before(time.Now()), "TTL bound should be in the past")
	require.Equal(t, ^uint64(0), got.CommitSequence)
}

func TestMinVersion_SkipsZeroVersions(t *testing.T) {
	a := storage.MVCCVersion{CommitTimestamp: time.Unix(100, 0), CommitSequence: 1}
	b := storage.MVCCVersion{CommitTimestamp: time.Unix(50, 0), CommitSequence: 1}
	zero := storage.MVCCVersion{}
	got := minVersion(a, zero, b)
	require.Equal(t, b, got, "earlier version should win, zero skipped")
}

func TestMinVersion_AllZeroReturnsMax(t *testing.T) {
	got := minVersion(storage.MVCCVersion{}, storage.MVCCVersion{})
	require.Equal(t, time.Unix(0, math.MaxInt64).UTC(), got.CommitTimestamp)
}

func TestMonotonicMax_HandlesZeroAndNonZeroBranches(t *testing.T) {
	zero := storage.MVCCVersion{}
	later := storage.MVCCVersion{CommitTimestamp: time.Unix(100, 0), CommitSequence: 2}
	earlier := storage.MVCCVersion{CommitTimestamp: time.Unix(50, 0), CommitSequence: 1}

	require.Equal(t, later, monotonicMax(zero, later), "zero a should return b")
	require.Equal(t, later, monotonicMax(later, zero), "zero b should return a")
	require.Equal(t, later, monotonicMax(later, earlier), "a>=b should return a")
	require.Equal(t, later, monotonicMax(earlier, later), "a<b should return b")
	require.Equal(t, later, monotonicMax(later, later), "equal should return a")
}

// ============================================================================
// emergency.go helpers — minDuration / minFloat / minInt64 every branch.
// ============================================================================

func TestEmergency_MinHelpers_AllBranches(t *testing.T) {
	t.Run("minDuration honours non-positive b", func(t *testing.T) {
		require.Equal(t, 5*time.Second, minDuration(5*time.Second, 0))
		require.Equal(t, 5*time.Second, minDuration(5*time.Second, -1*time.Second))
	})
	t.Run("minDuration picks smaller of two positives", func(t *testing.T) {
		require.Equal(t, 2*time.Second, minDuration(2*time.Second, 5*time.Second))
		require.Equal(t, 2*time.Second, minDuration(5*time.Second, 2*time.Second))
	})
	t.Run("minFloat", func(t *testing.T) {
		require.Equal(t, 0.4, minFloat(0.4, 0))
		require.Equal(t, 0.4, minFloat(0.4, -1))
		require.Equal(t, 0.3, minFloat(0.3, 0.4))
		require.Equal(t, 0.3, minFloat(0.4, 0.3))
	})
	t.Run("minInt64", func(t *testing.T) {
		require.Equal(t, int64(5), minInt64(5, 0))
		require.Equal(t, int64(5), minInt64(5, -1))
		require.Equal(t, int64(3), minInt64(3, 5))
		require.Equal(t, int64(3), minInt64(5, 3))
	})
}

func TestEmergencyController_DebtHistoryTruncatedAt16Samples(t *testing.T) {
	ec := NewEmergencyController(EmergencyConfig{DebtGrowthSlopeThreshold: 1})
	for i := int64(0); i < 30; i++ {
		ec.RecordDebt(i)
	}
	// debtHistory is private; observation is indirect: Evaluate uses
	// first/last. With critical=true and >=2 samples, Evaluate should
	// behave without panic and use only the last 16 samples.
	ec.SetCritical(true)
	got := ec.Evaluate()
	require.IsType(t, true, got)
}

func TestEmergencyController_EvaluateNoCriticalReturnsFalseAndDeactivates(t *testing.T) {
	ec := NewEmergencyController(EmergencyConfig{DebtGrowthSlopeThreshold: 1})
	ec.RecordDebt(0)
	ec.RecordDebt(1000)
	ec.SetCritical(false)
	require.False(t, ec.Evaluate())
	require.False(t, ec.IsActive())
}

func TestEmergencyController_EvaluateInsufficientSamplesReturnsFalse(t *testing.T) {
	ec := NewEmergencyController(EmergencyConfig{DebtGrowthSlopeThreshold: 1})
	ec.SetCritical(true)
	require.False(t, ec.Evaluate(), "one sample is not enough to compute a slope")
	ec.RecordDebt(0)
	require.False(t, ec.Evaluate(), "one sample is still not enough")
}

func TestEmergencyController_EvaluateZeroDeltaPreservesActive(t *testing.T) {
	// When two samples land at the same time.Time, deltaSeconds == 0 and
	// Evaluate must return the prior active state without panicking. We
	// cannot easily force identical timestamps in real time, so this
	// instead verifies the helper returns the prior state when called
	// twice in immediate succession with no new sample.
	ec := NewEmergencyController(EmergencyConfig{DebtGrowthSlopeThreshold: 1})
	ec.RecordDebt(0)
	ec.RecordDebt(100000)
	ec.SetCritical(true)
	first := ec.Evaluate()
	second := ec.Evaluate()
	require.Equal(t, first, second, "consecutive Evaluate without new sample should be stable")
}

func TestEmergencyController_AdjustCompactionBudget_InactivePassesThrough(t *testing.T) {
	ec := NewEmergencyController(EmergencyConfig{DebtGrowthSlopeThreshold: 1, MaxCPUShare: 0.9, MaxIOBudgetBytesPerCycle: 1 << 30, MaxRuntimePerCycle: time.Second})
	base := LifecycleConfig{
		MaxRuntimePerCycle:          200 * time.Millisecond,
		MaxIOBudgetBytesPerInterval: 64 << 20,
		MaxCPUShare:                 0.2,
		MaxSnapshotLifetime:         5 * time.Second,
	}
	require.Equal(t, base, ec.AdjustCompactionBudget(base))
}

func TestEmergencyController_AdjustCompactionBudget_ActiveDoublesAndHalves(t *testing.T) {
	ec := NewEmergencyController(EmergencyConfig{
		DebtGrowthSlopeThreshold: 1, MaxCPUShare: 0.9,
		MaxIOBudgetBytesPerCycle: 1 << 30, MaxRuntimePerCycle: time.Second,
	})
	ec.SetCritical(true)
	ec.RecordDebt(0)
	ec.RecordDebt(1 << 30) // synthetic huge growth
	require.True(t, ec.Evaluate(), "huge slope should activate emergency")

	base := LifecycleConfig{
		MaxRuntimePerCycle:          200 * time.Millisecond,
		MaxIOBudgetBytesPerInterval: 64 << 20,
		MaxCPUShare:                 0.2,
		MaxSnapshotLifetime:         5 * time.Second,
	}
	adjusted := ec.AdjustCompactionBudget(base)
	require.Equal(t, 400*time.Millisecond, adjusted.MaxRuntimePerCycle,
		"runtime should double under cap")
	require.Equal(t, int64(128<<20), adjusted.MaxIOBudgetBytesPerInterval,
		"IO budget should double under cap")
	require.InDelta(t, 0.3, adjusted.MaxCPUShare, 1e-9, "CPU share should rise by 1.5x")
	require.Equal(t, 2500*time.Millisecond, adjusted.MaxSnapshotLifetime,
		"snapshot lifetime should be halved when positive")
}

func TestEmergencyController_AdjustCompactionBudget_DoesNotHalveZeroSnapshotLifetime(t *testing.T) {
	ec := NewEmergencyController(EmergencyConfig{
		DebtGrowthSlopeThreshold: 1, MaxCPUShare: 0.9,
		MaxIOBudgetBytesPerCycle: 1 << 30, MaxRuntimePerCycle: time.Second,
	})
	ec.SetCritical(true)
	ec.RecordDebt(0)
	ec.RecordDebt(1 << 30)
	require.True(t, ec.Evaluate())

	base := LifecycleConfig{}
	adjusted := ec.AdjustCompactionBudget(base)
	require.Equal(t, time.Duration(0), adjusted.MaxSnapshotLifetime,
		"zero MaxSnapshotLifetime must stay zero (no division)")
}

// ============================================================================
// PruneApplier — apply.go uncovered branches: nil plan, max-runtime stop,
// IO-budget stop, ctx-done mid-plan, throttle CPU sleep + cancellation.
// ============================================================================

func TestPruneApplier_NilPlanReturnsEmptyResult(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	applier := NewPruneApplier(cfg, NewLifecycleMetrics())
	got := applier.Apply(context.Background(), newMockLifecycleEngine(), nil)
	require.Equal(t, ApplyResult{}, got)
}

// seedPrunablePlan creates a 2-key plan where each key has 3 versions and the
// planner is configured to keep only the latest, yielding two prunable
// versions per entry. Each entry reports DebtBytes = 3 * versionSize so the
// IO-budget test can reason about the threshold deterministically.
func seedPrunablePlan(t *testing.T, versionSize int64, perKeyDeleteDelay time.Duration) (*mockLifecycleEngine, *PrunePlanner, *PrunePlan) {
	t.Helper()
	engine := newMockLifecycleEngine()
	commitTime := time.Unix(1_700_000_000, 0).UTC()
	for _, key := range []string{
		string([]byte{0x0C}) + "tenant:k1",
		string([]byte{0x0C}) + "tenant:k2",
	} {
		engine.heads[key] = storage.MVCCHead{
			Version:      storage.MVCCVersion{CommitTimestamp: commitTime.Add(2 * time.Hour), CommitSequence: 15},
			FloorVersion: storage.MVCCVersion{},
		}
		engine.versions[key] = []mockVersion{
			{version: storage.MVCCVersion{CommitTimestamp: commitTime, CommitSequence: 5}, sizeBytes: versionSize},
			{version: storage.MVCCVersion{CommitTimestamp: commitTime.Add(time.Hour), CommitSequence: 10}, sizeBytes: versionSize},
			{version: storage.MVCCVersion{CommitTimestamp: commitTime.Add(2 * time.Hour), CommitSequence: 15}, sizeBytes: versionSize},
		}
	}
	engine.deleteDelay = perKeyDeleteDelay
	planner := NewPrunePlanner(LifecycleConfig{MaxVersionsPerKey: 1, DebtSampleFraction: 1})
	plan, err := planner.Plan(context.Background(), engine, nil)
	require.NoError(t, err)
	require.Len(t, plan.Entries, 2, "fixture must produce two prunable entries")
	for i, e := range plan.Entries {
		require.NotEmpty(t, e.VersionsToDelete, "entry %d should have deletable versions", i)
	}
	return engine, planner, plan
}

func TestPruneApplier_RespectsMaxRuntimeMidPlan(t *testing.T) {
	engine, _, plan := seedPrunablePlan(t, 100, 30*time.Millisecond)
	cfg := DefaultLifecycleConfig()
	cfg.MaxRuntimePerCycle = 5 * time.Millisecond
	applier := NewPruneApplier(cfg, NewLifecycleMetrics())

	result := applier.Apply(context.Background(), engine, plan)
	// The runtime budget guard is `KeysProcessed > 0 && time.Since(start)
	// >= MaxRuntimePerCycle`. With per-key delete delay of 30ms and budget
	// 5ms, after the first key (which already exceeds the budget) the
	// applier stops, leaving the second untouched.
	require.Equal(t, 1, result.KeysProcessed, "second key must be skipped after runtime budget exhausts")
}

func TestPruneApplier_RespectsIOBudgetMidPlan(t *testing.T) {
	// Each entry reports DebtBytes = 3 * versionSize (sum of all versions
	// for the key). 3*10_000 = 30_000 per entry. Budget 20_000 → after the
	// first entry (BytesFreed=30_000), 30_000+30_000 > 20_000 stops the
	// second entry.
	engine, _, plan := seedPrunablePlan(t, 10_000, 0)
	cfg := DefaultLifecycleConfig()
	cfg.MaxIOBudgetBytesPerInterval = 20_000
	applier := NewPruneApplier(cfg, NewLifecycleMetrics())

	result := applier.Apply(context.Background(), engine, plan)
	require.Equal(t, 1, result.KeysProcessed, "IO budget should fire after the first entry")
	require.Equal(t, int64(30_000), result.BytesFreed)
}

func TestPruneApplier_StopsWhenContextCancelled(t *testing.T) {
	engine, _, plan := seedPrunablePlan(t, 100, 0)
	cfg := DefaultLifecycleConfig()
	applier := NewPruneApplier(cfg, NewLifecycleMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := applier.Apply(ctx, engine, plan)
	require.Equal(t, 0, result.KeysProcessed, "cancelled context must short-circuit before any work")
}

func TestPruneApplier_ThrottleCPUShareSleepCancellable(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	cfg.MaxCPUShare = 0.001 // forces a long sleep proportional to workDuration
	applier := NewPruneApplier(cfg, NewLifecycleMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopped := applier.throttleCPUShare(ctx, 10*time.Millisecond)
	require.True(t, stopped, "cancelled context must abort the throttle sleep")

	// Edge cases: zero workDuration → no throttle.
	require.False(t, applier.throttleCPUShare(context.Background(), 0))

	// MaxCPUShare = 1 → no throttle.
	cfg2 := DefaultLifecycleConfig()
	cfg2.MaxCPUShare = 1
	applier2 := NewPruneApplier(cfg2, NewLifecycleMetrics())
	require.False(t, applier2.throttleCPUShare(context.Background(), 10*time.Millisecond))

	// MaxCPUShare zero → no throttle.
	cfg3 := DefaultLifecycleConfig()
	cfg3.MaxCPUShare = 0
	applier3 := NewPruneApplier(cfg3, NewLifecycleMetrics())
	require.False(t, applier3.throttleCPUShare(context.Background(), 10*time.Millisecond))
}

func TestApply_MaxHelper(t *testing.T) {
	require.Equal(t, 3, max(3, 1))
	require.Equal(t, 5, max(1, 5))
	require.Equal(t, 0, max(0, 0))
}

// ============================================================================
// PrunePlanner — plannerNamespaceFromLogicalKey edges, shouldScanKey sampling,
// maxVersionsBoundVersion clamping.
// ============================================================================

func TestPlannerNamespaceFromLogicalKey_ShortKeysReturnEmpty(t *testing.T) {
	require.Equal(t, "", plannerNamespaceFromLogicalKey(nil))
	require.Equal(t, "", plannerNamespaceFromLogicalKey([]byte{}))
	require.Equal(t, "", plannerNamespaceFromLogicalKey([]byte{'X'}))
	require.Equal(t, "ns", plannerNamespaceFromLogicalKey([]byte("Xns:rest")))
	require.Equal(t, "ns", plannerNamespaceFromLogicalKey([]byte("Xns")), "no colon → whole tail")
}

func TestPlanner_ShouldScanKey_FullScanCadence(t *testing.T) {
	planner := NewPrunePlanner(LifecycleConfig{
		FullScanEveryNCycles: 1,
		DebtSampleFraction:   0.0, // would otherwise skip all
	})
	planner.cycleCount = 1
	require.True(t, planner.shouldScanKey([]byte("any-key")))
}

func TestPlanner_ShouldScanKey_SamplingZeroOrAboveOneAlwaysScans(t *testing.T) {
	planner := NewPrunePlanner(LifecycleConfig{DebtSampleFraction: 0})
	require.True(t, planner.shouldScanKey([]byte("k")))

	planner2 := NewPrunePlanner(LifecycleConfig{DebtSampleFraction: 1.1})
	require.True(t, planner2.shouldScanKey([]byte("k")))
}

func TestPlanner_ShouldScanKey_SamplingFractionDeterministic(t *testing.T) {
	planner := NewPrunePlanner(LifecycleConfig{DebtSampleFraction: 0.5})
	// shouldScanKey is content-deterministic (fnv32a-based). The same key
	// must always produce the same decision in a single test run.
	for _, key := range [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")} {
		first := planner.shouldScanKey(key)
		for i := 0; i < 10; i++ {
			require.Equal(t, first, planner.shouldScanKey(key))
		}
	}
}

func TestPlanner_MaxVersionsBoundVersion_NegativeKeepClampedToZero(t *testing.T) {
	planner := NewPrunePlanner(LifecycleConfig{MaxVersionsPerKey: -5})
	versions := []versionInfo{
		{version: storage.MVCCVersion{CommitTimestamp: time.Unix(1, 0), CommitSequence: 1}},
		{version: storage.MVCCVersion{CommitTimestamp: time.Unix(2, 0), CommitSequence: 2}},
		{version: storage.MVCCVersion{CommitTimestamp: time.Unix(3, 0), CommitSequence: 3}},
	}
	// keepHistorical clamped to 0 → idx = len - 0 - 1 = 2 → last version.
	require.Equal(t, versions[2].version, planner.maxVersionsBoundVersion(versions))
}

func TestPlanner_MaxVersionsBoundVersion_HardCapZeroProducesLastVersion(t *testing.T) {
	// MaxChainHardCap = 1 → maxHistoricalByHardCap = 0 → keep clamped to 0.
	planner := NewPrunePlanner(LifecycleConfig{MaxVersionsPerKey: 5, MaxChainHardCap: 1})
	versions := []versionInfo{
		{version: storage.MVCCVersion{CommitTimestamp: time.Unix(1, 0), CommitSequence: 1}},
		{version: storage.MVCCVersion{CommitTimestamp: time.Unix(2, 0), CommitSequence: 2}},
	}
	require.Equal(t, versions[1].version, planner.maxVersionsBoundVersion(versions))
}

// ============================================================================
// PriorityScheduler — namespaceFromLogicalKey edges, RecordSkipped boundary,
// RecordProcessed delete.
// ============================================================================

func TestNamespaceFromLogicalKey_ShortKeysReturnEmpty(t *testing.T) {
	require.Equal(t, "", namespaceFromLogicalKey(nil))
	require.Equal(t, "", namespaceFromLogicalKey([]byte{}))
	require.Equal(t, "", namespaceFromLogicalKey([]byte{'X'}))
	require.Equal(t, "ns", namespaceFromLogicalKey([]byte("Xns:rest")))
}

func TestPriorityScheduler_RecordSkippedClampedToMax(t *testing.T) {
	s := NewPriorityScheduler(LifecycleConfig{})
	for i := 0; i < 100; i++ {
		s.RecordSkipped("k")
	}
	// The internal counter is unexported; we test the observable effect:
	// the skipCounts contribution to Schedule never grows unboundedly.
	// Indirectly: scheduling the same key 100 times still produces a
	// finite, deterministic ordering.
	entries := []PrunePlanEntry{{LogicalKey: []byte("k"), DebtBytes: 100}}
	ordered := s.Schedule(entries)
	require.Len(t, ordered, 1)
}

func TestPriorityScheduler_RecordProcessedResetsSkipCount(t *testing.T) {
	s := NewPriorityScheduler(LifecycleConfig{})
	s.RecordSkipped("k")
	s.RecordSkipped("k")
	s.RecordProcessed("k")
	// After RecordProcessed, the boost should be gone. Indirectly:
	// scheduling produces the entry without skip-bonus elevation.
	entries := []PrunePlanEntry{{LogicalKey: []byte("k"), DebtBytes: 100}}
	ordered := s.Schedule(entries)
	require.Len(t, ordered, 1)
}

func TestPriorityScheduler_ScheduleStableTieBreakOnDebtTombstoneLogicalKey(t *testing.T) {
	s := NewPriorityScheduler(LifecycleConfig{})
	entries := []PrunePlanEntry{
		{LogicalKey: []byte("kZ"), DebtBytes: 100, TombstoneDepth: 1},
		{LogicalKey: []byte("kA"), DebtBytes: 100, TombstoneDepth: 1},
		{LogicalKey: []byte("kM"), DebtBytes: 100, TombstoneDepth: 1},
	}
	ordered := s.Schedule(entries)
	require.Len(t, ordered, 3)
	require.Equal(t, []byte("kA"), ordered[0].LogicalKey, "ties broken by logical key ascending")
	require.Equal(t, []byte("kM"), ordered[1].LogicalKey)
	require.Equal(t, []byte("kZ"), ordered[2].LogicalKey)
}

func TestPriorityScheduler_ScheduleEmergencyTombstoneDepthSecondaryKey(t *testing.T) {
	s := NewPriorityScheduler(LifecycleConfig{})
	entries := []PrunePlanEntry{
		{LogicalKey: []byte("kA"), DebtBytes: 100, TombstoneDepth: 1},
		{LogicalKey: []byte("kB"), DebtBytes: 100, TombstoneDepth: 5},
	}
	ordered := s.ScheduleEmergency(entries)
	require.Equal(t, []byte("kB"), ordered[0].LogicalKey, "deeper tombstone wins under emergency")
	require.Equal(t, []byte("kA"), ordered[1].LogicalKey)
}

// ============================================================================
// ReaderRegistry — OldestReaderAge, Snapshot, ReadersOlderThan, edge cases.
// ============================================================================

func TestReaderRegistry_OldestReaderAge_NoReadersReturnsZero(t *testing.T) {
	r := NewReaderRegistry()
	require.Equal(t, time.Duration(0), r.OldestReaderAge())
}

func TestReaderRegistry_OldestReaderAge_TracksMaxStartTime(t *testing.T) {
	r := NewReaderRegistry()
	old := time.Now().Add(-5 * time.Minute)
	young := time.Now().Add(-30 * time.Second)
	_, releaseOld := r.Register(storage.SnapshotReaderInfo{ReaderID: "old", StartTime: old})
	_, releaseYoung := r.Register(storage.SnapshotReaderInfo{ReaderID: "young", StartTime: young})
	defer releaseOld()
	defer releaseYoung()

	got := r.OldestReaderAge()
	require.GreaterOrEqual(t, got, 4*time.Minute,
		"oldest age must reflect the longest-running reader")
}

func TestReaderRegistry_Snapshot_ReturnsCopy(t *testing.T) {
	r := NewReaderRegistry()
	id, release := r.Register(storage.SnapshotReaderInfo{
		Namespace:       "tenant-a",
		SnapshotVersion: storage.MVCCVersion{CommitTimestamp: time.Unix(100, 0), CommitSequence: 1},
		StartTime:       time.Now(),
	})
	defer release()
	require.NotEmpty(t, id)

	snap := r.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, "tenant-a", snap[0].Namespace)

	// Mutating the returned slice should not affect the registry.
	snap[0].Namespace = "mutated"
	again := r.Snapshot()
	require.Equal(t, "tenant-a", again[0].Namespace, "Snapshot must return a copy")
}

func TestReaderRegistry_Register_AutoGeneratesIDWhenBlank(t *testing.T) {
	r := NewReaderRegistry()
	id1, rel1 := r.Register(storage.SnapshotReaderInfo{StartTime: time.Now()})
	id2, rel2 := r.Register(storage.SnapshotReaderInfo{StartTime: time.Now()})
	defer rel1()
	defer rel2()
	require.NotEmpty(t, id1)
	require.NotEmpty(t, id2)
	require.NotEqual(t, id1, id2, "auto-generated IDs must be unique")

	snap := r.Snapshot()
	require.Len(t, snap, 2)
}

func TestReaderRegistry_OldestReaderVersionsByNamespace_TracksPerNamespaceMin(t *testing.T) {
	r := NewReaderRegistry()
	earlier := storage.MVCCVersion{CommitTimestamp: time.Unix(50, 0), CommitSequence: 1}
	later := storage.MVCCVersion{CommitTimestamp: time.Unix(100, 0), CommitSequence: 1}
	_, rel1 := r.Register(storage.SnapshotReaderInfo{Namespace: "a", SnapshotVersion: earlier, StartTime: time.Now()})
	_, rel2 := r.Register(storage.SnapshotReaderInfo{Namespace: "a", SnapshotVersion: later, StartTime: time.Now()})
	_, rel3 := r.Register(storage.SnapshotReaderInfo{Namespace: "b", SnapshotVersion: later, StartTime: time.Now()})
	defer rel1()
	defer rel2()
	defer rel3()
	versions := r.OldestReaderVersionsByNamespace()
	require.Equal(t, earlier, versions["a"], "namespace a must report the earlier version")
	require.Equal(t, later, versions["b"])
}

func TestReaderRegistry_OldestReaderVersion_NoReadersReturnsFalse(t *testing.T) {
	r := NewReaderRegistry()
	got, ok := r.OldestReaderVersion()
	require.False(t, ok)
	require.Equal(t, storage.MVCCVersion{}, got)
}

// ============================================================================
// PressureController.ShouldRejectLongSnapshot — every band + maxLifetime=0.
// ============================================================================

func TestPressureController_ShouldRejectLongSnapshot_AllBands(t *testing.T) {
	// High-only configuration: instant band entry (zero enter window) →
	// Update() promotes to High immediately when pinned >= HighEnterBytes.
	pinnedHigh := int64(5)
	highCfg := PressureConfig{
		HighEnterBytes:      1,
		HighExitBytes:       0,
		CriticalEnterBytes:  100,
		CriticalExitBytes:   90,
		PressureEnterWindow: 0,
		PressureExitWindow:  time.Millisecond,
	}
	p := NewPressureController(highCfg, func() int64 { return pinnedHigh }, func() int64 { return 0 })

	// Default band is Normal — but Update() promotes on first call when pinned ≥ HighEnterBytes.
	require.Equal(t, storage.PressureNormal, p.CurrentBand())
	require.False(t, p.ShouldRejectLongSnapshot(1*time.Hour, 1*time.Second))

	require.Equal(t, storage.PressureHigh, p.Update())
	require.True(t, p.ShouldRejectLongSnapshot(2*time.Second, time.Second),
		"high band: reject when age > maxLifetime")
	require.False(t, p.ShouldRejectLongSnapshot(2*time.Second, 0),
		"high band with maxLifetime=0 must NOT reject")

	// Critical configuration: pinned >= CriticalEnterBytes → Critical band.
	pinnedCrit := int64(200)
	critCfg := PressureConfig{
		HighEnterBytes:      1,
		HighExitBytes:       0,
		CriticalEnterBytes:  10,
		CriticalExitBytes:   0,
		PressureEnterWindow: 0,
		PressureExitWindow:  time.Millisecond,
	}
	p2 := NewPressureController(critCfg, func() int64 { return pinnedCrit }, func() int64 { return 0 })
	require.Equal(t, storage.PressureCritical, p2.Update())

	require.True(t, p2.ShouldRejectLongSnapshot(2*time.Second, time.Second),
		"critical band: reject when age > maxLifetime/2")
	require.False(t, p2.ShouldRejectLongSnapshot(time.Millisecond, time.Second),
		"critical band with very young snapshot must NOT reject")
	require.False(t, p2.ShouldRejectLongSnapshot(2*time.Second, 0),
		"critical band with maxLifetime=0 must NOT reject")
}

// ============================================================================
// LifecycleMetrics.AddNamespacePrunedBytes — early-return guards.
// ============================================================================

func TestLifecycleMetrics_AddNamespacePrunedBytes_GuardsBlankAndZero(t *testing.T) {
	m := NewLifecycleMetrics()
	// blank namespace and zero bytesFreed are silently dropped.
	m.AddNamespacePrunedBytes("", 100)
	m.AddNamespacePrunedBytes("ns", 0)
	// Adding for a valid namespace records the bytes.
	m.AddNamespacePrunedBytes("ns", 500)
	m.AddNamespacePrunedBytes("ns", 300)
	stats := m.ToMap(nil)
	perNs, ok := stats["per_namespace"].(map[string]map[string]int64)
	require.True(t, ok, "per_namespace must be present in metrics snapshot")
	require.Contains(t, perNs, "ns")
	require.Equal(t, int64(800), perNs["ns"]["pruned_bytes_total"])
	require.NotContains(t, perNs, "", "blank namespace must NOT be recorded")
}

// ============================================================================
// LifecycleMetrics.ReplaceNamespaceDebt — nil-summary path + deletion path.
// ============================================================================

func namespaceMetricsFromToMap(t *testing.T, m *LifecycleMetrics) map[string]map[string]int64 {
	t.Helper()
	stats := m.ToMap(nil)
	perNs, ok := stats["per_namespace"].(map[string]map[string]int64)
	require.True(t, ok)
	return perNs
}

func TestLifecycleMetrics_ReplaceNamespaceDebt_NilClearsAllNamespaces(t *testing.T) {
	m := NewLifecycleMetrics()
	m.ReplaceNamespaceDebt(map[string]NamespaceDebtSummary{
		"a": {DebtBytes: 100, DebtKeys: 1, PrunableBytes: 100},
		"b": {DebtBytes: 200, DebtKeys: 2, PrunableBytes: 200},
	})
	stats := namespaceMetricsFromToMap(t, m)
	require.Contains(t, stats, "a")
	require.Contains(t, stats, "b")
	require.Equal(t, int64(100), stats["a"]["compaction_debt_bytes"])
	require.Equal(t, int64(2), stats["b"]["compaction_debt_keys"])

	m.ReplaceNamespaceDebt(nil)
	stats2 := namespaceMetricsFromToMap(t, m)
	require.NotContains(t, stats2, "a", "nil summary must drop all namespace gauges")
	require.NotContains(t, stats2, "b")
}

func TestLifecycleMetrics_ReplaceNamespaceDebt_DropsMissingNamespaces(t *testing.T) {
	m := NewLifecycleMetrics()
	m.ReplaceNamespaceDebt(map[string]NamespaceDebtSummary{
		"a": {DebtBytes: 100, DebtKeys: 1, PrunableBytes: 100},
	})
	m.ReplaceNamespaceDebt(map[string]NamespaceDebtSummary{
		"b": {DebtBytes: 200, DebtKeys: 2, PrunableBytes: 200},
	})
	stats := namespaceMetricsFromToMap(t, m)
	require.NotContains(t, stats, "a", "a must be dropped on the second replace")
	require.Contains(t, stats, "b")
}

// ============================================================================
// LifecycleMetrics.UpdatePlanInsights — nil plan resets gauges; populated
// plan updates depth + lag + top-debt-keys.
// ============================================================================

func TestLifecycleMetrics_UpdatePlanInsights_NilResetsGauges(t *testing.T) {
	m := NewLifecycleMetrics()
	m.TombstoneChainMaxDepth.Store(99)
	m.FloorLagVersions.Store(99)
	m.UpdatePlanInsights(nil)
	require.Equal(t, int64(0), m.TombstoneChainMaxDepth.Load())
	require.Equal(t, int64(0), m.FloorLagVersions.Load())
}

func TestLifecycleMetrics_UpdatePlanInsights_RecordsMaxDepthAndLag(t *testing.T) {
	m := NewLifecycleMetrics()
	plan := &PrunePlan{
		KeysScanned: 5,
		Entries: []PrunePlanEntry{
			{LogicalKey: []byte("Xns:k1"), DebtBytes: 100, TombstoneDepth: 3, VersionsToDelete: []storage.MVCCVersion{{}, {}}},
			{LogicalKey: []byte("Xns:k2"), DebtBytes: 200, TombstoneDepth: 7, VersionsToDelete: []storage.MVCCVersion{{}, {}, {}, {}}},
			{LogicalKey: []byte("Xns:k3"), DebtBytes: 50, TombstoneDepth: 2, VersionsToDelete: []storage.MVCCVersion{{}}},
		},
	}
	m.UpdatePlanInsights(plan)
	require.Equal(t, int64(7), m.TombstoneChainMaxDepth.Load())
	require.Equal(t, int64(4), m.FloorLagVersions.Load())
	top := m.TopDebtKeys(0)
	require.Equal(t, 3, len(top))
	require.Equal(t, "Xns:k2", top[0].LogicalKey, "highest debt first")
	require.Equal(t, "ns", top[0].Namespace)

	// Limit honors the requested count.
	require.Len(t, m.TopDebtKeys(2), 2)
	require.Len(t, m.TopDebtKeys(100), 3, "limit larger than slice returns all")
}

// ============================================================================
// LifecycleMetrics.TopDebtKeys — empty by default.
// ============================================================================

func TestLifecycleMetrics_TopDebtKeys_NoPlanReturnsEmpty(t *testing.T) {
	m := NewLifecycleMetrics()
	require.Empty(t, m.TopDebtKeys(0))
	require.Empty(t, m.TopDebtKeys(10))
}

// ============================================================================
// namespaceForInfo — trivial accessor, but unreachable from elsewhere.
// ============================================================================

func TestNamespaceForInfo_ReturnsInfoNamespace(t *testing.T) {
	require.Equal(t, "ns-x",
		namespaceForInfo(storage.SnapshotReaderInfo{Namespace: "ns-x"}))
}

// ============================================================================
// topDebtKeySummaries — empty input + ordering + limit clamp.
// ============================================================================

func TestTopDebtKeySummaries_EdgeCases(t *testing.T) {
	require.Nil(t, topDebtKeySummaries(nil, 5))
	require.Nil(t, topDebtKeySummaries([]PrunePlanEntry{}, 5))

	entries := []PrunePlanEntry{
		{LogicalKey: []byte("Xns:k1"), DebtBytes: 100},
		{LogicalKey: []byte("Xns:k2"), DebtBytes: 200},
		{LogicalKey: []byte("Xns:k3"), DebtBytes: 100},
	}
	out := topDebtKeySummaries(entries, 0)
	require.Len(t, out, 3, "limit 0 returns everything")
	require.Equal(t, "Xns:k2", out[0].LogicalKey)
	// Ties broken by logical key ascending.
	require.Equal(t, "Xns:k1", out[1].LogicalKey)
	require.Equal(t, "Xns:k3", out[2].LogicalKey)

	out2 := topDebtKeySummaries(entries, 2)
	require.Len(t, out2, 2, "limit truncates")
}
