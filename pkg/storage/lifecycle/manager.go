package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// MVCCLifecycleManager coordinates MVCC pruning, pressure handling, and reader tracking.
type MVCCLifecycleManager struct {
	config     LifecycleConfig
	registry   *ReaderRegistry
	metrics    *LifecycleMetrics
	pressure   *PressureController
	planner    *PrunePlanner
	applier    *PruneApplier
	priority   *PriorityScheduler
	emergency  *EmergencyController
	engine     LifecycleStorageEngine
	mu         sync.RWMutex
	running    bool
	paused     bool
	cancelFn   context.CancelFunc
	done       chan struct{}
	currentCfg LifecycleConfig
	parentCtx  context.Context
}

// NewMVCCLifecycleManager creates a lifecycle manager.
func NewMVCCLifecycleManager(config LifecycleConfig, engine LifecycleStorageEngine) *MVCCLifecycleManager {
	metrics := NewLifecycleMetrics()
	registry := NewReaderRegistry()
	freeSpaceFn := func() int64 {
		if engine == nil {
			return 0
		}
		free, err := engine.DataDirFreeSpace()
		if err != nil {
			return 0
		}
		return free
	}
	pressure := NewPressureController(PressureConfig{
		HighEnterBytes:      config.HighEnterBytes,
		HighExitBytes:       config.HighExitBytes,
		CriticalEnterBytes:  config.CriticalEnterBytes,
		CriticalExitBytes:   config.CriticalExitBytes,
		PressureEnterWindow: config.PressureEnterWindow,
		PressureExitWindow:  config.PressureExitWindow,
	}, metrics.BytesPinnedByOldestReader.Load, freeSpaceFn)
	return &MVCCLifecycleManager{
		config:     config,
		registry:   registry,
		metrics:    metrics,
		pressure:   pressure,
		planner:    NewPrunePlanner(config),
		applier:    NewPruneApplier(config, metrics),
		priority:   NewPriorityScheduler(config),
		emergency:  NewEmergencyController(EmergencyConfig{DebtGrowthSlopeThreshold: config.DebtGrowthSlopeThreshold, MaxCPUShare: config.MaxCPUShare, MaxIOBudgetBytesPerCycle: config.MaxIOBudgetBytesPerInterval, MaxRuntimePerCycle: config.MaxRuntimePerCycle}),
		engine:     engine,
		done:       make(chan struct{}),
		currentCfg: config,
	}
}

// StartLifecycle starts the background lifecycle loop.
func (m *MVCCLifecycleManager) StartLifecycle(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	m.parentCtx = ctx
	if m.running || !m.config.Enabled || m.config.CycleInterval <= 0 {
		return
	}
	interval := m.config.CycleInterval
	runCtx, cancel := context.WithCancel(ctx)
	m.cancelFn = cancel
	m.done = make(chan struct{})
	m.running = true
	go m.loop(runCtx, interval)
}

// StopLifecycle stops the lifecycle loop.
func (m *MVCCLifecycleManager) StopLifecycle() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	cancel := m.cancelFn
	done := m.done
	m.running = false
	m.cancelFn = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// PauseLifecycle pauses automatic lifecycle work.
func (m *MVCCLifecycleManager) PauseLifecycle() {
	m.mu.Lock()
	m.paused = true
	m.mu.Unlock()
}

// ResumeLifecycle resumes automatic lifecycle work.
func (m *MVCCLifecycleManager) ResumeLifecycle() {
	m.mu.Lock()
	m.paused = false
	m.mu.Unlock()
}

// IsLifecycleEnabled reports whether lifecycle is enabled.
func (m *MVCCLifecycleManager) IsLifecycleEnabled() bool {
	return m.config.Enabled
}

// IsLifecycleRunning reports whether lifecycle is running.
func (m *MVCCLifecycleManager) IsLifecycleRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// ReaderRegistry returns the active reader registry.
func (m *MVCCLifecycleManager) ReaderRegistry() storage.SnapshotReaderRegistry {
	return m.registry
}

// RegisterSnapshotReader registers a reader without admission checks.
func (m *MVCCLifecycleManager) RegisterSnapshotReader(info storage.SnapshotReaderInfo) func() {
	_, deregister := m.registry.Register(info)
	return deregister
}

// AcquireSnapshotReader registers a reader and applies pressure-based rejection.
func (m *MVCCLifecycleManager) AcquireSnapshotReader(info storage.SnapshotReaderInfo) (func(), error) {
	if m.pressure.ShouldRejectLongSnapshot(time.Since(info.StartTime), m.currentCfg.MaxSnapshotLifetime) {
		return nil, storage.ErrMVCCResourcePressure
	}
	_, deregister := m.registry.Register(info)
	return deregister, nil
}

// EvaluateSnapshotReader reports whether an active reader should be cancelled or expired.
func (m *MVCCLifecycleManager) EvaluateSnapshotReader(info storage.SnapshotReaderInfo) (graceful bool, hard bool) {
	graceful, hard = m.pressure.ShouldExpireReader(info, m.currentCfg.MaxSnapshotLifetime)
	if graceful || hard {
		m.metrics.RecordReaderExpiration(graceful, hard)
	}
	return graceful, hard
}

// LifecycleStatus returns status suitable for admin endpoints.
func (m *MVCCLifecycleManager) LifecycleStatus() map[string]interface{} {
	return m.Status()
}

// TopLifecycleDebtKeys returns the highest-debt logical keys from the latest evaluated lifecycle plan.
func (m *MVCCLifecycleManager) TopLifecycleDebtKeys(limit int) []storage.MVCCLifecycleDebtKey {
	return m.metrics.TopDebtKeys(limit)
}

// Status returns lifecycle state for diagnostics.
func (m *MVCCLifecycleManager) Status() map[string]interface{} {
	status := m.metrics.ToMap(m.registry)
	status["enabled"] = m.config.Enabled
	status["running"] = m.IsLifecycleRunning()
	status["paused"] = m.isPaused()
	status["automatic"] = m.automaticEnabled()
	status["cycle_interval"] = m.cycleInterval().String()
	status["pressure_band"] = m.pressure.CurrentBand()
	status["emergency_mode"] = m.emergency.IsActive()
	return status
}

// SetLifecycleSchedule updates automatic lifecycle cadence. Zero disables automatic runs.
func (m *MVCCLifecycleManager) SetLifecycleSchedule(interval time.Duration) error {
	if interval < 0 {
		return fmt.Errorf("invalid lifecycle interval: %s", interval)
	}
	m.mu.Lock()
	m.config.CycleInterval = interval
	m.currentCfg.CycleInterval = interval
	parentCtx := m.parentCtx
	running := m.running
	cancel := m.cancelFn
	done := m.done
	m.running = false
	m.cancelFn = nil
	m.mu.Unlock()

	if running {
		if cancel != nil {
			cancel()
		}
		if done != nil {
			<-done
		}
	}

	if interval > 0 && m.config.Enabled {
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		m.StartLifecycle(parentCtx)
	}
	return nil
}

// TriggerPruneNow runs a prune cycle immediately.
func (m *MVCCLifecycleManager) TriggerPruneNow(ctx context.Context) error {
	_, err := m.RunPruneNow(ctx, storage.MVCCPruneOptions{})
	return err
}

// RunPruneNow runs an immediate lifecycle prune.
func (m *MVCCLifecycleManager) RunPruneNow(ctx context.Context, opts storage.MVCCPruneOptions) (int64, error) {
	start := time.Now()
	result, err := m.runCycle(ctx, opts)
	if err != nil {
		return 0, err
	}
	m.metrics.RecordPruneRun(result, time.Since(start))
	return result.VersionsDeleted, nil
}

func (m *MVCCLifecycleManager) loop(ctx context.Context, interval time.Duration) {
	defer close(m.done)
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if m.isPaused() {
				continue
			}
			start := time.Now()
			result, err := m.runCycle(ctx, storage.MVCCPruneOptions{})
			if err == nil {
				m.metrics.RecordPruneRun(result, time.Since(start))
			}
		}
	}
}

func (m *MVCCLifecycleManager) runCycle(ctx context.Context, opts storage.MVCCPruneOptions) (ApplyResult, error) {
	if engine, ok := m.engine.(storage.MVCCMaintenanceEngine); ok {
		if opts.MaxVersionsPerKey <= 0 {
			opts.MaxVersionsPerKey = m.config.MaxVersionsPerKey
		}
		if opts.MinRetentionAge <= 0 {
			opts.MinRetentionAge = m.config.TTL
		}
		deleted, err := engine.PruneMVCCVersions(ctx, opts)
		return ApplyResult{VersionsDeleted: deleted}, err
	}
	band := m.pressure.Update()
	m.emergency.SetCritical(band == storage.PressureCritical)
	// Per-namespace MVCC counters mean a reader's CommitSequence in
	// namespace A has no ordering relationship to a head's
	// CommitSequence in namespace B. The planner therefore resolves a
	// per-namespace safe floor for each head it visits; namespaces with
	// no active readers fall through to maxVersion(), letting TTL and
	// MaxVersionsPerKey alone bound pruning.
	floors := m.registry.OldestReaderVersionsByNamespace()
	safeFloor := func(namespace string) storage.MVCCVersion {
		if v, ok := floors[namespace]; ok {
			return v
		}
		return maxVersion()
	}
	plan, err := m.planner.Plan(ctx, m.engine, safeFloor)
	if err != nil {
		return ApplyResult{}, err
	}
	m.metrics.UpdatePlanInsights(plan)
	debtBytes := int64(0)
	debtKeys := int64(0)
	namespaceDebt := make(map[string]NamespaceDebtSummary)
	for _, entry := range plan.Entries {
		debtBytes += entry.DebtBytes
		debtKeys++
		namespace := namespaceFromLogicalKey(entry.LogicalKey)
		summary := namespaceDebt[namespace]
		summary.DebtBytes += entry.DebtBytes
		summary.DebtKeys++
		summary.PrunableBytes += entry.DebtBytes
		namespaceDebt[namespace] = summary
	}
	m.metrics.ReplaceNamespaceDebt(namespaceDebt)
	m.metrics.UpdateDebt(debtBytes, debtKeys)
	m.metrics.UpdatePinnedBytes(debtBytes)
	m.metrics.PrunableBytesTotal.Store(debtBytes)
	m.emergency.RecordDebt(debtBytes)
	emergencyActive := false
	if m.emergency.Evaluate() {
		m.currentCfg = m.emergency.AdjustCompactionBudget(m.config)
		emergencyActive = true
	} else {
		m.currentCfg = m.config
	}
	ordered := m.priority.Schedule(plan.Entries)
	if emergencyActive {
		ordered = m.priority.ScheduleEmergency(ordered)
	}
	plan.Entries = ordered
	result := m.applier.Apply(ctx, m.engine, plan)
	for namespace, bytesFreed := range result.NamespaceBytesFreed {
		m.metrics.AddNamespacePrunedBytes(namespace, bytesFreed)
	}
	for _, entry := range ordered {
		if len(entry.VersionsToDelete) > 0 {
			m.priority.RecordProcessed(string(entry.LogicalKey))
		} else {
			m.priority.RecordSkipped(string(entry.LogicalKey))
		}
	}
	return result, nil
}

func (m *MVCCLifecycleManager) isPaused() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.paused
}

func (m *MVCCLifecycleManager) automaticEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.CycleInterval > 0
}

func (m *MVCCLifecycleManager) cycleInterval() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.CycleInterval
}
