package lifecycle

import (
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type debtSample struct {
	at    time.Time
	bytes int64
}

// EmergencyConfig controls emergency-mode activation.
type EmergencyConfig struct {
	DebtGrowthSlopeThreshold float64
	MaxCPUShare              float64
	MaxIOBudgetBytesPerCycle int64
	MaxRuntimePerCycle       time.Duration
}

// EmergencyController activates emergency behavior when debt grows too quickly.
type EmergencyController struct {
	mu          sync.Mutex
	active      bool
	activeSince time.Time
	debtHistory []debtSample
	config      EmergencyConfig
	critical    bool
}

// NewEmergencyController creates a controller.
func NewEmergencyController(config EmergencyConfig) *EmergencyController {
	return &EmergencyController{config: config, debtHistory: make([]debtSample, 0, 16)}
}

// RecordDebt appends a debt sample.
func (e *EmergencyController) RecordDebt(bytes int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.debtHistory = append(e.debtHistory, debtSample{at: time.Now(), bytes: bytes})
	if len(e.debtHistory) > 16 {
		e.debtHistory = e.debtHistory[len(e.debtHistory)-16:]
	}
}

// SetCritical marks whether the current pressure band is critical.
func (e *EmergencyController) SetCritical(critical bool) {
	e.mu.Lock()
	e.critical = critical
	e.mu.Unlock()
}

// Evaluate updates and returns the emergency state.
func (e *EmergencyController) Evaluate() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.critical || len(e.debtHistory) < 2 {
		e.active = false
		return false
	}
	first := e.debtHistory[0]
	last := e.debtHistory[len(e.debtHistory)-1]
	deltaSeconds := last.at.Sub(first.at).Seconds()
	if deltaSeconds <= 0 {
		// Equal clock samples with increasing debt represent an effectively
		// unbounded positive slope. Treat them deterministically instead of
		// retaining a stale state that depends on timer resolution or suite load.
		switch {
		case last.bytes > first.bytes:
			e.active = true
		case last.bytes < first.bytes:
			e.active = false
		default:
			e.active = 0 > e.config.DebtGrowthSlopeThreshold
		}
		if e.active && e.activeSince.IsZero() {
			e.activeSince = time.Now()
		}
		if !e.active {
			e.activeSince = time.Time{}
		}
		return e.active
	}
	slope := float64(last.bytes-first.bytes) / deltaSeconds
	e.active = slope > e.config.DebtGrowthSlopeThreshold
	if e.active && e.activeSince.IsZero() {
		e.activeSince = time.Now()
	}
	if !e.active {
		e.activeSince = time.Time{}
	}
	return e.active
}

// IsActive returns whether emergency mode is active.
func (e *EmergencyController) IsActive() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.active
}

// AdjustCompactionBudget tightens admission and raises pruning intensity within resource ceilings.
func (e *EmergencyController) AdjustCompactionBudget(base LifecycleConfig) LifecycleConfig {
	if !e.IsActive() {
		return base
	}
	updated := base
	updated.MaxRuntimePerCycle = minDuration(base.MaxRuntimePerCycle*2, e.config.MaxRuntimePerCycle)
	updated.MaxIOBudgetBytesPerInterval = minInt64(base.MaxIOBudgetBytesPerInterval*2, e.config.MaxIOBudgetBytesPerCycle)
	updated.MaxCPUShare = minFloat(base.MaxCPUShare*1.5, e.config.MaxCPUShare)
	if updated.MaxSnapshotLifetime > 0 {
		updated.MaxSnapshotLifetime /= 2
	}
	return updated
}

func minDuration(a, b time.Duration) time.Duration {
	if b <= 0 || a < b {
		return a
	}
	return b
}

func minFloat(a, b float64) float64 {
	if b <= 0 || a < b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if b <= 0 || a < b {
		return a
	}
	return b
}

var _ = storage.PressureNormal
