package lifecycle

import "time"

// NamespaceBudget bounds lifecycle work for a namespace within one cycle.
type NamespaceBudget struct {
	MaxBytesPerCycle int64
	MinSliceFraction float64
}

// LifecycleConfig controls MVCC lifecycle manager behavior.
type LifecycleConfig struct {
	Enabled                     bool
	CycleInterval               time.Duration
	MaxVersionsPerKey           int
	TTL                         time.Duration
	MaxChainHardCap             int
	HighEnterBytes              int64
	HighExitBytes               int64
	CriticalEnterBytes          int64
	CriticalExitBytes           int64
	PressureEnterWindow         time.Duration
	PressureExitWindow          time.Duration
	MaxSnapshotLifetime         time.Duration
	MaxCPUShare                 float64
	MaxIOBudgetBytesPerInterval int64
	MaxRuntimePerCycle          time.Duration
	FenceRetryInitialDelay      time.Duration
	FenceRetryMaxDelay          time.Duration
	FenceRetryPerKeyLimit       int
	FenceRetryCrossRunLimit     int
	FenceRetryCooldown          time.Duration
	DebtSampleFraction          float64
	FullScanEveryNCycles        int
	NamespaceBudgets            map[string]NamespaceBudget
	DebtGrowthSlopeThreshold    float64
}

// DefaultLifecycleConfig returns the baseline lifecycle configuration.
func DefaultLifecycleConfig() LifecycleConfig {
	const (
		gib = int64(1024 * 1024 * 1024)
	)
	return LifecycleConfig{
		Enabled:                     false,
		CycleInterval:               30 * time.Second,
		MaxVersionsPerKey:           100,
		TTL:                         0,
		MaxChainHardCap:             1000,
		HighEnterBytes:              5 * gib,
		HighExitBytes:               4 * gib,
		CriticalEnterBytes:          20 * gib,
		CriticalExitBytes:           16 * gib,
		PressureEnterWindow:         30 * time.Second,
		PressureExitWindow:          120 * time.Second,
		MaxSnapshotLifetime:         time.Hour,
		MaxCPUShare:                 0.25,
		MaxIOBudgetBytesPerInterval: 512 * 1024 * 1024,
		MaxRuntimePerCycle:          5 * time.Second,
		FenceRetryInitialDelay:      100 * time.Millisecond,
		FenceRetryMaxDelay:          5 * time.Second,
		FenceRetryPerKeyLimit:       3,
		FenceRetryCrossRunLimit:     20,
		FenceRetryCooldown:          60 * time.Second,
		DebtSampleFraction:          0.05,
		FullScanEveryNCycles:        20,
		NamespaceBudgets:            map[string]NamespaceBudget{},
		DebtGrowthSlopeThreshold:    1 << 20,
	}
}
