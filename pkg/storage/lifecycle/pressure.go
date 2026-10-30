package lifecycle

import (
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// PressureConfig contains thresholds and debounce windows.
type PressureConfig struct {
	HighEnterBytes      int64
	HighExitBytes       int64
	CriticalEnterBytes  int64
	CriticalExitBytes   int64
	PressureEnterWindow time.Duration
	PressureExitWindow  time.Duration
}

// PressureController manages lifecycle pressure bands with hysteresis.
type PressureController struct {
	mu                 sync.Mutex
	currentBand        storage.PressureBand
	config             PressureConfig
	highEnterStart     time.Time
	highExitStart      time.Time
	criticalEnterStart time.Time
	criticalExitStart  time.Time
	pinnedBytes        func() int64
	freeSpace          func() int64
}

// NewPressureController creates a pressure controller.
func NewPressureController(config PressureConfig, pinnedBytes func() int64, freeSpace func() int64) *PressureController {
	return &PressureController{currentBand: storage.PressureNormal, config: config, pinnedBytes: pinnedBytes, freeSpace: freeSpace}
}

// Update recalculates the current band using hysteresis.
func (p *PressureController) Update() storage.PressureBand {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	pinned := int64(0)
	if p.pinnedBytes != nil {
		pinned = p.pinnedBytes()
	}
	critical := pinned >= p.config.CriticalEnterBytes && p.config.CriticalEnterBytes > 0
	high := pinned >= p.config.HighEnterBytes && p.config.HighEnterBytes > 0
	switch p.currentBand {
	case storage.PressureNormal:
		if critical {
			if p.criticalEnterStart.IsZero() {
				p.criticalEnterStart = now
			}
			if now.Sub(p.criticalEnterStart) >= p.config.PressureEnterWindow {
				p.currentBand = storage.PressureCritical
			}
		} else if high {
			if p.highEnterStart.IsZero() {
				p.highEnterStart = now
			}
			if now.Sub(p.highEnterStart) >= p.config.PressureEnterWindow {
				p.currentBand = storage.PressureHigh
			}
		}
	case storage.PressureHigh:
		if critical {
			if p.criticalEnterStart.IsZero() {
				p.criticalEnterStart = now
			}
			if now.Sub(p.criticalEnterStart) >= p.config.PressureEnterWindow {
				p.currentBand = storage.PressureCritical
			}
			p.highExitStart = time.Time{}
		} else if pinned <= p.config.HighExitBytes {
			if p.highExitStart.IsZero() {
				p.highExitStart = now
			}
			if now.Sub(p.highExitStart) >= p.config.PressureExitWindow {
				p.currentBand = storage.PressureNormal
			}
		}
	case storage.PressureCritical:
		if pinned <= p.config.CriticalExitBytes {
			if p.criticalExitStart.IsZero() {
				p.criticalExitStart = now
			}
			if now.Sub(p.criticalExitStart) >= p.config.PressureExitWindow {
				if pinned > p.config.HighExitBytes {
					p.currentBand = storage.PressureHigh
				} else {
					p.currentBand = storage.PressureNormal
				}
			}
		}
	}
	if !critical {
		p.criticalEnterStart = time.Time{}
	}
	if !high {
		p.highEnterStart = time.Time{}
	}
	return p.currentBand
}

// CurrentBand returns the current pressure band.
func (p *PressureController) CurrentBand() storage.PressureBand {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.currentBand
}

// ShouldRejectLongSnapshot decides whether a snapshot should be rejected under pressure.
func (p *PressureController) ShouldRejectLongSnapshot(snapshotAge time.Duration, maxLifetime time.Duration) bool {
	switch p.CurrentBand() {
	case storage.PressureHigh:
		return maxLifetime > 0 && snapshotAge > maxLifetime
	case storage.PressureCritical:
		return maxLifetime > 0 && snapshotAge > maxLifetime/2
	default:
		return false
	}
}

// ShouldExpireReader decides whether a reader should be expired.
func (p *PressureController) ShouldExpireReader(reader storage.SnapshotReaderInfo, maxLifetime time.Duration) (graceful bool, hard bool) {
	age := time.Since(reader.StartTime)
	switch p.CurrentBand() {
	case storage.PressureHigh:
		return maxLifetime > 0 && age > maxLifetime, false
	case storage.PressureCritical:
		return maxLifetime > 0 && age > maxLifetime/2, maxLifetime > 0 && age > maxLifetime
	default:
		return false, false
	}
}
