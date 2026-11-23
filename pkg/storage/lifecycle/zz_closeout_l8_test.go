package lifecycle

// Close-out lane L8 (v1.5.2 verification campaign, 2026-11-22): hunt lead
// L-H9-010. docs/operations/mvcc-lifecycle-compaction.md:99-104 gives
// high_enter = max(5 GiB, 0.15 * data_dir_free_space) and critical_enter =
// max(20 GiB, 0.35 * free). The controller the manager builds
// (manager.go:46-53) is given the free-space reader; the test counts its
// calls during Update. Test-only, not for merge.

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestL8_H9_010_PressureScalesWithFreeSpace(t *testing.T) {
	const gib = int64(1 << 30)
	cfg := DefaultLifecycleConfig()
	for _, c := range []struct {
		pinned, free int64
		want         storage.PressureBand
		lead         bool
	}{
		{6 * gib, 100 * gib, storage.PressureNormal, true},
		{25 * gib, 100 * gib, storage.PressureNormal, true},
		{16 * gib, 100 * gib, storage.PressureHigh, false},
		{6 * gib, 10 * gib, storage.PressureHigh, false},
		{40 * gib, 100 * gib, storage.PressureCritical, false},
	} {
		pinned, free := c.pinned, c.free
		var freeCalls, pinnedCalls atomic.Int64
		pc := NewPressureController(PressureConfig{
			HighEnterBytes: cfg.HighEnterBytes, HighExitBytes: cfg.HighExitBytes,
			CriticalEnterBytes: cfg.CriticalEnterBytes, CriticalExitBytes: cfg.CriticalExitBytes,
		}, func() int64 { pinnedCalls.Add(1); return pinned }, func() int64 { freeCalls.Add(1); return free })
		band := pc.Update()
		band = pc.Update()
		data, _ := json.Marshal(map[string]any{"lead": "L-H9-010", "pinned_gib": pinned / gib, "free_gib": free / gib, "band": band, "documented_band": c.want,
			"freeSpace_calls": freeCalls.Load(), "pinnedBytes_calls": pinnedCalls.Load(), "config_high_enter": cfg.HighEnterBytes, "config_critical_enter": cfg.CriticalEnterBytes})
		t.Logf("L8_OWNER %s", data)
		if band != c.want {
			if c.lead {
				t.Errorf("pinned %d GiB, %d GiB free: band %v, documented thresholds give %v", pinned/gib, free/gib, band, c.want)
			} else {
				t.Errorf("control pinned %d GiB, %d GiB free: band %v, want %v", pinned/gib, free/gib, band, c.want)
			}
		}
	}
}
