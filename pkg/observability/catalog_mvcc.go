// Package observability — MVCC metric bag (Plan 04-04 GREEN).
//
// Owns four families per MET-11 + ADR §2.3:
//
//	nornicdb_mvcc_pressure_band{[database], band}
//	nornicdb_mvcc_pinned_bytes               (GaugeFunc; D-15b)
//	nornicdb_mvcc_oldest_reader_age_seconds  (GaugeFunc; D-15b)
//	nornicdb_mvcc_active_readers             (GaugeFunc; D-15b)
//
// Closed enums (CONTEXT D-14):
//
//	band ∈ AllowedMVCCBands = {normal, warn, high, critical}
//
// D-14 thresholds (constants below):
//
//	ratio < 0.50           → normal
//	0.50 ≤ ratio < 0.75    → warn
//	0.75 ≤ ratio < 0.90    → high
//	ratio ≥ 0.90           → critical
//
// PressureBand is set indicator-style: the active band gauge holds value 1
// while the other three bands hold value 0 for the same database. The
// indicator pattern lets dashboards do `max by (database) (... == 1)` to
// pick the current band; alert rules can fire on
// `nornicdb_mvcc_pressure_band{band="critical"} == 1`.
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `query`, `user`, `user_id`, `ip`, `uuid`, `email` are never used here;
// `band` is bounded by AllowedMVCCBands; `database` is bounded by the
// per-tenant-flag axis (D-08).
//
// RISK-2 fixed (Plan 04-04-01): the three GaugeFunc callbacks read the
// MVCCProbe accessors (PinnedBytes / OldestReaderAgeSeconds /
// ActiveReaders) which now exist on *BadgerEngine. Each callback is wrapped
// in a defer-recover that returns 0 on panic per RESEARCH RISK-8 /
// Pitfall 1 — concurrent shutdown cannot poison the /metrics scrape.
//
// D-02d leaf-package boundary: pkg/observability never imports pkg/storage.
// MVCCProbe is declared HERE; pkg/storage's *BadgerEngine satisfies it via
// the accessors in pkg/storage/badger_mvcc.go.
//
// D-08 forward-compat: NewMVCCMetrics(reg, tenantLabelsEnabled, probe)
// decides whether the `database` label is included in pressure_band ONCE
// at construction. The three live-read gauges have no labels (single
// process-wide value) so they are tenant-flag-independent.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// D-14 pressure-band thresholds. Ratio = PinnedBytes / MVCCBudgetBytes.
const (
	MVCCBandWarn     = 0.50
	MVCCBandHigh     = 0.75
	MVCCBandCritical = 0.90
)

// AllowedMVCCBands is the closed enum for the `band` label per CONTEXT D-14.
// Adding a new band = enum update + ADR §2.3 amendment + threshold review.
var AllowedMVCCBands = []string{"normal", "warn", "high", "critical"}

// MVCCProbe is the seam between pkg/storage MVCC accessors and the
// observability GaugeFunc callbacks (D-02d leaf-package boundary —
// pkg/observability never imports pkg/storage). *BadgerEngine satisfies
// this interface via Plan 04-04-01 (RISK-2 fix).
//
// Plan 04-01 Wave-0 published this interface stub so the RED tests
// compile; Plan 04-04 ships the production GREEN bag here. The signatures
// are stable across both — int64 and float64 to keep the wire-shape
// trivial and panic-free even when the engine is in shutdown.
type MVCCProbe interface {
	PinnedBytes() int64
	OldestReaderAgeSeconds() float64
	ActiveReaders() int64
}

// MVCCMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the MVCC
// subsystem. One bag per Provider; constructed at cmd/nornicdb startup.
//
// PressureBand is exposed as a struct field so subsystem callers
// (pkg/storage at MVCC reader open/close sites) can call
// `bag.UpdateBand(database, ratio)` to flip the active band gauge. The
// three live-read gauges (pinned_bytes / oldest_reader_age_seconds /
// active_readers) are NOT struct fields — they are GaugeFunc registrations
// that read the probe on every scrape. RESEARCH Pattern 1.
type MVCCMetrics struct {
	// PressureBand holds one indicator gauge per (database, band) tuple.
	// Use UpdateBand(database, ratio) to set the active band to 1 and
	// reset the other three to 0 for the same database.
	PressureBand *prometheus.GaugeVec

	// tenantLabelsEnabled captured at construction so UpdateBand knows
	// whether to thread the database arg through WithLabelValues.
	tenantLabelsEnabled bool
}

// NewMVCCMetrics constructs the MVCC bag against reg.
//
// tenantLabelsEnabled (D-08) decides whether `database` is included in the
// pressure_band label-set. The three GaugeFunc gauges have no labels.
//
// probe is the MVCCProbe accessor surface — typically *BadgerEngine. nil
// is tolerated as a defensive fallback (returns 0 from each gauge); the
// RISK-2 accessors are always safe to call so this is paranoia, not a
// real fallback path in production.
func NewMVCCMetrics(reg *prometheus.Registry, tenantLabelsEnabled bool, probe MVCCProbe) *MVCCMetrics {
	bandLabels := []string{"band"}
	if tenantLabelsEnabled {
		bandLabels = []string{"database", "band"}
	}

	bag := &MVCCMetrics{
		tenantLabelsEnabled: tenantLabelsEnabled,
	}

	bag.PressureBand = NewGaugeVec(reg,
		MetricOpts{
			Subsystem: "mvcc",
			Name:      "pressure_band",
			Help: "MVCC pressure band indicator gauge per CONTEXT D-14. " +
				"Active band = 1, others = 0. Closed band enum: " +
				"normal, warn, high, critical. Thresholds 0.50 / 0.75 / 0.90.",
		},
		bandLabels)

	// MET-11 / D-15b: three live-read gauges via prometheus.NewGaugeFunc.
	// Each callback wraps defer-recover that returns 0 on panic per
	// RESEARCH RISK-8 / Pitfall 1 — concurrent shutdown cannot poison
	// the /metrics scrape.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "mvcc",
		Name:      "pinned_bytes",
		Help: "Cumulative bytes pinned by all active MVCC reader snapshots " +
			"(RISK-2 accessor on storage.BadgerEngine). Returns 0 on probe " +
			"panic (RISK-8 mitigation).",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.PinnedBytes())
	}))

	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "mvcc",
		Name:      "oldest_reader_age_seconds",
		Help: "Wall-clock age (seconds) of the oldest active MVCC reader " +
			"snapshot. Returns 0 on probe panic (RISK-8 mitigation).",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return probe.OldestReaderAgeSeconds()
	}))

	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "nornicdb",
		Subsystem: "mvcc",
		Name:      "active_readers",
		Help: "Count of currently-open MVCC reader snapshots. Returns 0 " +
			"on probe panic (RISK-8 mitigation).",
	}, func() (val float64) {
		defer func() {
			if r := recover(); r != nil {
				val = 0
			}
		}()
		if probe == nil {
			return 0
		}
		return float64(probe.ActiveReaders())
	}))

	return bag
}

// TenantLabelsEnabled reports whether this bag was constructed with the
// D-08 tenant-flag enabled.
func (m *MVCCMetrics) TenantLabelsEnabled() bool { return m.tenantLabelsEnabled }

// UpdateBand sets the indicator gauge for the active band to 1 and resets
// the other three bands to 0 for the same database. Threshold mapping per
// D-14 — callers pass the raw ratio (PinnedBytes / MVCCBudgetBytes) and
// this helper picks the band.
//
// When the bag was constructed with tenantLabelsEnabled=false, the
// database arg is dropped at the WithLabelValues call site.
func (m *MVCCMetrics) UpdateBand(database string, ratio float64) {
	active := classifyMVCCBand(ratio)
	for _, band := range AllowedMVCCBands {
		val := 0.0
		if band == active {
			val = 1.0
		}
		if m.tenantLabelsEnabled {
			m.PressureBand.WithLabelValues(database, band).Set(val)
		} else {
			m.PressureBand.WithLabelValues(band).Set(val)
		}
	}
}

// classifyMVCCBand maps a ratio (PinnedBytes / MVCCBudgetBytes) to one of
// the four closed-enum bands per D-14 thresholds. Pure function; safe in
// hot paths.
func classifyMVCCBand(ratio float64) string {
	switch {
	case ratio >= MVCCBandCritical:
		return "critical"
	case ratio >= MVCCBandHigh:
		return "high"
	case ratio >= MVCCBandWarn:
		return "warn"
	default:
		return "normal"
	}
}
