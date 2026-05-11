package observability

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Plan 04-04-02 GREEN: MVCCMetrics bag with 4 families. PressureBand closed
// enum {normal, warn, high, critical} per CONTEXT D-14. Three live-read
// gauges via prometheus.NewGaugeFunc reading the MVCCProbe accessors
// (PinnedBytes / OldestReaderAgeSeconds / ActiveReaders — implemented on
// *BadgerEngine in Plan 04-04-01 per RISK-2).

// mvccProbeStub is the test seam against the MVCCProbe interface.
type mvccProbeStub struct {
	pinned int64
	age    float64
	active int64
}

func (m mvccProbeStub) PinnedBytes() int64              { return m.pinned }
func (m mvccProbeStub) OldestReaderAgeSeconds() float64 { return m.age }
func (m mvccProbeStub) ActiveReaders() int64            { return m.active }

// panickyProbe simulates RESEARCH RISK-8 / Pitfall 1: a probe whose
// accessors panic during /metrics scrape (e.g. concurrent shutdown).
// The GaugeFunc callbacks must wrap defer recover and return 0 so the
// scrape does not 500.
type panickyProbe struct{}

func (p panickyProbe) PinnedBytes() int64              { panic("pinned-bytes panic") }
func (p panickyProbe) OldestReaderAgeSeconds() float64 { panic("age panic") }
func (p panickyProbe) ActiveReaders() int64            { panic("active panic") }

// findFamily helper.
func findFamily(t *testing.T, mfs []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

// TestMVCCMetrics_RegistersFour asserts that all four families surface from
// a single Gather() — pressure_band + three live-read gauges.
func TestMVCCMetrics_RegistersFour(t *testing.T) {
	te := NewTestEnv(t)
	probe := mvccProbeStub{pinned: 100, age: 1.5, active: 3}
	bag := NewMVCCMetrics(te.Registry, false, probe)
	require.NotNil(t, bag)

	// Touch the GaugeVec so it surfaces in Gather (counters/gauges with no
	// recorded series do not appear).
	bag.PressureBand.WithLabelValues("normal").Set(1)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	want := []string{
		"nornicdb_mvcc_pressure_band",
		"nornicdb_mvcc_pinned_bytes",
		"nornicdb_mvcc_oldest_reader_age_seconds",
		"nornicdb_mvcc_active_readers",
	}
	for _, name := range want {
		require.NotNilf(t, findFamily(t, mfs, name), "family %s missing from Gather()", name)
	}
}

// TestMVCCMetrics_PressureBandClosedEnum asserts MET-11 + CONTEXT D-14:
// pressure_band{band} accepts only {normal, warn, high, critical}.
// Cardinality ceiling = 4 (RESEARCH §Q11; tenant-OFF mode).
func TestMVCCMetrics_PressureBandClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewMVCCMetrics(te.Registry, false, mvccProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_mvcc_pressure_band", 4, func(tenant string) {
		for _, band := range AllowedMVCCBands {
			bag.PressureBand.WithLabelValues(band).Set(0)
		}
		_ = tenant
	})
}

// TestMVCCGaugeFunc_PinnedBytes asserts CONTEXT D-15b: pinned_bytes is a
// GaugeFunc reading mvcc.PinnedBytes() (RISK-2). The probe value flows
// through to the registry on Gather().
func TestMVCCGaugeFunc_PinnedBytes(t *testing.T) {
	te := NewTestEnv(t)
	probe := mvccProbeStub{pinned: 12345}
	bag := NewMVCCMetrics(te.Registry, false, probe)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	mf := findFamily(t, mfs, "nornicdb_mvcc_pinned_bytes")
	require.NotNil(t, mf, "pinned_bytes family must surface")
	require.Len(t, mf.Metric, 1)
	require.InDelta(t, 12345.0, mf.Metric[0].GetGauge().GetValue(), 0.0001)
}

// TestMVCCGaugeFunc_OldestReaderAge mirrors TestMVCCGaugeFunc_PinnedBytes
// for the oldest_reader_age_seconds family.
func TestMVCCGaugeFunc_OldestReaderAge(t *testing.T) {
	te := NewTestEnv(t)
	probe := mvccProbeStub{age: 2.75}
	bag := NewMVCCMetrics(te.Registry, false, probe)
	require.NotNil(t, bag)
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	mf := findFamily(t, mfs, "nornicdb_mvcc_oldest_reader_age_seconds")
	require.NotNil(t, mf)
	require.Len(t, mf.Metric, 1)
	require.InDelta(t, 2.75, mf.Metric[0].GetGauge().GetValue(), 0.0001)
}

// TestMVCCGaugeFunc_ActiveReaders mirrors for active_readers.
func TestMVCCGaugeFunc_ActiveReaders(t *testing.T) {
	te := NewTestEnv(t)
	probe := mvccProbeStub{active: 7}
	bag := NewMVCCMetrics(te.Registry, false, probe)
	require.NotNil(t, bag)
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	mf := findFamily(t, mfs, "nornicdb_mvcc_active_readers")
	require.NotNil(t, mf)
	require.Len(t, mf.Metric, 1)
	require.InDelta(t, 7.0, mf.Metric[0].GetGauge().GetValue(), 0.0001)
}

// TestMVCCGaugeFuncs_PanicSafe verifies RESEARCH RISK-8 / Pitfall 1: when
// the probe accessors panic (concurrent shutdown), the GaugeFunc callbacks
// recover and return 0 so the scrape does not 500.
func TestMVCCGaugeFuncs_PanicSafe(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewMVCCMetrics(te.Registry, false, panickyProbe{})
	require.NotNil(t, bag)
	mfs, err := te.Registry.Gather()
	require.NoError(t, err, "gather must not propagate probe panic")
	for _, name := range []string{
		"nornicdb_mvcc_pinned_bytes",
		"nornicdb_mvcc_oldest_reader_age_seconds",
		"nornicdb_mvcc_active_readers",
	} {
		mf := findFamily(t, mfs, name)
		require.NotNilf(t, mf, "family %s should still surface on probe panic", name)
		require.Len(t, mf.Metric, 1)
		require.Equal(t, 0.0, mf.Metric[0].GetGauge().GetValue(), "panic ⇒ 0")
	}
}

// TestMVCCGaugeFuncs_NilProbe verifies that a nil probe (defensive
// fallback) yields 0-valued gauges without panic — equivalent to the
// shutdown race window.
func TestMVCCGaugeFuncs_NilProbe(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewMVCCMetrics(te.Registry, false, nil)
	require.NotNil(t, bag)
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, name := range []string{
		"nornicdb_mvcc_pinned_bytes",
		"nornicdb_mvcc_oldest_reader_age_seconds",
		"nornicdb_mvcc_active_readers",
	} {
		mf := findFamily(t, mfs, name)
		require.NotNilf(t, mf, "family %s should still surface on nil probe", name)
	}
}

// TestPressureBand_ThresholdMapping drives ratios 0.30 / 0.60 / 0.80 / 0.95
// through UpdateBand(database, ratio) and asserts the appropriate band is
// indicator-set to 1 with the others at 0.
//
// D-14 thresholds: warn≥0.50, high≥0.75, critical≥0.90 (else normal).
func TestPressureBand_ThresholdMapping(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewMVCCMetrics(te.Registry, false, mvccProbeStub{})

	cases := []struct {
		ratio    float64
		expected string
	}{
		{0.30, "normal"},
		{0.60, "warn"},
		{0.80, "high"},
		{0.95, "critical"},
	}
	for _, tc := range cases {
		bag.UpdateBand("" /* database */, tc.ratio)
		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		mf := findFamily(t, mfs, "nornicdb_mvcc_pressure_band")
		require.NotNil(t, mf)
		var got string
		for _, m := range mf.Metric {
			if m.GetGauge().GetValue() == 1 {
				for _, l := range m.GetLabel() {
					if l.GetName() == "band" {
						got = l.GetValue()
					}
				}
			}
		}
		require.Equalf(t, tc.expected, got, "ratio %.2f ⇒ band", tc.ratio)
	}
}
