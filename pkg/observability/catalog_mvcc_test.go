package observability

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: NewMVCCMetrics + the closed band enum and GaugeFunc
// callbacks land in Plan 04-04 (with RISK-2 PinnedBytes accessor).
// Compile-fail until then.

// TestMVCCMetrics_PressureBandClosedEnum asserts MET-11 + CONTEXT D-14:
// pressure_band{band} accepts only {normal, warn, high, critical}.
// Cardinality ceiling = 4 (RESEARCH §Q11; tenant-OFF mode).
func TestMVCCMetrics_PressureBandClosedEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-04 (MVCC bag delivery; RISK-2 accessors)")

	te := NewTestEnv(t)
	bag := NewMVCCMetrics(te.Registry, false /* tenantLabelsEnabled */, mvccProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_mvcc_pressure_band", 4, func(tenant string) {
		for _, band := range []string{"normal", "warn", "high", "critical"} {
			bag.PressureBand.WithLabelValues(band).Set(0)
		}
		_ = tenant
	})
}

// TestMVCCGaugeFunc_PinnedBytes asserts CONTEXT D-15b: pinned_bytes is a
// GaugeFunc reading mvcc.PinnedBytes() (RISK-2 — accessor ships in 04-04).
// The probe value flows through to the registry on Gather().
func TestMVCCGaugeFunc_PinnedBytes(t *testing.T) {
	t.Skip("RED: pending Plan 04-04 (MVCC bag delivery; RISK-2 PinnedBytes accessor)")

	te := NewTestEnv(t)
	probe := &mvccProbeStub{pinned: 12345}
	bag := NewMVCCMetrics(te.Registry, false, probe)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "nornicdb_mvcc_pinned_bytes" {
			require.Len(t, mf.Metric, 1)
			require.InDelta(t, 12345.0, mf.Metric[0].GetGauge().GetValue(), 0.0001)
		}
	}
}

// mvccProbeStub is the testing-only seam against the MVCCProbe interface that
// 04-04 ships. Each accessor is an int64/Duration/int. The interface itself
// lives next to NewMVCCMetrics; this stub here proves the test compiles when
// the interface lands.
type mvccProbeStub struct {
	pinned int64
}

func (m mvccProbeStub) PinnedBytes() int64           { return m.pinned }
func (m mvccProbeStub) OldestReaderAgeSeconds() float64 { return 0 }
func (m mvccProbeStub) ActiveReaders() int64          { return 0 }
