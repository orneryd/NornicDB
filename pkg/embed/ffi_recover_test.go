package embed

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-05-03: D-09 FFI recovery wrapper tests.
//
// recoverFFI is the deferred-recover function called at every llama.cpp
// FFI call site. Synthetic panic verifies the counter increment + error
// wrapping; a normal-return path verifies no false positives.

// TestRecoverFFI_PanicCounted asserts that a panic inside the protected
// scope increments ffi_panics_total{mode} and converts to an error.
func TestRecoverFFI_PanicCounted(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewEmbedMetrics(reg, nil /* no queue probe needed */)
	require.NotNil(t, bag)

	err := callWithPanicProtection(bag, "cpu", func() error {
		panic("simulated llama.cpp segfault")
	})
	require.Error(t, err, "panic must convert to error")
	assert.Contains(t, err.Error(), "ffi panic")

	mfs, gerr := reg.Gather()
	require.NoError(t, gerr)
	val := ffiCounterValue(mfs, "cpu")
	assert.Equal(t, 1.0, val, "ffi_panics_total{mode=cpu} must be 1 after one panic")
}

// TestRecoverFFI_NoPanicNoIncrement asserts the counter stays at 0 when
// the protected callback returns normally (no false positives).
func TestRecoverFFI_NoPanicNoIncrement(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewEmbedMetrics(reg, nil)

	err := callWithPanicProtection(bag, "cpu", func() error { return nil })
	require.NoError(t, err)

	mfs, gerr := reg.Gather()
	require.NoError(t, gerr)
	assert.Equal(t, 0.0, ffiCounterValue(mfs, "cpu"))
}

// TestRecoverFFI_PreservesError asserts that a non-panic error from the
// protected callback flows through unchanged (the recover wrapper is for
// panics only, not for error conversion).
func TestRecoverFFI_PreservesError(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewEmbedMetrics(reg, nil)

	wantErr := errors.New("model out of memory")
	err := callWithPanicProtection(bag, "metal", func() error { return wantErr })
	assert.ErrorIs(t, err, wantErr, "non-panic errors must propagate verbatim")

	mfs, gerr := reg.Gather()
	require.NoError(t, gerr)
	assert.Equal(t, 0.0, ffiCounterValue(mfs, "metal"),
		"non-panic errors do not increment ffi_panics_total")
}

// TestRecoverFFI_NilMetricsTolerated asserts that calling with a nil
// EmbedMetrics handle does NOT panic — the recover wrapper still recovers,
// it just does not increment any counter (defensive for embedded-library
// callers that have no observability bag wired).
func TestRecoverFFI_NilMetricsTolerated(t *testing.T) {
	err := callWithPanicProtection(nil, "cpu", func() error {
		panic("nil-metrics test")
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ffi panic")
}

// TestRecoverFFI_ClosedModeEnum asserts the wrapper handles every value in
// AllowedEmbedBackends; the closed enum is enforced at the call sites
// (passing embedder.Backend() return values).
func TestRecoverFFI_ClosedModeEnum(t *testing.T) {
	for _, mode := range observability.AllowedEmbedBackends {
		t.Run(mode, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			bag := observability.NewEmbedMetrics(reg, nil)
			err := callWithPanicProtection(bag, mode, func() error {
				panic("enum-test panic")
			})
			require.Error(t, err)

			mfs, _ := reg.Gather()
			assert.Equal(t, 1.0, ffiCounterValue(mfs, mode))
		})
	}
}

// callWithPanicProtection mirrors the production usage pattern:
//
//	func (e *LocalGGUFEmbedder) Embed(...) (vec []float32, err error) {
//	    defer recoverFFI(e.metrics, e.Backend(), &err)
//	    // ... cgo call
//	}
func callWithPanicProtection(metrics *observability.EmbedMetrics, mode string, fn func() error) (err error) {
	defer recoverFFI(metrics, mode, &err)
	return fn()
}

// ffiCounterValue extracts ffi_panics_total{mode=<mode>} from a Gather()
// snapshot. Returns 0 if no matching series is found.
func ffiCounterValue(mfs []*dto.MetricFamily, mode string) float64 {
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_embed_ffi_panics_total" {
			continue
		}
		for _, m := range mf.Metric {
			for _, lbl := range m.GetLabel() {
				if lbl.GetName() == "mode" && lbl.GetValue() == mode {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
