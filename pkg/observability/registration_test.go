package observability

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForbiddenLabels_PanicsAtRegistration is the MET-04 Layer 1
// falsifiability gate: cartesian product of every entry in ForbiddenLabels
// (10 entries) × every typed constructor (6) = 60 sub-tests, each asserting
// the documented panic message via PanicsWithError.
func TestForbiddenLabels_PanicsAtRegistration(t *testing.T) {
	constructors := []struct {
		name   string
		invoke func(reg *prometheus.Registry, labels []string)
	}{
		{"NewCounterVec", func(reg *prometheus.Registry, lbl []string) {
			NewCounterVec(reg, MetricOpts{Subsystem: "cypher", Name: "x_total", Help: "h"}, lbl)
		}},
		{"NewGaugeVec", func(reg *prometheus.Registry, lbl []string) {
			NewGaugeVec(reg, MetricOpts{Subsystem: "cypher", Name: "x", Help: "h"}, lbl)
		}},
		{"NewLatencyHistogramVec", func(reg *prometheus.Registry, lbl []string) {
			NewLatencyHistogramVec(reg, MetricOpts{Subsystem: "cypher", Name: "x_seconds", Help: "h"}, lbl)
		}},
		{"NewSizeHistogramVec", func(reg *prometheus.Registry, lbl []string) {
			NewSizeHistogramVec(reg, MetricOpts{Subsystem: "storage", Name: "x_bytes", Help: "h"}, lbl)
		}},
		{"NewRowCountHistogramVec", func(reg *prometheus.Registry, lbl []string) {
			NewRowCountHistogramVec(reg, MetricOpts{Subsystem: "cypher", Name: "x_rows", Help: "h"}, lbl)
		}},
		{"NewEmbeddingLatencyHistogramVec", func(reg *prometheus.Registry, lbl []string) {
			NewEmbeddingLatencyHistogramVec(reg, MetricOpts{Subsystem: "embed", Name: "x_seconds", Help: "h"}, lbl)
		}},
	}

	for _, ctor := range constructors {
		for _, fb := range ForbiddenLabels {
			ctor, fb := ctor, fb
			t.Run(ctor.name+"/"+fb, func(t *testing.T) {
				te := NewTestEnv(t)
				want := fmt.Sprintf("observability: label %q is forbidden (cardinality bomb / PII); see ForbiddenLabels", fb)
				require.PanicsWithError(t, want, func() {
					ctor.invoke(te.Registry, []string{"valid_label", fb})
				})
			})
		}
	}
}

// TestForbiddenLabels_CaseInsensitive verifies D-03a: the ForbiddenLabels
// match is case-insensitive. Uppercased variants of every entry must still
// panic with the same documented error message (echoing back the caller's
// supplied label value verbatim).
func TestForbiddenLabels_CaseInsensitive(t *testing.T) {
	for _, fb := range ForbiddenLabels {
		upper := strings.ToUpper(fb)
		if upper == fb {
			continue // skip purely-numeric or already-upper entries (none expected)
		}
		t.Run(upper, func(t *testing.T) {
			te := NewTestEnv(t)
			want := fmt.Sprintf("observability: label %q is forbidden (cardinality bomb / PII); see ForbiddenLabels", upper)
			require.PanicsWithError(t, want, func() {
				NewCounterVec(te.Registry, MetricOpts{Subsystem: "cypher", Name: "x_total", Help: "h"}, []string{upper})
			})
		})
	}
}

// TestForbiddenLabels_AllTenEntriesPresent is the allow-list completeness
// gate (D-03a falsifiability): catches accidental allow-list weakening if
// a future PR drops an entry.
func TestForbiddenLabels_AllTenEntriesPresent(t *testing.T) {
	expect := []string{"path", "query", "user", "user_id", "ip", "uuid", "embedding_text", "trace_id", "span_id", "email"}
	require.GreaterOrEqual(t, len(ForbiddenLabels), len(expect),
		"D-03a: ForbiddenLabels must contain at least %d entries; mutating requires ADR amendment", len(expect))
	for _, e := range expect {
		assert.Contains(t, ForbiddenLabels, e,
			"D-03a: %q must remain in ForbiddenLabels (cardinality bomb / PII)", e)
	}
}

// TestSubsystemValidation_RejectsUnknownSubsystem closes the MET-01
// falsifiability gap: the closed allow-list (D-01d:
// http|bolt|cypher|storage|mvcc|embed|search|replication|auth|cache|process)
// must panic at registration when violated. Plan 03-02's validateSubsystem
// implements the panic; this test gates it.
func TestSubsystemValidation_RejectsUnknownSubsystem(t *testing.T) {
	cases := []string{"unknown", "metrics", "telemetry", "fabric", "graph"}
	for _, sub := range cases {
		sub := sub
		t.Run(sub, func(t *testing.T) {
			te := NewTestEnv(t)
			require.Panics(t, func() {
				NewCounterVec(te.Registry, MetricOpts{Subsystem: sub, Name: "x_total", Help: "h"}, nil)
			}, "MET-01 / D-01d: subsystem %q must be in the closed allow-list; registration must panic", sub)
		})
	}
}
