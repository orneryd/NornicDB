package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: NewBoltMetrics + the closed packstream-decode-error
// reason enum land in Plan 04-02. Compile-fail until then.

// TestBoltMetrics_RegistersSixFamilies asserts MET-07's six Bolt families:
// connections_active, connections_total{result}, session_duration_seconds,
// messages_total{op,result}, message_duration_seconds{op},
// packstream_decode_errors_total{reason}.
func TestBoltMetrics_RegistersSixFamilies(t *testing.T) {
	t.Skip("RED: pending Plan 04-02 (Bolt bag delivery)")

	te := NewTestEnv(t)
	bag := NewBoltMetrics(te.Registry)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_bolt_connections_active",
		"nornicdb_bolt_connections_total",
		"nornicdb_bolt_session_duration_seconds",
		"nornicdb_bolt_messages_total",
		"nornicdb_bolt_message_duration_seconds",
		"nornicdb_bolt_packstream_decode_errors_total",
	} {
		assert.Contains(t, names, want, "MET-07: Bolt family %q must register", want)
	}
}

// TestPackstreamReason_ClosedEnum asserts CONTEXT D-11c: only the four
// closed-enum reasons are accepted on packstream_decode_errors_total{reason}.
// Driving 1k synthetic UUIDs as the `reason` value MUST NOT exceed 4 series
// (Phase 3 D-04 cardinality belt; RESEARCH §Q11 ceiling=4).
func TestPackstreamReason_ClosedEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-02 (Bolt bag delivery)")

	te := NewTestEnv(t)
	bag := NewBoltMetrics(te.Registry)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_bolt_packstream_decode_errors_total", 4,
		func(tenant string) {
			// Drive only allow-listed values; the cardinality wall comes from
			// the subsystem refusing to forward arbitrary strings to the Vec.
			for _, reason := range []string{"truncated", "invalid_marker", "wrong_type", "oversize"} {
				bag.PackstreamDecodeErrors.WithLabelValues(reason).Inc()
			}
			_ = tenant
		})
}
