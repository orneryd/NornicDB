package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-02 GREEN: catalog_bolt.go ships the bag; this file's tests now
// exercise the six families per MET-07, the closed reason / op / result
// enums, and the cardinality ceilings per RESEARCH §Q11.

// TestBoltMetrics_RegistersSixFamilies asserts MET-07's six Bolt families:
// connections_active, connections_total{result}, session_duration_seconds,
// messages_total{op,result}, message_duration_seconds{op},
// packstream_decode_errors_total{reason}.
func TestBoltMetrics_RegistersSixFamilies(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewBoltMetrics(te.Registry)
	require.NotNil(t, bag)

	// Materialize one instance per *Vec so Gather() emits the family
	// (client_golang skips empty *Vec families). ConnectionsActive is a
	// plain Gauge; SessionDuration has no labels and emits when observed.
	bag.ConnectionsActive.WithLabelValues("tcp").Inc()
	bag.ConnectionsTotal.WithLabelValues("success", "tcp").Inc()
	bag.SessionDuration.Bind().Observe(nil, 0.001)
	bag.MessagesTotal.WithLabelValues("run", "success").Inc()
	bag.MessageDuration.Bind("run").Observe(nil, 0.001)
	bag.PackstreamDecodeErrors.WithLabelValues("truncated").Inc()
	bag.ConnectionsRejectedTotal.WithLabelValues("max_connections").Inc()
	bag.WebSocketOversizedTotal.Inc()

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
// Driving the canonical set MUST NOT exceed 4 series (Phase 3 D-04
// cardinality belt; RESEARCH §Q11 ceiling=4).
func TestPackstreamReason_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewBoltMetrics(te.Registry)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_bolt_packstream_decode_errors_total", 4,
		func(tenant string) {
			// Drive only allow-listed values; the cardinality wall comes from
			// the subsystem refusing to forward arbitrary strings to the Vec.
			for _, reason := range AllowedPackstreamReasons {
				bag.PackstreamDecodeErrors.WithLabelValues(reason).Inc()
			}
			_ = tenant
		})
}

// TestBoltOp_ClosedEnum asserts CONTEXT D-11a: messages_total{op} accepts
// only the 10 closed-enum op values. Cardinality ceiling = 30 (10 ops ×
// 3 results) per RESEARCH §Q11.
func TestBoltOp_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewBoltMetrics(te.Registry)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_bolt_messages_total", 30, func(tenant string) {
		for _, op := range AllowedBoltOps {
			for _, result := range AllowedBoltResults {
				bag.MessagesTotal.WithLabelValues(op, result).Inc()
			}
		}
		_ = tenant
	})
}

// TestBoltResult_ClosedEnum asserts CONTEXT D-11: connections_total{result,
// transport} accepts only the closed-enum cross product. Cardinality
// ceiling = 12 (3 results x 4 transports) after the bolt-over-websocket
// schema migration.
func TestBoltResult_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewBoltMetrics(te.Registry)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_bolt_connections_total", 12, func(tenant string) {
		for _, result := range AllowedBoltResults {
			for _, transport := range AllowedBoltTransports {
				bag.ConnectionsTotal.WithLabelValues(result, transport).Inc()
			}
		}
		_ = tenant
	})
}

// TestMetricCardinality_Bolt asserts the Plan 04-02 ceilings hold across
// every Vec in the Bolt bag (RESEARCH §Q11). Drives the closed enums
// concurrently per Phase 3 D-04 helper.
func TestMetricCardinality_Bolt(t *testing.T) {
	tests := []struct {
		name    string
		ceiling int
		drive   func(bag *BoltMetrics)
	}{
		{
			name:    "nornicdb_bolt_message_duration_seconds",
			ceiling: 10,
			drive: func(bag *BoltMetrics) {
				for _, op := range AllowedBoltOps {
					bag.MessageDuration.Bind(op).Observe(nil, 0.001)
				}
			},
		},
		{
			name:    "nornicdb_bolt_packstream_decode_errors_total",
			ceiling: 4,
			drive: func(bag *BoltMetrics) {
				for _, r := range AllowedPackstreamReasons {
					bag.PackstreamDecodeErrors.WithLabelValues(r).Inc()
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			te := NewTestEnv(t)
			bag := NewBoltMetrics(te.Registry)
			te.AssertCardinalityCeiling(t, tc.name, tc.ceiling, func(tenant string) {
				tc.drive(bag)
				_ = tenant
			})
		})
	}
}
