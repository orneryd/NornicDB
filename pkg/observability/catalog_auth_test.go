package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: NewAuthMetrics lands in Plan 04-06 (GAP-6 / MET-15).
// Compile-fail until then.

// TestAuthMetrics_AuthAttemptsTotal asserts MET-15: single family
// auth_attempts_total{result, protocol} per ADR §2.3 + GAP-6.
func TestAuthMetrics_AuthAttemptsTotal(t *testing.T) {
	t.Skip("RED: pending Plan 04-06 (Auth bag delivery)")

	te := NewTestEnv(t)
	bag := NewAuthMetrics(te.Registry)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	assert.Contains(t, names, "nornicdb_auth_attempts_total",
		"MET-15: auth_attempts_total{result,protocol} must register")
}

// TestAuthResult_ClosedEnum asserts CONTEXT D-05e: result label accepts only
// {success, failure, denied}. Combined with TestAuthProtocol_ClosedEnum the
// total cardinality ceiling = 9 (RESEARCH §Q11).
func TestAuthResult_ClosedEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-06 (Auth bag delivery)")

	te := NewTestEnv(t)
	bag := NewAuthMetrics(te.Registry)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_auth_attempts_total", 9, func(tenant string) {
		for _, res := range []string{"success", "failure", "denied"} {
			for _, proto := range []string{"bolt", "http", "grpc"} {
				bag.AuthAttempts.WithLabelValues(res, proto).Inc()
			}
		}
		_ = tenant
	})
}

// TestAuthProtocol_ClosedEnum asserts CONTEXT D-05e: protocol label accepts
// only {bolt, http, grpc}.
func TestAuthProtocol_ClosedEnum(t *testing.T) {
	t.Skip("RED: pending Plan 04-06 (Auth bag delivery)")

	te := NewTestEnv(t)
	bag := NewAuthMetrics(te.Registry)
	require.NotNil(t, bag)

	// Driving only allow-listed protocol values.
	for _, proto := range []string{"bolt", "http", "grpc"} {
		bag.AuthAttempts.WithLabelValues("success", proto).Inc()
	}

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_auth_attempts_total" {
			continue
		}
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == "protocol" {
					v := lp.GetValue()
					assert.Contains(t, []string{"bolt", "http", "grpc"}, v,
						"D-05e: protocol value %q outside closed enum", v)
				}
			}
		}
	}
}
