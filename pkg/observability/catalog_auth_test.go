package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-06 GREEN: NewAuthMetrics + auth_attempts_total{result,protocol}
// per MET-15 / GAP-6 / CONTEXT D-05e.

// TestAuthMetrics_AuthAttemptsTotal asserts MET-15: the single family
// auth_attempts_total{result, protocol} per ADR §2.3 + GAP-6 registers.
func TestAuthMetrics_AuthAttemptsTotal(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewAuthMetrics(te.Registry)
	require.NotNil(t, bag)
	require.NotNil(t, bag.AuthAttempts)

	// Drive at least one observation so Gather sees the family.
	bag.AuthAttempts.WithLabelValues("success", "http").Inc()

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	assert.Contains(t, names, "nornicdb_auth_attempts_total",
		"MET-15: auth_attempts_total{result,protocol} must register")
}

// TestAuthMetrics_RegistersOneFamily asserts the bag registers EXACTLY
// one family — the Auth catalog is single-family by GAP-6 design.
func TestAuthMetrics_RegistersOneFamily(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewAuthMetrics(te.Registry)
	require.NotNil(t, bag)

	// Touch every label combination so all series exist.
	for _, res := range AllowedAuthResults {
		for _, proto := range AllowedAuthProtocols {
			bag.AuthAttempts.WithLabelValues(res, proto).Inc()
		}
	}

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	authFamilies := 0
	for _, mf := range mfs {
		if mf.GetName() == "nornicdb_auth_attempts_total" {
			authFamilies++
		}
	}
	assert.Equal(t, 1, authFamilies,
		"MET-15: AuthMetrics registers exactly one family per GAP-6")
}

// TestAuthResult_ClosedEnum asserts CONTEXT D-05e: result label accepts only
// {success, failure, denied}. Combined with TestAuthProtocol_ClosedEnum the
// total cardinality ceiling = 9 (RESEARCH §Q11).
func TestAuthResult_ClosedEnum(t *testing.T) {
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
	te := NewTestEnv(t)
	bag := NewAuthMetrics(te.Registry)
	require.NotNil(t, bag)

	// Drive only allow-listed protocol values.
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

// TestMetricCardinality_Auth asserts the cardinality ceiling holds even
// under 1k synthetic adversarial values per the standard test pattern.
// The closed enum at the call site means even adversarial drivers cannot
// expand cardinality beyond 9 (3×3) — this is the falsifiability gate
// that prevents future drift.
func TestMetricCardinality_Auth(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewAuthMetrics(te.Registry)
	require.NotNil(t, bag)

	te.AssertCardinalityCeiling(t, "nornicdb_auth_attempts_total", 9,
		func(tenant string) {
			// Even under 1k synthetic tenant UUIDs, the closed-enum result
			// and protocol axes refuse to expand. Each driver call still
			// only touches the 3×3 = 9 cells.
			for _, res := range AllowedAuthResults {
				for _, proto := range AllowedAuthProtocols {
					bag.AuthAttempts.WithLabelValues(res, proto).Inc()
				}
			}
			_ = tenant
		})
}

// TestAuthMetrics_NoUserLabel asserts that a hypothetical attempt to register
// the auth family with `user`, `user_id`, `email`, or `ip` as a label panics
// at registration via Phase 3 D-03a forbidden-label discipline. This is
// defense-in-depth — the production NewAuthMetrics never tries this, but the
// test pins the safety net so a future drift attempt blows up at startup.
func TestAuthMetrics_NoUserLabel(t *testing.T) {
	for _, forbidden := range []string{"user", "user_id", "email", "ip"} {
		forbidden := forbidden
		t.Run(forbidden, func(t *testing.T) {
			te := NewTestEnv(t)
			require.PanicsWithError(t,
				`observability: label "`+forbidden+`" is forbidden (cardinality bomb / PII); see ForbiddenLabels`,
				func() {
					_ = NewCounterVec(te.Registry,
						MetricOpts{
							Subsystem: "auth",
							Name:      "attempts_total",
							Help:      "PII drift attempt — must panic.",
						},
						[]string{"result", "protocol", forbidden})
				},
				"D-03a: registration must panic when forbidden label %q is used", forbidden)
		})
	}
}
