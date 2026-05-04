package auth

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// metricsTestFixture builds a fresh Authenticator + observability bag.
func metricsTestFixture(t *testing.T) (*Authenticator, *observability.AuthMetrics, *prometheus.Registry) {
	t.Helper()
	a := &Authenticator{}
	reg := prometheus.NewRegistry()
	bag := observability.NewAuthMetrics(reg)
	a.SetAuthMetrics(bag)
	return a, bag, reg
}

// TestClassifyAuthResult_ClosedEnum asserts the (error → result) mapping
// matches CONTEXT D-05e for every documented error sentinel.
func TestClassifyAuthResult_ClosedEnum(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"nil-err-success", nil, "success"},
		{"invalid-credentials", ErrInvalidCredentials, "failure"},
		{"invalid-token", ErrInvalidToken, "failure"},
		{"session-expired", ErrSessionExpired, "failure"},
		{"account-locked", ErrAccountLocked, "denied"},
		{"insufficient-role", ErrInsufficientRole, "denied"},
		{"no-credentials", ErrNoCredentials, "denied"},
		{"unknown-storage-error", errors.New("storage broken"), "failure"},
		{"wrapped-invalid-creds", &wrappedErr{ErrInvalidCredentials}, "failure"},
		{"wrapped-locked", &wrappedErr{ErrAccountLocked}, "denied"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyAuthResult(tc.err)
			assert.Equal(t, tc.want, got, "case %q", tc.name)
			// Defense-in-depth: result must always be in the closed enum.
			assert.Contains(t, observability.AllowedAuthResults, got,
				"D-05e: classification result must be in closed enum")
		})
	}
}

// wrappedErr is a tiny errors.Is-compatible wrapper for the table-driven
// classification test.
type wrappedErr struct{ inner error }

func (w *wrappedErr) Error() string { return "wrapped: " + w.inner.Error() }
func (w *wrappedErr) Unwrap() error { return w.inner }

// TestRecordAttempt_NilSafe asserts that calling RecordAttempt before
// SetAuthMetrics is a no-op (production tests that don't inject the bag
// must not panic).
func TestRecordAttempt_NilSafe(t *testing.T) {
	a := &Authenticator{}
	require.NotPanics(t, func() {
		a.RecordAttempt("success", "http")
		a.RecordAttempt("failure", "bolt")
		a.RecordAttempt("denied", "grpc")
	})
}

// TestAuthAttempts_AllProtocols asserts the chokepoint increments the
// counter for every (result × protocol) cell — 9 cells total per
// CONTEXT D-05e × RESEARCH §Q11.
func TestAuthAttempts_AllProtocols(t *testing.T) {
	a, _, reg := metricsTestFixture(t)

	for _, result := range observability.AllowedAuthResults {
		for _, proto := range observability.AllowedAuthProtocols {
			a.RecordAttempt(result, proto)
		}
	}

	// One series per cell — 9 series total.
	got, err := testutil.GatherAndCount(reg, "nornicdb_auth_attempts_total")
	require.NoError(t, err)
	assert.Equal(t, 9, got, "D-05e: 3 results × 3 protocols = 9 cells")

	// Every cell must have value 1.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_auth_attempts_total" {
			continue
		}
		for _, m := range mf.Metric {
			assert.Equal(t, float64(1), m.GetCounter().GetValue(),
				"each (result, protocol) cell increments to 1")
		}
	}
}

// TestAuthAttempts_HTTP — successful HTTP auth via classify+RecordAttempt.
func TestAuthAttempts_HTTP(t *testing.T) {
	a, bag, _ := metricsTestFixture(t)

	// Simulate a successful HTTP auth flow at the chokepoint.
	a.RecordAttempt(ClassifyAuthResult(nil), "http")
	got := testutil.ToFloat64(bag.AuthAttempts.WithLabelValues("success", "http"))
	assert.Equal(t, float64(1), got)
}

// TestAuthAttempts_HTTPFail — bad credentials at HTTP path.
func TestAuthAttempts_HTTPFail(t *testing.T) {
	a, bag, _ := metricsTestFixture(t)

	a.RecordAttempt(ClassifyAuthResult(ErrInvalidCredentials), "http")
	got := testutil.ToFloat64(bag.AuthAttempts.WithLabelValues("failure", "http"))
	assert.Equal(t, float64(1), got)
}

// TestAuthAttempts_HTTPDenied — account locked (denied bucket).
func TestAuthAttempts_HTTPDenied(t *testing.T) {
	a, bag, _ := metricsTestFixture(t)

	a.RecordAttempt(ClassifyAuthResult(ErrAccountLocked), "http")
	got := testutil.ToFloat64(bag.AuthAttempts.WithLabelValues("denied", "http"))
	assert.Equal(t, float64(1), got)
}

// TestAuthAttempts_NoUserLabel asserts that no PII (username/email/IP)
// surfaces as a label value. Defense-in-depth — the registration helper
// in pkg/observability already panics on `user`/`user_id`/`email`/`ip`
// labels (Phase 3 D-03a). Here we verify the call sites only ever pass
// closed-enum values.
func TestAuthAttempts_NoUserLabel(t *testing.T) {
	a, _, reg := metricsTestFixture(t)

	// Drive every classification flow.
	a.RecordAttempt(ClassifyAuthResult(nil), "http")
	a.RecordAttempt(ClassifyAuthResult(ErrInvalidCredentials), "bolt")
	a.RecordAttempt(ClassifyAuthResult(ErrAccountLocked), "grpc")

	mfs, err := reg.Gather()
	require.NoError(t, err)
	forbidden := []string{"user", "user_id", "email", "ip", "username"}
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_auth_attempts_total" {
			continue
		}
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				for _, fb := range forbidden {
					assert.NotEqual(t, fb, lp.GetName(),
						"T-04-01: %q must NOT be a label", fb)
				}
				// And every label-VALUE must be in the closed enum.
				switch lp.GetName() {
				case "result":
					assert.Contains(t, observability.AllowedAuthResults, lp.GetValue())
				case "protocol":
					assert.Contains(t, observability.AllowedAuthProtocols, lp.GetValue())
				}
			}
		}
	}
}

// TestSetAuthMetrics_Idempotent asserts that calling SetAuthMetrics with
// nil disables observation, and re-injecting with a fresh bag re-enables it.
func TestSetAuthMetrics_Idempotent(t *testing.T) {
	a, _, _ := metricsTestFixture(t)

	// First call established the bag in fixture.
	require.NotNil(t, a.AuthMetrics())

	// nil disables.
	a.SetAuthMetrics(nil)
	assert.Nil(t, a.AuthMetrics())
	require.NotPanics(t, func() {
		a.RecordAttempt("success", "http")
	})

	// Re-inject with a fresh bag.
	reg := prometheus.NewRegistry()
	bag := observability.NewAuthMetrics(reg)
	a.SetAuthMetrics(bag)
	a.RecordAttempt("success", "http")
	got := testutil.ToFloat64(bag.AuthAttempts.WithLabelValues("success", "http"))
	assert.Equal(t, float64(1), got)
}
