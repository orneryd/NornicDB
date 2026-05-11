// Package observability — Auth metric bag (Plan 04-06 GREEN per MET-15 / GAP-6).
//
// Owns one family per ADR §2.3 + CONTEXT GAP-6:
//
//	nornicdb_auth_attempts_total{result, protocol}
//
// Closed enums (CONTEXT D-05e):
//
//	result   ∈ AllowedAuthResults    = {success, failure, denied}
//	protocol ∈ AllowedAuthProtocols  = {bolt, http, grpc}
//
// Cardinality ceiling = |result| × |protocol| = 3 × 3 = 9 (RESEARCH §Q11).
// No tenant flag — auth is per-process global, not per-database
// (CONTEXT MET-21 omits `database` for auth; surfacing tenant identity
// on an unauthenticated counter would itself be a leak).
//
// Forbidden-label discipline (Phase 3 D-03 / registration.go ForbiddenLabels):
// `user`, `user_id`, `email`, `ip` are all in ForbiddenLabels and the
// registration helper panics on attempt — defense in depth against PII
// surfacing through label values. The closed `result` enum prevents
// callers from passing a raw error message as the result label.
//
// MET-25 hot path: callers cache `Attempts.WithLabelValues(result, protocol)`
// in a local var or struct field; the per-attempt observation pays only an
// atomic add. The cardinality ceiling is so low (9) that we do not even
// expose a Bind helper — direct WithLabelValues at the call site is fine.
//
// D-02d leaf-package boundary: pkg/observability never imports pkg/auth.
// The bag is constructed at cmd/nornicdb startup and injected into:
//   - pkg/bolt/server.SetAuthMetrics (Plan 04-02 added the call site)
//   - pkg/auth.Authenticator.SetMetrics (HTTP path; this plan wires it)
//   - gRPC interceptor (TBD by integration site)
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// AllowedAuthResults is the closed enum for the `result` label. Mirrors the
// three semantic outcomes per CONTEXT D-05e:
//   - success: credentials validated; user identity established
//   - failure: credentials presented but rejected (bad password, expired token)
//   - denied:  request rejected before credential evaluation (auth required
//              but absent, unsupported scheme, account locked, role denied)
//
// Adding a new result requires an ADR amendment AND callers must update
// classifyAuthResult in pkg/auth.
var AllowedAuthResults = []string{"success", "failure", "denied"}

// AllowedAuthProtocols is the closed enum for the `protocol` label. The
// label is set at the protocol-specific adapter chokepoint (HELLO handler
// for Bolt; HTTP middleware for HTTP; UnaryInterceptor for gRPC) — the
// shared core Authenticator is protocol-agnostic and does NOT increment.
var AllowedAuthProtocols = []string{"bolt", "http", "grpc"}

// AuthMetrics is the typed handle-bag (CONTEXT D-02 / D-02a) for the
// Auth subsystem. One bag per Provider, constructed at cmd/nornicdb startup
// and injected into pkg/bolt and pkg/auth. Bolt's HELLO call site nil-checks
// before observing (Plan 04-02 contract); this plan supplies the non-nil bag.
//
// Dual-access pattern (D-02): the raw *prometheus.CounterVec is exposed so
// subsystem tests can drive AssertCardinalityCeiling directly. The hot path
// (Inc) does not need a wrapper — counters do not emit exemplars in M1
// (CONTEXT D-02e defers counter exemplars).
type AuthMetrics struct {
	// AuthAttempts counts authentication attempts by result and protocol.
	// Cardinality ceiling = 9 (RESEARCH §Q11). Closed enums enforced by the
	// call sites; the registration helper rejects any attempt to add `user`,
	// `user_id`, `email`, or `ip` as a label (Phase 3 D-03a).
	AuthAttempts *prometheus.CounterVec
}

// NewAuthMetrics constructs the auth bag against reg.
//
// No tenant flag — auth events are global per CONTEXT MET-21. Surfacing a
// `database` label on an unauthenticated counter would leak tenant identity
// at the K8s scrape boundary; the auth subsystem deliberately omits it.
//
// Validation chain inherited from pkg/observability.NewCounterVec:
//   - subsystem "auth" must be in allowedSubsystems (metrics.go line 59)
//   - name must end in _total (registration.go validateNameSuffix)
//   - labels must NOT include any ForbiddenLabel (registration.go validateLabels)
//
// Pitfall 8 / MustRegister precedent: validation failure panics — programming
// bug surfaces at startup before any traffic.
func NewAuthMetrics(reg *prometheus.Registry) *AuthMetrics {
	bag := &AuthMetrics{}

	bag.AuthAttempts = NewCounterVec(reg,
		MetricOpts{
			Subsystem: "auth",
			Name:      "attempts_total",
			Help: "Authentication attempts by result (closed enum: success, " +
				"failure, denied) and protocol (closed enum: bolt, http, " +
				"grpc). No `user`/`user_id`/`email`/`ip` labels — Phase 3 " +
				"D-03a forbidden-label panic is the registration-time gate. " +
				"Per-protocol observation site lives in the protocol adapter " +
				"chokepoint (Bolt: handleHello; HTTP: AuthenticateHTTP; gRPC: " +
				"UnaryInterceptor). Cardinality ceiling = 9.",
		},
		[]string{"result", "protocol"})

	return bag
}
