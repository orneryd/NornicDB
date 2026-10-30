// Plan 04-06-05: auth_attempts_total wiring (D-11 + D-05e + GAP-6 / MET-15).
//
// Design (CONTEXT D-05e + RESEARCH §Q11):
//
//   - The pkg/observability.AuthMetrics bag is the single owner of the
//     `nornicdb_auth_attempts_total{result, protocol}` counter family.
//     pkg/auth never imports prometheus directly — it talks to the bag.
//   - The shared core Authenticator.Authenticate is protocol-agnostic and
//     does NOT increment the counter; the protocol label belongs at the
//     protocol-specific adapter chokepoint (Bolt: handleHello; HTTP:
//     middleware; gRPC: UnaryInterceptor).
//   - classifyAuthResult is the closed-enum mapper from the
//     Authenticate (..., error) return shape to the result enum.
//   - RecordAttempt is the SOLE observation entry point — DRY chokepoint
//     used by every protocol adapter. Nil-safe: when the bag is not
//     injected (e.g., test fixtures, embedded usage without a Provider),
//     RecordAttempt is a no-op.
//
// **Audit boundary (T-04-06):** this file ships ZERO changes to
// pkg/audit/audit.go. The auth_attempts_total counter is a parallel
// observability signal, not a replacement for the audit log. Operators
// observe `auth_attempts_total{result="failure"}` rate; compliance
// auditors read the audit log.
//
// **PII discipline (T-04-01 / ASVS V2):** no `user`, `user_id`, `ip`, or
// `email` label is exposed. Phase 3 D-03a forbidden-label panic at
// observability.NewCounterVec catches any attempt to drift.
package auth

import (
	"errors"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// AuthMetricsRecorder is the seam the protocol adapters use to publish
// the auth_attempts_total counter. The shared Authenticator owns one
// (set via SetAuthMetrics); protocol adapters can also own their own
// bag pointer for direct wiring at the chokepoint.
//
// The interface lives in pkg/auth (not pkg/observability) because the
// caller domain — the protocol adapters — is the one that knows the
// protocol label value. pkg/observability only knows about the underlying
// CounterVec.
type AuthMetricsRecorder interface {
	// RecordAttempt observes a single auth attempt with the given result
	// and protocol. Nil-safe — implementations no-op when the bag is nil.
	RecordAttempt(result, protocol string)
}

// authMetricsHolder is a tiny race-clean container for the bag pointer.
// Embedded in *Authenticator so SetAuthMetrics is safe to call after
// construction (e.g., from cmd/nornicdb startup after the Authenticator
// has been wired into the storage engine).
type authMetricsHolder struct {
	bag atomic.Pointer[observability.AuthMetrics]
}

// SetAuthMetrics injects the observability AuthMetrics bag for the
// auth_attempts_total counter (D-11 / D-05e / MET-15).
//
// Idempotent. Calling with nil disables observation.
//
// IMPORTANT: this is a parallel observability signal — the audit log
// (pkg/audit/audit.go) is the compliance source of truth and remains
// unchanged.
func (a *Authenticator) SetAuthMetrics(bag *observability.AuthMetrics) {
	a.metrics.bag.Store(bag)
}

// AuthMetrics returns the currently-injected bag (nil if unset).
// Used by protocol adapters to obtain the bag for direct wiring.
func (a *Authenticator) AuthMetrics() *observability.AuthMetrics {
	return a.metrics.bag.Load()
}

// RecordAttempt implements AuthMetricsRecorder on the shared Authenticator.
// Protocol adapters that share a single Authenticator instance can call
// `auth.RecordAttempt(...)` directly with the appropriate protocol label.
//
// Closed-enum discipline at the call site: callers must pass values from
// observability.AllowedAuthResults × observability.AllowedAuthProtocols.
// Any drift is caught by Phase 3 D-03a forbidden-label panic at
// registration (defense in depth — the call site gate is the actual check).
func (a *Authenticator) RecordAttempt(result, protocol string) {
	bag := a.metrics.bag.Load()
	if bag == nil || bag.AuthAttempts == nil {
		return
	}
	bag.AuthAttempts.WithLabelValues(result, protocol).Inc()
}

// ClassifyAuthResult maps the (User, error) return from Authenticate or
// ValidateToken into the closed result enum {success, failure, denied}.
//
// Mapping per CONTEXT D-05e:
//
//	err == nil                                 → "success"
//	errors.Is(err, ErrInvalidCredentials)      → "failure" (bad password)
//	errors.Is(err, ErrInvalidToken)            → "failure" (bad token)
//	errors.Is(err, ErrSessionExpired)          → "failure" (expired)
//	errors.Is(err, ErrAccountLocked)           → "denied"  (lockout)
//	errors.Is(err, ErrInsufficientRole)        → "denied"  (RBAC reject)
//	errors.Is(err, ErrNoCredentials)           → "denied"  (no creds)
//	other (e.g. storage error)                 → "failure" (defensive)
//
// The mapping is intentional:
//   - "success" = identity established
//   - "failure" = credentials presented but rejected
//   - "denied"  = request rejected before / despite credentials (gate)
//
// Operators write per-result-rate alerts:
//
//	rate(auth_attempts_total{result="failure"}[5m]) > 1  ⇒ brute force
//	rate(auth_attempts_total{result="denied"}[5m]) > 0.1 ⇒ misconfig
func ClassifyAuthResult(err error) string {
	if err == nil {
		return "success"
	}
	switch {
	case errors.Is(err, ErrInvalidCredentials),
		errors.Is(err, ErrInvalidToken),
		errors.Is(err, ErrSessionExpired):
		return "failure"
	case errors.Is(err, ErrAccountLocked),
		errors.Is(err, ErrInsufficientRole),
		errors.Is(err, ErrNoCredentials):
		return "denied"
	default:
		// Defensive bucket — storage errors, unexpected wraps. We pick
		// "failure" over "denied" so RBAC misconfig (which is "denied")
		// stays distinguishable from data-plane errors.
		return "failure"
	}
}
