// Package observability — registration-time validation primitives for the
// metrics-helper layer (Phase 3, MET-01..MET-05). Each helper in metrics.go
// calls these primitives BEFORE invoking reg.MustRegister; a violation is
// a programming bug per Phase 1 Pitfall 8 / MustRegister precedent —
// panic IS the desired startup behavior.
//
// Doc analog: pkg/observability/registry.go:28-29 (MustRegister panic-as-feature).
package observability

import (
	"fmt"
	"strings"
)

// ForbiddenLabels enumerates label names rejected at registration time.
// Case-insensitive match. Mirrors REQ MET-04 + ADR §2.2 verbatim (D-03a).
//
// Mutating this list requires an ADR amendment — these label names represent
// either cardinality bombs (full HTTP path, raw Cypher text, UUIDs, IPs) or
// PII (user, user_id, email) or values that belong elsewhere in the
// exposition format (trace_id and span_id belong in exemplars, never as
// labels — see exemplar.go).
//
// Falsifiability: TestForbiddenLabels_PanicsAtRegistration (60 cases:
// 10 labels × 6 helper constructors) + TestForbiddenLabels_CaseInsensitive +
// TestForbiddenLabels_AllTenEntriesPresent.
var ForbiddenLabels = []string{
	"path",           // raw HTTP path; only path_template is allowed
	"query",          // raw Cypher text
	"user",           // user identity (PII)
	"user_id",        // user identity (PII)
	"ip",             // client IP (PII / cardinality bomb)
	"uuid",           // node UUID, edge UUID — cardinality bomb
	"embedding_text", // raw embedding input
	"trace_id",       // belongs in exemplars, never as a label
	"span_id",        // belongs in exemplars, never as a label
	"email",          // PII (defensive; not in REQ but logical extension)
}

// validateSubsystem panics if s is not in allowedSubsystems (D-01d).
// NOTE: allowedSubsystems is declared in metrics.go per CONTEXT D-01d
// location lock ("Subsystem allow-list lives in pkg/observability/metrics.go").
// Same package — direct package-private reference, no import needed.
//
// Panic value: string (matches require.PanicsWithValue in
// TestSubsystemValidation tests; chosen because subsystem-typo errors are
// less likely to flow through assert.Error chains than label errors).
func validateSubsystem(s string) {
	for _, a := range allowedSubsystems {
		if s == a {
			return
		}
	}
	panic(fmt.Sprintf("observability: subsystem %q is not in allowedSubsystems %v", s, allowedSubsystems))
}

// validateNameSuffix panics if name does not end with the type-required
// suffix (D-01a / MET-02).
//
// Suffix pairing per ADR §2.2:
//
//	counter        ⇒ _total
//	latency hist   ⇒ _seconds
//	size hist      ⇒ _bytes
//	rowcount hist  ⇒ _rows
//	embedding lat  ⇒ _seconds (same suffix as latency; long-tail buckets only)
//
// Panic value: string (matches require.PanicsWithValue in
// TestNamingValidation_RejectsBadSuffix per Plan 03-01 Task 1).
func validateNameSuffix(name, requiredSuffix string) {
	if !strings.HasSuffix(name, requiredSuffix) {
		panic(fmt.Sprintf("observability: metric name %q must end in %q (MET-02)", name, requiredSuffix))
	}
}

// validateLabels panics if any entry in labels matches ForbiddenLabels
// (case-insensitive). MET-04 Layer 1 enforcement (D-03a).
//
// Panic value: error (via fmt.Errorf) so require.PanicsWithError matches —
// chosen because subsystem authors are likeliest to wrap or assert against
// the panic value; an error is friendlier than a bare string.
//
// The original caller-supplied case is preserved in the error message so
// the developer can find the offending source line via grep.
func validateLabels(labels []string) {
	for _, lbl := range labels {
		lower := strings.ToLower(lbl)
		for _, fb := range ForbiddenLabels {
			if lower == fb {
				panic(fmt.Errorf("observability: label %q is forbidden (cardinality bomb / PII); see ForbiddenLabels", lbl))
			}
		}
	}
}
