// Closeout lane L3 (v1.5.2 verification campaign), 22 Nov 2026. Test-side only.
// Root-cause proofs for P-NGEN-26 (NOR-370) and P-NGEN-25 (NOR-369).
package errors

import (
	stderrors "errors"
	"strings"
	"testing"

	driverdb "github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// P-NGEN-26. Owner: IsRetriableStatus, terminal_outcome.go:213-221 (the
// fallback `strings.HasPrefix(code, "Neo.TransientError.")` at :220), a copy of
// the driver's rule. Its consumer committedStatus (:228-233) must never hand a
// client a status its driver retries after committed work.
// (a) Diagnostic: per code, the branch IsRetriableStatus takes and what
// committedStatus returns, against the pinned driver's IsRetriable on that output.
// (b) Bypass: a test-side committedStatus that delegates to the pinned driver;
// on the same codes it never returns a driver-retriable status.
func TestCloseoutL3_RC_NGEN26_RetryPredicateOwner(t *testing.T) {
	bypassCommitted := func(code string) string {
		if (&driverdb.Neo4jError{Code: code}).IsRetriable() {
			return TerminalCommit.fallbackStatus()
		}
		return code
	}
	for _, tc := range []struct {
		code    string
		control bool
	}{
		{"Neo.TransientError.Transaction.DeadlockDetected", true},
		{"Neo.ClientError.Statement.SyntaxError", true},
		{"Neo.TransientError.Transaction.Terminated", true},
		{"Vendor.TransientError.Transaction.ProviderFailure", false},
		{"Neo.TransientError.Transaction.ProviderFailure.extra", false},
		{"Neo.TransientError.ProviderFailure", false},
	} {
		t.Run(tc.code, func(t *testing.T) {
			driverRetries := (&driverdb.Neo4jError{Code: tc.code}).IsRetriable()
			prefixBranch := strings.HasPrefix(tc.code, "Neo.TransientError.")
			ours := IsRetriableStatus(tc.code)
			out := committedStatus(tc.code)
			outRetriedByDriver := (&driverdb.Neo4jError{Code: out}).IsRetriable()
			bypass := bypassCommitted(tc.code)
			bypassRetried := (&driverdb.Neo4jError{Code: bypass}).IsRetriable()
			t.Logf("CLOSEOUT_L3_RC NGEN26 code=%s control=%v driver.IsRetriable=%v prefixBranch(:220)=%v IsRetriableStatus=%v committedStatus=%s driverRetriesThat=%v | bypass=%s driverRetriesThat=%v",
				tc.code, tc.control, driverRetries, prefixBranch, ours, out, outRetriedByDriver, bypass, bypassRetried)
			if bypassRetried {
				t.Fatalf("BYPASS: driver-delegating committedStatus still returns a retriable status %s", bypass)
			}
			if tc.control {
				if ours != driverRetries || outRetriedByDriver {
					t.Fatalf("control: valid code %s disagrees or committedStatus returns a retriable status", tc.code)
				}
				return
			}
			// Mechanism: the copied predicate disagrees with the driver exactly here.
			if ours == driverRetries {
				t.Fatalf("MECHANISM: IsRetriableStatus agrees with the driver for %s; the item's disagreement is absent", tc.code)
			}
			t.Logf("CLOSEOUT_L3_RC NGEN26 MECHANISM code=%s copied predicate %v vs driver %v; committed-work status handed to a client the driver would retry=%v", tc.code, ours, driverRetries, outRetriedByDriver)
		})
	}
}

type closeoutL3RCMutablePrimary struct{ message string }

func (e *closeoutL3RCMutablePrimary) Error() string { return e.message }

// P-NGEN-25. Owner: TerminalOutcomes.Record, terminal_outcomes.go:44, which
// keeps `value := outcome` (the caller's complete returned value) by reference.
// (a) Diagnostic: the retained collection still contains the caller's very
// primary object (errors.Is identity) and its text follows a later mutation.
// (b) Bypass: recording a value whose ordinary primary was frozen to its text
// before Record keeps the rendered outcome stable under the same mutation.
func TestCloseoutL3_RC_NGEN25_RecordKeepsCallerPrimary(t *testing.T) {
	finish := func() *TerminalFailure {
		return NewTerminalFailure(TerminalRollback, false, stderrors.New("rollback primary"), stderrors.New("WAL abort failed"), localization.Message{Fallback: "WAL abort failed"})
	}
	primary := &closeoutL3RCMutablePrimary{message: "admitted primary"}
	var raw TerminalOutcomes
	raw, changed := raw.Record(stderrors.Join(primary, finish()))
	if !changed {
		t.Fatalf("setup: Record did not retain the outcome")
	}
	retained := raw.Retain(nil)
	identity := stderrors.Is(retained, primary)
	_, before := RenderStatus(retained, nil)
	primary.message = "producer reused primary"
	_, after := RenderStatus(raw.Retain(nil), nil)
	t.Logf("CLOSEOUT_L3_RC NGEN25 raw: retainedHoldsCallerPrimary=%v before=%q after=%q", identity, before, after)

	frozenPrimary := &closeoutL3RCMutablePrimary{message: "admitted primary"}
	var frozen TerminalOutcomes
	frozen, changed = frozen.Record(stderrors.Join(stderrors.New(frozenPrimary.Error()), finish()))
	if !changed {
		t.Fatalf("setup: bypass Record did not retain the outcome")
	}
	_, frozenBefore := RenderStatus(frozen.Retain(nil), nil)
	frozenPrimary.message = "producer reused primary"
	_, frozenAfter := RenderStatus(frozen.Retain(nil), nil)
	t.Logf("CLOSEOUT_L3_RC NGEN25 bypass(frozen primary): holdsCallerPrimary=%v before=%q after=%q", stderrors.Is(frozen.Retain(nil), frozenPrimary), frozenBefore, frozenAfter)

	if frozenBefore != frozenAfter {
		t.Fatalf("BYPASS: frozen primary still changed the rendered outcome")
	}
	if !identity {
		t.Fatalf("MECHANISM: the retained outcome does not hold the caller's primary object")
	}
	if before == after {
		t.Fatalf("MECHANISM: the rendered outcome did not follow the mutation (defect absent)")
	}
}
