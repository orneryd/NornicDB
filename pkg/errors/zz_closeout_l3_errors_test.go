// Closeout lane L3 (v1.5.2 verification campaign), 22 Nov 2026. Test-side only.
// Items: P-NGEN-26 (retry-status parsing vs the pinned driver), P-NGEN-25
// (TerminalOutcomes retains a mutable custom primary by reference).
package errors_test

import (
	"errors"
	"testing"

	driverdb "github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// P-NGEN-26: IsRetriableStatus (terminal_outcome.go:213) is a copy of the
// driver's retry rule. Controls are valid Neo4j codes; the item's cases are a
// foreign namespace and Neo.TransientError codes with missing/extra parts.
func TestCloseoutL3_NGEN26_RetryStatusMatchesPinnedDriver(t *testing.T) {
	for _, tc := range []struct {
		code    string
		control bool
	}{
		{"Neo.TransientError.Transaction.DeadlockDetected", true},
		{"Neo.TransientError.General.DatabaseUnavailable", true},
		{"Neo.ClientError.Cluster.NotALeader", true},
		{"Neo.ClientError.Security.AuthorizationExpired", true},
		{"Neo.TransientError.Transaction.Terminated", true},
		{"Neo.TransientError.Transaction.LockClientStopped", true},
		{"Neo.ClientError.Statement.SyntaxError", true},
		{"Neo.DatabaseError.General.UnknownError", true},
		{"Vendor.TransientError.Transaction.ProviderFailure", false},
		{"Neo.TransientError.Transaction.ProviderFailure.extra", false},
		{"Neo.TransientError.ProviderFailure", false},
	} {
		t.Run(tc.code, func(t *testing.T) {
			driver := (&driverdb.Neo4jError{Code: tc.code}).IsRetriable()
			ours := nornicerrors.IsRetriableStatus(tc.code)
			t.Logf("DECISIVE code=%s control=%v driver.IsRetriable=%v IsRetriableStatus=%v", tc.code, tc.control, driver, ours)
			if driver != ours {
				if tc.control {
					t.Fatalf("control: valid status %s disagrees (driver %v, ours %v)", tc.code, driver, ours)
				}
				t.Errorf("NOR-370: %s: pinned driver retries=%v, IsRetriableStatus=%v", tc.code, driver, ours)
			}
		})
	}
}

type closeoutL3MutablePrimary struct{ message string }

func (e *closeoutL3MutablePrimary) Error() string { return e.message }

// P-NGEN-25: Record keeps the complete returned value; a custom primary that its
// producer mutates after Record changes the later rendered outcome.
func TestCloseoutL3_NGEN25_RecordedOutcomeTextStable(t *testing.T) {
	newFinish := func() *nornicerrors.TerminalFailure {
		return nornicerrors.NewTerminalFailure(nornicerrors.TerminalRollback, false, errors.New("rollback primary"), errors.New("WAL abort failed"), localization.Message{Fallback: "WAL abort failed"})
	}
	// Control: an immutable primary renders the same text before and after.
	{
		returned := errors.Join(errors.New("admitted primary"), newFinish())
		var collection nornicerrors.TerminalOutcomes
		collection, changed := collection.Record(returned)
		if !changed {
			t.Fatalf("control: Record did not retain the outcome")
		}
		_, before := nornicerrors.RenderStatus(collection.Retain(nil), nil)
		_, after := nornicerrors.RenderStatus(collection.Retain(nil), nil)
		t.Logf("DECISIVE control before=%q after=%q", before, after)
		if before != after {
			t.Fatalf("control: immutable outcome text changed")
		}
	}
	primary := &closeoutL3MutablePrimary{message: "admitted primary"}
	returned := errors.Join(primary, newFinish())
	var collection nornicerrors.TerminalOutcomes
	collection, changed := collection.Record(returned)
	if !changed {
		t.Fatalf("setup: Record did not retain the outcome")
	}
	beforeCode, before := nornicerrors.RenderStatus(collection.Retain(nil), nil)
	primary.message = "producer reused primary"
	afterCode, after := nornicerrors.RenderStatus(collection.Retain(nil), nil)
	t.Logf("DECISIVE mutable before=(%s) %q after=(%s) %q", beforeCode, before, afterCode, after)
	if before != after {
		t.Errorf("NOR-369: the recorded outcome's text changed after its producer mutated the primary: %q -> %q", before, after)
	}
}
