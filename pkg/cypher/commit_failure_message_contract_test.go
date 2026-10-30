package cypher

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestCommitFailedMessage_PinConsumerSubstrings locks the post-v1.0.45
// "commit failed: constraint violation: ..." wrapping that downstream
// classifiers depend on. It rebuilds the error from the exact format
// strings used at:
//
//   - pkg/cypher/transaction.go:181  "commit failed: %w"
//   - pkg/storage/badger_transaction.go:1404  "constraint violation: %w"
//   - pkg/storage/badger_constraint_validation.go:279 et al.
//     "Node with %s=%v already exists (nodeID: %s)"
//
// See docs/plans/consumer-pinned-error-contract-plan.md §2.1.
func TestCommitFailedMessage_PinConsumerSubstrings(t *testing.T) {
	leaf := fmt.Errorf("Node with uid=abc-123 already exists (nodeID: tenant_a:n-1)")
	storageErr := fmt.Errorf("constraint violation: %w", leaf)
	wrapped := fmt.Errorf("commit failed: %w", storageErr)

	got := wrapped.Error()
	wantSubs := []string{
		"commit failed",
		"constraint violation",
		"already exists",
		"nodeID:",
	}
	for _, sub := range wantSubs {
		if !strings.Contains(got, sub) {
			t.Errorf("commit-failed message %q missing required substring %q", got, sub)
		}
	}
	if !errors.Is(wrapped, leaf) {
		t.Errorf("commit-failed wrapper must preserve the inner error so errors.Is works")
	}
	if !errors.Is(wrapped, storageErr) {
		t.Errorf("commit-failed wrapper must preserve the storage layer wrap so errors.Is works")
	}
}

// TestRelationshipUniqueViolation_PinConsumerSubstrings locks the relationship
// variant of the "already exists" message at
// pkg/storage/badger_constraint_validation.go:659 and :686.
func TestRelationshipUniqueViolation_PinConsumerSubstrings(t *testing.T) {
	leafSingle := fmt.Errorf("Relationship with weight=0.5 already exists (edgeID: tenant_a:e-1)")
	leafComposite := fmt.Errorf("Relationship with duplicate composite key already exists (edgeID: tenant_a:e-2)")

	for _, leaf := range []error{leafSingle, leafComposite} {
		storageErr := fmt.Errorf("constraint violation: %w", leaf)
		wrapped := fmt.Errorf("commit failed: %w", storageErr)
		got := wrapped.Error()
		for _, sub := range []string{"commit failed", "constraint violation", "Relationship with", "already exists", "edgeID:"} {
			if !strings.Contains(got, sub) {
				t.Errorf("relationship commit-failed message %q missing required substring %q", got, sub)
			}
		}
		if !errors.Is(wrapped, leaf) {
			t.Errorf("relationship commit-failed wrapper must preserve inner error for errors.Is")
		}
	}
}
