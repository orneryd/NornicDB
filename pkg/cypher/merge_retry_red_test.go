// Author: Claude, NOR-180 red-first overlay on 8176d4b97, 2026-10-31. The unit test
// of the executed-effects correction, unchanged; expected to fail on this code.
package cypher

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// The decision reads what the statements did in the storage transaction, nested
// bodies included, and never the live registry.
func TestMergeRetryRestsOnWhatStatementsDid(t *testing.T) {
	const prefix = "CREATE OR REPLACE PROCEDURE mergeretry."
	runner := "mergeretry.run"
	for _, tc := range []struct {
		name, companion string
		retrySafe       bool
	}{
		{"builtin_read", "CALL db.labels() YIELD label RETURN label", true},
		{"read_body", "CALL " + runner + "('RETURN 1 AS n')", true},
		{"read_declared_body_writes", "CALL " + runner + "('CREATE (:Other) RETURN 1 AS n')", false},
		{"body_updates_a_pending_create", "CALL " + runner + "('MATCH (n:Keep) SET n.note = 1 RETURN 1 AS n')", false},
		{"write_declared_body_writes", "CALL mergeretry.claim()", false},
		{"write_declared_body_writes_nothing", "CALL mergeretry.quiet()", false},
		{"admin_procedure", "CALL db.clearQueryCaches()", false},
		{"unknown_procedure", "CALL custom.unknownProc()", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
			ctx := context.Background()
			for _, setup := range []string{
				prefix + "run($q) MODE READ AS CALL apoc.cypher.run($q, {}) YIELD value RETURN value",
				prefix + "claim() MODE WRITE AS CREATE (:Other) RETURN 1 AS value",
				prefix + "quiet() MODE WRITE AS RETURN 1 AS value",
			} {
				if _, err := exec.Execute(ctx, setup, nil); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			if _, err := exec.Execute(ctx, "MERGE (n:Keep {uid: 'X'})", nil); err != nil {
				t.Fatal(err)
			}
			_, statementErr := exec.Execute(ctx, tc.companion, nil)
			if tc.retrySafe && statementErr != nil {
				t.Fatal(statementErr)
			}
			// A later registration changes nothing already done.
			other := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
			for _, replace := range []string{
				prefix + "claim() MODE READ AS RETURN 1 AS value",
				prefix + "quiet() MODE READ AS RETURN 1 AS value",
			} {
				if _, err := other.Execute(ctx, replace, nil); err != nil {
					t.Fatal(err)
				}
			}
			violation := fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{
				Type: storage.ConstraintUnique, Label: "Keep", Properties: []string{"uid"}, Message: "exists",
			})
			got := classifyMergeRetry(ctx, exec.analyzer, exec.txContext.mergeRetry, violation)
			if safe := stderrors.Is(got, nornicerrors.ErrMergeCommitTimeUniqueConflict); safe != tc.retrySafe {
				t.Fatalf("retry-safe = %v, want %v (statement error %v)", safe, tc.retrySafe, statementErr)
			}
		})
	}
}
