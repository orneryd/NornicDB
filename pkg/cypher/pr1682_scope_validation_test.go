package cypher

import (
	"context"
	"strings"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type pr1682CancelOnSecondCheck struct {
	context.Context
	checks int
}

func (ctx *pr1682CancelOnSecondCheck) Err() error {
	ctx.checks++
	if ctx.checks >= 2 {
		return context.Canceled
	}
	return nil
}

func TestPR1682CallNodeWalkStopsOnCancellation(t *testing.T) {
	query := "WITH 1 AS x " + strings.Repeat("CALL (x) { RETURN x AS y } ", 20) + "RETURN x"
	syntax, err := cypherantlr.ParseSyntax(t.Context(), query, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &pr1682CancelOnSecondCheck{Context: context.Background()}
	if nodes, err := callSubqueryNodes(ctx, syntax); err != context.Canceled || nodes != nil {
		t.Fatalf("got nodes=%v err=%v, want cancellation during the walk", nodes, err)
	}
	if ctx.checks != 2 {
		t.Fatalf("got %d cancellation checks, want 2", ctx.checks)
	}
}

func TestPR1682CallScopeValidationBoundaries(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope"))
	cases := []struct{ name, query, errorContains string }{
		{"multiple", "WITH 1 AS x CALL (x) { RETURN x AS y } CALL (y) { RETURN y AS z } RETURN z", ""},
		{"nested", "WITH 1 AS x CALL (x) { CALL (x) { RETURN x AS y } RETURN y } RETURN y", ""},
		{"optional", "WITH 1 AS x OPTIONAL CALL (x) { RETURN x AS y } RETURN y", ""},
		{"procedure", "CALL db.labels() YIELD label RETURN label", ""},
		{"literal", "RETURN 'CALL' AS text", ""},
		{"empty", "CALL () { RETURN 1 AS x } RETURN x", ""},
		{"all", "WITH 1 AS x CALL (*) { RETURN x AS y } RETURN y", ""},
		{"bad-import", "WITH 1 AS x CALL (x, 1) { RETURN x AS y } RETURN y", "CALL imports must contain only variable names"},
		{"bad-nested-import", "WITH 1 AS x CALL (x) { CALL (x, 1) { RETURN x AS y } RETURN y } RETURN y", "CALL imports must contain only variable names"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := exec.validateCallSubqueryScopes(context.Background(), tc.query, false)
			if tc.errorContains == "" && err != nil {
				t.Fatalf("unexpected: %v", err)
			}
			if tc.errorContains != "" && (err == nil || !strings.Contains(err.Error(), tc.errorContains)) {
				t.Fatalf("got %v, want %q", err, tc.errorContains)
			}
		})
	}
}

func pr1682TestCallClause(t *testing.T, query string) pipelineClause {
	t.Helper()
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(t.Context(), query)
	if !ok || len(clauses) != 1 || !clauses[0].syntax.Valid() {
		t.Fatalf("could not split CALL: %q", query)
	}
	return clauses[0]
}

func TestPR1682CallOperatorCancellation(t *testing.T) {
	clause := pr1682TestCallClause(t, "CALL () { RETURN 1 AS value }")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope"))
	_, _, handled, err := exec.pipelineApplyCallSubqueryWithMetadata(ctx, []pipelineRow{{}}, clause, nil)
	if !handled || err != context.Canceled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}
