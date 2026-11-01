// Author: R17 implementation lane, 1 November 2026.
package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func TestCompatibilityPipelineCachesKeepCurrentArena(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "compat"))
	const child = "UNWIND [] AS value RETURN *"
	const parent = "CALL () { " + child + " } RETURN *"
	// Metadata and pipeline inspection commonly precede execution (cold fields,
	// EXPLAIN and participant planning). Warm the existing standalone caches.
	prior, ok := splitPipelineClausesAllowingProcedureCalls(t.Context(), child)
	if !ok || len(prior) != 2 {
		t.Fatalf("standalone split: %v", ok)
	}
	old := e.projectedStatementScope(t.Context(), child, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
	if !old.complete {
		t.Fatal("standalone metadata declined")
	}
	ctx, source := cyphertext.WithSource(t.Context(), parent)
	ctx = cypherantlr.WithNewSyntaxExecution(ctx, source.Text)
	retained, err := cypherantlr.ParseSyntax(ctx, parent, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(ctx, child)
	if !ok || len(clauses) != 2 {
		t.Fatalf("owned split: %v", ok)
	}
	for _, clause := range clauses {
		if !retained.Contains(clause.syntax) {
			t.Fatal("pipeline cache imported another arena")
		}
	}
	program := e.projectedStatementScope(ctx, child, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
	if !program.complete || program.bound == nil {
		t.Fatal("owned metadata declined")
	}
	bound, err := e.bindPipelineProgram(withBoundStatementProgram(ctx, old.bound), clauses, nil)
	if err != nil {
		t.Fatal(err)
	}
	if returnProjectionPlanFor(bound, clauses[1].text, clauses[1]) == nil {
		t.Fatal("bound projection IDs came from another arena")
	}
}

func TestCompatibilityPublicEmptyAndLoweredPrograms(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "compat"))
	for _, query := range []string{
		"UNWIND [] AS value RETURN *",
		"CALL { RETURN 1 AS before } CREATE (:Compat)",
		"WITH 1 AS value CALL db.labels() YIELD label RETURN *",
		"RETURN [COLLECT { UNWIND [3,1,2] AS x ORDER BY x LIMIT 2 RETURN x }] AS v",
		"RETURN (COUNT { LIMIT 1 RETURN 1 AS x }) AS v",
		"WITH 1 AS value RETURN *, 2 AS extra",
	} {
		for _, prefix := range []string{"", "PROFILE ", "EXPLAIN "} {
			t.Run(prefix+query, func(t *testing.T) {
				if _, err := e.Execute(t.Context(), prefix+query, nil); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
