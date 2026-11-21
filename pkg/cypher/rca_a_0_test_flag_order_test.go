package cypher

// Root-cause diagnostic (campaign v1.5.2, lane D, group G-test-wal-flag-order; run by lane A).
// Test-side only. Run ALONE (-run '^TestRCAFlagOrderBuiltInRegistry$') so no
// earlier test of the process has constructed a StorageExecutor.
// withMergeNodeWrites (merge_node_writes.go:111-116) and the query-body
// output policy look procedures up in globalProcedureRegistry without
// ensureBuiltInProceduresRegistered(); only NewStorageExecutor
// (executor.go:827) and the CALL paths register the built-ins. The two tests
// construct no executor, so their result depends on whether an earlier test
// in the same process did. Logs the decisive values before and after the
// test-side registration step.

import (
	"testing"
)

func TestRCAFlagOrderBuiltInRegistry(t *testing.T) {
	ctx := t.Context()
	report := func(phase string) {
		_, found := globalProcedureRegistry.Get("db.labels")
		clauses := []pipelineClause{{kind: pipelineClauseMerge, text: "MERGE (n:Doc {id: 1})"}, {kind: pipelineClauseCall, text: "CALL db.labels() YIELD label"}}
		writes := mergeNodeWritesFromContext(withMergeNodeWrites(ctx, clauses))
		q := "CALL db.labels()"
		body := readQueryBody(ctx, q, 0, len(q))
		declared := queryBodyProducesColumns(ctx, q, body, false)
		t.Logf("RCA_FLAGORDER phase=%s registry_has_db.labels=%v mergeWrites.all(read-only procedure)=%v (test expects false) queryBodyProducesColumns(CALL db.labels(), declared)=%v (test expects true)",
			phase, found, writes.all, declared)
	}
	report("fresh-process")
	ensureBuiltInProceduresRegistered()
	report("after-ensureBuiltInProceduresRegistered")
}
