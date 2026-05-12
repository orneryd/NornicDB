package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Tests for the four canonical entity-write UNWIND templates a typed
// Neo4j-canonical Cypher client emits when projecting code-intelligence
// facts into the graph, plus two HAS_PARAMETER edge-write variants that
// expose the multi-variable-SET bail condition.
//
// The companion file unwind_merge_chain_canonical_node_test.go pinned
// chain-batch engagement on the simple annotation upsert shapes. These
// tests cover four more variations that surface in practice — orphan
// entity, top-level $file_path parameter, row-scoped file_path, and
// containment-edge-only — plus a composite-key HAS_PARAMETER edge with
// a multi-variable SET clause that documents a real bail.
//
// A CPU profile of an UNWIND-MERGE-heavy canonical-write workload
// attributed 21.78% of cumulative CPU to executeMatchForContext under
// executeCompoundMatchMerge, meaning some production cypher shape was
// reaching the per-statement interpreter instead of the chain-batch.
// These tests narrow which shape is responsible.

// orphanEntityUpsertCypher: the simplest variant — no MATCH, no
// containment edge, just MERGE-on-uid plus SET. A client that emits
// entities not yet anchored to a containing file uses this shape.
const orphanEntityUpsertCypher = `UNWIND $rows AS row
MERGE (n:Function {uid: row.entity_id})
SET n += row.props`

func TestUnwindMergeChainBatch_OrphanEntity(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE CONSTRAINT function_uid_unique IF NOT EXISTS FOR (n:Function) REQUIRE n.uid IS UNIQUE", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(orphanEntityUpsertCypher), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id": "function:src/a.go:foo",
				"props":     map[string]interface{}{"name": "foo", "path": "src/a.go"},
			},
		},
	})
	require.NoError(t, err)

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch,
		"orphan entity upsert must engage executeUnwindMergeChainBatch")
	require.True(t, trace.MergeSchemaLookupUsed,
		"unique constraint exists on Function.uid; must route through schema lookup")
	require.False(t, trace.MergeScanFallbackUsed,
		"no scan fallback justified when unique constraint is present")
}

// fileScopedEntityUpsertCypher: UNWIND with MATCH (f:File {path: $file_path}) —
// note the top-level $file_path parameter, NOT row-scoped. A client that
// batches many entities from a single source file into one statement
// emits this shape. The mix of a top-level parameter with row-scoped
// references in the same UNWIND is a subtle variation worth covering
// separately from the row-scoped form below.
const fileScopedEntityUpsertCypher = `UNWIND $rows AS row
MATCH (f:File {path: $file_path})
MERGE (n:Function {uid: row.entity_id})
SET n += row.props
MERGE (f)-[rel:CONTAINS]->(n)
SET rel.evidence_source = 'projector/canonical',
    rel.generation_id = row.generation_id`

func TestUnwindMergeChainBatch_FileScopedEntity(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE",
		"CREATE CONSTRAINT function_uid_unique IF NOT EXISTS FOR (n:Function) REQUIRE n.uid IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "CREATE (:File {path: 'src/a.go'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(fileScopedEntityUpsertCypher), map[string]interface{}{
		"file_path": "src/a.go",
		"rows": []map[string]interface{}{
			{
				"entity_id":     "function:src/a.go:foo",
				"props":         map[string]interface{}{"name": "foo"},
				"generation_id": "gen-1",
			},
		},
	})
	require.NoError(t, err)

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch,
		"file-scoped template (top-level $file_path parameter) must engage executeUnwindMergeChainBatch")
	require.True(t, trace.MergeSchemaLookupUsed,
		"both unique constraints present; must route through schema lookup")
	require.False(t, trace.MergeScanFallbackUsed,
		"no scan fallback justified when both unique constraints are present")
}

// rowScopedEntityUpsertCypher: equivalent logical shape to the file-scoped
// form above, but the file path comes from row.file_path instead of a
// top-level parameter. Each row carries its own file_path, so multiple
// files can be projected in a single UNWIND. Also covered: the trailing
// "SET rel.evidence_source = ..." on the edge MERGE, which the simpler
// annotation tests did not exercise.
const rowScopedEntityUpsertCypher = `UNWIND $rows AS row
MATCH (f:File {path: row.file_path})
MERGE (n:Function {uid: row.entity_id})
SET n += row.props
MERGE (f)-[rel:CONTAINS]->(n)
SET rel.evidence_source = 'projector/canonical',
    rel.generation_id = row.generation_id`

func TestUnwindMergeChainBatch_RowScopedEntity(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE",
		"CREATE CONSTRAINT function_uid_unique IF NOT EXISTS FOR (n:Function) REQUIRE n.uid IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "CREATE (:File {path: 'src/a.go'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(rowScopedEntityUpsertCypher), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":     "function:src/a.go:foo",
				"file_path":     "src/a.go",
				"props":         map[string]interface{}{"name": "foo"},
				"generation_id": "gen-1",
			},
		},
	})
	require.NoError(t, err)

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch,
		"row-scoped template with edge SET must engage executeUnwindMergeChainBatch")
	require.True(t, trace.MergeSchemaLookupUsed,
		"both unique constraints present; must route through schema lookup")
	require.False(t, trace.MergeScanFallbackUsed,
		"no scan fallback justified when both unique constraints are present")
}

// containmentEdgeOnlyCypher: UNWIND with two MATCH clauses (file via
// top-level $file_path parameter, entity via row.entity_id) followed by
// an edge MERGE. A client that backfills containment edges separately
// from entity creation emits this shape. Structurally different from
// the above three: both anchors are MATCH lookups, no node MERGE, only
// an edge MERGE. The chain-batch parser must split it into two lookup
// steps plus one relationship step.
const containmentEdgeOnlyCypher = `UNWIND $rows AS row
MATCH (f:File {path: $file_path})
MATCH (n:Function {uid: row.entity_id})
MERGE (f)-[rel:CONTAINS]->(n)
SET rel.evidence_source = 'projector/canonical',
    rel.generation_id = row.generation_id`

func TestUnwindMergeChainBatch_ContainmentEdgeOnly(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE",
		"CREATE CONSTRAINT function_uid_unique IF NOT EXISTS FOR (n:Function) REQUIRE n.uid IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}
	for _, stmt := range []string{
		"CREATE (:File {path: 'src/a.go'})",
		"CREATE (:Function {uid: 'function:src/a.go:foo'})",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, strings.TrimSpace(containmentEdgeOnlyCypher), map[string]interface{}{
		"file_path": "src/a.go",
		"rows": []map[string]interface{}{
			{
				"entity_id":     "function:src/a.go:foo",
				"generation_id": "gen-1",
			},
		},
	})
	require.NoError(t, err)

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch,
		"two-MATCH edge-only template must engage executeUnwindMergeChainBatch")
	require.True(t, trace.MergeSchemaLookupUsed,
		"both unique constraints present; must route through schema lookup")
	require.False(t, trace.MergeScanFallbackUsed,
		"no scan fallback justified when both unique constraints are present")
}

// multiVariableSetEdgeCypher exercises a composite-key MERGE pattern: the
// MATCH side anchors a Function by three properties (name, path,
// line_number) and the MERGE side creates or finds a Parameter by three
// properties (name, path, function_line_number). The relationship MERGE
// is followed by a SET clause that mixes assignments to BOTH p.X and
// rel.X — the documented bail condition for the chain-batch parser.
const multiVariableSetEdgeCypher = `UNWIND $rows AS row
MATCH (fn:Function {name: row.func_name, path: row.file_path, line_number: row.func_line})
MERGE (p:Parameter {name: row.param_name, path: row.file_path, function_line_number: row.func_line})
MERGE (fn)-[rel:HAS_PARAMETER]->(p)
SET p.evidence_source = 'projector/canonical',
    p.generation_id = row.generation_id,
    rel.evidence_source = 'projector/canonical',
    rel.generation_id = row.generation_id`

// NornicDB does not support composite-key unique constraints — the syntax
// `REQUIRE (n.a, n.b) IS UNIQUE` is rejected with "invalid CREATE
// CONSTRAINT syntax". Clients that emit composite-key MERGE patterns
// like this one cannot register a backing unique constraint, so the
// MERGE must resolve via property-index lookup or, failing that, a
// label scan.
//
// TestUnwindMergeChainBatch_MultiVariableSet_BailsToScan characterizes
// the current behavior: chain-batch bails entirely on this shape because
// parseUnwindSimpleSetAssignments at clauses.go:1020-1029 enforces that
// every assignment in a SET clause targets the same mergeVar. The
// trailing SET clause mixes leftVar="p" and leftVar="rel", the helper
// returns nil/false on the first variable mismatch, and
// parseUnwindMergeChainPattern bubbles that up as plan.supported=false.
// Every row then falls through to the per-row generic interpreter at
// clauses.go:562, which dispatches each substituted query to
// executeCompoundMatchMerge — the exact CPU signature that an
// UNWIND-MERGE-heavy workload profile captured.
func TestUnwindMergeChainBatch_MultiVariableSet_BailsToScan(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// No composite unique constraint exists (NornicDB syntax does not
	// permit one). The setup creates a Function row matching the MERGE
	// anchor so the cypher is exercising the real lookup path.
	_, err := exec.Execute(ctx, "CREATE (:Function {name: 'foo', path: 'src/a.go', line_number: 10})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(multiVariableSetEdgeCypher), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"func_name":     "foo",
				"file_path":     "src/a.go",
				"func_line":     int64(10),
				"param_name":    "x",
				"generation_id": "gen-1",
			},
		},
	})
	require.NoError(t, err)

	trace := exec.LastHotPathTrace()
	require.False(t, trace.UnwindMergeChainBatch,
		"DOCUMENTS BAIL: multi-variable SET clauses (p.X and rel.X mixed) are not "+
			"supported by parseUnwindSimpleSetAssignments — chain-batch never engages")
}

// splitSetEdgeCypher is the same logical write as
// multiVariableSetEdgeCypher with the trailing multi-variable SET split
// into two single-variable SET clauses — one immediately after the
// Parameter MERGE for p.X assignments, one immediately after the
// HAS_PARAMETER MERGE for rel.X assignments. Per the Cypher spec the
// two forms are semantically equivalent because both apply atomically
// within the same query execution. The split shape is the safe form
// for clients to emit when they want to engage the chain-batch fast
// path; it is also the form the cookbook recommends.
const splitSetEdgeCypher = `UNWIND $rows AS row
MATCH (fn:Function {name: row.func_name, path: row.file_path, line_number: row.func_line})
MERGE (p:Parameter {name: row.param_name, path: row.file_path, function_line_number: row.func_line})
SET p.evidence_source = 'projector/canonical',
    p.generation_id = row.generation_id
MERGE (fn)-[rel:HAS_PARAMETER]->(p)
SET rel.evidence_source = 'projector/canonical',
    rel.generation_id = row.generation_id`

func TestUnwindMergeChainBatch_SplitSet_FastPathEngages(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Function {name: 'foo', path: 'src/a.go', line_number: 10})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(splitSetEdgeCypher), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"func_name":     "foo",
				"file_path":     "src/a.go",
				"func_line":     int64(10),
				"param_name":    "x",
				"generation_id": "gen-1",
			},
		},
	})
	require.NoError(t, err)

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch,
		"split form (SET p.X clause separated from SET rel.X clause) must engage chain-batch "+
			"because each SET body targets a single variable")
}

// TestUnwindMergeChainBatch_ParameterizedCypher_HandlesTriggerSubstrings
// verifies that an UNWIND-batched canonical entity-write template handles
// row property values containing literal "remove ", "shortestpath", and
// "allshortestpaths" substrings safely.
//
// A defensive client might add a check that routes rows whose property
// values contain those substrings away from the UNWIND-batched fast path
// and into per-statement parameterized singletons, on the theory that
// the parser could confuse them with reserved syntax. For parameterized
// cypher this concern is unfounded: parameters are bound separately
// from cypher text per the Bolt protocol, so parameter values never
// become cypher syntax regardless of content. A client doing such
// substring-based routing pays the per-row cost on every match (in a
// real Go codebase, the English word "remove" appears in comments often
// enough to route many thousands of rows away from the fast path), with
// no security or correctness benefit.
//
// This test exercises that proposition directly: row.props with
// parameter values containing all three trigger substrings, executed
// via UNWIND-batched cypher, asserting both correctness and chain-batch
// engagement.
func TestUnwindMergeChainBatch_ParameterizedCypher_HandlesTriggerSubstrings(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE",
		"CREATE CONSTRAINT function_uid_unique IF NOT EXISTS FOR (n:Function) REQUIRE n.uid IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "CREATE (:File {path: 'src/a.go'})", nil)
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{
			"entity_id":     "function:src/a.go:RemoveContainer",
			"file_path":     "src/a.go",
			"props":         map[string]interface{}{"name": "RemoveContainer", "docstring": "RemoveContainer should remove the container from the runtime"},
			"generation_id": "gen-1",
		},
		{
			"entity_id":     "function:src/a.go:findShortestPath",
			"file_path":     "src/a.go",
			"props":         map[string]interface{}{"name": "findShortestPath", "docstring": "computes shortestpath between nodes"},
			"generation_id": "gen-1",
		},
		{
			"entity_id":     "function:src/a.go:AllShortestPathsImpl",
			"file_path":     "src/a.go",
			"props":         map[string]interface{}{"name": "AllShortestPathsImpl", "docstring": "implements allshortestpaths traversal"},
			"generation_id": "gen-1",
		},
	}

	_, err = exec.Execute(ctx, strings.TrimSpace(rowScopedEntityUpsertCypher), map[string]interface{}{
		"rows": rows,
	})
	require.NoError(t, err, "parameterized cypher must handle any row property value safely")

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch,
		"chain-batch must engage for rows containing 'remove ' / 'shortestpath' / 'allshortestpaths' in property values — "+
			"parameters are bound separately from cypher text, no parser confusion possible")
	require.True(t, trace.MergeSchemaLookupUsed,
		"both unique constraints present; routing should remain schema-backed even with trigger substrings in row data")
	require.False(t, trace.MergeScanFallbackUsed,
		"unique constraints satisfy lookups regardless of property content")

	// Verify the three Function nodes exist with their properties intact —
	// confirming parameter substitution preserved the literal strings.
	for _, expected := range []struct {
		uid     string
		name    string
		docFrag string
	}{
		{"function:src/a.go:RemoveContainer", "RemoveContainer", "remove the container"},
		{"function:src/a.go:findShortestPath", "findShortestPath", "shortestpath between"},
		{"function:src/a.go:AllShortestPathsImpl", "AllShortestPathsImpl", "allshortestpaths traversal"},
	} {
		result, err := exec.Execute(ctx, "MATCH (n:Function {uid: $uid}) RETURN n.name AS name, n.docstring AS doc",
			map[string]interface{}{"uid": expected.uid})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "function %s must exist after upsert", expected.uid)
		require.Equal(t, expected.name, result.Rows[0][0], "name property preserved verbatim through parameterization")
		docstring, _ := result.Rows[0][1].(string)
		require.Contains(t, docstring, expected.docFrag,
			"docstring containing trigger substring preserved verbatim — parameter handling is safe")
	}
}
