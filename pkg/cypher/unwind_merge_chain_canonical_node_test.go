package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These tests pin the engagement contract of executeUnwindMergeChainBatch
// for canonical entity-write shapes that a typed Neo4j-canonical Cypher
// client emits when projecting code-intelligence facts into the graph. A
// CPU profile of an UNWIND-MERGE-heavy projection workload showed
// executeCompoundMatchMerge dominating the canonical-write phase, with
// findMergeNode at only 0.66% cumulative — implying the chain-batch fast
// path was bailing on some production shape and every row was going
// through the per-row generic interpreter. The tests in this file walk
// the space of shapes that such a client emits and identify which
// engage cleanly, which engage but route lookup work to label scans,
// and which bail entirely.

// matchFirstAnnotationUpsertCypher: the natural shape a code-intelligence
// projector emits when annotation-style entities live inside a known file —
// MATCH the file first, then MERGE the entity, then MERGE the containment
// edge.
const matchFirstAnnotationUpsertCypher = `UNWIND $rows AS row
MATCH (f:File {path: row.file_path})
MERGE (n:Annotation {uid: row.entity_id})
SET n.id = row.entity_id,
    n.name = row.entity_name,
    n.path = row.file_path,
    n.relative_path = row.relative_path,
    n.line_number = row.start_line,
    n.start_line = row.start_line,
    n.end_line = row.end_line,
    n.repo_id = row.repo_id,
    n.language = row.language,
    n.lang = row.language,
    n.kind = row.kind,
    n.target_kind = row.target_kind,
    n.semantic_kind = coalesce(row.semantic_kind, row.entity_type),
    n.evidence_source = row.evidence_source
MERGE (f)-[:CONTAINS]->(n)`

// TestUnwindMergeChainBatch_MatchFirstAnnotation_FastPathEngages asserts
// the chain-batch fast path engages on the MATCH-first annotation upsert
// shape. If this fails, parseUnwindMergeChainPattern is rejecting the
// shape (Bail Point A at pkg/cypher/clauses.go:1813) and the per-row
// generic interpreter handles every entity write — exactly the failure
// mode the canonical-write CPU profile suggested.
func TestUnwindMergeChainBatch_MatchFirstAnnotation_FastPathEngages(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE",
		"CREATE CONSTRAINT annotation_uid_unique IF NOT EXISTS FOR (n:Annotation) REQUIRE n.uid IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, "CREATE (:File {path: 'src/a/main.go'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(matchFirstAnnotationUpsertCypher), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":       "annotation:src/a/main.go:42",
				"entity_name":     "deprecated",
				"file_path":       "src/a/main.go",
				"relative_path":   "a/main.go",
				"start_line":      int64(42),
				"end_line":        int64(42),
				"repo_id":         "repo-1",
				"language":        "go",
				"kind":            "decorator",
				"target_kind":     "function",
				"semantic_kind":   "annotation",
				"entity_type":     "annotation",
				"evidence_source": "parser/semantic-entities",
			},
		},
	})
	require.NoError(t, err)

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch,
		"MATCH-first annotation upsert MUST engage executeUnwindMergeChainBatch; "+
			"if false, parseUnwindMergeChainPattern is rejecting the shape at clauses.go:1813")
	require.True(t, trace.MergeSchemaLookupUsed,
		"fast path must use the schema's unique-constraint lookup, not a label scan")
	require.False(t, trace.MergeScanFallbackUsed,
		"both File.path and Annotation.uid have unique constraints; no scan fallback is justified")
}

// mergeFirstAnnotationUpsertCypher: the same logical upsert, reordered so
// the entity MERGE comes before the file MATCH. A client that rewrites
// upserts into MERGE-first form to keep entity creation independent of
// file-presence will emit this shape. The test asserts the chain-batch
// fast path engages on the rewritten shape too. If only one of the two
// forms engages, the bail is shape-specific and a rewriter does or does
// not effectively route the canonical write onto the hot path.
const mergeFirstAnnotationUpsertCypher = `UNWIND $rows AS row
MERGE (n:Annotation {uid: row.entity_id})
SET n.id = row.entity_id,
    n.name = row.entity_name,
    n.path = row.file_path,
    n.relative_path = row.relative_path,
    n.line_number = row.start_line,
    n.start_line = row.start_line,
    n.end_line = row.end_line,
    n.repo_id = row.repo_id,
    n.language = row.language,
    n.lang = row.language,
    n.kind = row.kind,
    n.target_kind = row.target_kind,
    n.semantic_kind = coalesce(row.semantic_kind, row.entity_type),
    n.evidence_source = row.evidence_source
MATCH (f:File {path: row.file_path})
MERGE (f)-[:CONTAINS]->(n)`

func TestUnwindMergeChainBatch_MergeFirstAnnotation_FastPathEngages(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, stmt := range []string{
		"CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE",
		"CREATE CONSTRAINT annotation_uid_unique IF NOT EXISTS FOR (n:Annotation) REQUIRE n.uid IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, "CREATE (:File {path: 'src/a/main.go'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, strings.TrimSpace(mergeFirstAnnotationUpsertCypher), map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"entity_id":       "annotation:src/a/main.go:42",
				"entity_name":     "deprecated",
				"file_path":       "src/a/main.go",
				"relative_path":   "a/main.go",
				"start_line":      int64(42),
				"end_line":        int64(42),
				"repo_id":         "repo-1",
				"language":        "go",
				"kind":            "decorator",
				"target_kind":     "function",
				"semantic_kind":   "annotation",
				"entity_type":     "annotation",
				"evidence_source": "parser/semantic-entities",
			},
		},
	})
	require.NoError(t, err)

	trace := exec.LastHotPathTrace()
	require.True(t, trace.UnwindMergeChainBatch,
		"MERGE-first reordered upsert MUST engage executeUnwindMergeChainBatch")
	require.True(t, trace.MergeSchemaLookupUsed,
		"fast path must use the schema's unique-constraint lookup, not a label scan")
	require.False(t, trace.MergeScanFallbackUsed,
		"both File.path and Annotation.uid have unique constraints; no scan fallback is justified")
}
