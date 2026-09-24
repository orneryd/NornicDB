package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingEdgeLookupEngine records relationship existence probes while
// delegating all storage behavior to the wrapped engine.
type countingEdgeLookupEngine struct {
	storage.Engine
	edgeBetweenCalls int64
}

// GetEdgeBetween counts committed relationship lookups so hot-path tests can
// distinguish true batch-local reuse from storage-level fanout scans.
func (e *countingEdgeLookupEngine) GetEdgeBetween(startID, endID storage.NodeID, edgeType string) *storage.Edge {
	atomic.AddInt64(&e.edgeBetweenCalls, 1)
	return e.Engine.GetEdgeBetween(startID, endID, edgeType)
}

func TestUnwindCollectDistinctProjection_UsesHelperRoute(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	rows := []map[string]interface{}{
		{"textKey128": "k1"},
		{"textKey128": "k2"},
		{"textKey128": "k1"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
WITH collect(DISTINCT row.textKey128) AS keys
RETURN keys
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"keys"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	values, ok := res.Rows[0][0].([]interface{})
	require.True(t, ok)
	require.Len(t, values, 2)
	require.Equal(t, "k1", values[0])
	require.Equal(t, "k2", values[1])
}

func TestParseUnwindCollectDistinctProjection(t *testing.T) {
	exec := &StorageExecutor{}
	plan, ok := exec.parseUnwindCollectDistinctProjection("WITH collect(DISTINCT row.textKey128) AS keys RETURN keys")
	require.True(t, ok)
	require.Equal(t, "row", plan.srcVar)
	require.Equal(t, "textKey128", plan.prop)
	require.Equal(t, "keys", plan.alias)

	_, ok = exec.parseUnwindCollectDistinctProjection("WITH collect(DISTINCT row.textKey128) AS keys RETURN other")
	require.False(t, ok)
}

// TestParseUnwindMergeChainPattern_NamedRelationshipSet verifies that the
// generalized UNWIND MERGE parser keeps relationship variables and assignments.
func TestParseUnwindMergeChainPattern_NamedRelationshipSet(t *testing.T) {
	plan := parseUnwindMergeChainPattern(`
MATCH (f:File {path: $file_path})
MERGE (n:Function {uid: row.entity_id})
SET n += row.props
MERGE (f)-[rel:CONTAINS]->(n)
SET rel.evidence_source = 'projector/canonical',
    rel.generation_id = row.generation_id
`)

	require.True(t, plan.supported)
	require.Len(t, plan.steps, 3)

	lookup := plan.steps[0].lookup
	require.NotNil(t, lookup)
	require.Equal(t, "f", lookup.varName)
	require.Equal(t, []string{"File"}, lookup.labels)
	require.Equal(t, "path", lookup.matchAssignments[0].prop)
	require.Equal(t, "$file_path", lookup.matchAssignments[0].expr)

	node := plan.steps[1].node
	require.NotNil(t, node)
	require.Equal(t, "n", node.mergeVar)
	require.Equal(t, []string{"Function"}, node.labels)
	require.Len(t, node.setAssignments, 1)
	require.True(t, node.setAssignments[0].mergeMap)
	require.Equal(t, "row.props", node.setAssignments[0].expr)

	rel := plan.steps[2].relationship
	require.NotNil(t, rel)
	require.Equal(t, "f", rel.fromVar)
	require.Equal(t, "n", rel.toVar)
	require.Equal(t, "rel", rel.relVar)
	require.Equal(t, "CONTAINS", rel.relType)
	require.Len(t, rel.setAssignments, 2)
	require.Equal(t, "evidence_source", rel.setAssignments[0].prop)
	require.Equal(t, "'projector/canonical'", rel.setAssignments[0].expr)
	require.Equal(t, "generation_id", rel.setAssignments[1].prop)
	require.Equal(t, "row.generation_id", rel.setAssignments[1].expr)
}

func TestUnwindMergeBatch_HopUpsertShape(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	hops := make([]map[string]interface{}, 0, 72)
	for row := 0; row < 12; row++ {
		for depth := 1; depth <= 6; depth++ {
			hops = append(hops, map[string]interface{}{
				"hopId": fmt.Sprintf("benchhop-%03d:%d", row, depth),
				"runID": "bench-deep-hop-v1",
			})
		}
	}

	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": hops})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(72), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 72)
	for _, n := range nodes {
		require.Equal(t, "bench-deep-hop-v1", n.Properties["benchmarkRun"])
	}
}

func TestUnwindMergeBatch_MatchMergeSetMapPropertiesUsesHotPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CREATE CONSTRAINT annotation_uid_unique IF NOT EXISTS FOR (n:Annotation) REQUIRE n.uid IS UNIQUE
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MERGE (f:File {path: $path})
SET f.repo_id = $repo_id
`, map[string]interface{}{"path": "src/handler.go", "repo_id": "repo-1"})
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{
			"file_path": "src/handler.go",
			"entity_id": "annotation-1",
			"properties": map[string]interface{}{
				"id":              "annotation-1",
				"name":            "Route",
				"path":            "src/handler.go",
				"line_number":     int64(12),
				"semantic_kind":   "Annotation",
				"evidence_source": "parser/semantic-entities",
			},
		},
		{
			"file_path": "src/handler.go",
			"entity_id": "annotation-2",
			"properties": map[string]interface{}{
				"id":              "annotation-2",
				"name":            "Auth",
				"path":            "src/handler.go",
				"line_number":     int64(18),
				"semantic_kind":   "Annotation",
				"evidence_source": "parser/semantic-entities",
			},
		},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (f:File {path: row.file_path})
MERGE (n:Annotation {uid: row.entity_id})
SET n += row.properties
MERGE (f)-[:CONTAINS]->(n)
RETURN count(n) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected generalized unwind merge chain hot path")

	nodes, err := store.GetNodesByLabel("Annotation")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, node := range nodes {
		require.Equal(t, "parser/semantic-entities", node.Properties["evidence_source"])
		require.NotEmpty(t, node.Properties["semantic_kind"])
	}

	contains, err := exec.Execute(ctx, `
MATCH (f:File)-[:CONTAINS]->(n:Annotation)
RETURN f.path, n.uid
ORDER BY n.uid
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"src/handler.go", "annotation-1"},
		{"src/handler.go", "annotation-2"},
	}, contains.Rows)
}

// TestUnwindMergeBatch_SkipsRelationshipExistenceLookupForBatchCreatedEndpoint
// verifies that a batch-created relationship target avoids committed-store
// existence probes while duplicate rows still reuse the batch-created edges.
func TestUnwindMergeBatch_SkipsRelationshipExistenceLookupForBatchCreatedEndpoint(t *testing.T) {
	base := newTestMemoryEngine(t)
	namespaced := storage.NewNamespacedEngine(base, "test")
	store := &countingEdgeLookupEngine{Engine: namespaced}
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, query := range []string{
		`CREATE CONSTRAINT repo_id_unique IF NOT EXISTS FOR (r:Repository) REQUIRE r.id IS UNIQUE`,
		`CREATE CONSTRAINT dir_path_unique IF NOT EXISTS FOR (d:Directory) REQUIRE d.path IS UNIQUE`,
		`CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE`,
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, `
MERGE (r:Repository {id: $repo_id})
MERGE (d:Directory {path: $dir_path})
`, map[string]interface{}{"repo_id": "repo-1", "dir_path": "src"})
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{"path": "src/a.go", "name": "a.go", "relative_path": "src/a.go", "language": "go", "repo_id": "repo-1", "scope_id": "scope-1", "generation_id": "gen-1", "dir_path": "src"},
		{"path": "src/b.go", "name": "b.go", "relative_path": "src/b.go", "language": "go", "repo_id": "repo-1", "scope_id": "scope-1", "generation_id": "gen-1", "dir_path": "src"},
		{"path": "src/a.go", "name": "a.go", "relative_path": "src/a.go", "language": "go", "repo_id": "repo-1", "scope_id": "scope-1", "generation_id": "gen-1", "dir_path": "src"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (f:File {path: row.path})
SET f.name = row.name, f.relative_path = row.relative_path,
    f.language = row.language, f.lang = row.language,
    f.repo_id = row.repo_id,
    f.scope_id = row.scope_id, f.generation_id = row.generation_id,
    f.evidence_source = 'projector/canonical'
WITH f, row
MATCH (r:Repository {id: row.repo_id})
MERGE (r)-[repoRel:REPO_CONTAINS]->(f)
SET repoRel.evidence_source = 'projector/canonical',
    repoRel.generation_id = row.generation_id
WITH f, row
MATCH (d:Directory {path: row.dir_path})
MERGE (d)-[dirRel:CONTAINS]->(f)
SET dirRel.evidence_source = 'projector/canonical',
    dirRel.generation_id = row.generation_id
RETURN count(f) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, int64(3), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected generalized unwind merge chain hot path")
	require.Zero(t, atomic.LoadInt64(&store.edgeBetweenCalls), "batch-created File endpoints should not scan existing relationship fanout")

	edges, err := store.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 4, "duplicate file row must reuse batch-created relationships")
}

func TestUnwindMergeBatch_MatchMergeCountReflectsFilteredRows(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MERGE (f:File {path: $path})
`, map[string]interface{}{"path": "src/handler.go"})
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{"file_path": "src/handler.go", "entity_id": "annotation-1"},
		{"file_path": "missing.go", "entity_id": "annotation-2"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (f:File {path: row.file_path})
MERGE (n:Annotation {uid: row.entity_id})
MERGE (f)-[:CONTAINS]->(n)
RETURN count(n) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected generalized unwind merge chain hot path")

	nodes, err := store.GetNodesByLabel("Annotation")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "annotation-1", nodes[0].Properties["uid"])
}

// TestUnwindMergeBatch_NamedRelationshipSetUsesHotPath protects the PCG
// canonical edge-write shape from falling back to generic per-row execution.
func TestUnwindMergeBatch_NamedRelationshipSetUsesHotPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE CONSTRAINT file_path_unique IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MERGE (f:File {path: $path})
`, map[string]interface{}{"path": "src/page.ts"})
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{
			"entity_id":     "function-1",
			"generation_id": "generation-1",
			"props": map[string]interface{}{
				"uid":    "function-1",
				"name":   "removeMenuItem",
				"source": "function removeMenuItem() { return 'REMOVE n.flag'; }",
			},
		},
		{
			"entity_id":     "function-2",
			"generation_id": "generation-1",
			"props": map[string]interface{}{
				"uid":    "function-2",
				"name":   "archiveDocument",
				"source": "function archiveDocument() { return 'ok'; }",
			},
		},
	}

	_, err = exec.Execute(ctx, `
UNWIND $rows AS row
MATCH (f:File {path: $file_path})
MERGE (n:Function {uid: row.entity_id})
SET n += row.props
MERGE (f)-[rel:CONTAINS]->(n)
SET rel.evidence_source = 'projector/canonical',
    rel.generation_id = row.generation_id
`, map[string]interface{}{"file_path": "src/page.ts", "rows": rows})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch, "expected named relationship SET shape to use generalized unwind merge chain hot path")

	contains, err := exec.Execute(ctx, `
MATCH (f:File)-[rel:CONTAINS]->(n:Function)
RETURN n.uid, rel.evidence_source, rel.generation_id
ORDER BY n.uid
`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"function-1", "projector/canonical", "generation-1"},
		{"function-2", "projector/canonical", "generation-1"},
	}, contains.Rows)
}

func TestUnwindOptionalMatchMutationFallsBackPerRow(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
MERGE (f:File {path: $path})
`, map[string]interface{}{"path": "src/handler.go"})
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{"file_path": "src/handler.go", "entity_id": "annotation-1"},
		{"file_path": "missing.go", "entity_id": "annotation-2"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
OPTIONAL MATCH (f:File {path: row.file_path})-[:CONTAINS]->(old)
MERGE (n:Annotation {uid: row.entity_id})
RETURN count(n) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Equal(t, [][]interface{}{{int64(2)}}, res.Rows)

	nodes, err := store.GetNodesByLabel("Annotation")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	seen := make(map[interface{}]bool, len(nodes))
	for _, node := range nodes {
		seen[node.Properties["uid"]] = true
	}
	require.True(t, seen["annotation-1"])
	require.True(t, seen["annotation-2"])
}

func TestUnwindMergeBatch_HopUpsertUpdatesExisting(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	first := []map[string]interface{}{
		{"hopId": "benchhop-000:1", "runID": "v1"},
		{"hopId": "benchhop-000:2", "runID": "v1"},
	}
	_, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": first})
	require.NoError(t, err)

	second := []map[string]interface{}{
		{"hopId": "benchhop-000:1", "runID": "v2"},
		{"hopId": "benchhop-000:2", "runID": "v2"},
	}
	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": second})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		require.Equal(t, "v2", n.Properties["benchmarkRun"])
	}
}

func TestUnwindMergeBatch_HopUpsertDuplicateKeys_LastRowWinsAndCountPreserved(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Ensure index-aware path can be used for existing lookups.
	require.NoError(t, store.GetSchema().AddPropertyIndex("idx_benchhop_hopid", "BenchmarkHop", []string{"hopId"}))

	rows := []map[string]interface{}{
		{"hopId": "benchhop-dupe:1", "runID": "v1"},
		{"hopId": "benchhop-dupe:2", "runID": "v1"},
		{"hopId": "benchhop-dupe:1", "runID": "v2"}, // duplicate key, should win
	}

	res, err := exec.Execute(ctx, `
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
RETURN count(h) AS prepared
`, map[string]interface{}{"hops": rows})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	// Cypher semantics: count(h) in this shape counts input rows, including duplicates.
	require.Equal(t, int64(3), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("BenchmarkHop")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		if n.Properties["hopId"] == "benchhop-dupe:1" {
			require.Equal(t, "v2", n.Properties["benchmarkRun"])
		}
	}
}

func TestUnwindMergeBatch_MultiPropertyMerge_UsesHotPath(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_translated_key_lang", "TranslatedText", []string{"translationId", "language"}))

	rows := []map[string]interface{}{
		{"translationId": "src-1", "language": "es", "translatedText": "hola"},
		{"translationId": "src-2", "language": "fr", "translatedText": "bonjour"},
		{"translationId": "src-1", "language": "es", "translatedText": "hola-2"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (t:TranslatedText {translationId: row.translationId, language: row.language})
ON CREATE SET t.translatedText = row.translatedText
ON MATCH SET t.translatedText = row.translatedText
RETURN count(t) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("TranslatedText")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	for _, n := range nodes {
		if n.Properties["translationId"] == "src-1" && n.Properties["language"] == "es" {
			require.Equal(t, "hola-2", n.Properties["translatedText"])
		}
	}
}

func TestUnwindMergeBatch_MultiPropertyMerge_DistinctTypesDoNotCollapse(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_type_safe_translation_key_lang", "TranslatedText", []string{"translationId", "language"}))

	rows := []map[string]interface{}{
		{"translationId": 1, "language": "es", "translatedText": "numeric-one"},
		{"translationId": "1", "language": "es", "translatedText": "string-one"},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (t:TranslatedText {translationId: row.translationId, language: row.language})
ON CREATE SET t.translatedText = row.translatedText
ON MATCH SET t.translatedText = row.translatedText
RETURN count(t) AS prepared
`, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, []string{"prepared"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(2), toInt64ForTest(t, res.Rows[0][0]))
	require.True(t, exec.LastHotPathTrace().UnwindSimpleMergeBatch, "expected unwind simple merge batch hot path")

	nodes, err := store.GetNodesByLabel("TranslatedText")
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	seenNumeric := false
	seenString := false
	for _, n := range nodes {
		if n.Properties["language"] != "es" {
			continue
		}
		switch v := n.Properties["translationId"].(type) {
		case int:
			if v == 1 {
				seenNumeric = true
				require.Equal(t, "numeric-one", n.Properties["translatedText"])
			}
		case int64:
			if v == 1 {
				seenNumeric = true
				require.Equal(t, "numeric-one", n.Properties["translatedText"])
			}
		case string:
			if v == "1" {
				seenString = true
				require.Equal(t, "string-one", n.Properties["translatedText"])
			}
		}
	}
	require.True(t, seenNumeric, "expected numeric translationId row to remain distinct")
	require.True(t, seenString, "expected string translationId row to remain distinct")
}

func TestUnwindMergeBatch_MultiPropertyMerge_NestedMapValuesAreRejected(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_nested_map_translation_key_lang", "TranslatedText", []string{"translationId", "language"}))

	rows := []map[string]interface{}{
		{
			"translationId":  map[string]interface{}{"id": 1, "meta": map[string]interface{}{"kind": "n"}},
			"language":       "es",
			"translatedText": "nested-numeric",
		},
		{
			"translationId":  map[string]interface{}{"id": "1", "meta": map[string]interface{}{"kind": "n"}},
			"language":       "es",
			"translatedText": "nested-string",
		},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (t:TranslatedText {translationId: row.translationId, language: row.language})
ON CREATE SET t.translatedText = row.translatedText
ON MATCH SET t.translatedText = row.translatedText
RETURN count(t) AS prepared
`, map[string]interface{}{"rows": rows})
	// A map, or a list holding a map or null, is not a property value
	// (#583): the batch MERGE fails and writes nothing.
	_ = res
	require.Error(t, err)
	require.Contains(t, err.Error(), "TypeError")

	nodes, err := store.GetNodesByLabel("TranslatedText")
	require.NoError(t, err)
	require.Len(t, nodes, 0)
}

func TestUnwindMergeBatch_MultiPropertyMerge_NestedSliceValuesAreRejected(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.NoError(t, store.GetSchema().AddCompositeIndex("idx_nested_slice_translation_key_lang", "TranslatedText", []string{"translationId", "language"}))

	rows := []map[string]interface{}{
		{
			"translationId":  []interface{}{1, "alpha", nil, map[string]interface{}{"flag": true}},
			"language":       "es",
			"translatedText": "slice-numeric",
		},
		{
			"translationId":  []interface{}{"1", "alpha", nil, map[string]interface{}{"flag": true}},
			"language":       "es",
			"translatedText": "slice-string",
		},
	}

	res, err := exec.Execute(ctx, `
UNWIND $rows AS row
MERGE (t:TranslatedText {translationId: row.translationId, language: row.language})
ON CREATE SET t.translatedText = row.translatedText
ON MATCH SET t.translatedText = row.translatedText
RETURN count(t) AS prepared
`, map[string]interface{}{"rows": rows})
	// A map, or a list holding a map or null, is not a property value
	// (#583): the batch MERGE fails and writes nothing.
	_ = res
	require.Error(t, err)
	require.Contains(t, err.Error(), "TypeError")

	nodes, err := store.GetNodesByLabel("TranslatedText")
	require.NoError(t, err)
	require.Len(t, nodes, 0)
}

func TestGenericMerge_MultiPropertyLookup_UsesCompositeSchemaPath(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}

	_, err := eng.CreateNode(&storage.Node{
		ID:     "nornic:translated-1",
		Labels: []string{"TranslatedText"},
		Properties: map[string]interface{}{
			"translationId":  "src-1",
			"language":       "es",
			"translatedText": "hola",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(eng)
	_, err = exec.Execute(context.Background(), "CREATE INDEX idx_tt_translation_id FOR (n:TranslatedText) ON (n.translationId)", nil)
	require.NoError(t, err)
	require.NoError(t, eng.GetSchema().AddCompositeIndex("idx_tt_translation_lang", "TranslatedText", []string{"translationId", "language"}))
	eng.forbidScan = true

	res, err := exec.Execute(context.Background(), `
MERGE (t:TranslatedText {translationId: 'src-1', language: 'es'})
ON MATCH SET t.translatedText = 'hola-2'
RETURN t.translatedText AS translatedText
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"translatedText"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "hola-2", res.Rows[0][0])
}
