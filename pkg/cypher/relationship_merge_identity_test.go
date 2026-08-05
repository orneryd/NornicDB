package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type relationshipLookupErrorEngine struct {
	storage.Engine
	err error
}

func (e *relationshipLookupErrorEngine) GetEdgesBetween(
	storage.NodeID,
	storage.NodeID,
) ([]*storage.Edge, error) {
	return nil, e.err
}

const scopedRelationshipMergeQuery = `UNWIND $rows AS row
MATCH (source:ContainerImage {digest: row.digest})
MATCH (target:Repository {id: row.repository_id})
MERGE (source)-[rel:BUILT_FROM {
  scope_id: row.scope_id,
  evidence_source: row.evidence_source
}]->(target)
SET rel.generation_id = row.generation_id,
    rel.evidence_kinds = row.evidence_kinds`

func TestUnwindRelationshipMergeIdentityIncludesPatternProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (:ContainerImage {digest: 'sha256:abc'}),
       (:Repository {id: 'repository:acme/app'})`, nil)
	require.NoError(t, err)

	rows := []map[string]interface{}{
		{
			"digest":          "sha256:abc",
			"repository_id":   "repository:acme/app",
			"scope_id":        "scope-a",
			"evidence_source": "reducer/source-a",
			"generation_id":   "generation-a-1",
			"evidence_kinds":  []string{"A"},
		},
		{
			"digest":          "sha256:abc",
			"repository_id":   "repository:acme/app",
			"scope_id":        "scope-b",
			"evidence_source": "reducer/source-b",
			"generation_id":   "generation-b-1",
			"evidence_kinds":  []string{"B"},
		},
		{
			"digest":          "sha256:abc",
			"repository_id":   "repository:acme/app",
			"scope_id":        "scope-a",
			"evidence_source": "reducer/source-a",
			"generation_id":   "generation-a-1",
			"evidence_kinds":  []string{"A"},
		},
	}
	_, err = exec.Execute(ctx, scopedRelationshipMergeQuery, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)

	assertScopedRelationshipRows(t, exec, ctx, [][]interface{}{
		{"scope-a", "reducer/source-a", "generation-a-1"},
		{"scope-b", "reducer/source-b", "generation-b-1"},
	})

	rows[0]["generation_id"] = "generation-a-2"
	_, err = exec.Execute(ctx, scopedRelationshipMergeQuery, map[string]interface{}{"rows": rows[:1]})
	require.NoError(t, err)
	assertScopedRelationshipRows(t, exec, ctx, [][]interface{}{
		{"scope-a", "reducer/source-a", "generation-a-2"},
		{"scope-b", "reducer/source-b", "generation-b-1"},
	})
}

func TestRelationshipMergeEdgeIDUsesPropertyIdentityOnlyWhenPresent(t *testing.T) {
	exec := &StorageExecutor{}
	first := exec.newRelationshipMergeEdgeID("source", "target", "ASSERTS", map[string]interface{}{
		"scope_id":        "scope-a",
		"evidence_source": "source-a",
	})
	reordered := exec.newRelationshipMergeEdgeID("source", "target", "ASSERTS", map[string]interface{}{
		"evidence_source": "source-a",
		"scope_id":        "scope-a",
	})
	different := exec.newRelationshipMergeEdgeID("source", "target", "ASSERTS", map[string]interface{}{
		"scope_id":        "scope-b",
		"evidence_source": "source-a",
	})
	require.Equal(t, first, reordered)
	require.NotEqual(t, first, different)
	require.True(t, strings.HasPrefix(string(first), "merge-"))

	bareFirst := exec.newRelationshipMergeEdgeID("source", "target", "ASSERTS", nil)
	bareSecond := exec.newRelationshipMergeEdgeID("source", "target", "ASSERTS", nil)
	require.NotEqual(t, bareFirst, bareSecond)
	require.False(t, strings.HasPrefix(string(bareFirst), "merge-"))
}

func TestRelationshipMatchesMergePatternRequiresTypeAndEveryProperty(t *testing.T) {
	edge := &storage.Edge{
		Type: "ASSERTS",
		Properties: map[string]interface{}{
			"scope_id":        "scope-a",
			"evidence_source": "source-a",
			"mutable":         "extra",
		},
	}
	require.False(t, relationshipMatchesMergePattern(nil, "ASSERTS", nil))
	require.False(t, relationshipMatchesMergePattern(edge, "DIFFERENT", nil))
	require.True(t, relationshipMatchesMergePattern(edge, "ASSERTS", nil))
	require.False(t, relationshipMatchesMergePattern(edge, "ASSERTS", map[string]interface{}{"missing": "value"}))
	require.False(t, relationshipMatchesMergePattern(edge, "ASSERTS", map[string]interface{}{"scope_id": "scope-b"}))
	require.True(t, relationshipMatchesMergePattern(edge, "ASSERTS", map[string]interface{}{
		"scope_id":        "scope-a",
		"evidence_source": "source-a",
	}))
	require.True(t, relationshipMergeValuesEqual([]interface{}{int64(1)}, []interface{}{int64(1)}))
	require.True(t, relationshipMergeValuesEqual(int64(1), int(1)))
	require.False(t, relationshipMergeValuesEqual(int64(1), int(2)))
}

func TestFindRelationshipForMergeSupportsBareLookupAndPropagatesScanError(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, err := store.CreateNode(&storage.Node{ID: "source", Labels: []string{"Source"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "target", Labels: []string{"Target"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "edge",
		Type:      "ASSERTS",
		StartNode: "source",
		EndNode:   "target",
	}))

	found, err := findRelationshipForMerge(store, "source", "target", "ASSERTS", nil)
	require.NoError(t, err)
	require.NotNil(t, found)
	require.Equal(t, storage.EdgeID("edge"), found.ID)

	wantErr := errors.New("relationship scan failed")
	errorStore := &relationshipLookupErrorEngine{Engine: store, err: wantErr}
	found, err = findRelationshipForMerge(
		errorStore,
		"source",
		"target",
		"ASSERTS",
		map[string]interface{}{"scope_id": "scope-a"},
	)
	require.Nil(t, found)
	require.ErrorIs(t, err, wantErr)
}

func TestFindRelationshipForMergeUsesDeterministicPointLookupBeforePairScan(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, err := store.CreateNode(&storage.Node{ID: "source", Labels: []string{"Source"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "target", Labels: []string{"Target"}})
	require.NoError(t, err)
	matchProps := map[string]interface{}{"scope_id": "scope-a"}
	edge := &storage.Edge{
		ID:         deterministicRelationshipMergeEdgeID("source", "target", "ASSERTS", matchProps, 0),
		Type:       "ASSERTS",
		StartNode:  "source",
		EndNode:    "target",
		Properties: matchProps,
	}
	require.NoError(t, store.CreateEdge(edge))

	noScanStore := &relationshipLookupErrorEngine{Engine: store, err: errors.New("pair scan must not run")}
	found, err := findRelationshipForMerge(noScanStore, "source", "target", "ASSERTS", matchProps)
	require.NoError(t, err)
	require.Equal(t, edge.ID, found.ID)
}

func TestRelationshipMergeIdentityIncludesPatternProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Source {id: 'source'}), (:Target {id: 'target'})`, nil)
	require.NoError(t, err)
	for _, scopeID := range []string{"scope-a", "scope-b", "scope-a"} {
		_, err = exec.Execute(ctx, `
MATCH (source:Source {id: 'source'})
MATCH (target:Target {id: 'target'})
MERGE (source)-[rel:ASSERTS {scope_id: $scope_id}]->(target)
SET rel.last_scope = $scope_id`, map[string]interface{}{"scope_id": scopeID})
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, `
MATCH (:Source {id: 'source'})-[rel:ASSERTS]->(:Target {id: 'target'})
RETURN rel.scope_id, rel.last_scope
ORDER BY rel.scope_id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"scope-a", "scope-a"},
		{"scope-b", "scope-b"},
	}, result.Rows)
}

func TestRelationshipMergeSegmentIdentityIncludesPatternProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:A {key: 'a1'}), (:B {key: 'b1'})`, nil)
	require.NoError(t, err)
	aRows, err := exec.Execute(ctx, `MATCH (a:A {key: 'a1'}) RETURN a`, nil)
	require.NoError(t, err)
	bRows, err := exec.Execute(ctx, `MATCH (b:B {key: 'b1'}) RETURN b`, nil)
	require.NoError(t, err)
	a, ok := aRows.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	b, ok := bRows.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	nodeContext := map[string]*storage.Node{"a": a, "b": b}

	for _, scopeID := range []string{"scope-a", "scope-b", "scope-a"} {
		pattern := "(a)-[:ASSERTS {scope_id: '" + scopeID + "'}]->(b)"
		require.NoError(t, exec.executeMergeRelSegment(ctx, pattern, nodeContext))
	}
	assertAssertRelationshipRows(t, exec, ctx, [][]interface{}{
		{"scope-a", nil},
		{"scope-b", nil},
	})
}

func TestRelationshipMergeIdentityDoesNotMatchPartialPattern(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Source {id: 'source'}), (:Target {id: 'target'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
MATCH (source:Source {id: 'source'})
MATCH (target:Target {id: 'target'})
CREATE (source)-[:ASSERTS {mutable: 'existing'}]->(target)`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
MATCH (source:Source {id: 'source'})
MATCH (target:Target {id: 'target'})
MERGE (source)-[rel:ASSERTS {scope_id: 'scope-a', evidence_source: 'source-a'}]->(target)
SET rel.mutable = 'updated'`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
MATCH (:Source {id: 'source'})-[rel:ASSERTS]->(:Target {id: 'target'})
RETURN rel.scope_id, rel.evidence_source, rel.mutable
ORDER BY rel.scope_id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"scope-a", "source-a", "updated"},
		{nil, nil, "existing"},
	}, result.Rows)
}

func TestRelationshipMergeIdentityCanBeRecreatedAfterPatternPropertyMutation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Source {id: 'source'}), (:Target {id: 'target'})`, nil)
	require.NoError(t, err)
	query := `
MATCH (source:Source {id: 'source'})
MATCH (target:Target {id: 'target'})
MERGE (source)-[rel:ASSERTS {scope_id: 'scope-a'}]->(target)
SET rel.scope_id = $stored_scope`
	for _, storedScope := range []string{"retired-1", "retired-2", "scope-a"} {
		_, err = exec.Execute(ctx, query, map[string]interface{}{"stored_scope": storedScope})
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, `
MATCH (:Source {id: 'source'})-[rel:ASSERTS]->(:Target {id: 'target'})
RETURN rel.scope_id
ORDER BY rel.scope_id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"retired-1"}, {"retired-2"}, {"scope-a"}}, result.Rows)
}

func TestUnwindRelationshipMergeIdentityCanBeRecreatedAfterPatternPropertyMutation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:A {key: 'a1'}), (:B {key: 'b1'})`, nil)
	require.NoError(t, err)
	query := `UNWIND $rows AS row
MATCH (a:A {key: 'a1'})
MATCH (b:B {key: 'b1'})
MERGE (a)-[rel:ASSERTS {scope_id: row.match_scope}]->(b)
SET rel.scope_id = row.stored_scope`
	for _, storedScope := range []string{"retired-1", "retired-2", "scope-a"} {
		_, err = exec.Execute(ctx, query, map[string]interface{}{
			"rows": []map[string]interface{}{{
				"match_scope":  "scope-a",
				"stored_scope": storedScope,
			}},
		})
		require.NoError(t, err)
		require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)
	}

	result, err := exec.Execute(ctx, `
MATCH (:A {key: 'a1'})-[rel:ASSERTS]->(:B {key: 'b1'})
RETURN rel.scope_id
ORDER BY rel.scope_id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"retired-1"}, {"retired-2"}, {"scope-a"}}, result.Rows)
}

func TestRelationshipMergeIdentityConcurrentDifferentIdentitiesPreserveBoth(t *testing.T) {
	engine := edgeConflictTestEngine(t)
	firstExec := NewStorageExecutor(engine)
	secondExec := NewStorageExecutor(engine)
	ctx := context.Background()
	seedRelEndpoints(t, firstExec)

	_, err := firstExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = firstExec.Execute(ctx, scopedAssertMergeQuery, scopedAssertMergeParams("scope-a", "source-a"))
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, scopedAssertMergeQuery, scopedAssertMergeParams("scope-b", "source-b"))
	require.NoError(t, err)
	_, err = firstExec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	assertAssertRelationshipRows(t, firstExec, ctx, [][]interface{}{
		{"scope-a", "source-a"},
		{"scope-b", "source-b"},
	})
}

func TestRelationshipMergeIdentityConcurrentSameIdentityConvergesAfterRetry(t *testing.T) {
	engine := edgeConflictTestEngine(t)
	firstExec := NewStorageExecutor(engine)
	secondExec := NewStorageExecutor(engine)
	ctx := context.Background()
	seedRelEndpoints(t, firstExec)

	params := scopedAssertMergeParams("scope-a", "source-a")
	_, err := firstExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = firstExec.Execute(ctx, scopedAssertMergeQuery, params)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, scopedAssertMergeQuery, params)
	require.NoError(t, err)
	_, err = firstExec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, "COMMIT", nil)
	require.ErrorIs(t, err, storage.ErrConflict)

	_, err = secondExec.Execute(ctx, scopedAssertMergeQuery, params)
	require.NoError(t, err)
	assertAssertRelationshipRows(t, firstExec, ctx, [][]interface{}{
		{"scope-a", "source-a"},
	})
}

func TestUnwindRelationshipMergeBatchConcurrentSameIdentityConvergesAfterRetry(t *testing.T) {
	engine := edgeConflictTestEngine(t)
	firstExec := NewStorageExecutor(engine)
	secondExec := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := firstExec.Execute(ctx, `
CREATE (:Service {key: 'svc-a'}),
       (:Topic {key: 'topic-a'}),
       (:Tenant {key: 'tenant-a'})`, nil)
	require.NoError(t, err)
	params := specializedRelationshipMergeParams("edge-a")

	_, err = firstExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = firstExec.Execute(ctx, specializedRelationshipMergeQuery, params)
	require.NoError(t, err)
	require.True(t, firstExec.LastHotPathTrace().UnwindRelationshipMergeBatch)
	_, err = secondExec.Execute(ctx, specializedRelationshipMergeQuery, params)
	require.NoError(t, err)
	require.True(t, secondExec.LastHotPathTrace().UnwindRelationshipMergeBatch)
	_, err = firstExec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, "COMMIT", nil)
	require.ErrorIs(t, err, storage.ErrConflict)

	_, err = secondExec.Execute(ctx, specializedRelationshipMergeQuery, params)
	require.NoError(t, err)
	count := mustCountRows(t, firstExec, ctx, `
MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES]->(:Topic {key: 'topic-a'})
WHERE rel.uuid = 'edge-a'
RETURN count(rel)`, nil)
	require.Equal(t, int64(1), count)
}

const scopedAssertMergeQuery = `UNWIND $rows AS row
MATCH (a:A {key: 'a1'})
MATCH (b:B {key: 'b1'})
MERGE (a)-[:ASSERTS {scope_id: row.scope_id, evidence_source: row.evidence_source}]->(b)`

const specializedRelationshipMergeQuery = `UNWIND $rows AS row
MATCH (source:Service {key: row.source_key})
MATCH (target:Topic {key: row.target_key})
MATCH (tenant:Tenant {key: row.tenant})
MERGE (source)-[rel:PUBLISHES {uuid: row.uuid, tenant: row.tenant}]->(target)
SET rel = row
WITH rel, row CALL db.create.setRelationshipVectorProperty(rel, "embedding", row.embedding)
RETURN row.uuid AS uuid`

func specializedRelationshipMergeParams(uuid string) map[string]interface{} {
	return map[string]interface{}{
		"rows": []map[string]interface{}{{
			"source_key": "svc-a",
			"target_key": "topic-a",
			"tenant":     "tenant-a",
			"uuid":       uuid,
			"embedding":  []float64{1, 0, 0},
		}},
	}
}

func scopedAssertMergeParams(scopeID, evidenceSource string) map[string]interface{} {
	return map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"scope_id":        scopeID,
				"evidence_source": evidenceSource,
			},
		},
	}
}

func assertAssertRelationshipRows(
	t *testing.T,
	exec *StorageExecutor,
	ctx context.Context,
	want [][]interface{},
) {
	t.Helper()
	result, err := exec.Execute(ctx, `
MATCH (:A {key: 'a1'})-[rel:ASSERTS]->(:B {key: 'b1'})
RETURN rel.scope_id, rel.evidence_source
ORDER BY rel.scope_id`, nil)
	require.NoError(t, err)
	require.Equal(t, want, result.Rows)
}

func assertScopedRelationshipRows(
	t *testing.T,
	exec *StorageExecutor,
	ctx context.Context,
	want [][]interface{},
) {
	t.Helper()
	result, err := exec.Execute(ctx, `
MATCH (:ContainerImage {digest: 'sha256:abc'})-[rel:BUILT_FROM]->(:Repository {id: 'repository:acme/app'})
RETURN rel.scope_id, rel.evidence_source, rel.generation_id
ORDER BY rel.scope_id`, nil)
	require.NoError(t, err)
	require.Equal(t, want, result.Rows)
}
