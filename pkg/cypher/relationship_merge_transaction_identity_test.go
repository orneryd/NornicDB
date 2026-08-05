package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipBatchMatchPropertiesIncludesRowAndLiteralIdentity(t *testing.T) {
	merge := relationshipMergeSpec{
		keyProps: []relationshipMergeKeyProp{
			{propName: "scope_id", rowField: "scope"},
			{propName: "source", literal: "projector", literalSet: true},
		},
	}
	got := relationshipBatchMatchProperties(merge, map[string]interface{}{"scope": "scope-a"})
	require.Equal(t, map[string]interface{}{
		"scope_id": "scope-a",
		"source":   "projector",
	}, got)
}

func TestRelationshipBatchEdgeKeysNormalizeNumericWidths(t *testing.T) {
	merge := relationshipMergeSpec{
		relType:      "ASSERTS",
		rowFieldRefs: map[string]string{"ordinal": "ordinal"},
	}
	merge.keyProps = relationshipMergeKeyProps(merge.rowFieldRefs, nil)
	rowKey := relationshipBatchEdgeKeyFromRow(
		"source",
		"target",
		merge,
		map[string]interface{}{"ordinal": int(1)},
	)
	storedKey, ok := relationshipBatchEdgeKeyFromProperties(
		"source",
		"target",
		merge,
		map[string]interface{}{"ordinal": int8(1)},
	)
	require.True(t, ok)
	require.Equal(t, rowKey, storedKey)
	require.Equal(t,
		relationshipBatchEdgeKey("source", "target", "ASSERTS", map[string]interface{}{
			"path": []interface{}{int(1)},
		}),
		relationshipBatchEdgeKey("source", "target", "ASSERTS", map[string]interface{}{
			"path": []interface{}{int8(1)},
		}),
	)
	require.Equal(t,
		relationshipBatchEdgeKey("source", "target", "ASSERTS", map[string]interface{}{
			"path": []int{1, 2},
		}),
		relationshipBatchEdgeKey("source", "target", "ASSERTS", map[string]interface{}{
			"path": []int64{1, 2},
		}),
	)
	require.Equal(t,
		relationshipMergeIdentityKey("source", "target", "ASSERTS", map[string]interface{}{
			"nested": map[string]interface{}{"weights": [2]float32{1, 2}},
		}),
		relationshipMergeIdentityKey("source", "target", "ASSERTS", map[string]interface{}{
			"nested": map[string]interface{}{"weights": []float64{1, 2}},
		}),
	)

	fallbackMerge := relationshipMergeSpec{
		relType:      "ASSERTS",
		rowFieldRefs: map[string]string{"path": "path"},
	}
	fallbackMerge.keyProps = relationshipMergeKeyProps(fallbackMerge.rowFieldRefs, nil)
	fallbackRowKey := relationshipBatchEdgeKeyFromRow(
		"source",
		"target",
		fallbackMerge,
		map[string]interface{}{"path": []interface{}{int(1)}},
	)
	fallbackStoredKey, ok := relationshipBatchEdgeKeyFromProperties(
		"source",
		"target",
		fallbackMerge,
		map[string]interface{}{"path": []interface{}{int8(1)}},
	)
	require.True(t, ok)
	require.Equal(t, fallbackRowKey, fallbackStoredKey)
	_, ok = relationshipBatchEdgeKeyFromProperties(
		"source",
		"target",
		fallbackMerge,
		map[string]interface{}{},
	)
	require.False(t, ok)
}

func TestRelationshipMergeIdentityExplicitTransactionPreservesMutatedIdentity(t *testing.T) {
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
	_, err = exec.Execute(ctx, query, map[string]interface{}{"stored_scope": "retired"})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, query, map[string]interface{}{"stored_scope": "scope-a"})
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
MATCH (:Source {id: 'source'})-[rel:ASSERTS]->(:Target {id: 'target'})
RETURN rel.scope_id
ORDER BY rel.scope_id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"retired"}, {"scope-a"}}, result.Rows)
}

func TestUnwindRelationshipMergeIdentityReevaluatesMutatedIdentityBetweenRows(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:A {key: 'a1'}), (:B {key: 'b1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `UNWIND $rows AS row
MATCH (a:A {key: 'a1'})
MATCH (b:B {key: 'b1'})
MERGE (a)-[rel:ASSERTS {scope_id: row.match_scope}]->(b)
SET rel.scope_id = row.stored_scope`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"match_scope": "scope-a", "stored_scope": "retired-1"},
			{"match_scope": "scope-a", "stored_scope": "retired-2"},
			{"match_scope": "scope-a", "stored_scope": "scope-a"},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)

	result, err := exec.Execute(ctx, `
MATCH (:A {key: 'a1'})-[rel:ASSERTS]->(:B {key: 'b1'})
RETURN rel.scope_id
ORDER BY rel.scope_id`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"retired-1"}, {"retired-2"}, {"scope-a"}}, result.Rows)
}

func TestUnwindRelationshipMergeIdentityNormalizesNumericReplayValues(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	baseStore, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err = exec.Execute(ctx, `CREATE (:A {key: 'a1'}), (:B {key: 'b1'})`, nil)
	require.NoError(t, err)
	query := `UNWIND $rows AS row
MATCH (a:A {key: 'a1'})
MATCH (b:B {key: 'b1'})
MERGE (a)-[rel:ASSERTS {scope_id: row.scope_id}]->(b)
SET rel.last_seen = row.last_seen`
	params := map[string]interface{}{
		"rows": []map[string]interface{}{{"scope_id": int(1), "last_seen": int(2)}},
	}
	_, err = exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)
	require.NoError(t, baseStore.Close())

	reopened, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})
	reopenedExec := NewStorageExecutor(storage.NewNamespacedEngine(reopened, "test"))
	_, err = reopenedExec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.True(t, reopenedExec.LastHotPathTrace().UnwindMergeChainBatch)

	count := mustCountRows(t, reopenedExec, ctx, `
MATCH (:A {key: 'a1'})-[rel:ASSERTS]->(:B {key: 'b1'})
RETURN count(rel)`, nil)
	require.Equal(t, int64(1), count)
}

func TestUnwindRelationshipMergeIdentityNormalizesTypedSliceReplayValues(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	baseStore, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err = exec.Execute(ctx, `CREATE (:A {key: 'a1'}), (:B {key: 'b1'})`, nil)
	require.NoError(t, err)
	query := `UNWIND $rows AS row
MATCH (a:A {key: 'a1'})
MATCH (b:B {key: 'b1'})
MERGE (a)-[rel:ASSERTS {path: row.path}]->(b)
SET rel.last_seen = row.last_seen`
	params := map[string]interface{}{
		"rows": []map[string]interface{}{{"path": []int{1, 2}, "last_seen": int(1)}},
	}
	_, err = exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)
	require.NoError(t, baseStore.Close())

	reopened, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})
	reopenedExec := NewStorageExecutor(storage.NewNamespacedEngine(reopened, "test"))
	_, err = reopenedExec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.True(t, reopenedExec.LastHotPathTrace().UnwindMergeChainBatch)

	count := mustCountRows(t, reopenedExec, ctx, `
MATCH (:A {key: 'a1'})-[rel:ASSERTS]->(:B {key: 'b1'})
RETURN count(rel)`, nil)
	require.Equal(t, int64(1), count)
}

func TestUnwindRelationshipMergeBatchPreservesMutatedPatternIdentity(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	seedSpecializedRelationshipMergeEndpoints(t, exec, ctx)

	query := `UNWIND $rows AS row
MATCH (source:Service {key: row.source_key})
MATCH (target:Topic {key: row.target_key})
MATCH (tenant:Tenant {key: row.tenant})
MERGE (source)-[rel:PUBLISHES {uuid: row.match_uuid, tenant: row.tenant}]->(target)
SET rel = row
WITH rel, row CALL db.create.setRelationshipVectorProperty(rel, "embedding", row.embedding)
RETURN row.match_uuid AS uuid`
	_, err := exec.Execute(ctx, query, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"source_key": "svc-a",
				"target_key": "topic-a",
				"tenant":     "tenant-a",
				"match_uuid": "edge-a",
				"embedding":  []float64{1, 0, 0},
			},
			{
				"source_key": "svc-a",
				"target_key": "topic-a",
				"tenant":     "tenant-a",
				"match_uuid": "edge-a",
				"embedding":  []float64{0, 1, 0},
			},
			{
				"source_key": "svc-a",
				"target_key": "topic-a",
				"tenant":     "tenant-a",
				"match_uuid": "edge-a",
				"embedding":  []float64{0, 0, 1},
			},
		},
	})
	require.NoError(t, err)
	require.False(t, exec.LastHotPathTrace().UnwindRelationshipMergeBatch)

	count := mustCountRows(t, exec, ctx, `
MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES]->(:Topic {key: 'topic-a'})
RETURN count(rel)`, nil)
	require.Equal(t, int64(3), count)
}

func TestUnwindRelationshipMergeBatchNormalizesMixedNumericIdentityWidths(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	seedSpecializedRelationshipMergeEndpoints(t, exec, ctx)

	query := `UNWIND $rows AS row
MATCH (source:Service {key: row.source_key})
MATCH (target:Topic {key: row.target_key})
MATCH (tenant:Tenant {key: row.tenant})
MERGE (source)-[rel:PUBLISHES {ordinal: row.ordinal, tenant: row.tenant}]->(target)
SET rel = row
WITH rel, row CALL db.create.setRelationshipVectorProperty(rel, "embedding", row.embedding)
RETURN row.ordinal AS ordinal`
	_, err := exec.Execute(ctx, query, map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"source_key": "svc-a",
				"target_key": "topic-a",
				"tenant":     "tenant-a",
				"ordinal":    int(1),
				"embedding":  []float64{1, 0},
			},
			{
				"source_key": "svc-a",
				"target_key": "topic-a",
				"tenant":     "tenant-a",
				"ordinal":    int64(1),
				"embedding":  []float64{0, 1},
			},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindRelationshipMergeBatch)

	count := mustCountRows(t, exec, ctx, `
MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES]->(:Topic {key: 'topic-a'})
RETURN count(rel)`, nil)
	require.Equal(t, int64(1), count)
}

func TestUnwindRelationshipMergeBatchRecreatesExternallyMutatedIdentity(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	seedSpecializedRelationshipMergeEndpoints(t, exec, ctx)

	params := specializedRelationshipMergeParams("edge-a")
	for _, storedUUID := range []string{"retired-1", "retired-2"} {
		_, err := exec.Execute(ctx, specializedRelationshipMergeQuery, params)
		require.NoError(t, err)
		require.True(t, exec.LastHotPathTrace().UnwindRelationshipMergeBatch)

		_, err = exec.Execute(ctx, `
MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES {uuid: 'edge-a'}]->(:Topic {key: 'topic-a'})
SET rel.uuid = $stored_uuid`, map[string]interface{}{"stored_uuid": storedUUID})
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, specializedRelationshipMergeQuery, params)
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindRelationshipMergeBatch)

	result, err := exec.Execute(ctx, `
MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES]->(:Topic {key: 'topic-a'})
RETURN rel.uuid
ORDER BY rel.uuid`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"edge-a"}, {"retired-1"}, {"retired-2"}}, result.Rows)
}

func TestUnwindRelationshipMergeBatchConcurrentDifferentIdentitiesPreserveBoth(t *testing.T) {
	engine := edgeConflictTestEngine(t)
	firstExec := NewStorageExecutor(engine)
	secondExec := NewStorageExecutor(engine)
	ctx := context.Background()
	seedSpecializedRelationshipMergeEndpoints(t, firstExec, ctx)

	_, err := firstExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = firstExec.Execute(ctx, specializedRelationshipMergeQuery, specializedRelationshipMergeParams("edge-a"))
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, specializedRelationshipMergeQuery, specializedRelationshipMergeParams("edge-b"))
	require.NoError(t, err)
	_, err = firstExec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	_, err = secondExec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	count := mustCountRows(t, firstExec, ctx, `
MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES]->(:Topic {key: 'topic-a'})
RETURN count(rel)`, nil)
	require.Equal(t, int64(2), count)
}

func seedSpecializedRelationshipMergeEndpoints(
	t *testing.T,
	exec *StorageExecutor,
	ctx context.Context,
) {
	t.Helper()
	_, err := exec.Execute(ctx, `
CREATE (:Service {key: 'svc-a'}),
       (:Topic {key: 'topic-a'}),
       (:Tenant {key: 'tenant-a'})`, nil)
	require.NoError(t, err)
}
