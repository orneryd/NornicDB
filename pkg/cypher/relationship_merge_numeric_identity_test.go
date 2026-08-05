package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipMergeNumericIdentityCanonicalization(t *testing.T) {
	exec := &StorageExecutor{}
	edgeID := func(value interface{}) storage.EdgeID {
		return exec.newRelationshipMergeEdgeID("source", "target", "ASSERTS", map[string]interface{}{"weight": value})
	}

	require.Equal(t, edgeID(int(1)), edgeID(int64(1)))
	require.Equal(t, edgeID(int64(1)), edgeID(float64(1)))
	require.Equal(t, edgeID(float32(1)), edgeID(float64(1)))
	require.Equal(t, edgeID(int64(0)), edgeID(math.Copysign(0, -1)))
	require.Equal(t, edgeID([]int64{1, 2}), edgeID([]float64{1, 2}))
	require.Equal(t,
		edgeID(map[string]interface{}{"nested": []interface{}{int64(1)}}),
		edgeID(map[string]interface{}{"nested": []interface{}{float64(1)}}),
	)
	require.NotEqual(t, edgeID(int64(1)), edgeID(float64(1.5)))
	require.NotEqual(t, edgeID(int64(1<<53)), edgeID(int64(1<<53)+1))
	require.Equal(t, edgeID(int64(math.MinInt64)), edgeID(float64(math.MinInt64)))
	require.NotEqual(t, edgeID(int64(math.MaxInt64)), edgeID(float64(math.MaxInt64)))
	require.Equal(t, edgeID(math.Inf(1)), edgeID(math.Inf(1)))
	require.Equal(t, edgeID(math.Inf(-1)), edgeID(math.Inf(-1)))
	require.NotEqual(t, edgeID(math.Inf(-1)), edgeID(math.Inf(1)))
	require.False(t, relationshipMergeValuesEqual(math.NaN(), math.NaN()))
}

func TestRelationshipMergeNaNIdentityNeverMatches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Source {id: 'source'}), (:Target {id: 'target'})`, nil)
	require.NoError(t, err)
	query := `UNWIND $rows AS row
MATCH (source:Source {id: 'source'})
MATCH (target:Target {id: 'target'})
MERGE (source)-[rel:ASSERTS {weight: row.weight}]->(target)
RETURN count(rel) AS merged`
	_, err = exec.Execute(ctx, query, map[string]interface{}{
		"rows": []map[string]interface{}{{"weight": math.NaN()}, {"weight": math.NaN()}},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMergeChainBatch)

	result, err := exec.Execute(ctx, `
MATCH (:Source {id: 'source'})-[rel:ASSERTS]->(:Target {id: 'target'})
RETURN count(rel)`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}

func TestUnwindRelationshipMergeBatchFallsBackForNaNIdentity(t *testing.T) {
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
	rows := []map[string]interface{}{
		{
			"source_key": "svc-a", "target_key": "topic-a", "tenant": "tenant-a",
			"ordinal": math.NaN(), "embedding": []float64{1, 0},
		},
		{
			"source_key": "svc-a", "target_key": "topic-a", "tenant": "tenant-a",
			"ordinal": math.NaN(), "embedding": []float64{0, 1},
		},
	}
	_, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.False(t, exec.LastHotPathTrace().UnwindRelationshipMergeBatch)

	count := mustCountRows(t, exec, ctx, `
MATCH (:Service {key: 'svc-a'})-[rel:PUBLISHES]->(:Topic {key: 'topic-a'})
RETURN count(rel)`, nil)
	require.Equal(t, int64(2), count)
}

func TestRelationshipMergeIdentityTreatsEquivalentNumbersAsOneIdentity(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Source {id: 'source'}), (:Target {id: 'target'})`, nil)
	require.NoError(t, err)
	for _, weight := range []interface{}{int64(1), float64(1)} {
		_, err = exec.Execute(ctx, `
MATCH (source:Source {id: 'source'})
MATCH (target:Target {id: 'target'})
MERGE (source)-[rel:ASSERTS {weight: $weight}]->(target)`, map[string]interface{}{"weight": weight})
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, `
MATCH (:Source {id: 'source'})-[rel:ASSERTS]->(:Target {id: 'target'})
RETURN count(rel)`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	integerID := exec.newRelationshipMergeEdgeID("source", "target", "ASSERTS", map[string]interface{}{"weight": int64(1)})
	floatID := exec.newRelationshipMergeEdgeID("source", "target", "ASSERTS", map[string]interface{}{"weight": float64(1)})
	require.Equal(t, integerID, floatID)
}
