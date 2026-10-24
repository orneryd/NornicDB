package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestE2E_VectorCosine_RelationshipProjectionFastPath_GraphitiShape(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := &relationshipVectorReadCounter{Engine: storage.NewNamespacedEngine(base, "test")}
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	for _, q := range []string{
		"CREATE (:Entity {uuid:'a', group_id:'g'})",
		"CREATE (:Entity {uuid:'b', group_id:'g'})",
		"CREATE (:Entity {uuid:'c', group_id:'g'})",
		"CREATE (:Entity {uuid:'d', group_id:'other'})",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	for _, q := range []string{
		"MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}) CREATE (a)-[:RELATES_TO {uuid:'e1', fact_embedding:[1.0,0.0,0.0]}]->(b)",
		"MATCH (a:Entity {uuid:'a'}), (c:Entity {uuid:'c'}) CREATE (a)-[:RELATES_TO {uuid:'e2', fact_embedding:[0.9,0.1,0.0]}]->(c)",
		"MATCH (a:Entity {uuid:'a'}), (d:Entity {uuid:'d'}) CREATE (a)-[:RELATES_TO {uuid:'e3', fact_embedding:[0.0,1.0,0.0]}]->(d)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	relationshipVectorIndexedFixture(t, exec, ns, 3)
	probe, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rel_idx', 5, [1.0,0.0,0.0]) YIELD relationship, score RETURN relationship, score", nil)
	require.NoError(t, err)
	require.NotEmpty(t, probe.Rows)

	query := `MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
WHERE n.group_id IN $groups
WITH DISTINCT e, n, m, vector.similarity.cosine(e.fact_embedding, $q) AS score
WHERE score > $min
RETURN e.uuid AS edge_uuid, n.uuid AS src_uuid, m.uuid AS dst_uuid, score
ORDER BY score DESC
LIMIT $limit`
	ns.reset()
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"groups": []string{"g"},
		"q":      []float64{1.0, 0.0, 0.0},
		"min":    0.6, // Neo4j's cosine score: e3, orthogonal, is 0.5
		"limit":  5,
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Zero(t, ns.edgeScans.Load(), "populated service must perform an index seek")
	require.Positive(t, ns.edgeReads.Load(), "seek must hydrate indexed relationships")
	require.Len(t, res.Rows, 2)
	require.Equal(t, "e1", res.Rows[0][0])
}

func TestE2E_VectorCosine_RelationshipProjectionFastPath_DirectAnalog(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := &relationshipVectorReadCounter{Engine: storage.NewNamespacedEngine(base, "test")}
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	for _, q := range []string{
		"CREATE (:Entity {uuid:'a'})",
		"CREATE (:Entity {uuid:'b'})",
		"CREATE (:Entity {uuid:'c'})",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	for _, q := range []string{
		"MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}) CREATE (a)-[:RELATES_TO {uuid:'r1', fact_embedding:[1.0,0.0,0.0]}]->(b)",
		"MATCH (a:Entity {uuid:'a'}), (c:Entity {uuid:'c'}) CREATE (a)-[:RELATES_TO {uuid:'r2', fact_embedding:[0.0,1.0,0.0]}]->(c)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	relationshipVectorIndexedFixture(t, exec, ns, 3)
	probe, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rel_idx', 5, [1.0,0.0,0.0]) YIELD relationship, score RETURN relationship, score", nil)
	require.NoError(t, err)
	require.NotEmpty(t, probe.Rows)

	direct := "MATCH ()-[e:RELATES_TO]->() WITH e, vector.similarity.cosine(e.fact_embedding,$q) AS s RETURN e.uuid, s ORDER BY s DESC LIMIT 5"
	ns.reset()
	res, err := exec.Execute(ctx, direct, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Zero(t, ns.edgeScans.Load(), "populated service must perform an index seek")
	require.Positive(t, ns.edgeReads.Load(), "seek must hydrate indexed relationships")
	require.Len(t, res.Rows, 2)
	require.Equal(t, "r1", res.Rows[0][0])
}

func TestE2E_VectorCosine_RelationshipProjectionFastPath_ExplicitTransaction(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := &relationshipVectorReadCounter{Engine: storage.NewNamespacedEngine(base, "test")}
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	for i := 0; i < 12; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'n-%d', group_id:'g'})", i), nil)
		require.NoError(t, err)
	}
	for i := 0; i < 10; i++ {
		vec := "[0.0,1.0,0.0]"
		if i < 3 {
			vec = "[1.0,0.0,0.0]"
		}
		_, err := exec.Execute(ctx, fmt.Sprintf(
			"MATCH (a:Entity {uuid:'n-%d'}), (b:Entity {uuid:'n-%d'}) CREATE (a)-[:RELATES_TO {uuid:'e-%d', fact_embedding:%s}]->(b)",
			i, i+1, i, vec,
		), nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX rel_tx_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	relationshipVectorIndexedFixture(t, exec, ns, 3)

	query := `MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
WITH DISTINCT e, n, m, vector.similarity.cosine(e.fact_embedding, $q) AS score
WHERE score > $min
RETURN e.uuid AS edge_uuid, n.uuid AS src_uuid, m.uuid AS dst_uuid, score
ORDER BY score DESC
LIMIT $limit`

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	for _, mode := range []string{"EXPLAIN ", "", "PROFILE "} {
		ns.reset()
		res, err := exec.Execute(ctx, mode+query, map[string]interface{}{
			"q":     []float64{1.0, 0.0, 0.0},
			"min":   0.5,
			"limit": 5,
		})
		require.NoError(t, err, mode)
		// Transaction reads use the snapshot engine, below this outer observer.
		// Assert isolation from autocommit rather than inventing point-read counts.
		require.Zero(t, ns.edgeScans.Load(), "transaction must not scan the outer engine")
		require.Zero(t, ns.edgeReads.Load(), "transaction must not hydrate from the outer engine")
		require.Zero(t, ns.nodeReads.Load(), "transaction must not read outer-engine nodes")
		if mode == "EXPLAIN " {
			require.Empty(t, res.Rows)
			require.False(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
		} else {
			require.Len(t, res.Rows, 3)
			require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, mode)
		}
		if mode != "" {
			root := res.Metadata["plan"].(*ExecutionPlan).Root
			seek := relationshipPlanOperator(root, "RelationshipVectorIndexSeek")
			require.NotNil(t, seek, mode)
			if mode == "EXPLAIN " {
				require.Equal(t, true, seek.Arguments["conditional"])
			} else {
				require.EqualValues(t, 1, seek.Arguments["indexLookups"])
				require.EqualValues(t, 3, seek.ActualRows)
				require.Positive(t, seek.DBHits)
			}
		}
	}

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}
