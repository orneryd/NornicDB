package cypher

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateVectorIndex_RelationshipSyntaxFormsAccepted(t *testing.T) {
	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	q1 := "CREATE VECTOR INDEX rel_emb_idx IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
	_, err := exec.Execute(ctx, q1, nil)
	require.NoError(t, err)

	q2 := "CREATE VECTOR INDEX rel_emb_idx_dir IF NOT EXISTS FOR ()-[e:RELATES_TO_DIR]->() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
	_, err = exec.Execute(ctx, q2, nil)
	require.NoError(t, err)

	idx1, ok := store.GetSchema().GetVectorIndex("rel_emb_idx")
	require.True(t, ok)
	require.Equal(t, "RELATES_TO", idx1.Label)
	require.Equal(t, "fact_embedding", idx1.Property)
	require.Equal(t, 3, idx1.Dimensions)
	require.Equal(t, "cosine", idx1.SimilarityFunc)

	idx2, ok := store.GetSchema().GetVectorIndex("rel_emb_idx_dir")
	require.True(t, ok)
	require.Equal(t, "RELATES_TO_DIR", idx2.Label)
	require.Equal(t, "fact_embedding", idx2.Property)
}

func TestCreateVectorIndex_Relationship_E2EQueryRelationships(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Entity {uuid:'a'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (b:Entity {uuid:'b'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (c:Entity {uuid:'c'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}) CREATE (a)-[:RELATES_TO {uuid:'r1', fact_embedding:[1.0,0.0,0.0]}]->(b)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:Entity {uuid:'a'}), (c:Entity {uuid:'c'}) CREATE (a)-[:RELATES_TO {uuid:'r2', fact_embedding:[0.0,1.0,0.0]}]->(c)", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_emb_idx IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rel_emb_idx', 2, [1.0,0.0,0.0]) YIELD relationship, score RETURN relationship.uuid AS rid, score ORDER BY score DESC LIMIT 2", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "r1", res.Rows[0][0])
}

func TestVectorProcedures_NodeAndRelationshipManualVectorParityE2E(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('nzz_idx','NZZ','emb',4,'cosine')", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:NZZ {uuid:'n1'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:NZZ {uuid:'n2'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:NZZ {uuid:'n1'}) WITH a CALL db.create.setNodeVectorProperty(a,'emb',$v) RETURN a", map[string]interface{}{
		"v": []interface{}{1.0, 0.0, 0.0, 0.0},
	})
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:NZZ {uuid:'n2'}) WITH a CALL db.create.setNodeVectorProperty(a,'emb',$v) RETURN a", map[string]interface{}{
		"v": []interface{}{0.0, 1.0, 0.0, 0.0},
	})
	require.NoError(t, err)

	nodeProp, err := exec.Execute(ctx, "MATCH (a:NZZ {uuid:'n1'}) RETURN a.emb AS emb", nil)
	require.NoError(t, err)
	require.Len(t, nodeProp.Rows, 1)
	requireVectorEqual(t, []float64{1.0, 0.0, 0.0, 0.0}, nodeProp.Rows[0][0])

	nodeHits, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('nzz_idx',5,$v) YIELD node,score RETURN node.uuid AS u, score ORDER BY score DESC", map[string]interface{}{
		"v": []interface{}{0.9, 0.1, 0.0, 0.0},
	})
	require.NoError(t, err)
	require.Len(t, nodeHits.Rows, 2)
	require.Equal(t, "n1", nodeHits.Rows[0][0])
	// Neo4j's cosine scores, max((1 + cos) / 2, 0): vector.similarity.cosine
	// gives 0.996941864490509 and 0.5552157759666443 for these vectors.
	require.InEpsilon(t, 0.996941864490509, nodeHits.Rows[0][1].(float64), 1e-6)
	require.Equal(t, "n2", nodeHits.Rows[1][0])
	require.InEpsilon(t, 0.5552157759666443, nodeHits.Rows[1][1].(float64), 1e-6)

	_, err = exec.Execute(ctx, "CALL db.index.vector.createRelationshipIndex('rzz_idx','RZZ_REL','emb',4,'cosine')", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:RZZ {uuid:'a'})-[:RZZ_REL {uuid:'z1'}]->(:RZZ {uuid:'b'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:RZZ {uuid:'c'})-[:RZZ_REL {uuid:'z2'}]->(:RZZ {uuid:'d'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (:RZZ)-[r:RZZ_REL {uuid:'z1'}]->(:RZZ) WITH r CALL db.create.setRelationshipVectorProperty(r,'emb',$v) RETURN r", map[string]interface{}{
		"v": []interface{}{1.0, 0.0, 0.0, 0.0},
	})
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (:RZZ)-[r:RZZ_REL {uuid:'z2'}]->(:RZZ) WITH r CALL db.create.setRelationshipVectorProperty(r,'emb',$v) RETURN r", map[string]interface{}{
		"v": []interface{}{0.0, 1.0, 0.0, 0.0},
	})
	require.NoError(t, err)

	indexes, err := exec.Execute(ctx, "SHOW INDEXES", nil)
	require.NoError(t, err)
	requireIndexOnline(t, indexes, "rzz_idx", "VECTOR", "RELATIONSHIP")

	relProp, err := exec.Execute(ctx, "MATCH (:RZZ)-[r:RZZ_REL {uuid:'z1'}]->(:RZZ) RETURN r.emb AS emb", nil)
	require.NoError(t, err)
	require.Len(t, relProp.Rows, 1)
	requireVectorEqual(t, []float64{1.0, 0.0, 0.0, 0.0}, relProp.Rows[0][0])

	relHits, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rzz_idx',5,$v) YIELD relationship,score RETURN relationship.uuid AS u, score ORDER BY score DESC", map[string]interface{}{
		"v": []interface{}{0.9, 0.1, 0.0, 0.0},
	})
	require.NoError(t, err)
	require.Len(t, relHits.Rows, 2)
	require.Equal(t, "z1", relHits.Rows[0][0])
	require.InEpsilon(t, 0.996941864490509, relHits.Rows[0][1].(float64), 1e-6)
	require.Equal(t, "z2", relHits.Rows[1][0])
	require.InEpsilon(t, 0.5552157759666443, relHits.Rows[1][1].(float64), 1e-6)
}

func requireVectorEqual(t *testing.T, expected []float64, actual interface{}) {
	t.Helper()
	values, ok := numericSliceToFloat64(actual)
	require.Truef(t, ok, "expected numeric vector, got %T", actual)
	require.Len(t, values, len(expected))
	for i := range expected {
		require.InDelta(t, expected[i], values[i], 1e-9, "vector value %d", i)
	}
}

func numericSliceToFloat64(value interface{}) ([]float64, bool) {
	switch v := value.(type) {
	case []float64:
		out := make([]float64, len(v))
		copy(out, v)
		return out, true
	case []float32:
		out := make([]float64, len(v))
		for i, n := range v {
			out[i] = float64(n)
		}
		return out, true
	case []interface{}:
		out := make([]float64, len(v))
		for i, item := range v {
			switch n := item.(type) {
			case float64:
				out[i] = n
			case float32:
				out[i] = float64(n)
			case int:
				out[i] = float64(n)
			case int64:
				out[i] = float64(n)
			default:
				return nil, false
			}
		}
		return out, true
	default:
		return nil, false
	}
}

func requireIndexOnline(t *testing.T, indexes *ExecuteResult, name, indexType, entityType string) {
	t.Helper()
	nameCol := indexColumn(t, indexes, "name")
	typeCol := indexColumn(t, indexes, "type")
	entityCol := indexColumn(t, indexes, "entityType")
	stateCol := indexColumn(t, indexes, "state")
	populationCol := indexColumn(t, indexes, "populationPercent")

	for _, row := range indexes.Rows {
		if row[nameCol] != name {
			continue
		}
		require.Equal(t, indexType, row[typeCol])
		require.Equal(t, entityType, row[entityCol])
		require.Equal(t, "ONLINE", row[stateCol])
		population, ok := row[populationCol].(float64)
		require.Truef(t, ok, "populationPercent should be float64, got %T", row[populationCol])
		require.True(t, math.Abs(population-100.0) < 1e-9)
		return
	}
	require.Failf(t, "missing index", "SHOW INDEXES did not include %q: %#v", name, indexes.Rows)
}

func indexColumn(t *testing.T, indexes *ExecuteResult, name string) int {
	t.Helper()
	for i, col := range indexes.Columns {
		if col == name {
			return i
		}
	}
	require.Failf(t, "missing column", "SHOW INDEXES did not include column %q: %#v", name, indexes.Columns)
	return -1
}
