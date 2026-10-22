package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// vectorPopulationFixture holds vector-indexed node and relationship
// populations larger than one batch, all of them within the query's limit.
func vectorPopulationFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
	for _, query := range []string{
		"CREATE VECTOR INDEX vec_population_nodes FOR (n:VecItem) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		"CREATE VECTOR INDEX vec_population_relationships FOR ()-[r:SIM]-() ON (r.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		"UNWIND range(0, 2047) AS i CREATE (:VecItem {uuid: i, emb: [1.0, toFloat(i) / 2048.0, 0.0]})",
		"UNWIND range(0, 2047) AS i CREATE (:VecEnd {i: i})-[:SIM {uuid: i, emb: [1.0, toFloat(i) / 2048.0, 0.0]}]->(:VecEnd {i: i})",
	} {
		_, err := executor.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
	}
	return executor
}

// The vector fast paths' scored candidates stop inside the pass that projects
// them, through public Execute: the context expires at the pass's second
// batch, and the statement fails instead of reporting the rows.
func TestVectorScorePopulationsExecuteCancellation(t *testing.T) {
	cases := []entityPopulationCase{
		{name: "vector_nodes", query: "MATCH (n:VecItem) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT $k", consumer: "tryFastPathMatchVectorCosine", after: 2, rows: 2048},
		{name: "vector_node_projection", query: "MATCH (n:VecItem) WITH n, vector.similarity.cosine(n.emb, $q) AS score RETURN n.uuid AS uuid, score ORDER BY score DESC LIMIT $k", consumer: "tryFastPathMatchWithVectorCosineProjection", after: 2, rows: 2048},
		{name: "vector_relationships", query: "MATCH ()-[r:SIM]->() RETURN r.uuid AS rid, vector.similarity.cosine(r.emb, $q) AS score ORDER BY score DESC LIMIT $k", consumer: "tryFastPathMatchRelationshipVectorCosine", after: 2, rows: 2048},
		{name: "vector_relationship_projection", query: "MATCH (n)-[r:SIM]->(m) WITH r, n, m, vector.similarity.cosine(r.emb, $q) AS score RETURN r.uuid AS rid, score ORDER BY score DESC LIMIT $k", consumer: "tryFastPathMatchWithRelationshipVectorCosineProjection", after: 2, rows: 2048},
	}
	for index := range cases {
		cases[index].fixture = vectorPopulationFixture
	}
	runEntityPopulationCases(t, cases, map[string]interface{}{"q": []float64{1, 0, 0}, "k": int64(4096)})
}
