package cypher

import (
	"context"
	"errors"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// vector.similarity.cosine / .euclidean return Neo4j's scores and raise its
// errors (#907); every expected value is Neo4j 5.26's and 2026.09's.
func TestVectorSimilarityFunctionsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_similarity"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN vector.similarity.cosine([1, 2, 3], [4, 5, 7])":                     0.9930066466331482,
		"RETURN vector.similarity.cosine([0.3, 0.7], [0.9, 0.1])":                   0.7465062737464905,
		"RETURN vector.similarity.cosine([1, 0], [-1, 0])":                          0.0,
		"RETURN vector.similarity.cosine([1e40, 1], [1, 2])":                        0.7236068248748779,
		"RETURN vector.similarity.euclidean([1, 2, 3], [4, 5, 7])":                  0.02857142873108387,
		"RETURN vector.similarity.euclidean([0.3, 0.7], [0.9, 0.1])":                0.5813953876495361,
		"RETURN vector.similarity.euclidean([0, 0], [0, 0])":                        1.0,
		"RETURN vector.similarity.cosine([1, 0], null)":                             nil,
		"RETURN vector.similarity.euclidean(null, [1])":                             nil,
		"WITH [1.0, 0.0] AS v RETURN vector.similarity.cosine(v, [0.0, 1.0])":       0.5,
		"WITH [toFloat(1), 2.5] AS v RETURN vector.similarity.euclidean(v, [1, 2])": 0.800000011920929,
	} {
		result, err := exec.Execute(ctx, query+" AS s", nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	for query, want := range map[string]string{
		"RETURN vector.similarity.cosine([1, 0], [0, 0])":            "Invalid input for 'vector.similarity.cosine()': Argument b is not a valid vector for this similarity function.",
		"RETURN vector.similarity.cosine([0, 0], [1])":               "Invalid input for 'vector.similarity.cosine()': Argument a is not a valid vector for this similarity function.",
		"RETURN vector.similarity.cosine([1, 0], [1])":               "Invalid input for 'vector.similarity.cosine()': The supplied vectors do not have the same number of dimensions.",
		"RETURN vector.similarity.euclidean([1, 0], ['a', 1])":       "Invalid input for 'vector.similarity.euclidean()': Argument b is not a valid vector for this similarity function.",
		"RETURN vector.similarity.cosine([], [])":                    "Invalid input for 'vector.similarity.cosine()': Argument a is not a valid vector for this similarity function.",
		"RETURN vector.similarity.euclidean([], [])":                 "Invalid input for 'vector.similarity.euclidean()': Argument a is not a valid vector for this similarity function.",
		"RETURN vector.similarity.cosine([1, null], [1, 2])":         "Invalid input for 'vector.similarity.cosine()': Argument a is not a valid vector for this similarity function.",
		"RETURN vector.similarity.euclidean([1e40], [1])":            "Invalid input for 'vector.similarity.euclidean()': Argument a is not a valid vector for this similarity function.",
		"WITH {a: 1} AS m RETURN vector.similarity.cosine(m.a, [1])": "Invalid input for function 'vector.similarity.cosine()': Expected LIST<INTEGER | FLOAT>",
	} {
		_, err := exec.Execute(ctx, query+" AS s", nil)
		require.ErrorContains(t, err, want, query)
	}
	_, err := exec.Execute(ctx, "RETURN vector.similarity.cosine([1, 0], [0, 0]) AS s", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	_, err = exec.Execute(ctx, "WITH {a: 1} AS m RETURN vector.similarity.cosine(m.a, [1]) AS s", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")

	// What the compile-time checks keep a statement from reaching.
	failed := errors.New("evaluation failed")
	call := cypherfn.Context{Eval: func(expr string) (interface{}, error) {
		if expr == "fail" {
			return nil, failed
		}
		return []interface{}{1.0, 2.0}, nil
	}}
	cosine := fnVectorSimilarity("cosine", vector.Neo4jCosineVectorValid[float64], vector.Neo4jCosineSimilarity[float64])
	_, err = cosine(call, []string{"a"})
	require.Error(t, err, "one argument")
	_, err = cosine(call, []string{"fail", "b"})
	require.ErrorIs(t, err, failed)
	// Stored coordinates of other Go widths read as numbers.
	coordinates, err := similarityVectorArgument("cosine", "a", []interface{}{float32(1.5), 2, int32(3)}, vector.Neo4jCosineVectorValid[float64])
	require.NoError(t, err)
	require.Equal(t, []float64{1.5, 2, 3}, coordinates)
	_, err = similarityVectorArgument("cosine", "a", []interface{}{true}, vector.Neo4jCosineVectorValid[float64])
	require.ErrorContains(t, err, "Argument a is not a valid vector")

	// The fast paths and the index procedure score as the function does.
	for _, statement := range []string{
		"CREATE VECTOR INDEX simIdx FOR (n:Sim) ON (n.e) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		"CREATE (:Sim {t: 'A', e: [1.0, 0.0, 0.0]}), (:Sim {t: 'B', e: [0.3, 0.7, 0.2]}), (:Sim {t: 'C', e: [-1.0, 0.1, 0.0]})",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	want := [][]interface{}{{"A", 0.9852474927902222}, {"B", 0.7944376468658447}, {"C", 0.027890443801879883}}
	result, err := exec.Execute(ctx, "MATCH (n:Sim) RETURN n.t AS t, vector.similarity.cosine(n.e, [0.9, 0.2, 0.1]) AS s ORDER BY s DESC LIMIT 3", nil)
	require.NoError(t, err)
	require.Equal(t, want, result.Rows)
	result, err = exec.Execute(ctx, "MATCH (n:Sim) RETURN n.t AS t, vector.similarity.cosine(n.e, $q) AS s ORDER BY s ASC LIMIT 3", map[string]interface{}{"q": []float64{0.9, 0.2, 0.1}})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{want[2], want[1], want[0]}, result.Rows)
	// A relationship whose vector isn't valid for the index's similarity (a
	// zero vector, cosine) is no hit.
	for _, statement := range []string{
		"CREATE VECTOR INDEX simRelIdx FOR ()-[r:SIMREL]-() ON (r.e) OPTIONS {indexConfig: {`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}",
		"CREATE (:SimEnd)-[:SIMREL {k: 'zero', e: [0.0, 0.0]}]->(:SimEnd), (:SimEnd)-[:SIMREL {k: 'one', e: [1.0, 0.0]}]->(:SimEnd)",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	result, err = exec.Execute(ctx, "CALL db.index.vector.queryRelationships('simRelIdx', 5, [1.0, 0.0]) YIELD relationship, score RETURN relationship.k AS k, score", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"one", 1.0}}, result.Rows)

	result, err = exec.Execute(ctx, "CALL db.index.vector.queryNodes('simIdx', 3, [0.9, 0.2, 0.1]) YIELD node, score RETURN node.t AS t, score ORDER BY score DESC", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 3)
	for i, row := range result.Rows {
		require.Equal(t, want[i][0], row[0])
		require.InDelta(t, want[i][1], row[1], 1e-6, "an index score is on Neo4j's scale")
	}
}
