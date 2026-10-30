package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A map projection of a temporal value or duration is Neo4j 2026.09's
// compile-time SyntaxError in a Cypher 25 statement; a Cypher 5 statement
// projects its fields, as Neo4j 5.26 does. A point is a SyntaxError in both.
func TestCypher25TemporalMapProjection(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_projection"))
	ctx := context.Background()
	for _, query := range []string{
		"WITH date('2020-01-01') AS d RETURN d{.year} AS v",
		"WITH duration('P1D') AS du RETURN du{.days} AS v",
		"WITH localtime('12:00') AS t RETURN t{.hour, k: 1} AS v",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		_, err = exec.Execute(ctx, "CYPHER 5 "+query, nil)
		require.NoError(t, err, query)
	}
	_, err := exec.Execute(ctx, "CYPHER 25 WITH duration('P1D') AS du RETURN du{.*} AS v", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	result, err := exec.Execute(ctx, "CYPHER 5 WITH date('2020-01-01') AS d RETURN d{.year} AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{map[string]interface{}{"year": int64(2020)}}}, result.Rows)
	for _, prefix := range []string{"CYPHER 5 ", "CYPHER 25 "} {
		_, err := exec.Execute(ctx, prefix+"WITH point({x: 1, y: 2}) AS p RETURN p{.x} AS v", nil)
		require.Error(t, err, prefix)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}
