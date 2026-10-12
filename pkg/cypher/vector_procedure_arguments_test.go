package cypher

import (
	"context"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The vector index and embedding procedures read the call's evaluated
// arguments, so a value bound earlier in the statement is the same argument
// as a literal or a parameter. A null argument is the procedure's failure
// (Neo4j 5.26.30: ProcedureCallFailed for createNodeIndex(null, ...)), a
// value of another type a TypeError (#907).
func TestVectorProceduresReadEvaluatedArguments(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	exec.SetEmbedder(&mockQueryEmbedder{embedding: []float32{1, 0, 0}})

	indexNames := func(t *testing.T) []interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, "SHOW INDEXES YIELD name WHERE name STARTS WITH 'vi_' RETURN name ORDER BY name", nil)
		require.NoError(t, err)
		names := make([]interface{}, len(result.Rows))
		for i, row := range result.Rows {
			names[i] = row[0]
		}
		return names
	}
	status := func(err error) string {
		code, _ := nerrors.Neo4jStatus(err)
		return code
	}

	// createNodeIndex is a SCHEMA procedure: it is a clause like any other
	// after WITH or UNWIND.
	t.Run("node index from WITH-bound values", func(t *testing.T) {
		_, err := exec.Execute(ctx, "WITH 'vi_node' AS name, 'Doc' AS label, 3 AS dimension CALL db.index.vector.createNodeIndex(name, label, 'embedding', dimension, 'COSINE')", nil)
		require.NoError(t, err)
	})
	t.Run("node index from parameters after UNWIND", func(t *testing.T) {
		result, err := exec.Execute(ctx, "UNWIND [$name] AS name CALL db.index.vector.createNodeIndex(name, $label, 'embedding', $dimension) RETURN name",
			map[string]interface{}{"name": "vi_node2", "label": "Doc2", "dimension": int64(3)})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"vi_node2"}}, result.Rows)
	})
	t.Run("relationship index from WITH-bound values", func(t *testing.T) {
		result, err := exec.Execute(ctx, "WITH 'vi_rel' AS name, 'LINKS' AS type, 2 + 2 AS dimension CALL db.index.vector.createRelationshipIndex(name, type, 'embedding', dimension) YIELD name AS created RETURN created", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"vi_rel"}}, result.Rows)
	})
	require.Equal(t, []interface{}{"vi_node", "vi_node2", "vi_rel"}, indexNames(t))

	t.Run("null argument is the procedure's failure", func(t *testing.T) {
		for _, query := range []string{
			"CALL db.index.vector.createNodeIndex(null, 'Doc', 'embedding', 3, 'cosine')",
			"CALL db.index.vector.createNodeIndex('vi_null', 'Doc', 'embedding', null, 'cosine')",
			"CALL db.index.vector.createNodeIndex('vi_null', 'Doc', 'embedding', 3, null)",
			"CALL db.index.vector.createRelationshipIndex('vi_null', null, 'embedding', 3)",
		} {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err, query)
			require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", status(err), query)
		}
	})
	t.Run("value of another type is a TypeError", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.index.vector.createRelationshipIndex('vi_bad', 'LINKS', 'embedding', $dimension)", map[string]interface{}{"dimension": "3"})
		require.Error(t, err)
		require.Equal(t, "Neo.ClientError.Statement.TypeError", status(err))
	})
	t.Run("unsupported similarity function", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.index.vector.createRelationshipIndex('vi_bad', 'LINKS', 'embedding', 3, 'manhattan')", nil)
		require.Error(t, err)
		require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", status(err))
	})
	require.Equal(t, []interface{}{"vi_node", "vi_node2", "vi_rel"}, indexNames(t))

	t.Run("embed reads a WITH-bound text", func(t *testing.T) {
		result, err := exec.Execute(ctx, "WITH 'hello' AS text CALL db.index.vector.embed(text) YIELD embedding RETURN embedding", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]float32{1, 0, 0}}}, result.Rows)
	})
	t.Run("embed of null is the procedure's failure", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.index.vector.embed(null)", nil)
		require.Error(t, err)
		require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", status(err))
	})
}
