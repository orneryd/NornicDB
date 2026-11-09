package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR7EscapedStringConjunction(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r7_and"))
	_, err := e.Execute(context.Background(), `CREATE (:R7String {text: 'O\'Reilly AND Sons', id:1})`, nil)
	require.NoError(t, err)
	q := `MATCH (n:R7String) WHERE n.text = 'O\'Reilly AND Sons' AND n.id = 1 RETURN n.id`
	got, err := e.Execute(context.Background(), q, nil)
	require.NoError(t, err, q)
	require.Equal(t, [][]interface{}{{int64(1)}}, got.Rows, q)
}
