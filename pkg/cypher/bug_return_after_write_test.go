package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReturnAfterSet_ReturnsUpdatedValue(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (s:Step {title: "original", content: "old content"})`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `MATCH (s:Step {title: "original"}) SET s.content = "new content" RETURN s.title AS title`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "SET ... RETURN should produce 1 row")
	require.Equal(t, "original", res.Rows[0][0], "RETURN after SET should return the node's title")
}

func TestReturnAfterDetachDelete_ReturnsLiteral(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (s:Step {title: "to-delete"})`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `MATCH (s:Step {title: "to-delete"}) DETACH DELETE s RETURN 'done' AS result`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "DETACH DELETE ... RETURN should produce 1 row")
	require.Equal(t, "done", res.Rows[0][0], "RETURN after DETACH DELETE should return the literal 'done'")
}
