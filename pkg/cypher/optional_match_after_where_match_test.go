package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BUG: MATCH ... WHERE ... followed by a second MATCH and then an OPTIONAL
// MATCH that finds nothing returned no rows at all; OPTIONAL MATCH must keep
// every incoming row and bind its new variables to null.
func TestBug_OptionalMatchAfterWhereAndSecondMatchKeepsRows(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, `CREATE (:A {id: 1, k: 'x'})-[:R]->(:B {id: 1, y: 2025}),
		(:A {id: 2, k: 'x'})-[:R]->(:B {id: 2, y: 2025}),
		(:A {id: 3, k: 'z'})-[:R]->(:B {id: 3, y: 2025})`, nil)
	require.NoError(t, err)

	counts := func(t *testing.T, q string) []interface{} {
		t.Helper()
		res, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1, q)
		return res.Rows[0]
	}

	t.Run("no optional matches", func(t *testing.T) {
		row := counts(t, `MATCH (a:A) WHERE a.k = 'x' MATCH (a)-[:R]->(b:B) OPTIONAL MATCH (b)-[x:X]->(:C) RETURN count(b), count(x)`)
		require.EqualValues(t, []interface{}{int64(2), int64(0)}, []interface{}{optionalRowCount(row[0]), optionalRowCount(row[1])})
	})
	t.Run("rows are returned with null bindings", func(t *testing.T) {
		res, err := exec.Execute(ctx, `MATCH (a:A) WHERE a.k = 'x' MATCH (a)-[:R]->(b:B) OPTIONAL MATCH (b)-[x:X]->(c:C) RETURN b.id AS id, x, c ORDER BY id`, nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 2)
		for _, r := range res.Rows {
			require.Nil(t, r[1])
			require.Nil(t, r[2])
		}
	})
	t.Run("where on second match too", func(t *testing.T) {
		row := counts(t, `MATCH (a:A) WHERE a.k = 'x' MATCH (a)-[:R]->(b:B) WHERE b.y = 2025 OPTIONAL MATCH (b)-[x:X]->(:C) RETURN count(b), count(x)`)
		require.EqualValues(t, int64(2), optionalRowCount(row[0]))
	})
	t.Run("grouped by first variable", func(t *testing.T) {
		res, err := exec.Execute(ctx, `MATCH (a:A) WHERE a.k = 'x' MATCH (a)-[:R]->(b:B) OPTIONAL MATCH (b)-[x:X]->(:C) RETURN a.id AS id, count(b) AS n ORDER BY id`, nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 2)
	})
	t.Run("optional match that does match", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (b:B {id: 1}) CREATE (b)-[:X]->(:C {id: 9})`, nil)
		require.NoError(t, err)
		row := counts(t, `MATCH (a:A) WHERE a.k = 'x' MATCH (a)-[:R]->(b:B) OPTIONAL MATCH (b)-[x:X]->(:C) RETURN count(b), count(x)`)
		require.EqualValues(t, []interface{}{int64(2), int64(1)}, []interface{}{optionalRowCount(row[0]), optionalRowCount(row[1])})
	})
}

func optionalRowCount(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return -1
}
