package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A list literal argument whose elements are calls with several arguments
// keeps each call whole (#907): toStringList([substring('abc', 1, 1)]) was
// ["substring('abc'", "1", "1)"]. Expected values are Neo4j 5.26's and
// 2026.09's.
func TestListLiteralArgumentWithCallElements(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_literal_argument"))
	ctx := context.Background()
	result, err := exec.Execute(ctx, "RETURN toStringList([substring('abc', 1, 1)]) AS a, valueType([substring('abc', 1, 1), 1]) AS c, size([coalesce(null, 1), 2]) AS d", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{"b"}, "LIST<STRING NOT NULL | INTEGER NOT NULL> NOT NULL", int64(2)}}, result.Rows)

	result, err = exec.Execute(ctx, "CYPHER 25 RETURN toStringList([uuid(1, 2), toString(1)]) AS b, toIntegerList([size([1, 2]), toInteger('3')]) AS c, "+
		"valueType([vector([1], 1, INTEGER)]) AS v, valueType([uuid(1, 2), 1]) AS u, valueType([substring('abc', 1, 1), 'x']) AS s", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{
		[]interface{}{"00000000-0000-0001-0000-000000000002", "1"}, []interface{}{int64(2), int64(3)},
		"LIST<VECTOR<INTEGER NOT NULL>(1) NOT NULL> NOT NULL", "LIST<UUID NOT NULL | INTEGER NOT NULL> NOT NULL", "LIST<STRING NOT NULL> NOT NULL",
	}}, result.Rows)

	require.Equal(t, []string{"f(1, 2)", "`a,b`", "3"}, exec.splitArrayElements("f(1, 2), `a,b`, , 3"))
}
