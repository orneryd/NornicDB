package cypher

// #643: stored list properties follow Neo4j's array property rule. A list
// whose elements do not share one primitive or temporal kind, or that
// contains null, is a TypeError and nothing is stored; an int/float mix is
// stored as floats.

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTypedListPropertyRule(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_props"))
	ctx := context.Background()

	for _, query := range []string{
		"CREATE (n:T {l: ['text', 42, true, 3.14]}) RETURN n.l",
		"CREATE (n:T) SET n.l = ['a', 1] RETURN n.l",
		"CREATE (n:T {l: [1, null]}) RETURN n.l",
		"CREATE (n:T {l: [true, 1]}) RETURN n.l",
		"CREATE (n:T {l: [1, '1']}) RETURN n.l",
		"CREATE (n:T {l: [date('2025-01-01'), duration('P1D')]}) RETURN n.l",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.TypeError", code, query)
	}
	// Nothing was stored by the rejected statements.
	res, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.Rows[0][0])
}

func TestTypedListPropertyCoercionAndValidKinds(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_props_ok"))
	ctx := context.Background()

	cases := []struct {
		query string
		want  interface{}
	}{
		{"CREATE (n:T {l: [1, 2.5]}) RETURN n.l", []interface{}{float64(1), 2.5}},
		{"CREATE (n:T {l: [1.5, 2]}) RETURN n.l", []interface{}{1.5, float64(2)}},
		{"CREATE (n:T {l: [1, 2, 3]}) RETURN n.l", []interface{}{int64(1), int64(2), int64(3)}},
		{"CREATE (n:T {l: ['a', 'b']}) RETURN n.l", []interface{}{"a", "b"}},
		{"CREATE (n:T {l: [true, false]}) RETURN n.l", []interface{}{true, false}},
		{"CREATE (n:T {l: [1.5, 2.5]}) RETURN n.l", []interface{}{1.5, 2.5}},
		{"CREATE (n:T {l: []}) RETURN n.l", []interface{}{}},
	}
	for _, tc := range cases {
		res, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		items, list := cypherListValue(res.Rows[0][0])
		require.True(t, list, tc.query)
		require.Equal(t, tc.want, items, tc.query)
	}

	// SET and MERGE paths apply the same rule.
	_, err := exec.Execute(ctx, "CREATE (n:T {id: 1})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:T {id: 1}) SET n.m = [2, 3.5] RETURN n.m", nil)
	require.NoError(t, err)
	res, err := exec.Execute(ctx, "MATCH (n:T {id: 1}) RETURN n.m", nil)
	require.NoError(t, err)
	items, _ := cypherListValue(res.Rows[0][0])
	require.Equal(t, []interface{}{float64(2), 3.5}, items)

	_, err = exec.Execute(ctx, "MERGE (n:T {id: 2}) ON CREATE SET n.m = [4, 5.5]", nil)
	require.NoError(t, err)
	res, err = exec.Execute(ctx, "MATCH (n:T {id: 2}) RETURN n.m", nil)
	require.NoError(t, err)
	items, _ = cypherListValue(res.Rows[0][0])
	require.Equal(t, []interface{}{float64(4), 5.5}, items)

	// A relationship property array is coerced too.
	res, err = exec.Execute(ctx, "CREATE (:T)-[r:R {w: [1, 2.5]}]->(:T) RETURN r.w", nil)
	require.NoError(t, err)
	items, _ = cypherListValue(res.Rows[0][0])
	require.Equal(t, []interface{}{float64(1), 2.5}, items)
}

func TestTypedListPropertyExplicitTransactionRollsBack(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_props_tx"))
	ctx := context.Background()
	if _, err := exec.handleBegin(); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := exec.Execute(ctx, "CREATE (n:T {id: 1})", nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := exec.Execute(ctx, "MATCH (n:T {id: 1}) SET n.l = ['a', 1]", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
	if _, err := exec.handleCommit(); err != nil {
		t.Logf("commit (expected failed tx): %v", err)
	}
	res, err := exec.Execute(ctx, "MATCH (n:T) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.Rows[0][0], "the failed transaction's writes roll back")
}

// BenchmarkValidatePropertyArrayRule measures the array-rule validation cost
// per write for the three common shapes: a homogeneous int list, a string
// list, and an int/float mix that coerces.
func BenchmarkValidatePropertyArrayRule(b *testing.B) {
	shapes := [][]interface{}{
		{int64(1), int64(2), int64(3)},
		{"a", "b", "c"},
		{int64(1), 2.5, int64(3)},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		shape := append([]interface{}{}, shapes[i%len(shapes)]...)
		_ = validateSetPropertyValue(shape)
	}
}
