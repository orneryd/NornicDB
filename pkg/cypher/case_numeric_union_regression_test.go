package cypher

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCaseNumericUnionStaticFunctionArgument(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_union"))
	for _, q := range []string{
		"RETURN toUpper(CASE WHEN true THEN 1 ELSE 2.5 END) AS v",
		"UNWIND [] AS outer RETURN toUpper(CASE WHEN true THEN 1 ELSE 2.5 END) AS v",
		"RETURN toUpper((CASE WHEN true THEN 1 ELSE 2.5 END)) AS v",
		"WITH CASE WHEN true THEN 1 ELSE 2.5 END AS v RETURN toUpper(v)",
	} {
		t.Run(q, func(t *testing.T) {
			_, err := e.Execute(context.Background(), q, nil)
			require.ErrorContains(t, err, "SyntaxError")
		})
	}
}

func TestReviewedComputedTypeControls(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "reviewed_types"))
	for _, q := range []string{
		"RETURN [x IN [1] WHERE 1 | x] AS v",
		"UNWIND [] AS outer RETURN [x IN [1] WHERE 1 | x] AS v",
		"RETURN [x IN range(0, 2) | toUpper(x)] AS v",
		"UNWIND [] AS outer RETURN [x IN range(0,2) | toUpper(x)] AS v",
		"RETURN any(x IN [1] WHERE toUpper(x) = 'A') AS v",
		"RETURN reduce(a = 0, x IN [1] | toUpper(a)) AS v",
	} {
		t.Run(q, func(t *testing.T) {
			_, err := e.Execute(context.Background(), q, nil)
			require.ErrorContains(t, err, "SyntaxError")
		})
	}
	for _, op := range []string{"*1", "+0"} {
		r, err := e.Execute(context.Background(), "UNWIND [1,2.5] AS v WITH v WHERE v=1 RETURN [10,20,30][v"+op+"] AS x", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(20)}}, r.Rows)
	}
	r, err := e.Execute(context.Background(), "RETURN toUpper(CASE WHEN false THEN 1 ELSE 'a' END) AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"A"}}, r.Rows)
	for _, q := range []string{"RETURN toUpper(CASE WHEN true THEN 1 ELSE null END) AS v", "RETURN [x IN [1] | toUpper(CASE WHEN true THEN x ELSE 'a' END)] AS v", "UNWIND [1,2.5] AS v RETURN [10,20,30][v*1] AS x", "UNWIND [1,2.5] AS v WITH v*10 AS w RETURN date('2020-01-01') + w AS x"} {
		_, err := e.Execute(context.Background(), q, nil)
		require.Error(t, err)
		var classified interface{ BoltErrorCode() string }
		require.True(t, errors.As(err, &classified))
		require.Equal(t, "Neo.ClientError.Statement.TypeError", classified.BoltErrorCode())
	}
}
