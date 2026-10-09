package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An aggregate's argument of a type it doesn't take is a SyntaxError before
// the statement runs when the type is known then, an UNWIND over a list of
// one computed type included; a null percentile literal too. A type known
// only at run time is a TypeError then, a null percentile value included
// (Neo4j 5.26.30, #907).
func TestAggregateArgumentTypesMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate_argument_types"))
	ctx := context.Background()
	for query, code := range map[string]string{
		"UNWIND [date('2020-01-01')] AS x RETURN sum(x) AS v":                     "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [date('2020-01-01'), date('2021-01-01')] AS x RETURN avg(x) AS v": "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [duration('P1D'), duration('PT1H')] AS x RETURN stDev(x) AS v":    "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [duration('P1D'), duration('PT1H')] AS x RETURN stDevP(x) AS v":   "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [duration('P1D')] AS x RETURN percentileCont(x, 0.5) AS v":        "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [date('2020-01-01')] AS x RETURN percentileDisc(x, 0.5) AS v":     "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [1, 2, 3] AS x RETURN percentileCont(x, null) AS v":               "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [1, 2, 3] AS x RETURN percentileDisc(x, null) AS v":               "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [1, date('2020-01-01')] AS x RETURN sum(x) AS v":                  "Neo.ClientError.Statement.TypeError",
		"WITH null AS p UNWIND [1, 2, 3] AS x RETURN percentileCont(x, p) AS v":   "Neo.ClientError.Statement.TypeError",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			got, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, code, got)
		})
	}
	result, err := exec.Execute(ctx, "UNWIND [date('2020-01-01')] AS x RETURN x.year AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2020)}}, result.Rows)
	require.Equal(t, "Date", uniformListElementType("[date('2020-01-01'), date('2021-01-01')]"))
	require.Empty(t, uniformListElementType("[1, date('2020-01-01')]"))
	require.Empty(t, uniformListElementType("[]"))
	require.Empty(t, uniformListElementType("x"))
	require.Empty(t, uniformListElementType("[n.p]"))
}
