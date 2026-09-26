package cypher

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestDivisionByZeroMatchesNeo4j pins Neo4j 5.26.30's division and modulo by
// zero: / fails when the divisor is an INTEGER zero, except an all-literal
// FLOAT / 0, which Neo4j folds to Infinity; % fails only when both operands
// are INTEGERs; a FLOAT zero divisor or a FLOAT % operand is IEEE 754.
func TestDivisionByZeroMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "divzero"))
	ctx := context.Background()
	const failure = "error"
	inf, negInf, nan := math.Inf(1), math.Inf(-1), math.NaN()
	cells := []struct {
		dividend, op, divisor string
		// runtime, parameters and variable operands; literal operands
		want, literal interface{}
	}{
		{"1", "/", "0", failure, failure},
		{"1", "%", "0", failure, failure},
		{"1.0", "/", "0", failure, inf},
		{"1.0", "%", "0", nan, nan},
		{"1", "/", "0.0", inf, inf},
		{"1", "%", "0.0", nan, nan},
		{"1.0", "/", "0.0", inf, inf},
		{"1.0", "%", "0.0", nan, nan},
		{"0.0", "/", "0.0", nan, nan},
		{"0.0", "%", "0.0", nan, nan},
		{"-1.0", "/", "0.0", negInf, negInf},
		{"-1.0", "%", "0.0", nan, nan},
	}
	value := func(text string) interface{} {
		if strings.Contains(text, ".") {
			if text[0] == '-' {
				return -1.0
			}
			if text == "0.0" {
				return 0.0
			}
			return 1.0
		}
		if text == "0" {
			return int64(0)
		}
		return int64(1)
	}
	for _, cell := range cells {
		params := map[string]interface{}{"a": value(cell.dividend), "b": value(cell.divisor)}
		for query, want := range map[string]interface{}{
			"UNWIND [$a] AS a UNWIND [$b] AS b RETURN a " + cell.op + " b AS v":      cell.want,
			"RETURN $a " + cell.op + " $b AS v":                                      cell.want,
			"WITH $a AS a RETURN a " + cell.op + " " + cell.divisor + " AS v":        cell.want,
			"RETURN " + cell.dividend + " " + cell.op + " " + cell.divisor + " AS v": cell.literal,
		} {
			result, err := exec.Execute(ctx, query, params)
			if want == failure {
				require.Error(t, err, query)
				require.Contains(t, err.Error(), "/ by zero", query)
				require.Contains(t, err.Error(), "ArithmeticError", query)
				continue
			}
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 1, query)
			got, isFloat := result.Rows[0][0].(float64)
			require.True(t, isFloat, "%s: %#v", query, result.Rows[0][0])
			if math.IsNaN(want.(float64)) {
				require.True(t, math.IsNaN(got), "%s: %v", query, got)
			} else {
				require.Equal(t, want, got, query)
			}
		}
	}

	// Folding covers literal-only operands; a function call is not folded.
	for query, want := range map[string]float64{
		"RETURN (1.0 + 1) / 0 AS v":           inf,
		"RETURN 1.0 / (1 - 1) AS v":           inf,
		"RETURN -1.0 / 0 AS v":                negInf,
		"RETURN 2.5 / 0 + 1 AS v":             inf,
		"RETURN 1e0 / 0 AS v":                 inf,
		"RETURN 1.0 / 0x0 AS v":               inf,
		"UNWIND [1] AS x RETURN 1.0 / 0 AS v": inf,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows[0][0], query)
	}
	for _, query := range []string{
		"RETURN toFloat(1) / 0 AS v",
		"RETURN 1 / (1 - 1) AS v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "/ by zero", query)
	}

	// A stored FLOAT divided by an INTEGER zero fails, in RETURN and WHERE.
	_, err := exec.Execute(ctx, "CREATE (:DivZero {f: 1.5})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:DivZero) RETURN n.f / 0 AS v",
		"MATCH (n:DivZero) WHERE n.f / 0 > 1 RETURN n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "/ by zero", query)
	}
}
