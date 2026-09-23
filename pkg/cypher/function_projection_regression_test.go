package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuiltInFunctionsProjectFromLiteralRows(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	tests := []struct {
		query string
		want  [][]interface{}
	}{
		{"RETURN round(2.5) AS value", [][]interface{}{{float64(3)}}},
		{"RETURN round(2.567, 2) AS value", [][]interface{}{{float64(2.57)}}},
		{"RETURN round(-2.5) AS value", [][]interface{}{{float64(-2)}}},
		{"RETURN round(2.5, 0, 'HALF_EVEN') AS value", [][]interface{}{{float64(2)}}},
		{"RETURN round(-2.5, 0, 'HALF_UP') AS value", [][]interface{}{{float64(-3)}}},
		{"RETURN round(-2.59, 1, 'DOWN') AS value", [][]interface{}{{float64(-2.5)}}},
		{"RETURN round(-2.51, 1, 'UP') AS value", [][]interface{}{{float64(-2.6)}}},
		{"RETURN toInteger(round(2.4)) AS value", [][]interface{}{{int64(2)}}},
		{"RETURN split('a,b', ',') AS value", [][]interface{}{{[]interface{}{"a", "b"}}}},
		{"RETURN size(randomUUID()) AS value", [][]interface{}{{int64(36)}}},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, result.Rows)
		})
	}
}

func TestBuiltInFunctionsProjectFromUnwindRows(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "UNWIND [1.5, 2.5] AS x RETURN round(x) AS value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{float64(2)}, {float64(3)}}, result.Rows)
}
