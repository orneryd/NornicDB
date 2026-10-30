package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandaloneReturnPreservesSharedParameterRow(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := withQueryParams(t.Context(), map[string]interface{}{"value": int64(7), "items": []interface{}{int64(1), int64(2), int64(3)}})
	parameters := parameterRowValues(ctx)
	before := make(map[string]interface{}, len(parameters))
	for key, value := range parameters {
		before[key] = value
	}
	cases := []struct {
		query string
		want  interface{}
	}{
		{"RETURN $value AS result", int64(7)},
		{"RETURN sum($value) AS result", int64(7)},
		{"RETURN $items[0] AS result ORDER BY result", int64(1)},
		{"RETURN [x IN $items WHERE x > 1 | x + 1] AS result", []interface{}{int64(3), int64(4)}},
		{"RETURN reduce(total = 0, x IN $items | total + x) AS result", int64(6)},
		{"RETURN DISTINCT $value AS result", int64(7)},
		{"RETURN *, $value AS result", int64(7)},
	}
	for _, test := range cases {
		t.Run(test.query, func(t *testing.T) {
			for repeat := 0; repeat < 2; repeat++ {
				result, err := exec.executeReturn(ctx, test.query)
				require.NoError(t, err)
				require.Equal(t, []string{"result"}, result.Columns)
				require.Len(t, result.Rows, 1)
				require.Len(t, result.Rows[0], 1)
				require.Equal(t, test.want, result.Rows[0][0])
				require.Equal(t, before, parameters, "RETURN must not alter the cached parameter row")
			}
		})
	}
}
