package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Exact unmapped report cases supplement the existing per-family regressions.
func TestReleaseFamilyNestedArithmeticSubscripts(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, query := range []string{
			"UNWIND [0, 1] AS i RETURN [10, 20, 30][[1, 2][i] + 5] AS r",
			"UNWIND [0, 1] AS i WITH [10, 20, 30][([1, 2][i]) * 10] AS r RETURN r",
		} {
			exec, ctx := newUnitExecutor(t)
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{nil}, {nil}}, result.Rows, query)
		}
	})
}

func TestReleaseFamilyParameterMapComprehensionWrites(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		params := map[string]interface{}{"rows": []interface{}{
			map[string]interface{}{"props": map[string]interface{}{"id": int64(1), "title": "first"}},
			map[string]interface{}{"props": map[string]interface{}{"id": int64(2), "title": "second"}},
		}}
		result, err := exec.Execute(ctx, "WITH $rows AS rows UNWIND [row IN rows | row.props] AS m MERGE (n:P {id: m.id}) SET n += m RETURN n.id ORDER BY n.id", params)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
		stored, err := exec.Execute(ctx, "MATCH (n:P) RETURN n.id,n.title ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1), "first"}, {int64(2), "second"}}, stored.Rows)
	})
}
