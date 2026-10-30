package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SKIP / LIMIT after RETURN take expressions, as they already do after WITH (#533).
func TestReturnSkipLimitExpressions(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "UNWIND range(1, 5) AS i CREATE (:P {id: i})", nil)
	require.NoError(t, err)
	ids := func(q string, params map[string]interface{}) []int64 {
		res, err := exec.Execute(ctx, q, params)
		require.NoError(t, err, q)
		out := make([]int64, 0, len(res.Rows))
		for _, row := range res.Rows {
			out = append(out, row[0].(int64))
		}
		return out
	}

	assert.Equal(t, []int64{1, 2}, ids("MATCH (n:P) RETURN n.id AS id ORDER BY id LIMIT 1 + 1", nil))
	assert.Equal(t, []int64{3, 4, 5}, ids("MATCH (n:P) RETURN n.id AS id ORDER BY id SKIP 1 + 1", nil))
	assert.Equal(t, []int64{3, 4, 5}, ids("MATCH (n:P) RETURN n.id AS id ORDER BY id SKIP $s + 1", map[string]interface{}{"s": int64(1)}))
	assert.Equal(t, []int64{1, 2}, ids("MATCH (n:P) RETURN n.id AS id ORDER BY id LIMIT toInteger('2')", nil))
	assert.Equal(t, []int64{1, 2}, ids("MATCH (n:P) RETURN n.id AS id ORDER BY id LIMIT $l", map[string]interface{}{"l": int64(2)}))
	assert.Equal(t, []int64{3, 4}, ids("MATCH (n:P) RETURN n.id AS id ORDER BY id SKIP $page * $size LIMIT $size", map[string]interface{}{"page": int64(1), "size": int64(2)}))
	assert.Equal(t, []int64{1, 2}, ids("MATCH (n:P) WITH n ORDER BY n.id LIMIT 1 + 1 RETURN n.id AS id", nil))
	assert.Equal(t, []int64{2, 3}, ids("UNWIND [1, 2, 3, 4] AS id RETURN id SKIP 2 - 1 LIMIT 4 / 2", nil))
}
