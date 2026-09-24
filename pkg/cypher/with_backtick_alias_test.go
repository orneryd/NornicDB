package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A backtick-quoted WITH alias must be keyed the same way on every WITH path,
// so WITH DISTINCT keeps one row per distinct value (#534).
func TestWithDistinctBacktickAlias(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	run := func(q string) [][]interface{} {
		res, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
		return res.Rows
	}

	assert.Equal(t, [][]interface{}{{int64(3)}}, run("UNWIND [1, 2, 2, 3] AS x WITH DISTINCT x AS `my x` RETURN count(*) AS c"))
	assert.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, run("UNWIND [1, 2, 2, 3] AS x WITH DISTINCT x AS `my x` RETURN `my x` AS v ORDER BY v"))
	assert.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, run("UNWIND [1, 2, 2, 3] AS x WITH DISTINCT x AS myx RETURN myx AS v ORDER BY v"))
	assert.Equal(t, [][]interface{}{{int64(4)}}, run("UNWIND [1, 2, 2, 3] AS x WITH x AS `my x` RETURN count(*) AS c"))
	assert.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(2)}, {int64(3)}}, run("UNWIND [1, 2, 2, 3] AS x WITH x AS `my x` RETURN `my x` AS v ORDER BY v"))
	assert.Equal(t, [][]interface{}{{int64(3)}}, run("UNWIND [1, 2, 2, 3] AS x WITH DISTINCT x AS `my x` RETURN count(`my x`) AS c"))
	assert.Equal(t, [][]interface{}{{int64(3)}}, run("UNWIND [1, 2, 2, 3] AS x WITH DISTINCT x AS `my x` WITH count(`my x`) AS c RETURN c"))
	assert.Equal(t, [][]interface{}{{int64(8)}}, run("UNWIND [1, 2, 2, 3] AS x WITH x AS `my x` RETURN sum(`my x`) AS s"))
	assert.Equal(t, [][]interface{}{{int64(4)}}, run("UNWIND [1, 2, 2, 3] AS `my x` RETURN count(`my x`) AS c"))
}
