package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareCypherIntegersIsExactAbove2To53(t *testing.T) {
	for _, tc := range []struct {
		left, right interface{}
		want        int
	}{
		{int64(9007199254740993), int64(9007199254740992), 1},
		{int64(9007199254740992), int64(9007199254740993), -1},
		{int64(9007199254740993), int64(9007199254740993), 0},
		{uint64(18446744073709551615), uint64(18446744073709551614), 1},
		{int64(-1), uint64(0), -1},
		{uint64(9007199254740993), int64(9007199254740992), 1},
		{int(5), int64(5), 0},
	} {
		got, ok := compareCypherIntegers(tc.left, tc.right)
		require.True(t, ok, "%v vs %v", tc.left, tc.right)
		assert.Equal(t, tc.want, got, "%v vs %v", tc.left, tc.right)
	}
	_, ok := compareCypherIntegers(int64(1), 1.5)
	assert.False(t, ok, "mixed integer/float falls back to float comparison")
}

// Integers above 2^53 must compare and sort exactly (#540).
func TestLargeIntegerComparisonAndOrdering(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	run := func(q string) [][]interface{} {
		res, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
		return res.Rows
	}

	rows := run("RETURN 9007199254740993 > 9007199254740992 AS gt, 9007199254740993 < 9007199254740992 AS lt, 9007199254740993 >= 9007199254740993 AS ge, 9007199254740993 = 9007199254740992 AS eq")
	assert.Equal(t, []interface{}{true, false, true, false}, rows[0])

	rows = run("UNWIND [9007199254740993, 9007199254740992] AS x RETURN x ORDER BY x")
	assert.Equal(t, [][]interface{}{{int64(9007199254740992)}, {int64(9007199254740993)}}, rows)

	rows = run("UNWIND [9007199254740992, 9007199254740993] AS x RETURN x ORDER BY x DESC")
	assert.Equal(t, [][]interface{}{{int64(9007199254740993)}, {int64(9007199254740992)}}, rows)

	rows = run("UNWIND [9007199254740993, 9007199254740992] AS x WITH x ORDER BY x RETURN collect(x) AS l")
	assert.Equal(t, []interface{}{int64(9007199254740992), int64(9007199254740993)}, rows[0][0])

	rows = run("UNWIND [9007199254740993, 9007199254740992] AS x RETURN max(x) AS m")
	assert.Equal(t, int64(9007199254740993), rows[0][0])

	rows = run("RETURN CASE WHEN 9007199254740993 > 9007199254740992 THEN 'gt' ELSE 'not' END AS c")
	assert.Equal(t, "gt", rows[0][0])

	run("CREATE (:Big {id: 9007199254740993}), (:Big {id: 9007199254740992})")
	rows = run("MATCH (n:Big) WHERE n.id > 9007199254740992 RETURN n.id AS id")
	assert.Equal(t, [][]interface{}{{int64(9007199254740993)}}, rows)
	rows = run("MATCH (n:Big) RETURN n.id AS id ORDER BY id DESC")
	assert.Equal(t, [][]interface{}{{int64(9007199254740993)}, {int64(9007199254740992)}}, rows)
}
