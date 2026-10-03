package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// projectionExpressions is the one reading of a projection clause (its
// expressions, aliases dropped) that the row validators share (#823).
func TestProjectionExpressions(t *testing.T) {
	for _, tc := range []struct {
		clause, keyword string
		want            []string
	}{
		{"RETURN DISTINCT a.x AS x, count(b) ORDER BY x LIMIT 3", "RETURN", []string{"a.x", "count(b)"}},
		{"WITH n, m WHERE n.v > 1", "WITH", []string{"n", "m"}},
		{"WITH *", "WITH", nil},
		{"UNWIND range(1, 3) AS i", "UNWIND", []string{"range(1, 3)"}},
		{"UNWIND $list", "UNWIND", nil},
		{"WITH n", "RETURN", nil},
		{"RET", "RETURN", nil},
	} {
		require.Equal(t, tc.want, projectionExpressions(tc.clause, tc.keyword), "%s / %s", tc.keyword, tc.clause)
	}
}

// A CALL subquery that produced no result still yields one row carrying
// the statement's parameters.
func TestCallPipelineRowsFromNilResult(t *testing.T) {
	ctx := withQueryParams(context.Background(), map[string]interface{}{"a": int64(1)})
	rows := callPipelineRowsFromResult(ctx, nil)
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0]["$a"])
}
