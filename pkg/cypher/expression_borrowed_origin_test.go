package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBorrowedExpressionOriginUsesSourceIdentity(t *testing.T) {
	text := strings.Clone("火 + COUNT { RETURN 1 } + COUNT { RETURN 1 }")
	first := strings.Index(text, "COUNT")
	second := strings.LastIndex(text, "COUNT")
	width := len("COUNT { RETURN 1 }")
	values := pipelineRow{rowExpressionOriginKey: 19}
	parent := rowExpressionOrigin(values)
	for _, start := range []int{first, second} {
		saved, bound := bindBorrowedExpressionOrigin(values, text, text[start:start+width], parent)
		require.True(t, bound)
		require.Equal(t, 19+start, rowExpressionOrigin(values).previous)
		restoreRowExpressionOrigin(values, saved)
		require.Equal(t, parent, rowExpressionOrigin(values))
	}
	for _, outside := range []string{"", strings.Clone(text[first : first+width]), text + "extra"} {
		_, bound := bindBorrowedExpressionOrigin(values, text, outside, parent)
		require.False(t, bound, "copied or synthesized bytes have no source extent")
		require.Equal(t, parent, rowExpressionOrigin(values))
	}
	_, bound := bindBorrowedExpressionOrigin(values, text, text[first:first+width], rowExpressionOriginBinding{})
	require.False(t, bound, "ordinary evaluation must not acquire metadata")
	require.Zero(t, testing.AllocsPerRun(100, func() {
		saved, bound := bindBorrowedExpressionOrigin(values, text, text[second:second+width], parent)
		if !bound {
			panic("missing owned extent")
		}
		restoreRowExpressionOrigin(values, saved)
	}))
}

func TestProfileReportsDeduplicatedLogicalOperandOnce(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t, "CREATE (:Seed)")
		query := "PROFILE RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AND COUNT { RETURN 1 UNION ALL RETURN 1 } = 2 AS value"
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{true}}, result.Rows)
		unions := profileScalarUnionOperators(executionPlanFromResult(t, result))
		require.Len(t, unions, 1, "the canonical logical planner eliminates repeated identical operands")
		require.Equal(t, int64(1), unions[0].Arguments["executions"])
		require.Equal(t, int64(2), unions[0].ActualRows)
	})
}
